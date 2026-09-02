import base64
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from scripts import ledger_backup_transfer as transfer


BACKUP_ID = "a" * 64
PAYLOAD_BUCKET = "hype-ledger-payload"
ANCHOR_BUCKET = "hype-ledger-anchor"
OWNER = "123456789012"


class FakeAws:
    def __init__(self) -> None:
        self.versioned = {PAYLOAD_BUCKET, ANCHOR_BUCKET}
        self.objects: dict[tuple[str, str], tuple[transfer.StoredObject, bytes]] = {}
        self.put_calls = 0
        self.tamper_key: str | None = None

    def require_versioning(self, bucket: str, owner: str) -> None:
        if owner != OWNER or bucket not in self.versioned:
            raise transfer.TransferError("bucket versioning is not enabled")

    def put_immutable(
        self,
        *,
        bucket: str,
        key: str,
        source: Path,
        owner: str,
        kms_key_id: str,
        backup_id: str,
        sha256: str,
    ) -> transfer.StoredObject:
        identity = (bucket, key)
        payload = source.read_bytes()
        if identity in self.objects:
            stored, existing = self.objects[identity]
            if existing != payload or stored.sha256 != sha256:
                raise transfer.TransferError("immutable object differs")
            return stored
        self.put_calls += 1
        checksum = base64.b64encode(hashlib.sha256(payload).digest()).decode("ascii")
        stored = transfer.StoredObject(
            bucket=bucket,
            key=key,
            version_id=f"version-{self.put_calls}",
            etag=f'"etag-{self.put_calls}"',
            checksum_sha256=checksum,
            sha256=sha256,
            size_bytes=len(payload),
            expected_bucket_owner=owner,
            kms_key_id=f"arn:aws:kms:eu-central-1:{owner}:key/test",
        )
        self.objects[identity] = (stored, payload)
        return stored

    def get_exact(self, stored: transfer.StoredObject, destination: Path) -> None:
        current, payload = self.objects[(stored.bucket, stored.key)]
        if current.version_id != stored.version_id:
            raise transfer.TransferError("wrong version requested")
        if self.tamper_key == stored.key:
            payload += b"tampered"
        destination.write_bytes(payload)
        os.chmod(destination, 0o600)


def fake_verify(_verifier: Path, bundle: Path, anchor: Path) -> None:
    if tuple(sorted(path.name for path in bundle.iterdir())) != transfer.BUNDLE_FILES:
        raise transfer.TransferError("verifier saw an invalid bundle")
    if not anchor.is_file():
        raise transfer.TransferError("verifier did not see an anchor")


class LedgerBackupTransferTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.bundle = self.root / "bundle"
        self.bundle.mkdir(mode=0o700)
        os.chmod(self.bundle, 0o700)
        manifest = {
            "schema_version": 1,
            "backup_id": BACKUP_ID,
            "captured_at": "2026-09-02T00:00:00Z",
            "record_count": 1,
            "head_hash": "b" * 64,
            "files": {},
            "protected_anchor_export": {},
        }
        members = {
            ".ledger.lock": b"",
            "ledger.jsonl": b'{"record":1}\n',
            "manifest.json": (json.dumps(manifest) + "\n").encode(),
            "manifest.json.sha256": b"fixture checksum\n",
            "snapshot.json": b'{"snapshot":1}\n',
        }
        for name, payload in members.items():
            path = self.bundle / name
            path.write_bytes(payload)
            os.chmod(path, 0o600)
        self.anchor = self.root / "anchor.json"
        self.anchor.write_text('{"anchor":1}\n', encoding="utf-8")
        os.chmod(self.anchor, 0o600)
        self.aws = FakeAws()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def upload(self, receipt_name: str = "receipt.json", **changes: object) -> dict[str, object]:
        arguments: dict[str, object] = {
            "bundle": self.bundle,
            "anchor": self.anchor,
            "receipt": self.root / receipt_name,
            "verifier": Path("/bin/true"),
            "aws": self.aws,
            "payload_bucket": PAYLOAD_BUCKET,
            "payload_owner": OWNER,
            "payload_kms_key": f"arn:aws:kms:eu-central-1:{OWNER}:key/payload-key",
            "anchor_bucket": ANCHOR_BUCKET,
            "anchor_owner": OWNER,
            "anchor_kms_key": f"arn:aws:kms:eu-central-1:{OWNER}:key/anchor-key",
            "prefix": "hype-accumulator/ledger-backups",
            "verify": fake_verify,
        }
        arguments.update(changes)
        return transfer.upload_backup(**arguments)  # type: ignore[arg-type]

    def test_upload_records_exact_versions_in_private_receipt(self) -> None:
        document = self.upload()
        receipt = self.root / "receipt.json"
        self.assertEqual(document["backup_id"], BACKUP_ID)
        self.assertEqual(self.aws.put_calls, 6)
        self.assertEqual(receipt.stat().st_mode & 0o777, 0o600)
        payload = document["payload_objects"]
        self.assertEqual(set(payload), set(transfer.BUNDLE_FILES))
        self.assertEqual(
            {value["bucket"] for value in payload.values()},  # type: ignore[union-attr]
            {PAYLOAD_BUCKET},
        )
        self.assertEqual(document["protected_anchor"]["bucket"], ANCHOR_BUCKET)  # type: ignore[index]

    def test_retry_recovers_the_same_immutable_versions(self) -> None:
        first = self.upload("first.json")
        second = self.upload("second.json")
        self.assertEqual(self.aws.put_calls, 6)
        self.assertEqual(first, second)

    def test_upload_uses_the_exact_bytes_accepted_by_the_verifier(self) -> None:
        original = (self.bundle / "ledger.jsonl").read_bytes()

        def verify_then_mutate(_verifier: Path, bundle: Path, anchor: Path) -> None:
            fake_verify(_verifier, bundle, anchor)
            source = self.bundle / "ledger.jsonl"
            source.write_bytes(b"changed after capture\n")
            os.chmod(source, 0o600)

        document = self.upload(verify=verify_then_mutate)
        ledger = document["payload_objects"]["ledger.jsonl"]  # type: ignore[index]
        _, uploaded = self.aws.objects[(ledger["bucket"], ledger["key"])]  # type: ignore[index]
        self.assertEqual(uploaded, original)

    def test_upload_rejects_one_storage_boundary(self) -> None:
        with self.assertRaisesRegex(transfer.TransferError, "different buckets"):
            self.upload(anchor_bucket=PAYLOAD_BUCKET)
        self.assertEqual(self.aws.put_calls, 0)

    def test_upload_rejects_suspended_versioning_before_put(self) -> None:
        self.aws.versioned.remove(ANCHOR_BUCKET)
        with self.assertRaisesRegex(transfer.TransferError, "versioning"):
            self.upload()
        self.assertEqual(self.aws.put_calls, 0)

    def test_download_uses_receipt_versions_and_verifies_payload(self) -> None:
        self.upload()
        result = transfer.download_backup(
            receipt=self.root / "receipt.json",
            destination_root=self.root / "download",
            verifier=Path("/bin/true"),
            aws=self.aws,
            verify=fake_verify,
        )
        destination = self.root / "download"
        self.assertEqual(result["backup_id"], BACKUP_ID)
        self.assertEqual((destination / "payload" / "ledger.jsonl").read_bytes(), b'{"record":1}\n')
        self.assertEqual((destination / "protected-anchor.json").read_text(), '{"anchor":1}\n')
        self.assertEqual((destination / "transfer-receipt.json").stat().st_mode & 0o777, 0o600)

    def test_download_rejects_remote_payload_damage(self) -> None:
        document = self.upload()
        ledger = document["payload_objects"]["ledger.jsonl"]  # type: ignore[index]
        self.aws.tamper_key = ledger["key"]  # type: ignore[index]
        with self.assertRaisesRegex(transfer.TransferError, "receipt digest"):
            transfer.download_backup(
                receipt=self.root / "receipt.json",
                destination_root=self.root / "damaged",
                verifier=Path("/bin/true"),
                aws=self.aws,
                verify=fake_verify,
            )

    def test_download_binds_verified_manifest_to_receipt_backup_id(self) -> None:
        self.upload()
        with (
            mock.patch.object(transfer, "require_bundle", return_value=("c" * 64, {})),
            self.assertRaisesRegex(transfer.TransferError, "not bound"),
        ):
            transfer.download_backup(
                receipt=self.root / "receipt.json",
                destination_root=self.root / "foreign",
                verifier=Path("/bin/true"),
                aws=self.aws,
                verify=fake_verify,
            )

    def test_download_never_reuses_a_destination(self) -> None:
        self.upload()
        destination = self.root / "existing"
        destination.mkdir()
        with self.assertRaises(transfer.TransferError):
            transfer.download_backup(
                receipt=self.root / "receipt.json",
                destination_root=destination,
                verifier=Path("/bin/true"),
                aws=self.aws,
                verify=fake_verify,
            )

    def test_receipt_unknown_fields_fail_closed(self) -> None:
        self.upload()
        receipt = self.root / "receipt.json"
        document = json.loads(receipt.read_text())
        document["unexpected"] = True
        receipt.write_text(json.dumps(document), encoding="utf-8")
        os.chmod(receipt, 0o600)
        with self.assertRaisesRegex(transfer.TransferError, "unsupported schema"):
            transfer.load_receipt(receipt)

    def test_aws_subprocess_does_not_inherit_bot_signing_material(self) -> None:
        completed = SimpleNamespace(returncode=0, stdout="{}", stderr="")
        with (
            mock.patch.dict(
                os.environ,
                {"HYPE_SIGNING_KEY": "secret", "AWS_SESSION_TOKEN": "session"},
                clear=True,
            ),
            mock.patch.object(transfer.subprocess, "run", return_value=completed) as run,
        ):
            cli = transfer.AwsCli(Path("/usr/bin/true"), "eu-central-1")
            cli._json(
                ["s3api", "get-bucket-versioning"], "test"
            )
            cli._json(["s3api", "put-object"], "transfer", transfer=True)
        environment = run.call_args_list[0].kwargs["env"]
        self.assertNotIn("HYPE_SIGNING_KEY", environment)
        self.assertEqual(environment["AWS_SESSION_TOKEN"], "session")
        self.assertEqual(run.call_args_list[0].kwargs["timeout"], 120)
        self.assertIsNone(run.call_args_list[1].kwargs["timeout"])

    def test_full_replay_has_no_default_timeout(self) -> None:
        completed = SimpleNamespace(returncode=0, stdout="verified", stderr="")
        with mock.patch.object(transfer.subprocess, "run", return_value=completed) as run:
            transfer.run_verifier(Path("/usr/bin/true"), self.bundle, self.anchor)
        self.assertIsNone(run.call_args.kwargs["timeout"])

    def test_explicit_transfer_timeout_and_nonfinite_values(self) -> None:
        completed = SimpleNamespace(returncode=0, stdout="{}", stderr="")
        cli = transfer.AwsCli(
            Path("/usr/bin/true"),
            "eu-central-1",
            transfer_timeout_seconds=900,
        )
        with mock.patch.object(transfer.subprocess, "run", return_value=completed) as run:
            cli._json(["s3api", "get-object"], "transfer", transfer=True)
        self.assertEqual(run.call_args.kwargs["timeout"], 900)
        for value in ["0", "-1", "nan", "inf"]:
            with self.assertRaises(transfer.argparse.ArgumentTypeError):
                transfer.positive_seconds(value)

    def test_aws_cli_rejects_an_unexpected_kms_key(self) -> None:
        source = self.anchor
        expected_key = f"arn:aws:kms:eu-central-1:{OWNER}:key/expected"
        response = {
            "VersionId": "version-1",
            "ChecksumSHA256": transfer.checksum_b64(source),
            "ContentLength": source.stat().st_size,
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": f"arn:aws:kms:eu-central-1:{OWNER}:key/unexpected",
            "Metadata": {"sha256": transfer.sha256_file(source)},
            "ETag": '"etag"',
        }
        cli = transfer.AwsCli(Path("/usr/bin/true"), "eu-central-1")
        with self.assertRaisesRegex(transfer.TransferError, "verification failed"):
            cli._stored_from_response(
                bucket=ANCHOR_BUCKET,
                key="backup/anchor",
                owner=OWNER,
                kms_key_id=expected_key,
                source=source,
                sha256=transfer.sha256_file(source),
                response=response,
            )

    def test_aws_cli_rejects_delete_marker_before_any_put(self) -> None:
        cli = transfer.AwsCli(Path("/usr/bin/true"), "eu-central-1")
        with (
            mock.patch.object(cli, "_key_history", return_value=([], ["delete-1"])),
            mock.patch.object(cli, "_head") as head,
            self.assertRaisesRegex(transfer.TransferError, "replacement or deletion"),
        ):
            cli.put_immutable(
                bucket=ANCHOR_BUCKET,
                key="backup/anchor",
                source=self.anchor,
                owner=OWNER,
                kms_key_id=f"arn:aws:kms:eu-central-1:{OWNER}:key/anchor",
                backup_id=BACKUP_ID,
                sha256=transfer.sha256_file(self.anchor),
            )
        head.assert_not_called()

    def test_aws_cli_rejects_multiple_historical_versions(self) -> None:
        cli = transfer.AwsCli(Path("/usr/bin/true"), "eu-central-1")
        with (
            mock.patch.object(cli, "_key_history", return_value=(["v2", "v1"], [])),
            self.assertRaisesRegex(transfer.TransferError, "replacement or deletion"),
        ):
            cli.put_immutable(
                bucket=PAYLOAD_BUCKET,
                key="backup/ledger",
                source=self.anchor,
                owner=OWNER,
                kms_key_id=f"arn:aws:kms:eu-central-1:{OWNER}:key/payload",
                backup_id=BACKUP_ID,
                sha256=transfer.sha256_file(self.anchor),
            )


if __name__ == "__main__":
    unittest.main()
