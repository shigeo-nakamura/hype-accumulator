#!/usr/bin/env python3
"""Transfer verified ledger backups through immutable, versioned S3 objects.

The payload bundle and protected anchor must use different buckets.  This tool
never creates a ledger backup, restores active state, starts a service, or
constructs a signer.  It records exact object version IDs in a private receipt
so a later download cannot silently select a newer object version.
"""

from __future__ import annotations

import argparse
import base64
import ctypes
import errno
import hashlib
import json
import math
import os
import re
import stat
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from functools import partial
from pathlib import Path
from typing import Callable, Iterator, Protocol, Sequence

BUNDLE_FILES = (
    ".ledger.lock",
    "ledger.jsonl",
    "manifest.json",
    "manifest.json.sha256",
    "snapshot.json",
)
BACKUP_ID_RE = re.compile(r"[0-9a-f]{64}")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
BUCKET_RE = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
OWNER_RE = re.compile(r"[0-9]{12}")
PREFIX_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,510}")
KMS_KEY_ARN_RE = re.compile(
    r"arn:(?:aws|aws-us-gov|aws-cn):kms:[a-z0-9-]+:[0-9]{12}:key/[A-Za-z0-9-]{1,128}"
)
SINGLE_PUT_LIMIT_BYTES = 5_000_000_000
DEFAULT_MULTIPART_PART_BYTES = 64 * 1024 * 1024
MIN_MULTIPART_PART_BYTES = 5 * 1024 * 1024
MAX_MULTIPART_PARTS = 10_000
AT_FDCWD = -100
RENAME_NOREPLACE = 1


class TransferError(RuntimeError):
    """An off-host transfer invariant failed closed."""


class AwsCommandError(TransferError):
    """An AWS CLI request failed before returning structured output."""

    def __init__(self, label: str, detail: str) -> None:
        super().__init__(f"{label} failed")
        self.detail = detail


@dataclass(frozen=True)
class StoredObject:
    bucket: str
    key: str
    version_id: str
    etag: str
    checksum_sha256: str
    sha256: str
    size_bytes: int
    expected_bucket_owner: str
    kms_key_id: str


class AwsClient(Protocol):
    def require_versioning(self, bucket: str, owner: str) -> None: ...

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
    ) -> StoredObject: ...

    def get_exact(self, stored: StoredObject, destination: Path) -> None: ...


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1_048_576), b""):
            digest.update(block)
    return digest.hexdigest()


def checksum_b64(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1_048_576), b""):
            digest.update(block)
    return base64.b64encode(digest.digest()).decode("ascii")


def multipart_checksums_b64(path: Path, part_size: int) -> tuple[str, str]:
    """Return the S3 request and stored SHA-256 composite representations."""
    if part_size <= 0:
        raise TransferError("multipart part size must be positive")
    composite = hashlib.sha256()
    part_count = 0
    with path.open("rb") as handle:
        while payload := handle.read(part_size):
            composite.update(hashlib.sha256(payload).digest())
            part_count += 1
    if part_count == 0 or part_count > MAX_MULTIPART_PARTS:
        raise TransferError("multipart upload has an invalid part count")
    request_checksum = base64.b64encode(composite.digest()).decode("ascii")
    return request_checksum, f"{request_checksum}-{part_count}"


def require_absolute_canonical(path: Path, label: str, *, exists: bool) -> Path:
    if not path.is_absolute():
        raise TransferError(f"{label} must be an absolute path")
    try:
        resolved = path.resolve(strict=exists)
    except OSError as error:
        raise TransferError(f"{label} cannot be resolved safely: {error}") from error
    if resolved != path:
        raise TransferError(f"{label} must not contain aliases or symlink components")
    return resolved


def require_private_regular_file(path: Path, label: str) -> None:
    try:
        info = path.lstat()
    except OSError as error:
        raise TransferError(f"{label} cannot be inspected: {error}") from error
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise TransferError(f"{label} must be a single-link regular file")
    if info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise TransferError(f"{label} must be owner-controlled and private")


def read_private_file(path: Path, label: str, maximum_size: int = 1_048_576) -> bytes:
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise TransferError(f"{label} cannot be read safely: {error}") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_mode & 0o077
            or before.st_size > maximum_size
        ):
            raise TransferError(f"{label} is not a bounded private regular file")
        chunks: list[bytes] = []
        remaining = maximum_size + 1
        while remaining:
            block = os.read(descriptor, min(remaining, 65_536))
            if not block:
                break
            chunks.append(block)
            remaining -= len(block)
        if remaining == 0:
            raise TransferError(f"{label} exceeds its size bound")
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise TransferError(f"{label} changed while it was being read")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def require_executable_file(path: Path, label: str) -> None:
    try:
        info = path.lstat()
    except OSError as error:
        raise TransferError(f"{label} cannot be inspected: {error}") from error
    if not stat.S_ISREG(info.st_mode) or info.st_uid not in {0, os.geteuid()}:
        raise TransferError(f"{label} must be a root- or owner-controlled regular file")
    if info.st_mode & 0o022 or not os.access(path, os.X_OK):
        raise TransferError(f"{label} must be executable and not group/world writable")


def require_bundle(bundle: Path, anchor: Path) -> tuple[str, dict[str, str]]:
    bundle = require_absolute_canonical(bundle, "bundle directory", exists=True)
    anchor = require_absolute_canonical(anchor, "anchor export", exists=True)
    bundle_info = bundle.stat()
    if (
        not bundle.is_dir()
        or bundle_info.st_uid != os.geteuid()
        or bundle_info.st_mode & 0o077
    ):
        raise TransferError("bundle directory must be owner-controlled and private")
    names = tuple(sorted(entry.name for entry in bundle.iterdir()))
    if names != BUNDLE_FILES:
        raise TransferError("bundle contains missing or unexpected files")
    digests: dict[str, str] = {}
    for name in BUNDLE_FILES:
        path = bundle / name
        require_private_regular_file(path, f"bundle member {name}")
        digests[name] = sha256_file(path)
    require_private_regular_file(anchor, "anchor export")
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise TransferError(f"backup manifest is malformed: {error}") from error
    if not isinstance(manifest, dict) or set(manifest) != {
        "schema_version",
        "backup_id",
        "captured_at",
        "record_count",
        "head_hash",
        "files",
        "protected_anchor_export",
    }:
        raise TransferError("backup manifest has an unsupported schema")
    backup_id = manifest.get("backup_id")
    if not isinstance(backup_id, str) or BACKUP_ID_RE.fullmatch(backup_id) is None:
        raise TransferError("backup manifest has an invalid backup ID")
    return backup_id, digests | {"protected-anchor.json": sha256_file(anchor)}


def capture_private_file(source: Path, destination: Path, label: str) -> None:
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        source_fd = os.open(source, flags)
    except OSError as error:
        raise TransferError(f"{label} cannot be captured safely: {error}") from error
    try:
        before = os.fstat(source_fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or before.st_mode & 0o077
        ):
            raise TransferError(f"{label} changed to an unsafe file")
        destination_fd = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
            0o600,
        )
        try:
            while True:
                block = os.read(source_fd, 1_048_576)
                if not block:
                    break
                view = memoryview(block)
                while view:
                    written = os.write(destination_fd, view)
                    if written <= 0:
                        raise TransferError(f"{label} capture stopped making progress")
                    view = view[written:]
            os.fchmod(destination_fd, 0o600)
            os.fsync(destination_fd)
        finally:
            os.close(destination_fd)
        after = os.fstat(source_fd)
        identity = lambda value: (
            value.st_dev,
            value.st_ino,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
            value.st_nlink,
            value.st_mode,
        )
        if identity(before) != identity(after):
            raise TransferError(f"{label} changed while it was being captured")
    finally:
        os.close(source_fd)


@contextmanager
def captured_backup(bundle: Path, anchor: Path) -> Iterator[tuple[Path, Path, str, dict[str, str]]]:
    require_bundle(bundle, anchor)
    with tempfile.TemporaryDirectory(prefix="hype-ledger-transfer-") as temporary:
        root = Path(temporary)
        os.chmod(root, 0o700)
        captured_bundle = root / "payload"
        captured_bundle.mkdir(mode=0o700)
        captured_anchor = root / "protected-anchor.json"
        for name in BUNDLE_FILES:
            capture_private_file(bundle / name, captured_bundle / name, f"bundle member {name}")
        capture_private_file(anchor, captured_anchor, "anchor export")
        backup_id, digests = require_bundle(captured_bundle, captured_anchor)
        yield captured_bundle, captured_anchor, backup_id, digests


def validate_remote_name(value: str, pattern: re.Pattern[str], label: str) -> str:
    if pattern.fullmatch(value) is None or ".." in value.split("/"):
        raise TransferError(f"{label} is not canonical")
    return value


def run_verifier(
    verifier: Path,
    bundle: Path,
    anchor: Path,
    timeout_seconds: float | None = None,
) -> None:
    verifier = require_absolute_canonical(verifier, "verifier binary", exists=True)
    require_executable_file(verifier, "verifier binary")
    environment = {"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"}
    try:
        result = subprocess.run(
            [str(verifier), "--ledger-backup-verify", str(bundle), str(anchor)],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=environment,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise TransferError(f"ledger verifier could not run: {error}") from error
    if result.returncode != 0:
        raise TransferError("ledger verifier rejected the backup")


def rename_noreplace(source: Path, destination: Path) -> None:
    """Atomically move source to an absent destination on Linux."""
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        renameat2 = libc.renameat2
    except AttributeError as error:
        raise OSError(errno.ENOSYS, "renameat2 is unavailable") from error
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        AT_FDCWD,
        os.fsencode(source),
        AT_FDCWD,
        os.fsencode(destination),
        RENAME_NOREPLACE,
    )
    if result != 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number), str(destination))


def write_private_json(path: Path, document: dict[str, object]) -> None:
    path = require_absolute_canonical(path, "receipt path", exists=False)
    parent = require_absolute_canonical(path.parent, "receipt parent", exists=True)
    if path.exists() or path.is_symlink():
        raise TransferError("receipt output already exists")
    payload = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    temporary_path: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.tmp-", dir=parent
        )
        temporary_path = Path(temporary_name)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fchmod(handle.fileno(), 0o600)
            os.fsync(handle.fileno())
        rename_noreplace(temporary_path, path)
        temporary_path = None
        parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    except OSError as error:
        raise TransferError(f"receipt cannot be published: {error}") from error
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                # Never mask the original publish result with temporary cleanup.
                pass


def validate_receipt_output(receipt: Path, bundle: Path, anchor: Path) -> None:
    receipt = require_absolute_canonical(receipt, "receipt path", exists=False)
    bundle = require_absolute_canonical(bundle, "bundle directory", exists=True)
    anchor = require_absolute_canonical(anchor, "anchor export", exists=True)
    require_absolute_canonical(receipt.parent, "receipt parent", exists=True)
    if receipt.exists() or receipt.is_symlink():
        raise TransferError("receipt output already exists")
    if receipt == anchor or receipt == bundle or bundle in receipt.parents:
        raise TransferError("receipt output must not overlap the source backup")


def upload_backup(
    *,
    bundle: Path,
    anchor: Path,
    receipt: Path,
    verifier: Path,
    aws: AwsClient,
    payload_bucket: str,
    payload_owner: str,
    payload_kms_key: str,
    anchor_bucket: str,
    anchor_owner: str,
    anchor_kms_key: str,
    prefix: str,
    verify: Callable[[Path, Path, Path], None] = run_verifier,
) -> dict[str, object]:
    payload_bucket = validate_remote_name(payload_bucket, BUCKET_RE, "payload bucket")
    anchor_bucket = validate_remote_name(anchor_bucket, BUCKET_RE, "anchor bucket")
    if payload_bucket == anchor_bucket:
        raise TransferError("payload and protected anchor must use different buckets")
    validate_remote_name(payload_owner, OWNER_RE, "payload bucket owner")
    validate_remote_name(anchor_owner, OWNER_RE, "anchor bucket owner")
    validate_remote_name(payload_kms_key, KMS_KEY_ARN_RE, "payload KMS key ARN")
    validate_remote_name(anchor_kms_key, KMS_KEY_ARN_RE, "anchor KMS key ARN")
    prefix = validate_remote_name(prefix.strip("/"), PREFIX_RE, "object prefix")
    validate_receipt_output(receipt, bundle, anchor)
    with captured_backup(bundle, anchor) as (captured_bundle, captured_anchor, backup_id, digests):
        verify(verifier, captured_bundle, captured_anchor)
        aws.require_versioning(payload_bucket, payload_owner)
        aws.require_versioning(anchor_bucket, anchor_owner)

        objects: dict[str, StoredObject] = {}
        for name in BUNDLE_FILES:
            objects[name] = aws.put_immutable(
                bucket=payload_bucket,
                key=f"{prefix}/{backup_id}/payload/{name}",
                source=captured_bundle / name,
                owner=payload_owner,
                kms_key_id=payload_kms_key,
                backup_id=backup_id,
                sha256=digests[name],
            )
        protected = aws.put_immutable(
            bucket=anchor_bucket,
            key=f"{prefix}/{backup_id}/protected-anchor.json",
            source=captured_anchor,
            owner=anchor_owner,
            kms_key_id=anchor_kms_key,
            backup_id=backup_id,
            sha256=digests["protected-anchor.json"],
        )
        document: dict[str, object] = {
            "schema_version": 1,
            "backup_id": backup_id,
            "payload_objects": {name: asdict(item) for name, item in sorted(objects.items())},
            "protected_anchor": asdict(protected),
        }
    write_private_json(receipt, document)
    return document


def parse_stored_object(value: object, label: str) -> StoredObject:
    fields = {field.name for field in StoredObject.__dataclass_fields__.values()}
    if not isinstance(value, dict) or set(value) != fields:
        raise TransferError(f"{label} receipt object is malformed")
    string_fields = fields - {"size_bytes"}
    if (
        any(not isinstance(value[field], str) for field in string_fields)
        or not isinstance(value["size_bytes"], int)
        or isinstance(value["size_bytes"], bool)
    ):
        raise TransferError(f"{label} receipt object has invalid field types")
    try:
        stored = StoredObject(**value)
    except TypeError as error:
        raise TransferError(f"{label} receipt object is malformed") from error
    validate_remote_name(stored.bucket, BUCKET_RE, f"{label} bucket")
    validate_remote_name(stored.expected_bucket_owner, OWNER_RE, f"{label} owner")
    validate_remote_name(stored.kms_key_id, KMS_KEY_ARN_RE, f"{label} KMS key ARN")
    if (
        not stored.key
        or not stored.version_id
        or not stored.etag
        or not stored.checksum_sha256
        or SHA256_RE.fullmatch(stored.sha256) is None
        or stored.size_bytes < 0
    ):
        raise TransferError(f"{label} receipt object has invalid values")
    return stored


def load_receipt(path: Path) -> tuple[str, dict[str, StoredObject], StoredObject]:
    path = require_absolute_canonical(path, "receipt", exists=True)
    try:
        document = json.loads(read_private_file(path, "receipt").decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise TransferError(f"receipt is malformed: {error}") from error
    if not isinstance(document, dict) or set(document) != {
        "schema_version",
        "backup_id",
        "payload_objects",
        "protected_anchor",
    } or document.get("schema_version") != 1:
        raise TransferError("receipt has an unsupported schema")
    backup_id = document.get("backup_id")
    if not isinstance(backup_id, str) or BACKUP_ID_RE.fullmatch(backup_id) is None:
        raise TransferError("receipt backup ID is invalid")
    raw_payload = document.get("payload_objects")
    if not isinstance(raw_payload, dict) or set(raw_payload) != set(BUNDLE_FILES):
        raise TransferError("receipt payload file set is invalid")
    payload = {name: parse_stored_object(raw_payload[name], name) for name in BUNDLE_FILES}
    protected = parse_stored_object(document.get("protected_anchor"), "protected anchor")
    if any(
        not item.key.endswith(f"/{backup_id}/payload/{name}")
        or item.key.startswith("/")
        or ".." in item.key.split("/")
        for name, item in payload.items()
    ):
        raise TransferError("receipt payload key is not bound to its backup ID")
    if (
        not protected.key.endswith(f"/{backup_id}/protected-anchor.json")
        or protected.key.startswith("/")
        or ".." in protected.key.split("/")
    ):
        raise TransferError("receipt anchor key is not bound to its backup ID")
    if len({item.bucket for item in payload.values()}) != 1 or protected.bucket in {
        item.bucket for item in payload.values()
    }:
        raise TransferError("receipt does not preserve separate storage boundaries")
    return backup_id, payload, protected


def download_backup(
    *,
    receipt: Path,
    destination_root: Path,
    verifier: Path,
    aws: AwsClient,
    verify: Callable[[Path, Path, Path], None] = run_verifier,
) -> dict[str, str]:
    backup_id, payload, protected = load_receipt(receipt)
    payload_sample = next(iter(payload.values()))
    aws.require_versioning(payload_sample.bucket, payload_sample.expected_bucket_owner)
    aws.require_versioning(protected.bucket, protected.expected_bucket_owner)
    destination_root = require_absolute_canonical(
        destination_root, "download destination", exists=False
    )
    require_absolute_canonical(destination_root.parent, "download parent", exists=True)
    try:
        destination_root.mkdir(mode=0o700)
        os.chmod(destination_root, 0o700)
    except OSError as error:
        raise TransferError(f"download destination cannot be reserved: {error}") from error
    bundle = destination_root / "payload"
    anchor = destination_root / "protected-anchor.json"
    bundle.mkdir(mode=0o700)
    try:
        for name, stored in payload.items():
            target = bundle / name
            aws.get_exact(stored, target)
            with target.open("rb") as handle:
                os.fsync(handle.fileno())
            require_private_regular_file(target, f"downloaded {name}")
            if target.stat().st_size != stored.size_bytes or sha256_file(target) != stored.sha256:
                raise TransferError(f"downloaded {name} failed its receipt digest")
        aws.get_exact(protected, anchor)
        with anchor.open("rb") as handle:
            os.fsync(handle.fileno())
        require_private_regular_file(anchor, "downloaded protected anchor")
        if anchor.stat().st_size != protected.size_bytes or sha256_file(anchor) != protected.sha256:
            raise TransferError("downloaded protected anchor failed its receipt digest")
        verify(verifier, bundle, anchor)
        downloaded_backup_id, _ = require_bundle(bundle, anchor)
        if downloaded_backup_id != backup_id:
            raise TransferError("downloaded manifest is not bound to the receipt backup ID")
        receipt_copy = destination_root / "transfer-receipt.json"
        receipt_document: dict[str, object] = {
            "schema_version": 1,
            "backup_id": backup_id,
            "payload_objects": {
                name: asdict(item) for name, item in sorted(payload.items())
            },
            "protected_anchor": asdict(protected),
        }
        write_private_json(receipt_copy, receipt_document)
    except Exception:
        # Leave the private reserved directory intact for operator inspection.
        raise
    return {"backup_id": backup_id, "bundle": str(bundle), "anchor": str(anchor)}


class AwsCli:
    def __init__(
        self,
        aws_bin: Path,
        region: str,
        control_timeout_seconds: float = 120,
        transfer_timeout_seconds: float | None = None,
        put_object_limit_bytes: int = SINGLE_PUT_LIMIT_BYTES,
        multipart_part_bytes: int = DEFAULT_MULTIPART_PART_BYTES,
    ) -> None:
        aws_path = require_absolute_canonical(aws_bin, "AWS CLI", exists=True)
        require_executable_file(aws_path, "AWS CLI")
        self.aws_bin = str(aws_path)
        self.region = region
        self.control_timeout_seconds = control_timeout_seconds
        self.transfer_timeout_seconds = transfer_timeout_seconds
        self.put_object_limit_bytes = put_object_limit_bytes
        self.multipart_part_bytes = multipart_part_bytes

    def _json(
        self,
        arguments: Sequence[str],
        label: str,
        *,
        transfer: bool = False,
    ) -> dict[str, object]:
        allowed = {
            "AWS_ACCESS_KEY_ID",
            "AWS_CA_BUNDLE",
            "AWS_CONFIG_FILE",
            "AWS_DEFAULT_PROFILE",
            "AWS_EC2_METADATA_DISABLED",
            "AWS_PROFILE",
            "AWS_ROLE_ARN",
            "AWS_ROLE_SESSION_NAME",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_SHARED_CREDENTIALS_FILE",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "NO_PROXY",
        }
        environment = {key: value for key, value in os.environ.items() if key in allowed}
        environment.update(
            {
                "AWS_PAGER": "",
                "AWS_CLI_AUTO_PROMPT": "off",
                "LANG": "C",
                "LC_ALL": "C",
                "PATH": "/usr/local/bin:/usr/bin:/bin",
            }
        )
        try:
            result = subprocess.run(
                [self.aws_bin, *arguments, "--region", self.region, "--output", "json", "--no-cli-pager"],
                check=False,
                capture_output=True,
                text=True,
                timeout=(
                    self.transfer_timeout_seconds
                    if transfer
                    else self.control_timeout_seconds
                ),
                env=environment,
            )
        except (OSError, subprocess.SubprocessError) as error:
            raise TransferError(f"{label} could not run: {error}") from error
        if result.returncode != 0:
            raise AwsCommandError(label, (result.stderr or result.stdout).strip())
        try:
            value = json.loads(result.stdout or "{}")
        except json.JSONDecodeError as error:
            raise TransferError(f"{label} returned malformed JSON") from error
        if not isinstance(value, dict):
            raise TransferError(f"{label} returned a non-object response")
        return value

    def require_versioning(self, bucket: str, owner: str) -> None:
        result = self._json(
            ["s3api", "get-bucket-versioning", "--bucket", bucket, "--expected-bucket-owner", owner],
            f"versioning check for {bucket}",
        )
        if result.get("Status") != "Enabled":
            raise TransferError(f"bucket {bucket} does not have enabled versioning")

    def _head(self, bucket: str, key: str, owner: str) -> dict[str, object] | None:
        try:
            return self._json(
                ["s3api", "head-object", "--bucket", bucket, "--key", key,
                 "--checksum-mode", "ENABLED", "--expected-bucket-owner", owner],
                f"immutable object lookup for {bucket}/{key}",
            )
        except AwsCommandError as error:
            if any(marker in error.detail for marker in ("(404)", "Not Found", "NoSuchKey")):
                return None
            raise

    def _key_history(self, bucket: str, key: str, owner: str) -> tuple[list[str], list[str]]:
        result = self._json(
            [
                "s3api",
                "list-object-versions",
                "--bucket",
                bucket,
                "--prefix",
                key,
                "--expected-bucket-owner",
                owner,
            ],
            f"immutable object history for {bucket}/{key}",
        )
        raw_versions = result.get("Versions", [])
        raw_delete_markers = result.get("DeleteMarkers", [])
        if not isinstance(raw_versions, list) or not isinstance(raw_delete_markers, list):
            raise TransferError(f"remote object history is malformed for {bucket}/{key}")
        versions = [
            item.get("VersionId")
            for item in raw_versions
            if isinstance(item, dict) and item.get("Key") == key
        ]
        delete_markers = [
            item.get("VersionId")
            for item in raw_delete_markers
            if isinstance(item, dict) and item.get("Key") == key
        ]
        if any(not isinstance(value, str) or not value for value in versions + delete_markers):
            raise TransferError(f"remote object history is malformed for {bucket}/{key}")
        return versions, delete_markers  # type: ignore[return-value]

    def _require_no_pending_multipart(self, bucket: str, key: str, owner: str) -> None:
        result = self._json(
            [
                "s3api",
                "list-multipart-uploads",
                "--bucket",
                bucket,
                "--prefix",
                key,
                "--expected-bucket-owner",
                owner,
            ],
            f"multipart upload history for {bucket}/{key}",
        )
        uploads = result.get("Uploads", [])
        if not isinstance(uploads, list) or any(not isinstance(item, dict) for item in uploads):
            raise TransferError(f"multipart upload history is malformed for {bucket}/{key}")
        if any(item.get("Key") == key for item in uploads):
            raise TransferError(f"an incomplete multipart upload exists for {bucket}/{key}")

    @staticmethod
    def _require_single_history(
        bucket: str,
        key: str,
        versions: list[str],
        delete_markers: list[str],
    ) -> str:
        if delete_markers or len(versions) != 1:
            raise TransferError(
                f"remote object history is not one immutable version for {bucket}/{key}"
            )
        return versions[0]

    def _stored_from_response(
        self, *, bucket: str, key: str, owner: str, kms_key_id: str,
        source: Path, sha256: str, expected_checksum: str,
        response: dict[str, object]
    ) -> StoredObject:
        metadata = response.get("Metadata")
        version_id = response.get("VersionId")
        if (
            not isinstance(version_id, str) or not version_id or version_id == "null"
            or response.get("ChecksumSHA256") != expected_checksum
            or response.get("ContentLength") != source.stat().st_size
            or response.get("ServerSideEncryption") != "aws:kms"
            or response.get("SSEKMSKeyId") != kms_key_id
            or not isinstance(metadata, dict)
            or metadata.get("sha256") != sha256
        ):
            raise TransferError(f"remote object verification failed for {bucket}/{key}")
        etag = response.get("ETag")
        if not isinstance(etag, str) or not etag:
            raise TransferError(f"remote object ETag is missing for {bucket}/{key}")
        return StoredObject(
            bucket=bucket, key=key, version_id=version_id, etag=etag,
            checksum_sha256=expected_checksum, sha256=sha256,
            size_bytes=source.stat().st_size, expected_bucket_owner=owner,
            kms_key_id=str(response["SSEKMSKeyId"]),
        )

    def _put_single(
        self,
        *,
        bucket: str,
        key: str,
        source: Path,
        owner: str,
        kms_key_id: str,
        backup_id: str,
        sha256: str,
    ) -> None:
        self._json(
            ["s3api", "put-object", "--bucket", bucket, "--key", key,
             "--body", str(source), "--if-none-match", "*",
             "--checksum-algorithm", "SHA256", "--checksum-sha256", checksum_b64(source),
             "--server-side-encryption", "aws:kms", "--ssekms-key-id", kms_key_id,
             "--bucket-key-enabled", "--expected-bucket-owner", owner,
             "--metadata", f"backup-id={backup_id},sha256={sha256}"],
            f"immutable upload for {bucket}/{key}",
            transfer=True,
        )

    def _abort_multipart(
        self, bucket: str, key: str, owner: str, upload_id: str
    ) -> None:
        try:
            self._json(
                [
                    "s3api",
                    "abort-multipart-upload",
                    "--bucket",
                    bucket,
                    "--key",
                    key,
                    "--upload-id",
                    upload_id,
                    "--expected-bucket-owner",
                    owner,
                ],
                f"multipart abort for {bucket}/{key}",
            )
        except TransferError:
            # The complete request may have succeeded despite a lost response.
            # A later retry reconciles exact version history and object bytes.
            pass

    def _put_multipart(
        self,
        *,
        bucket: str,
        key: str,
        source: Path,
        owner: str,
        kms_key_id: str,
        backup_id: str,
        sha256: str,
    ) -> str:
        self._require_no_pending_multipart(bucket, key, owner)
        size = source.stat().st_size
        part_size = max(
            self.multipart_part_bytes,
            (size + MAX_MULTIPART_PARTS - 1) // MAX_MULTIPART_PARTS,
        )
        if part_size < MIN_MULTIPART_PART_BYTES or part_size > SINGLE_PUT_LIMIT_BYTES:
            raise TransferError("multipart part size cannot satisfy S3 limits")
        request_checksum, stored_checksum = multipart_checksums_b64(source, part_size)
        created = self._json(
            [
                "s3api", "create-multipart-upload", "--bucket", bucket, "--key", key,
                "--checksum-algorithm", "SHA256", "--checksum-type", "COMPOSITE",
                "--server-side-encryption", "aws:kms", "--ssekms-key-id", kms_key_id,
                "--bucket-key-enabled", "--expected-bucket-owner", owner,
                "--metadata", f"backup-id={backup_id},sha256={sha256}",
            ],
            f"multipart create for {bucket}/{key}",
        )
        upload_id = created.get("UploadId")
        if not isinstance(upload_id, str) or not upload_id:
            raise TransferError(f"multipart upload ID is missing for {bucket}/{key}")
        completed = False
        try:
            parts: list[dict[str, object]] = []
            with tempfile.TemporaryDirectory(prefix="hype-ledger-part-") as temporary:
                temporary_root = Path(temporary)
                os.chmod(temporary_root, 0o700)
                with source.open("rb") as source_handle:
                    part_number = 1
                    while True:
                        payload = source_handle.read(part_size)
                        if not payload:
                            break
                        part_path = temporary_root / "part"
                        descriptor = os.open(
                            part_path,
                            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                            0o600,
                        )
                        with os.fdopen(descriptor, "wb") as part_handle:
                            part_handle.write(payload)
                            part_handle.flush()
                            os.fchmod(part_handle.fileno(), 0o600)
                            os.fsync(part_handle.fileno())
                        part_checksum = checksum_b64(part_path)
                        response = self._json(
                            [
                                "s3api", "upload-part", "--bucket", bucket, "--key", key,
                                "--upload-id", upload_id, "--part-number", str(part_number),
                                "--body", str(part_path), "--checksum-algorithm", "SHA256",
                                "--checksum-sha256", part_checksum,
                                "--expected-bucket-owner", owner,
                            ],
                            f"multipart part {part_number} for {bucket}/{key}",
                            transfer=True,
                        )
                        etag = response.get("ETag")
                        if not isinstance(etag, str) or not etag or response.get("ChecksumSHA256") != part_checksum:
                            raise TransferError(
                                f"multipart part {part_number} verification failed for {bucket}/{key}"
                            )
                        parts.append(
                            {"ETag": etag, "PartNumber": part_number, "ChecksumSHA256": part_checksum}
                        )
                        part_path.unlink()
                        part_number += 1
                if not parts or len(parts) > MAX_MULTIPART_PARTS:
                    raise TransferError("multipart upload has an invalid part count")
                completion_path = temporary_root / "completion.json"
                write_private_json(completion_path, {"Parts": parts})
                self._json(
                    [
                        "s3api", "complete-multipart-upload", "--bucket", bucket, "--key", key,
                        "--upload-id", upload_id,
                        "--multipart-upload", f"file://{completion_path}",
                        "--checksum-type", "COMPOSITE",
                        "--checksum-sha256", request_checksum,
                        "--if-none-match", "*",
                        "--expected-bucket-owner", owner,
                    ],
                    f"multipart complete for {bucket}/{key}",
                    transfer=True,
                )
                completed = True
                return stored_checksum
        finally:
            if not completed:
                self._abort_multipart(bucket, key, owner, upload_id)

    def put_immutable(
        self, *, bucket: str, key: str, source: Path, owner: str,
        kms_key_id: str, backup_id: str, sha256: str
    ) -> StoredObject:
        size = source.stat().st_size
        multipart_part_size = max(
            self.multipart_part_bytes,
            (size + MAX_MULTIPART_PARTS - 1) // MAX_MULTIPART_PARTS,
        )
        if size <= self.put_object_limit_bytes:
            expected_checksum = checksum_b64(source)
        else:
            _, expected_checksum = multipart_checksums_b64(
                source, multipart_part_size
            )
        versions, delete_markers = self._key_history(bucket, key, owner)
        if delete_markers or len(versions) > 1:
            raise TransferError(
                f"remote object history shows replacement or deletion for {bucket}/{key}"
            )
        existing = self._head(bucket, key, owner)
        if versions:
            if existing is None or existing.get("VersionId") != versions[0]:
                raise TransferError(f"remote object current version is inconsistent for {bucket}/{key}")
        elif existing is not None:
            raise TransferError(f"remote object exists without version history for {bucket}/{key}")
        else:
            put = self._put_single if size <= self.put_object_limit_bytes else self._put_multipart
            uploaded_checksum = put(
                bucket=bucket,
                key=key,
                source=source,
                owner=owner,
                kms_key_id=kms_key_id,
                backup_id=backup_id,
                sha256=sha256,
            )
            if uploaded_checksum is not None and uploaded_checksum != expected_checksum:
                raise TransferError(f"local multipart checksum changed for {bucket}/{key}")
            existing = self._head(bucket, key, owner)
            versions, delete_markers = self._key_history(bucket, key, owner)
        if existing is None:
            raise TransferError(f"uploaded object cannot be read back: {bucket}/{key}")
        only_version = self._require_single_history(
            bucket, key, versions, delete_markers
        )
        if existing.get("VersionId") != only_version:
            raise TransferError(f"uploaded object version history changed for {bucket}/{key}")
        metadata = existing.get("Metadata")
        if not isinstance(metadata, dict) or metadata.get("backup-id") != backup_id:
            raise TransferError(f"remote object backup ID mismatch for {bucket}/{key}")
        return self._stored_from_response(
            bucket=bucket, key=key, owner=owner, kms_key_id=kms_key_id,
            source=source, sha256=sha256, expected_checksum=expected_checksum,
            response=existing,
        )

    def get_exact(self, stored: StoredObject, destination: Path) -> None:
        response = self._json(
            ["s3api", "get-object", "--bucket", stored.bucket, "--key", stored.key,
             "--version-id", stored.version_id, "--checksum-mode", "ENABLED",
             "--expected-bucket-owner", stored.expected_bucket_owner, str(destination)],
            f"exact download for {stored.bucket}/{stored.key}",
            transfer=True,
        )
        os.chmod(destination, 0o600)
        if response.get("VersionId") != stored.version_id or response.get("ChecksumSHA256") != stored.checksum_sha256:
            raise TransferError(f"download response mismatch for {stored.bucket}/{stored.key}")


def positive_seconds(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("timeout must be a positive number") from error
    if not math.isfinite(parsed) or not parsed > 0:
        raise argparse.ArgumentTypeError("timeout must be a positive number")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws-bin", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--control-timeout-seconds", type=positive_seconds, default=120.0)
    parser.add_argument("--transfer-timeout-seconds", type=positive_seconds)
    parser.add_argument("--verifier-timeout-seconds", type=positive_seconds)
    subparsers = parser.add_subparsers(dest="command", required=True)
    upload = subparsers.add_parser("upload")
    upload.add_argument("--bundle", required=True)
    upload.add_argument("--anchor", required=True)
    upload.add_argument("--receipt", required=True)
    upload.add_argument("--verifier", required=True)
    upload.add_argument("--payload-bucket", required=True)
    upload.add_argument("--payload-owner", required=True)
    upload.add_argument("--payload-kms-key", required=True)
    upload.add_argument("--anchor-bucket", required=True)
    upload.add_argument("--anchor-owner", required=True)
    upload.add_argument("--anchor-kms-key", required=True)
    upload.add_argument("--prefix", default="hype-accumulator/ledger-backups")
    download = subparsers.add_parser("download")
    download.add_argument("--receipt", required=True)
    download.add_argument("--destination-root", required=True)
    download.add_argument("--verifier", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        aws = AwsCli(
            Path(args.aws_bin),
            args.region,
            control_timeout_seconds=args.control_timeout_seconds,
            transfer_timeout_seconds=args.transfer_timeout_seconds,
        )
        verify = partial(
            run_verifier,
            timeout_seconds=args.verifier_timeout_seconds,
        )
        if args.command == "upload":
            receipt = Path(args.receipt)
            document = upload_backup(
                bundle=Path(args.bundle), anchor=Path(args.anchor), receipt=Path(args.receipt),
                verifier=Path(args.verifier), aws=aws, payload_bucket=args.payload_bucket,
                payload_owner=args.payload_owner, payload_kms_key=args.payload_kms_key,
                anchor_bucket=args.anchor_bucket, anchor_owner=args.anchor_owner,
                anchor_kms_key=args.anchor_kms_key, prefix=args.prefix, verify=verify,
            )
            result = {"backup_id": document["backup_id"], "receipt": str(receipt)}
        else:
            result = download_backup(
                receipt=Path(args.receipt), destination_root=Path(args.destination_root),
                verifier=Path(args.verifier), aws=aws, verify=verify,
            )
    except (TransferError, OSError, UnicodeError) as error:
        print(f"ledger backup transfer rejected: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
