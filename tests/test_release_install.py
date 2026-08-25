import argparse
import hashlib
import io
import json
import os
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import release_install


REPOSITORY = "shigeo-nakamura/hype-accumulator"
TARGET = "aarch64-unknown-linux-gnu"
BUILD_IMAGE = "amazonlinux:2023.12@sha256:" + "c" * 64
LOCK_DIGEST = "b" * 64


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def release_payload(commit: str, repository: str = REPOSITORY) -> dict[str, bytes]:
    provenance = "\n".join(
        [
            f"repository={repository}",
            f"commit={commit}",
            f"target={TARGET}",
            f"build_image={BUILD_IMAGE}",
            "system_release=Amazon Linux release 2023 test",
            "libc=glibc 2.34",
            f"{LOCK_DIGEST}  Cargo.lock",
            "rustc 1.85.1",
        ]
    ).encode()
    abi = "\n".join(
        [
            f"build_image={BUILD_IMAGE}",
            "system_release=Amazon Linux release 2023 test",
            "libc=glibc 2.34",
            f"target/al2023/{TARGET}/release/hype-accumulator: ELF 64-bit ARM aarch64",
            f"target/al2023/{TARGET}/release/hype-status: ELF 64-bit ARM aarch64",
        ]
    ).encode()
    payload = {
        "AL2023-ABI.txt": abi,
        "BUILD-PROVENANCE.txt": provenance,
        "hype-accumulator": b"fake-main-binary",
        "hype-status": b"fake-status-binary",
    }
    payload["SHA256SUMS"] = "".join(
        f"{digest(payload[name])}  {name}\n" for name in sorted(payload)
    ).encode()
    return payload


def write_archive(
    directory: Path,
    commit: str,
    *,
    repository: str = REPOSITORY,
    extra_member: str | None = None,
) -> tuple[Path, Path]:
    archive = directory / f"hype-accumulator-{commit}-{TARGET}.tar.gz"
    payload = release_payload(commit, repository)
    with tarfile.open(archive, "w:gz") as handle:
        for name, content in payload.items():
            info = tarfile.TarInfo(name)
            info.mode = release_install.ARCHIVE_FILES[name][0]
            info.size = len(content)
            info.mtime = 0
            handle.addfile(info, io.BytesIO(content))
        if extra_member is not None:
            content = b"unexpected"
            info = tarfile.TarInfo(extra_member)
            info.mode = 0o644
            info.size = len(content)
            handle.addfile(info, io.BytesIO(content))
    checksum = directory / f"{archive.name}.sha256"
    checksum.write_text(f"{release_install.sha256_file(archive)}  {archive.name}\n")
    return archive, checksum


def stage_args(
    root: Path,
    archive: Path,
    checksum: Path,
    commit: str,
) -> argparse.Namespace:
    # The installer requires every existing ancestor to be traversable by a
    # distinct runtime UID. TemporaryDirectory defaults to 0700.
    root.chmod(0o755)
    config = root / "config.toml"
    policy = root / "security-policy.toml"
    config.write_text("dry_run = true\nmanual_halt = true\nlive_approved = false\n")
    policy.write_text("[operator]\ndry_run = true\nmanual_halt = true\n")
    return argparse.Namespace(
        archive=str(archive),
        archive_sha256_file=str(checksum),
        expected_repository=REPOSITORY,
        expected_commit=commit,
        expected_target=TARGET,
        expected_build_image=BUILD_IMAGE,
        expected_cargo_lock_sha256=LOCK_DIGEST,
        config=str(config),
        security_policy=str(policy),
        install_root=str(root / "install"),
    )


def select_args(stage: argparse.Namespace, release_id: str) -> argparse.Namespace:
    return argparse.Namespace(
        release_id=release_id,
        config=stage.config,
        security_policy=stage.security_policy,
        install_root=stage.install_root,
    )


class ReleaseInstallTests(unittest.TestCase):
    def test_restrictive_umask_cannot_hide_release_parents_from_runtime(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root.chmod(0o755)
            previous_umask = os.umask(0o077)
            try:
                install_root, releases = release_install.ensure_install_root(
                    root / "install"
                )
            finally:
                os.umask(previous_umask)

            self.assertEqual(install_root.stat().st_mode & 0o7777, 0o755)
            self.assertEqual(releases.stat().st_mode & 0o7777, 0o755)

            install_root.chmod(0o700)
            with self.assertRaisesRegex(release_install.InstallError, "exact mode 0755"):
                release_install.ensure_install_root(install_root)

    def test_writable_ancestor_requires_sticky_bit_rename_protection(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root.chmod(0o755)
            shared = root / "shared"
            shared.mkdir()
            shared.chmod(0o777)

            with self.assertRaisesRegex(
                release_install.InstallError, "writable without sticky-bit"
            ):
                release_install.ensure_install_root(shared / "install")

            shared.chmod(0o1777)
            install_root, releases = release_install.ensure_install_root(
                shared / "install"
            )
            self.assertEqual(install_root.stat().st_mode & 0o7777, 0o755)
            self.assertEqual(releases.stat().st_mode & 0o7777, 0o755)

    def test_foreign_owned_ancestor_is_rejected_even_when_mode_is_0755(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            root.chmod(0o755)
            foreign_uid = os.geteuid() + 1
            real_stat = Path.stat

            def stat_with_foreign_owner(path, *args, **kwargs):
                status = real_stat(path, *args, **kwargs)
                if path == root:
                    return mock.Mock(st_mode=status.st_mode, st_uid=foreign_uid)
                return status

            with mock.patch.object(
                Path, "stat", autospec=True, side_effect=stat_with_foreign_owner
            ):
                with self.assertRaisesRegex(
                    release_install.InstallError, "owned by root or the deployment"
                ):
                    release_install.ensure_install_root(root / "install")

    def test_staging_stays_private_until_every_member_is_sealed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commit = "a" * 40
            archive, checksum = write_archive(root, commit)
            args = stage_args(root, archive, checksum, commit)
            observed_modes = []

            def inspect_private_staging(release_dir, _config, _policy):
                observed_modes.append(release_dir.stat().st_mode & 0o7777)
                self.assertTrue(release_dir.name.startswith(".stage-"))
                for name, (expected_mode, _) in release_install.ARCHIVE_FILES.items():
                    self.assertEqual(
                        (release_dir / name).stat().st_mode & 0o777,
                        expected_mode,
                    )

            previous_umask = os.umask(0o000)
            try:
                with mock.patch.object(
                    release_install,
                    "verify_runtime",
                    side_effect=inspect_private_staging,
                ):
                    staged = release_install.stage_release(args)
            finally:
                os.umask(previous_umask)

            self.assertEqual(observed_modes, [0o700])
            release_dir = Path(args.install_root) / "releases" / staged["release_id"]
            self.assertEqual(release_dir.stat().st_mode & 0o7777, 0o755)

    def test_stage_is_content_addressed_idempotent_and_does_not_activate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commit = "a" * 40
            archive, checksum = write_archive(root, commit)
            args = stage_args(root, archive, checksum, commit)
            with mock.patch.object(release_install, "verify_runtime") as runtime:
                first = release_install.stage_release(args)
                second = release_install.stage_release(args)

            self.assertEqual(first, second)
            self.assertEqual(runtime.call_count, 3)
            release_dir = Path(args.install_root) / "releases" / first["release_id"]
            manifest = json.loads(
                (release_dir / release_install.INSTALL_MANIFEST).read_text()
            )
            self.assertEqual(manifest["commit"], commit)
            self.assertEqual(manifest["archive_sha256"], first["archive_sha256"])
            self.assertEqual(release_dir.stat().st_mode & 0o7777, 0o755)
            self.assertFalse((Path(args.install_root) / "current").exists())

    def test_external_checksum_and_closed_archive_fail_before_staging(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commit = "a" * 40
            archive, checksum = write_archive(root, commit)
            args = stage_args(root, archive, checksum, commit)
            checksum.write_text(f"{'0' * 64}  {archive.name}\n")
            with self.assertRaisesRegex(release_install.InstallError, "external checksum"):
                release_install.stage_release(args)

            bad_root = root / "unexpected"
            bad_root.mkdir()
            bad_archive, bad_checksum = write_archive(
                bad_root, commit, extra_member="../escape"
            )
            bad_args = stage_args(bad_root, bad_archive, bad_checksum, commit)
            with self.assertRaisesRegex(release_install.InstallError, "unexpected members"):
                release_install.stage_release(bad_args)
            self.assertFalse((root / "escape").exists())

    def test_noncanonical_external_checksum_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commit = "a" * 40
            archive, checksum = write_archive(root, commit)
            checksum.write_text(
                f"{release_install.sha256_file(archive)}\t{archive.name}\n"
            )
            with self.assertRaisesRegex(release_install.InstallError, "exact archive"):
                release_install.stage_release(
                    stage_args(root, archive, checksum, commit)
                )

    def test_symlinked_install_lock_is_rejected_without_touching_target(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commit = "a" * 40
            archive, checksum = write_archive(root, commit)
            args = stage_args(root, archive, checksum, commit)
            install_root = Path(args.install_root)
            install_root.mkdir()
            target = root / "unrelated"
            target.write_text("unchanged")
            (install_root / ".release-install.lock").symlink_to(target)

            with self.assertRaisesRegex(release_install.InstallError, "install lock"):
                release_install.stage_release(args)
            self.assertEqual(target.read_text(), "unchanged")
            self.assertFalse((install_root / "current").exists())

    def test_concurrent_install_lock_fails_without_waiting(self):
        with tempfile.TemporaryDirectory() as temporary:
            Path(temporary).chmod(0o755)
            install_root, _ = release_install.ensure_install_root(
                Path(temporary) / "install"
            )
            with release_install.install_lock(install_root):
                with self.assertRaisesRegex(
                    release_install.InstallError, "operation is active"
                ):
                    with release_install.install_lock(install_root):
                        self.fail("concurrent lock unexpectedly succeeded")

    def test_provenance_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commit = "a" * 40
            archive, checksum = write_archive(root, commit, repository="other/repository")
            args = stage_args(root, archive, checksum, commit)
            with self.assertRaisesRegex(release_install.InstallError, "repository"):
                release_install.stage_release(args)
            self.assertFalse((Path(args.install_root) / "current").exists())

    def test_activate_and_explicit_rollback_swap_only_the_release_pointer(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first_commit = "a" * 40
            second_commit = "d" * 40
            first_dir = root / "first"
            second_dir = root / "second"
            first_dir.mkdir()
            second_dir.mkdir()
            first_archive, first_checksum = write_archive(first_dir, first_commit)
            second_archive, second_checksum = write_archive(second_dir, second_commit)
            first_args = stage_args(root, first_archive, first_checksum, first_commit)
            second_args = stage_args(root, second_archive, second_checksum, second_commit)
            sentinel = root / "operator-state"
            sentinel.write_text("must remain unchanged")

            with mock.patch.object(release_install, "verify_runtime"):
                first = release_install.stage_release(first_args)
                second = release_install.stage_release(second_args)
                release_install.select_release(
                    select_args(second_args, second["release_id"]), "activate"
                )
                current = Path(second_args.install_root) / "current"
                self.assertEqual(
                    os.readlink(current), f"releases/{second['release_id']}"
                )
                release_install.select_release(
                    select_args(first_args, first["release_id"]), "rollback"
                )

            self.assertEqual(
                os.readlink(Path(first_args.install_root) / "current"),
                f"releases/{first['release_id']}",
            )
            self.assertEqual(sentinel.read_text(), "must remain unchanged")
            self.assertEqual(
                Path(first_args.config).read_text(), Path(second_args.config).read_text()
            )

    def test_activation_rejects_files_and_manifest_rewritten_after_staging(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            commit = "a" * 40
            archive, checksum = write_archive(root, commit)
            args = stage_args(root, archive, checksum, commit)
            with mock.patch.object(release_install, "verify_runtime"):
                staged = release_install.stage_release(args)

            release_dir = Path(args.install_root) / "releases" / staged["release_id"]
            binary = release_dir / "hype-accumulator"
            binary.write_bytes(b"substituted-binary")
            binary.chmod(0o755)
            substituted_digest = release_install.sha256_file(binary)
            checksum_path = release_dir / "SHA256SUMS"
            checksum_lines = checksum_path.read_text().splitlines()
            checksum_path.write_text(
                "\n".join(
                    f"{substituted_digest}  hype-accumulator"
                    if line.endswith("  hype-accumulator")
                    else line
                    for line in checksum_lines
                )
                + "\n"
            )
            manifest_path = release_dir / release_install.INSTALL_MANIFEST
            manifest = json.loads(manifest_path.read_text())
            manifest["files"]["hype-accumulator"] = substituted_digest
            manifest_path.write_text(json.dumps(manifest))

            with mock.patch.object(release_install, "verify_runtime"):
                with self.assertRaisesRegex(release_install.InstallError, "source archive"):
                    release_install.select_release(
                        select_args(args, staged["release_id"]), "activate"
                    )
            self.assertFalse((Path(args.install_root) / "current").exists())

    def test_failed_activation_preflight_preserves_the_current_release(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first_commit = "a" * 40
            second_commit = "d" * 40
            first_dir = root / "first"
            second_dir = root / "second"
            first_dir.mkdir()
            second_dir.mkdir()
            first_archive, first_checksum = write_archive(first_dir, first_commit)
            second_archive, second_checksum = write_archive(second_dir, second_commit)
            first_args = stage_args(root, first_archive, first_checksum, first_commit)
            second_args = stage_args(root, second_archive, second_checksum, second_commit)
            with mock.patch.object(release_install, "verify_runtime"):
                first = release_install.stage_release(first_args)
                second = release_install.stage_release(second_args)
                release_install.select_release(
                    select_args(first_args, first["release_id"]), "activate"
                )
            current = Path(first_args.install_root) / "current"
            original_target = os.readlink(current)
            with mock.patch.object(
                release_install,
                "verify_runtime",
                side_effect=release_install.InstallError("unsafe config"),
            ):
                with self.assertRaisesRegex(release_install.InstallError, "unsafe config"):
                    release_install.select_release(
                        select_args(second_args, second["release_id"]), "activate"
                    )
            self.assertEqual(os.readlink(current), original_target)

    def test_runtime_subprocess_environment_excludes_signing_material(self):
        with mock.patch.dict(
            os.environ,
            {"HYPE_SIGNING_KEY": "must-not-propagate", "HYPE_ACCOUNT_ID": "private"},
        ):
            environment = release_install.safe_subprocess_environment()
        self.assertNotIn("HYPE_SIGNING_KEY", environment)
        self.assertNotIn("HYPE_ACCOUNT_ID", environment)


if __name__ == "__main__":
    unittest.main()
