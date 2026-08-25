#!/usr/bin/env python3
"""Verify and atomically select immutable HYPE accumulator releases.

This tool has no AWS, network, secret, or service-lifecycle capability. GitHub
attestation verification and artifact transport must complete before `stage`.
Configuration and security-policy documents remain outside the release tree.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

ARCHIVE_FILES = {
    "AL2023-ABI.txt": (0o644, 1_048_576),
    "BUILD-PROVENANCE.txt": (0o644, 1_048_576),
    "SHA256SUMS": (0o644, 1_048_576),
    "hype-accumulator": (0o755, 134_217_728),
    "hype-status": (0o755, 134_217_728),
}
CHECKSUMMED_FILES = frozenset(ARCHIVE_FILES) - {"SHA256SUMS"}
DIGEST_RE = re.compile(r"[0-9a-f]{64}")
COMMIT_RE = re.compile(r"[0-9a-f]{40}")
RELEASE_ID_RE = re.compile(r"([0-9a-f]{40})-([0-9a-f]{64})")
BUILD_IMAGE_RE = re.compile(r"[^@\s]+@sha256:[0-9a-f]{64}")
REPOSITORY_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
CHECKSUM_LINE_RE = re.compile(r"([0-9a-f]{64})  ([A-Za-z0-9._-]+)")
INSTALL_MANIFEST = "INSTALL-MANIFEST.json"
SOURCE_ARCHIVE = "SOURCE-ARCHIVE.tar.gz"
PREFLIGHT_OUTPUT = "mode=dry-run halted install-ready"


class InstallError(RuntimeError):
    """The release failed a closed-world install invariant."""


@dataclass(frozen=True)
class ReleaseExpectations:
    repository: str
    commit: str
    target: str
    build_image: str
    cargo_lock_sha256: str

    def validate(self) -> None:
        if REPOSITORY_RE.fullmatch(self.repository) is None:
            raise InstallError("expected repository is not canonical")
        if COMMIT_RE.fullmatch(self.commit) is None:
            raise InstallError("expected commit must be 40 lowercase hexadecimal characters")
        if self.target != "aarch64-unknown-linux-gnu":
            raise InstallError("expected target must be aarch64-unknown-linux-gnu")
        if BUILD_IMAGE_RE.fullmatch(self.build_image) is None:
            raise InstallError("expected build image must include one canonical immutable digest")
        require_digest(self.cargo_lock_sha256, "expected Cargo.lock")


def require_digest(value: str, label: str) -> str:
    if DIGEST_RE.fullmatch(value) is None:
        raise InstallError(f"{label} digest must be 64 lowercase hexadecimal characters")
    return value


def sha256_file(path: Path) -> str:
    require_regular_file(path, f"hash input {path.name}")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1_048_576), b""):
            digest.update(block)
    return digest.hexdigest()


def require_regular_file(path: Path, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise InstallError(f"{label} must be a regular non-symlink file")


@contextmanager
def verified_archive_copy(
    source_path: Path, expected_digest: str
) -> Iterator[tuple[Path, str]]:
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        descriptor = os.open(source_path, flags)
    except OSError as error:
        raise InstallError(f"release archive cannot be safely opened: {error}") from error
    try:
        status = os.fstat(descriptor)
        if not stat.S_ISREG(status.st_mode):
            raise InstallError("release archive must be a regular non-symlink file")
        with tempfile.TemporaryDirectory(prefix="hype-archive-verify-") as temporary:
            private_archive = Path(temporary) / source_path.name
            digest = hashlib.sha256()
            with (
                os.fdopen(descriptor, "rb", closefd=False) as input_handle,
                private_archive.open("xb") as output_handle,
            ):
                for block in iter(lambda: input_handle.read(1_048_576), b""):
                    digest.update(block)
                    output_handle.write(block)
                output_handle.flush()
                os.fchmod(output_handle.fileno(), 0o444)
                os.fsync(output_handle.fileno())
            archive_digest = digest.hexdigest()
            if archive_digest != expected_digest:
                raise InstallError("release archive failed its external checksum")
            yield private_archive, archive_digest
    finally:
        os.close(descriptor)


def extract_closed_archive(archive_path: Path, destination: Path) -> None:
    require_regular_file(archive_path, "release archive")
    try:
        with tarfile.open(archive_path, mode="r:gz") as archive:
            members = archive.getmembers()
            names = [member.name for member in members]
            if len(names) != len(set(names)) or set(names) != set(ARCHIVE_FILES):
                raise InstallError(
                    "release archive has missing, duplicate, or unexpected members"
                )
            for member in members:
                expected_mode, maximum_size = ARCHIVE_FILES[member.name]
                if not member.isfile() or member.name != Path(member.name).name:
                    raise InstallError(
                        f"archive member {member.name!r} is not a top-level regular file"
                    )
                if member.mode & 0o777 != expected_mode:
                    raise InstallError(f"archive member {member.name!r} has an unsafe mode")
                if member.size < 0 or member.size > maximum_size:
                    raise InstallError(
                        f"archive member {member.name!r} exceeds its size bound"
                    )
                source = archive.extractfile(member)
                if source is None:
                    raise InstallError(f"archive member {member.name!r} cannot be read")
                output = destination / member.name
                with source, output.open("xb") as target:
                    shutil.copyfileobj(source, target, length=1_048_576)
                    target.flush()
                    os.fchmod(target.fileno(), expected_mode)
                    os.fsync(target.fileno())
    except InstallError:
        raise
    except (OSError, tarfile.TarError) as error:
        raise InstallError(f"release archive cannot be safely read: {error}") from error


def parse_inner_checksums(release_dir: Path) -> dict[str, str]:
    checksum_path = release_dir / "SHA256SUMS"
    require_regular_file(checksum_path, "internal checksum manifest")
    checksums: dict[str, str] = {}
    for line in checksum_path.read_text(encoding="utf-8").splitlines():
        match = CHECKSUM_LINE_RE.fullmatch(line)
        if match is None:
            raise InstallError("internal checksum manifest contains a malformed entry")
        digest, name = match.groups()
        if name != Path(name).name or name in checksums:
            raise InstallError("internal checksum manifest contains an unsafe or duplicate path")
        checksums[name] = digest
    if set(checksums) != CHECKSUMMED_FILES:
        raise InstallError("internal checksum manifest does not cover the exact release files")
    for name, expected in checksums.items():
        path = release_dir / name
        require_regular_file(path, f"release member {name}")
        if sha256_file(path) != expected:
            raise InstallError(f"release member {name!r} failed its internal checksum")
    return checksums


def exact_key(lines: Sequence[str], key: str) -> str:
    prefix = f"{key}="
    values = [line[len(prefix) :] for line in lines if line.startswith(prefix)]
    if len(values) != 1 or not values[0]:
        raise InstallError(f"provenance must contain exactly one nonempty {key}")
    return values[0]


def verify_provenance(release_dir: Path, expected: ReleaseExpectations) -> None:
    expected.validate()
    provenance_path = release_dir / "BUILD-PROVENANCE.txt"
    require_regular_file(provenance_path, "build provenance")
    lines = provenance_path.read_text(encoding="utf-8").splitlines()
    for key, value in [
        ("repository", expected.repository),
        ("commit", expected.commit),
        ("target", expected.target),
        ("build_image", expected.build_image),
    ]:
        if exact_key(lines, key) != value:
            raise InstallError(f"provenance {key} does not match the selected release")
    lock_lines = [line for line in lines if CHECKSUM_LINE_RE.fullmatch(line)]
    lock_lines = [line for line in lock_lines if line.endswith("  Cargo.lock")]
    if len(lock_lines) != 1:
        raise InstallError("provenance must contain exactly one Cargo.lock checksum")
    lock_digest = lock_lines[0].split()[0]
    if require_digest(lock_digest, "provenance Cargo.lock") != expected.cargo_lock_sha256:
        raise InstallError("provenance Cargo.lock checksum does not match")


def verify_recorded_abi(release_dir: Path, expected: ReleaseExpectations) -> None:
    abi_path = release_dir / "AL2023-ABI.txt"
    require_regular_file(abi_path, "AL2023 ABI report")
    report = abi_path.read_text(encoding="utf-8")
    required = [
        f"build_image={expected.build_image}",
        "system_release=Amazon Linux release 2023",
        "libc=glibc 2.34",
        f"target/al2023/{expected.target}/release/hype-accumulator",
        f"target/al2023/{expected.target}/release/hype-status",
    ]
    if any(value not in report for value in required) or "not found" in report.lower():
        raise InstallError("recorded AL2023 ABI evidence is incomplete or unresolved")


def safe_subprocess_environment() -> dict[str, str]:
    return {
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": "/usr/bin:/bin",
    }


def run_checked(command: Sequence[str], label: str) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
            env=safe_subprocess_environment(),
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise InstallError(f"{label} could not run: {error}") from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise InstallError(f"{label} failed: {detail}")
    return result


def verify_runtime(
    release_dir: Path,
    config_path: Path,
    security_policy_path: Path,
) -> None:
    if platform.machine().lower() not in {"aarch64", "arm64"}:
        raise InstallError("runtime ABI verification requires an ARM64 host")
    require_regular_file(config_path, "runtime config")
    require_regular_file(security_policy_path, "security policy")
    for name in ["hype-accumulator", "hype-status"]:
        binary = release_dir / name
        require_regular_file(binary, f"release binary {name}")
        if binary.stat().st_mode & 0o777 != 0o755:
            raise InstallError(f"release binary {name} does not have mode 0755")
        description = run_checked(["file", "--brief", str(binary)], f"file {name}").stdout
        if "ELF 64-bit" not in description or "ARM aarch64" not in description:
            raise InstallError(f"release binary {name} is not an ARM64 ELF")
        dependencies = run_checked(["ldd", str(binary)], f"ldd {name}").stdout
        if "not found" in dependencies.lower():
            raise InstallError(f"release binary {name} has an unresolved shared library")
    preflight = run_checked(
        [
            str(release_dir / "hype-accumulator"),
            "--install-preflight",
            str(config_path),
            str(security_policy_path),
        ],
        "offline install config preflight",
    )
    if preflight.stdout.strip() != PREFLIGHT_OUTPUT or preflight.stderr.strip():
        raise InstallError("offline install config preflight returned an unexpected result")


def fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise InstallError(f"directory cannot be safely opened for sync: {error}") from error
    try:
        os.fsync(descriptor)
    except OSError as error:
        raise InstallError(f"directory metadata cannot be synced: {error}") from error
    finally:
        os.close(descriptor)


def make_staged_release_traversable(path: Path) -> None:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise InstallError(f"staged release cannot be safely opened: {error}") from error
    try:
        status = os.fstat(descriptor)
        if status.st_uid != os.geteuid() or status.st_mode & 0o7777 != 0o700:
            raise InstallError(
                "private staged release directory has unsafe ownership or mode"
            )
        os.fchmod(descriptor, 0o755)
        status = os.fstat(descriptor)
        if status.st_uid != os.geteuid() or status.st_mode & 0o7777 != 0o755:
            raise InstallError(
                "published staged release directory has unsafe ownership or mode"
            )
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def ensure_owned_traversable_directory(path: Path, label: str) -> Path:
    if path.is_symlink():
        raise InstallError(f"{label} must not be a symlink")
    created = False
    try:
        path.mkdir(mode=0o755)
        created = True
    except FileExistsError:
        pass
    except OSError as error:
        raise InstallError(f"{label} cannot be created: {error}") from error

    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise InstallError(f"{label} cannot be safely opened: {error}") from error
    try:
        if created:
            os.fchmod(descriptor, 0o755)
        status = os.fstat(descriptor)
        if status.st_uid != os.geteuid() or status.st_mode & 0o7777 != 0o755:
            raise InstallError(
                f"{label} must be owner-controlled with exact mode 0755"
            )
        if created:
            os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if created:
        fsync_directory(path.parent)
    return path.resolve(strict=True)


def ensure_install_root(install_root: Path) -> tuple[Path, Path]:
    if not install_root.is_absolute():
        raise InstallError("install root must be an absolute canonical path")
    requested_root = install_root.absolute()
    if requested_root != install_root.resolve(strict=False):
        raise InstallError("install root path must not contain aliases or symlink components")
    for ancestor in install_root.parents:
        try:
            ancestor_status = ancestor.stat()
        except OSError as error:
            raise InstallError(
                f"install root parent cannot be inspected: {ancestor}: {error}"
            ) from error
        if not stat.S_ISDIR(ancestor_status.st_mode) or not (
            ancestor_status.st_mode & stat.S_IXOTH
        ):
            raise InstallError(
                f"install root parent must be traversable by the runtime identity: {ancestor}"
            )
        if ancestor_status.st_uid not in {0, os.geteuid()}:
            raise InstallError(
                f"install root parent must be owned by root or the deployment identity: {ancestor}"
            )
        if ancestor_status.st_mode & (stat.S_IWGRP | stat.S_IWOTH) and not (
            ancestor_status.st_mode & stat.S_ISVTX
        ):
            raise InstallError(
                f"install root parent is writable without sticky-bit rename protection: {ancestor}"
            )
    install_root = ensure_owned_traversable_directory(install_root, "install root")
    releases = install_root / "releases"
    releases = ensure_owned_traversable_directory(releases, "releases directory")
    return install_root, releases


@contextmanager
def install_lock(install_root: Path) -> Iterator[None]:
    lock_path = install_root / ".release-install.lock"
    flags = os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW
    created = False
    try:
        descriptor = os.open(lock_path, flags | os.O_CREAT | os.O_EXCL, 0o600)
        created = True
    except FileExistsError:
        try:
            descriptor = os.open(lock_path, flags)
        except OSError as error:
            raise InstallError(f"install lock cannot be safely opened: {error}") from error
    except OSError as error:
        raise InstallError(f"install lock cannot be safely opened: {error}") from error
    try:
        if created:
            os.fchmod(descriptor, 0o600)
        status = os.fstat(descriptor)
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.geteuid()
            or status.st_nlink != 1
            or status.st_mode & 0o777 != 0o600
        ):
            raise InstallError("install lock has unsafe ownership, links, or mode")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise InstallError("another release install operation is active") from error
        yield
    finally:
        os.close(descriptor)


def install_manifest(
    expected: ReleaseExpectations,
    archive_digest: str,
    checksums: dict[str, str],
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "repository": expected.repository,
        "commit": expected.commit,
        "target": expected.target,
        "build_image": expected.build_image,
        "cargo_lock_sha256": expected.cargo_lock_sha256,
        "archive_sha256": archive_digest,
        "files": dict(sorted(checksums.items())),
    }


def write_manifest(path: Path, manifest: dict[str, object]) -> None:
    with path.open("x", encoding="utf-8") as handle:
        json.dump(manifest, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fchmod(handle.fileno(), 0o644)
        os.fsync(handle.fileno())


def read_manifest(release_dir: Path) -> dict[str, object]:
    path = release_dir / INSTALL_MANIFEST
    require_regular_file(path, "install manifest")
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise InstallError("install manifest is malformed") from error
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise InstallError("install manifest schema is unsupported")
    return manifest


def expectations_from_manifest(manifest: dict[str, object]) -> ReleaseExpectations:
    values = []
    for key in ["repository", "commit", "target", "build_image", "cargo_lock_sha256"]:
        value = manifest.get(key)
        if not isinstance(value, str):
            raise InstallError(f"install manifest {key} is invalid")
        values.append(value)
    expected = ReleaseExpectations(*values)
    expected.validate()
    return expected


def require_installed_member(path: Path, mode: int) -> None:
    require_regular_file(path, f"installed member {path.name}")
    status = path.stat()
    if status.st_uid != os.geteuid() or status.st_nlink != 1:
        raise InstallError(f"installed member {path.name} has unsafe ownership or links")
    if status.st_mode & 0o777 != mode:
        raise InstallError(f"installed member {path.name} has unsafe mode")


def verify_staged_release(release_dir: Path, manifest: dict[str, object]) -> None:
    if release_dir.is_symlink() or not release_dir.is_dir():
        raise InstallError("staged release is not a regular directory")
    release_status = release_dir.stat()
    if (
        release_status.st_uid != os.geteuid()
        or release_status.st_mode & 0o7777 != 0o755
    ):
        raise InstallError("staged release directory has unsafe ownership or mode")
    expected_names = set(ARCHIVE_FILES) | {INSTALL_MANIFEST, SOURCE_ARCHIVE}
    if {path.name for path in release_dir.iterdir()} != expected_names:
        raise InstallError("staged release contains missing or unexpected files")
    for name, (mode, _) in ARCHIVE_FILES.items():
        require_installed_member(release_dir / name, mode)
    require_installed_member(release_dir / INSTALL_MANIFEST, 0o644)
    require_installed_member(release_dir / SOURCE_ARCHIVE, 0o444)
    files = manifest.get("files")
    if not isinstance(files, dict) or set(files) != CHECKSUMMED_FILES:
        raise InstallError("install manifest file set is invalid")
    archive_digest = manifest.get("archive_sha256")
    if not isinstance(archive_digest, str):
        raise InstallError("install manifest archive checksum is invalid")
    require_digest(archive_digest, "installed archive")
    source_archive = release_dir / SOURCE_ARCHIVE
    if sha256_file(source_archive) != archive_digest:
        raise InstallError("retained source archive does not match its release ID")

    expected = expectations_from_manifest(manifest)
    with tempfile.TemporaryDirectory(prefix="hype-source-verify-") as temporary:
        source_release = Path(temporary)
        extract_closed_archive(source_archive, source_release)
        source_checksums = parse_inner_checksums(source_release)
        verify_provenance(source_release, expected)
        verify_recorded_abi(source_release, expected)
        if source_checksums != files:
            raise InstallError("install manifest is not bound to the retained source archive")
        if sha256_file(source_release / "SHA256SUMS") != sha256_file(
            release_dir / "SHA256SUMS"
        ):
            raise InstallError("installed checksum manifest changed from the source archive")

    for name, digest in files.items():
        if not isinstance(name, str) or not isinstance(digest, str):
            raise InstallError("install manifest checksum entry is invalid")
        require_digest(digest, f"installed {name}")
        if sha256_file(release_dir / name) != digest:
            raise InstallError(f"installed release member {name!r} changed")
    if parse_inner_checksums(release_dir) != files:
        raise InstallError("installed checksums do not match the install manifest")


def retain_source_archive(source: Path, destination: Path) -> None:
    require_regular_file(source, "release archive")
    with source.open("rb") as input_handle, destination.open("xb") as output_handle:
        shutil.copyfileobj(input_handle, output_handle, length=1_048_576)
        output_handle.flush()
        os.fchmod(output_handle.fileno(), 0o444)
        os.fsync(output_handle.fileno())


def stage_release(args: argparse.Namespace) -> dict[str, str]:
    expected = ReleaseExpectations(
        repository=args.expected_repository,
        commit=args.expected_commit,
        target=args.expected_target,
        build_image=args.expected_build_image,
        cargo_lock_sha256=args.expected_cargo_lock_sha256,
    )
    expected.validate()
    archive_path = Path(args.archive)
    expected_name = f"hype-accumulator-{expected.commit}-{expected.target}.tar.gz"
    if archive_path.name != expected_name:
        raise InstallError("release archive name does not match commit and target")
    expected_archive_digest = require_digest(
        args.expected_archive_sha256, "expected release archive"
    )

    with verified_archive_copy(
        archive_path, expected_archive_digest
    ) as (verified_archive, archive_digest):
        install_root, releases = ensure_install_root(Path(args.install_root))
        release_id = f"{expected.commit}-{archive_digest}"
        with install_lock(install_root):
            return stage_release_locked(
                args,
                expected,
                verified_archive,
                archive_digest,
                install_root,
                releases,
                release_id,
            )


def stage_release_locked(
    args: argparse.Namespace,
    expected: ReleaseExpectations,
    archive_path: Path,
    archive_digest: str,
    install_root: Path,
    releases: Path,
    release_id: str,
) -> dict[str, str]:
    final_release = releases / release_id
    with tempfile.TemporaryDirectory(prefix=".stage-", dir=releases) as temporary:
        temporary_release = Path(temporary)
        extract_closed_archive(archive_path, temporary_release)
        checksums = parse_inner_checksums(temporary_release)
        verify_provenance(temporary_release, expected)
        verify_recorded_abi(temporary_release, expected)
        verify_runtime(
            temporary_release,
            Path(args.config),
            Path(args.security_policy),
        )
        retain_source_archive(archive_path, temporary_release / SOURCE_ARCHIVE)
        manifest = install_manifest(expected, archive_digest, checksums)
        write_manifest(temporary_release / INSTALL_MANIFEST, manifest)
        fsync_directory(temporary_release)
        make_staged_release_traversable(temporary_release)
        verify_staged_release(temporary_release, manifest)
        if final_release.exists():
            if read_manifest(final_release) != manifest:
                raise InstallError("immutable release ID already exists with different metadata")
            verify_staged_release(final_release, manifest)
            verify_runtime(final_release, Path(args.config), Path(args.security_policy))
        else:
            os.replace(temporary_release, final_release)
            fsync_directory(releases)
    return {
        "action": "stage",
        "install_root": str(install_root),
        "release_id": release_id,
        "archive_sha256": archive_digest,
    }


def select_release(args: argparse.Namespace, action: str) -> dict[str, str]:
    match = RELEASE_ID_RE.fullmatch(args.release_id)
    if match is None:
        raise InstallError("release ID must bind a full commit and archive checksum")
    install_root, releases = ensure_install_root(Path(args.install_root))
    with install_lock(install_root):
        return select_release_locked(args, action, match, install_root, releases)


def select_release_locked(
    args: argparse.Namespace,
    action: str,
    match: re.Match[str],
    install_root: Path,
    releases: Path,
) -> dict[str, str]:
    release_dir = releases / args.release_id
    manifest = read_manifest(release_dir)
    if manifest.get("commit") != match.group(1) or manifest.get("archive_sha256") != match.group(2):
        raise InstallError("release ID does not match its immutable install manifest")
    verify_staged_release(release_dir, manifest)
    verify_runtime(release_dir, Path(args.config), Path(args.security_policy))

    current = install_root / "current"
    if current.exists() and not current.is_symlink():
        raise InstallError("current release selector must be absent or a symlink")
    relative_target = Path("releases") / args.release_id
    temporary_link = install_root / f".current-{os.getpid()}-{uuid.uuid4().hex}"
    os.symlink(relative_target, temporary_link)
    try:
        os.replace(temporary_link, current)
        fsync_directory(install_root)
    finally:
        temporary_link.unlink(missing_ok=True)
    if not current.is_symlink() or os.readlink(current) != str(relative_target):
        raise InstallError("atomic release selection did not persist")
    return {
        "action": action,
        "install_root": str(install_root),
        "release_id": args.release_id,
        "archive_sha256": match.group(2),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    stage = subparsers.add_parser("stage", help="verify and stage one immutable release")
    stage.add_argument("--archive", required=True)
    stage.add_argument("--expected-archive-sha256", required=True)
    stage.add_argument("--expected-repository", required=True)
    stage.add_argument("--expected-commit", required=True)
    stage.add_argument("--expected-target", default="aarch64-unknown-linux-gnu")
    stage.add_argument("--expected-build-image", required=True)
    stage.add_argument("--expected-cargo-lock-sha256", required=True)
    stage.add_argument("--config", required=True)
    stage.add_argument("--security-policy", required=True)
    stage.add_argument("--install-root", required=True)

    for command in ["activate", "rollback"]:
        selector = subparsers.add_parser(
            command,
            help=f"atomically {command} to an explicitly verified release",
        )
        selector.add_argument("--release-id", required=True)
        selector.add_argument("--config", required=True)
        selector.add_argument("--security-policy", required=True)
        selector.add_argument("--install-root", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "stage":
            result = stage_release(args)
        else:
            result = select_release(args, args.command)
    except (InstallError, OSError, UnicodeError) as error:
        print(f"release install rejected: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
