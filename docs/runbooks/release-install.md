# Offline release installation contract

This contract prepares an immutable AL2023 ARM64 release without adding an AWS,
network, secret, signer, or service-lifecycle capability. It does not deploy a
release by itself. Operational host IDs, filesystem paths, account identities,
and role names belong in a protected environment, not this repository.

## Safety boundary

The release flow is deliberately split into three explicit commands:

1. `stage` verifies and copies an immutable release. It never changes `current`.
2. `activate` atomically selects one already staged full commit plus archive
   checksum. It never starts, stops, enables, reloads, or restarts a service.
3. `rollback` atomically selects an explicitly named older verified release. It
   never rolls back configuration, credentials, ledger state, protected anchors,
   or backups.

Both selection commands reverify the retained source archive against the
content-addressed release ID, compare the installed files with that archive,
and rerun binary, ABI, checksum, and halted-configuration preflight before
changing the symlink. The preflight subprocess receives a fixed system `PATH`
and locale only; account and signing variables are not inherited.
Runtime configuration and the typed security policy must remain outside the
release tree.

`stage` is allowed only after an operator or protected workflow has independently:

- selected a successful `master` CI run and its exact 40-character commit;
- downloaded the commit-SHA-named ARM64 artifact without substituting a mutable
  branch or object key;
- verified GitHub provenance for the archive against this repository;
- recorded the published outer archive checksum, exact pinned build-image digest,
  target, and `Cargo.lock` SHA-256 from that same revision;
- provided a runtime config and security policy containing no credentials and
  accepted by the binary's `--install-preflight` mode.

Attestation verification stays outside the installer because it depends on the
trusted GitHub identity and transport context. A checksum downloaded from the
same unverified source is not a substitute for attestation.

## Stage

Use protected values for every placeholder:

```text
python3 scripts/release_install.py stage \
  --archive <download-directory>/hype-accumulator-<commit>-aarch64-unknown-linux-gnu.tar.gz \
  --expected-archive-sha256 <independently-recorded-full-digest> \
  --expected-repository shigeo-nakamura/hype-accumulator \
  --expected-commit <full-commit> \
  --expected-target aarch64-unknown-linux-gnu \
  --expected-build-image <name-at-sha256-digest> \
  --expected-cargo-lock-sha256 <full-digest> \
  --config <protected-runtime-config> \
  --security-policy <protected-security-policy> \
  --install-root <dedicated-install-root>
```

The expected archive digest must come from the protected operator record or
workflow input, not from a checksum path in the archive's writable download
directory. The archive is a closed set of five top-level regular files. `stage` rejects
path traversal, links, duplicates, unexpected files, unsafe modes, size excess,
outer or inner checksum mismatch, noncanonical commit/digest input, provenance
mismatch, unresolved shared libraries, a non-ARM64 host/binary, or a non-AL2023
build record. The binary must additionally return exactly:

```text
mode=dry-run halted install-ready
```

The verified source archive is retained read-only in the content-addressed
release directory so later activation cannot trust a rewritten install manifest
or substituted binary. The dedicated install root, `releases`, and each release
directory are exactly `0755`: non-writable by the service identity while
remaining traversable when deployment and runtime use different UIDs. Existing
ancestors of the dedicated install root must also permit traversal by other UIDs
and be owned by root or the deployment identity. Any group- or world-writable
ancestor must use sticky-bit rename protection, as `/tmp` does; otherwise the
installer rejects the path. It validates but never changes those external
directories. Installed file ownership, link count, and modes are checked again
before every selection. A non-symlink, single-link local lock serializes stage
and selection commands and rejects a concurrent invocation without waiting
indefinitely.

That result requires an explicit typed security policy plus `dry_run=true`,
`manual_halt=true`, and `live_approved=false`. A successful JSON result contains
the only valid release ID: `<full-commit>-<full-archive-sha256>`.

## Activate and rollback

Activation requires the full release ID rather than a mutable name:

```text
python3 scripts/release_install.py activate \
  --release-id <full-commit>-<full-archive-sha256> \
  --config <protected-runtime-config> \
  --security-policy <protected-security-policy> \
  --install-root <dedicated-install-root>
```

Rollback is the same fail-closed verification directed at a previously staged
release:

```text
python3 scripts/release_install.py rollback \
  --release-id <older-full-commit>-<older-full-archive-sha256> \
  --config <protected-runtime-config> \
  --security-policy <protected-security-policy> \
  --install-root <dedicated-install-root>
```

Orchestration around these commands must record the dedicated HYPE unit's
active/enabled state before and after and require exact invariance. It must also
verify that unrelated units were untouched. This repository tool intentionally
has no service-manager command, so artifact selection cannot silently become a
service start or restart.

No command here authorizes host access, artifact upload, IAM/KMS mutation,
configuration or ciphertext installation, service start, DRY_RUN deployment,
funding, staking, signed actions, or live enablement. Those remain separate
approval and evidence gates.
