# Durable-ledger backup and clean-restore drill

The backup CLI creates a point-in-time, checksummed copy of the append-only
ledger without constructing a signer, exchange client, order, staking action,
or network request. It produces two deliberately separate outputs:

- a private payload directory containing `ledger.jsonl`, `snapshot.json`, an
  empty `.ledger.lock`, `manifest.json`, and `manifest.json.sha256`;
- a private protected-head export bound to the manifest's backup ID.

The manifest records the exact size and SHA-256 digest of each payload file and
the protected-head export. Creation checkpoints the source, stages both
outputs with Unix mode `0700`/`0600`, replays the staged ledger against the
exported protected head, and only then publishes each new output through an
atomic no-replace bundle rename or anchor link. The two separate paths cannot
form one filesystem transaction; an interrupted publication leaves an existing
output for operator inspection and never overwrites it on a retry.
An existing output, unsafe link, aliased path, overlapping boundary, changed
file, stale anchor, or unexpected bundle entry fails closed.

The companion manifest checksum detects accidental damage but is not an
independent signature. An actor able to replace the payload, manifest, and
protected-head export could manufacture a different internally consistent
backup. Preserve the payload bundle and protected-head export in independently
versioned storage and IAM boundaries. Separate names under one principal with
equal delete permissions do not provide rollback protection.

## Create and verify a local backup

All four paths must be normal absolute paths, their parents must already exist
without symlink aliases, and no path may contain another. Output names must not
exist; use a new immutable backup ID or timestamp for each run.

```text
hype-accumulator --ledger-backup-create \
  <absolute-ledger-directory> \
  <absolute-source-protected-anchor> \
  <absolute-new-bundle-directory> \
  <absolute-new-anchor-export>

hype-accumulator --ledger-backup-verify \
  <absolute-new-bundle-directory> \
  <absolute-new-anchor-export>
```

Both commands print the backup ID, record count, and ledger head hash. Record
those values in private operator evidence; do not record wallet addresses,
credentials, ciphertext, action IDs, or production topology in repository
files or public issue comments. The source may continue appending after the
checkpoint. That does not invalidate the captured backup, which remains bound
to its exported point-in-time head.

## Transfer to versioned off-host storage

Transfer remains an operator/deployment gate and is intentionally absent from
the Rust CLI. `scripts/ledger_backup_transfer.py` provides an explicit AWS CLI
transport contract, but running its `upload` command against production AWS is
itself an approval-gated operation. It requires:

- two different S3 buckets so the payload and protected anchor can have
  separate IAM/delete boundaries;
- versioning in the `Enabled` state on both buckets and permission to inspect
  the complete object-version/delete-marker history;
- expected 12-digit bucket-owner IDs and explicit full KMS key ARNs;
- immutable conditional puts (`If-None-Match: *`), S3 SHA-256 checksums, KMS
  encryption, and a read-after-write check;
- a private, no-replace receipt containing the exact version ID, checksum,
  size, ETag, key, bucket owner, and returned KMS key ID for all six objects.

The receipt is written and fsynced through a mode-0600 sibling temporary file,
then atomically moved into place with Linux `renameat2(RENAME_NOREPLACE)`. A
pre-publication crash or I/O failure therefore cannot reserve the final receipt
path with partial or multiply-linked content. An unsupported filesystem fails
closed without publishing the final path.

Object Lock is preferred where fleet policy supports it. The runtime principal
must not have delete access to both boundaries. A single bucket with different
prefixes is rejected because it does not prove an independent protected-anchor
boundary.

After separately approving the AWS target and action, upload with:

```text
python3 scripts/ledger_backup_transfer.py \
  --aws-bin <absolute-canonical-aws-cli-binary> \
  --region <region> \
  upload \
  --bundle <absolute-new-bundle-directory> \
  --anchor <absolute-new-anchor-export> \
  --receipt <absolute-new-private-receipt.json> \
  --verifier <absolute-hype-accumulator-binary> \
  --payload-bucket <versioned-payload-bucket> \
  --payload-owner <12-digit-account-id> \
  --payload-kms-key <full-payload-kms-key-arn> \
  --anchor-bucket <separately-protected-anchor-bucket> \
  --anchor-owner <12-digit-account-id> \
  --anchor-kms-key <full-anchor-kms-key-arn> \
  --staging-root <absolute-private-capacity-checked-directory>
```

The tool first captures the private source files, then runs the Rust full-replay
verifier and uploads only those exact captured bytes. Object keys are derived
from the verified backup ID. A retry accepts a pre-existing object only when it
has exactly one version, no delete marker, the configured KMS key ARN, and exact
backup metadata, size, SHA-256, and content. Any deletion or replacement
history fails closed instead of creating another version. Keep the receipt
outside the repository and operator logs. It contains infrastructure
identifiers, but never wallet addresses, credentials, ciphertext, or signed
payloads. Successful upload stdout contains only the backup ID and receipt
path; the full infrastructure identifiers remain exclusively in the private
receipt.

`--staging-root` is optional, but should point to a mode-0700 operator-owned
directory on capacity-checked storage when the bundle might not fit in the
system temporary filesystem. It must not be the source bundle or a directory
inside that bundle. Both the full capture and bounded multipart scratch files
remain under the per-run capture root, which is removed after the operation.
AWS CLI and verifier binaries must also be beneath root- or operator-owned
ancestor directories that are not group/world writable.

Full replay and S3 `put-object`/`get-object` transfers have no wall-clock
timeout by default, so backup size or recovery-host bandwidth alone cannot
invalidate a correct backup. Control-plane calls retain a 120-second timeout.
An operator may add `--verifier-timeout-seconds`,
`--transfer-timeout-seconds`, or `--control-timeout-seconds` before the
subcommand when an environment requires explicit positive bounds.

Objects up to the conservative single-request limit use `PutObject`. Larger
ledger or snapshot files use a bounded, private-part multipart upload with
per-part SHA-256, S3's SHA-256 composite checksum, KMS encryption, and
conditional `CompleteMultipartUpload`. The direct full-object SHA-256 remains
bound in immutable object metadata and the receipt; the receipt's S3 checksum
preserves the returned multipart `-N` part-count suffix. At most 10,000
dynamically sized parts are used.
Existing incomplete multipart uploads, delete markers, or prior versions fail
closed; an interrupted upload must be inspected/aborted before retry.

## Clean-directory restore drill

Download every exact version from the private receipt into a new local root:

```text
python3 scripts/ledger_backup_transfer.py \
  --aws-bin <absolute-canonical-aws-cli-binary> \
  --region <region> \
  download \
  --receipt <absolute-private-receipt.json> \
  --destination-root <absolute-new-download-root> \
  --verifier <absolute-hype-accumulator-binary>
```

The destination must not exist. The tool reserves it with mode `0700`, requests
every recorded object version explicitly, checks the S3 and local SHA-256
values, and runs the Rust full-replay verifier before returning paths. A
failure leaves the private directory for operator inspection and never
publishes it over another destination.

Restore only into a missing or ledger-clean destination and a distinct,
previously unused protected-anchor scope:

```text
hype-accumulator --ledger-backup-restore \
  <absolute-downloaded-bundle-directory> \
  <absolute-downloaded-anchor-export> \
  <absolute-clean-restore-directory> \
  <absolute-clean-restore-protected-anchor>
```

Restore verifies every digest, the backup-ID/anchor binding, the hash chain,
snapshot equality, and full ledger replay before it creates destination state.
It preserves the exact verified payload bytes in a private temporary source,
then uses the durable ledger's clean-restore transaction and reopens the result
against the new protected anchor. Never restore over the active runtime
directory or active protected anchor. Replacing active state, installing the
restored copy, starting/restarting a service, or changing a deployment remains
a separate explicit approval gate.

For a repository-only rehearsal, run:

```text
cargo test --test backup --locked
python3 -m unittest tests.test_ledger_backup_transfer -v
```

The test covers exact round-trip restore, permissions, payload tampering,
anchor substitution, unexpected entries, overlapping paths, symbolic links,
and multiple hard links. The Python tests use an in-memory fake AWS client to
cover immutable retry, separate boundaries, versioning, exact-version download,
damage detection, and no-replace destinations. Passing these tests demonstrates
the local and transport contracts; it does not claim that an off-host upload,
retention policy, IAM boundary, or production restore drill has occurred.
