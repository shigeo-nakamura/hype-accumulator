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

Transfer is an operator/deployment gate and is intentionally absent from this
CLI. Use the existing fleet-approved S3 encryption, retention, and audit
policy. Upload the bundle contents under one immutable payload prefix and the
protected-head export to a separately protected bucket or IAM boundary. Enable
bucket versioning and retain both returned object version IDs. Object Lock is
preferred where the fleet policy supports it.

A schematic operator sequence is:

```text
aws s3 cp <absolute-new-bundle-directory>/ \
  s3://<versioned-payload-bucket>/<immutable-backup-id>/ \
  --recursive --no-follow-symlinks

aws s3 cp <absolute-new-anchor-export> \
  s3://<separately-protected-anchor-bucket>/<immutable-backup-id>.json
```

Do not upload the protected-head export into the payload prefix. Do not reuse
an object key, overwrite a prior version intentionally, or grant the runtime
principal delete access to both boundaries.

## Clean-directory restore drill

Download one payload version and its exact protected-head export version into
fresh local paths. The downloaded bundle directory must contain exactly the
five documented files. Verify it before selecting any restore target:

```text
hype-accumulator --ledger-backup-verify \
  <absolute-downloaded-bundle-directory> \
  <absolute-downloaded-anchor-export>
```

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
```

The test covers exact round-trip restore, permissions, payload tampering,
anchor substitution, unexpected entries, overlapping paths, symbolic links,
and multiple hard links. Passing it demonstrates the local backup contract; it
does not claim that an off-host upload, retention policy, or production restore
drill has occurred.
