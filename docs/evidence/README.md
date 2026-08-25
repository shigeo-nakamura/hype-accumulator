# Rollout evidence snapshots

This directory contains dated, append-only rollout evidence. Each JSON file
records a closed set of required gates with an explicit `PASS` or `FAIL` and
names every gate that blocks the aggregate result.

The companion SHA-256 file detects content changes. It is not an independent
signature: repository review and Git history are the trust boundary. Never
rewrite an existing dated snapshot to change a result. Add a new dated
snapshot after the underlying source state or verification evidence changes.

Stage 1 snapshots cover offline and deterministic replay evidence only. They
do not authorize release tags, network probes, funding, action submission,
deployment, restart, or live enablement.
