//! Optional S3 mirror for the published dashboard status document
//! (bot-strategy#343 / #908).
//!
//! When `STATUS_S3_BUCKET` and `STATUS_S3_KEY_PREFIX` are both set, each
//! status writer awaits one `PutObject` for the just-written `status.json`
//! after the local atomic write succeeds. Unlike pairtrade's long-lived
//! process (`s3_mirror.rs`), every hype-accumulator entry point that writes
//! a status document (`hype-status`, `--dry-run-cycle`) is a one-shot CLI
//! invocation that exits immediately after its cycle completes — a
//! fire-and-forget spawned task would very likely be killed by tokio
//! runtime shutdown before the PUT lands. This mirror is awaited directly
//! instead; its failure is logged and never fails the invocation, matching
//! the "does not block the local write" durability model.

use rusoto_core::Region;
use rusoto_s3::{PutObjectRequest, S3Client, S3};
use std::{env, time::Duration};
use thiserror::Error;
use tokio::time::timeout;

// Bucket region hard-coded to `eu-central-1` in `put` below (via
// `Region::EuCentral1`) to match the `debot-dashboard` bucket, which is
// single-region there; Tokyo bots cross-region write the same way
// pairtrade's mirror does.
//
// Uses `rusoto_s3`, not `aws-sdk-s3`: the current AWS SDK for Rust requires
// a newer rustc than this crate's pinned `1.85.1` toolchain, while rusoto
// is already in the dependency graph via `debot-utils`' KMS decrypt path
// and builds against it without issue.

/// Upper bound on one mirror PUT. See the doc comment on `put` for why
/// this exists even after the caller has released its own locks.
///
/// **Ordering** (bot-strategy#920) does not come from this bound. The
/// pre-PUT phase of a cycle — observation, movement history, persistence —
/// has no bound of its own, so the interval between two PUT *starts* is
/// not the interval between two timer ticks. It comes from the deployment
/// shape:
///
/// - Only one unit mirrors, so a key has a single writer rather than two
///   racing ones: `hype-accumulator-dryrun.service` carries the
///   `STATUS_S3_*` environment and the observer unit does not.
/// - A timer that elapses while the unit it triggers is still active
///   leaves that unit running rather than starting a second instance
///   (systemd.timer(5)). Cycle N+1 therefore cannot begin until cycle N's
///   process has exited, which is after N's PUT completed or was
///   abandoned. Two of this writer's PUTs are never in flight together,
///   however long N's pre-PUT phase took.
///
/// **What this bound does buy** is a limit on how far a stalled mirror
/// propagates. Because systemd skips rather than queues, a cycle still
/// running at a tick costs that tick outright — releasing the
/// state-directory lock before the PUT protects other lock takers, not
/// this unit's own next tick. Keeping one attempt far below the schedule
/// means a stall can only ever cost the single tick it overlaps: the unit
/// exits within `PUT_TIMEOUT` of its pre-PUT work finishing, so it is
/// always inactive again well before the following tick.
/// `mirror_stall_cannot_cost_more_than_one_cycle` pins that inequality.
///
/// Neither half of the ordering argument is observable from inside this
/// process, so both are written down in
/// `docs/runbooks/signer-free-runtime.md`. Giving a second unit the
/// `STATUS_S3_*` environment, mirroring from a long-lived process that can
/// overlap its own writes, or adding retries that outlive a cycle each
/// reopen the race and need an actual ordering mechanism — a mirror-scoped
/// lock, or a conditional write — not a different constant here.
const PUT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum StatusS3MirrorError {
    #[error("S3 put_object failed: {0}")]
    PutObject(String),
    #[error("S3 put_object timed out after {0:?}")]
    Timeout(Duration),
}

/// A configured mirror target, read once from the environment.
pub struct StatusS3Mirror {
    bucket: String,
    /// Trailing-slash-free prefix, e.g. `debot/status/hype-accumulator`.
    key_prefix: String,
}

impl StatusS3Mirror {
    /// Reads `STATUS_S3_BUCKET` / `STATUS_S3_KEY_PREFIX`. Returns `None`
    /// when either is unset, empty, or whitespace-only, so a deployment
    /// without the mirror feature configured pays no cost and never
    /// attempts a network call.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let bucket = env::var("STATUS_S3_BUCKET")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())?;
        let key_prefix = env::var("STATUS_S3_KEY_PREFIX")
            .ok()
            .map(|value| value.trim().trim_end_matches('/').to_owned())
            .filter(|value| !value.is_empty())?;
        Some(Self { bucket, key_prefix })
    }

    /// Uploads `body` to `<key_prefix>/<file_name>` with a short
    /// `Cache-Control` (the dashboard already polls every
    /// `poll_interval_secs`, so nothing benefits from a longer-lived CDN
    /// cache) and `Content-Type: application/json`.
    ///
    /// `file_name` must not include a leading slash.
    ///
    /// # Errors
    ///
    /// Returns [`StatusS3MirrorError::PutObject`] when the request fails.
    /// The caller decides whether that should be fatal; every current
    /// caller logs and continues, since the local atomic write already
    /// succeeded and remains the source of truth.
    pub async fn put(&self, file_name: &str, body: String) -> Result<(), StatusS3MirrorError> {
        let client = S3Client::new(Region::EuCentral1);
        let key = format!("{}/{file_name}", self.key_prefix);
        // Bounded even though the caller already releases any exclusive
        // lock before awaiting this: rusoto's default HTTP client has no
        // guaranteed short timeout of its own, and every caller here is a
        // one-shot CLI invocation that should fail fast on an unresponsive
        // S3 endpoint rather than idle past its systemd unit's own
        // TimeoutStartSec.
        let request = client.put_object(PutObjectRequest {
            bucket: self.bucket.clone(),
            key,
            cache_control: Some("max-age=2".to_owned()),
            content_type: Some("application/json".to_owned()),
            body: Some(body.into_bytes().into()),
            ..Default::default()
        });
        timeout(PUT_TIMEOUT, request)
            .await
            .map_err(|_| StatusS3MirrorError::Timeout(PUT_TIMEOUT))?
            .map_err(|error| StatusS3MirrorError::PutObject(error.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// The shortest scheduling interval any deployment may run a status
    /// writer at. Matches the deployed `OnCalendar=*-*-* *:0/5:00` timer.
    const MIN_SUPPORTED_CYCLE_INTERVAL: Duration = Duration::from_secs(300);

    /// How much shorter than a cycle one PUT attempt must stay. A PUT
    /// consuming a meaningful fraction of the interval would eat into the
    /// budget the next cycle needs.
    const MIN_CYCLE_INTERVAL_SAFETY_FACTOR: u64 = 10;

    // Env var access must serialize: `from_env` reads process env vars,
    // and parallel test execution can otherwise see each other's
    // mutations. Recover from poisoning so a panic in one test does not
    // cascade-fail the rest.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        match ENV_LOCK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// A mirror stall must not be able to cost more than the single cycle
    /// it overlaps. systemd skips a tick whose unit is still active rather
    /// than queueing it, so a cycle that is still awaiting its PUT when a
    /// tick arrives loses that tick outright; keeping one attempt far
    /// below the schedule means the unit is always inactive again before
    /// the *following* tick, bounding the loss at one. Raising
    /// `PUT_TIMEOUT` past a whole interval, or adding retries whose total
    /// budget does, would let one unresponsive endpoint swallow several
    /// consecutive cycles.
    ///
    /// This does not claim a stall causes no delay — it can still cost the
    /// tick it overlaps — and it is not part of the write-ordering
    /// argument; see `PUT_TIMEOUT`'s own comment for that.
    ///
    /// A `const _: () = assert!(..)` in the module body would be stronger,
    /// but the dead-code lint does not count a use inside one, so both
    /// constants would then need an `#[allow(dead_code)]` that would also
    /// hide genuinely unused constants later.
    #[test]
    fn mirror_stall_cannot_cost_more_than_one_cycle() {
        assert!(
            PUT_TIMEOUT.as_secs() * MIN_CYCLE_INTERVAL_SAFETY_FACTOR
                <= MIN_SUPPORTED_CYCLE_INTERVAL.as_secs(),
            "one PUT attempt ({PUT_TIMEOUT:?}) must stay at least \
             {MIN_CYCLE_INTERVAL_SAFETY_FACTOR}x shorter than the shortest supported cycle \
             interval ({MIN_SUPPORTED_CYCLE_INTERVAL:?}), so a stalled mirror can only ever \
             cost the single tick it overlaps"
        );
    }

    #[test]
    fn from_env_unset_returns_none() {
        let _guard = lock_env();
        env::remove_var("STATUS_S3_BUCKET");
        env::remove_var("STATUS_S3_KEY_PREFIX");
        assert!(StatusS3Mirror::from_env().is_none());
    }

    #[test]
    fn from_env_requires_both_variables() {
        let _guard = lock_env();
        env::set_var("STATUS_S3_BUCKET", "debot-dashboard");
        env::remove_var("STATUS_S3_KEY_PREFIX");
        assert!(StatusS3Mirror::from_env().is_none());
        env::remove_var("STATUS_S3_BUCKET");
        env::set_var("STATUS_S3_KEY_PREFIX", "debot/status/hype-accumulator");
        assert!(StatusS3Mirror::from_env().is_none());
        env::remove_var("STATUS_S3_KEY_PREFIX");
    }

    #[test]
    fn from_env_strips_trailing_slash_in_prefix() {
        let _guard = lock_env();
        env::set_var("STATUS_S3_BUCKET", "debot-dashboard");
        env::set_var("STATUS_S3_KEY_PREFIX", "debot/status/hype-accumulator/");
        let mirror = StatusS3Mirror::from_env().expect("present");
        assert_eq!(mirror.bucket, "debot-dashboard");
        assert_eq!(mirror.key_prefix, "debot/status/hype-accumulator");
        env::remove_var("STATUS_S3_BUCKET");
        env::remove_var("STATUS_S3_KEY_PREFIX");
    }

    #[test]
    fn from_env_treats_whitespace_as_unset() {
        let _guard = lock_env();
        env::set_var("STATUS_S3_BUCKET", "   ");
        env::set_var("STATUS_S3_KEY_PREFIX", "debot/status/hype-accumulator");
        assert!(StatusS3Mirror::from_env().is_none());
        env::remove_var("STATUS_S3_BUCKET");
        env::remove_var("STATUS_S3_KEY_PREFIX");
    }
}
