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
/// This bound is also what keeps the mirror's write ordering safe
/// (bot-strategy#920). The runtime deliberately releases its exclusive
/// state-directory lock before awaiting this PUT, so two consecutive
/// cycles' PUTs are not mutually excluded and a slow cycle N could in
/// principle overwrite the newer object cycle N+1 already wrote. That
/// interleaving needs cycle N's request to still be in flight when cycle
/// N+1's completes, which cannot happen while this timeout stays far
/// below the interval between cycles: the client abandons cycle N's
/// request after this timeout, and cycle N+1 does not start until its
/// schedule comes round — five minutes on the deployed
/// `OnCalendar=*-*-* *:0/5:00` timer. Only one unit mirrors, so a key has
/// a single writer rather than two racing ones:
/// `hype-accumulator-dryrun.service` carries the `STATUS_S3_*`
/// environment and the observer unit does not.
///
/// The deployment half of that argument cannot be checked here — nothing
/// in this process can observe its own systemd timer — so it is pinned as
/// an assertion in `put_timeout_cannot_outlive_a_cycle` and written down
/// in `docs/runbooks/signer-free-runtime.md`. Raising this timeout, adding
/// retries around the PUT, scheduling a status writer more often than five
/// minutes, or giving a second unit the `STATUS_S3_*` environment all
/// reopen the race and need an actual ordering mechanism — a mirror-scoped
/// lock, or a conditional write — rather than a bigger constant here.
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
    /// writer at; the other half of `PUT_TIMEOUT`'s ordering argument.
    /// Matches the deployed `OnCalendar=*-*-* *:0/5:00` timer.
    const MIN_SUPPORTED_CYCLE_INTERVAL: Duration = Duration::from_secs(300);

    /// How much shorter than a cycle one PUT attempt must stay. A PUT that
    /// consumed a meaningful fraction of the interval would leave the
    /// argument resting on timer accuracy alone.
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

    /// bot-strategy#920: the mirror's write ordering rests on an inequality
    /// between these two constants rather than on a lock. Raising
    /// `PUT_TIMEOUT`, or adding retries whose total budget exceeds it,
    /// silently reopens the stale-write race unless a real ordering
    /// mechanism comes with it — fail here rather than in production.
    ///
    /// A `const _: () = assert!(..)` in the module body would be stronger,
    /// but the dead-code lint does not count a use inside one, so both
    /// constants would then need an `#[allow(dead_code)]` that would also
    /// hide genuinely unused constants later.
    #[test]
    fn put_timeout_cannot_outlive_a_cycle() {
        assert!(
            PUT_TIMEOUT.as_secs() * MIN_CYCLE_INTERVAL_SAFETY_FACTOR
                <= MIN_SUPPORTED_CYCLE_INTERVAL.as_secs(),
            "one PUT attempt ({PUT_TIMEOUT:?}) must stay at least \
             {MIN_CYCLE_INTERVAL_SAFETY_FACTOR}x shorter than the shortest supported cycle \
             interval ({MIN_SUPPORTED_CYCLE_INTERVAL:?}); see this module's ordering argument"
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
