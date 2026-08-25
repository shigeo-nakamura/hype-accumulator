use crate::{metrics::MetricsSnapshot, status::DashboardStatus};
use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StatusIoError {
    #[error("status serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("status write failed: {0}")]
    Io(#[from] io::Error),
    #[error("status output path must name a file")]
    InvalidPath,
}

/// Atomically rewrites a local dashboard status file in the target directory.
///
/// # Errors
///
/// Returns [`StatusIoError`] when serialization, directory creation, durable
/// temporary-file write, or atomic rename fails.
pub fn write_status_atomic(
    path: impl AsRef<Path>,
    status: &DashboardStatus,
) -> Result<(), StatusIoError> {
    write_text_atomic(path.as_ref(), &status.to_json()?)
}

/// Atomically rewrites a local Prometheus text exposition.
///
/// # Errors
///
/// Returns `StatusIoError` when directory creation, durable temporary-file
/// write, or atomic rename fails.
pub fn write_metrics_atomic(
    path: impl AsRef<Path>,
    metrics: &MetricsSnapshot,
) -> Result<(), StatusIoError> {
    write_text_atomic(path.as_ref(), &metrics.to_prometheus())
}

fn write_text_atomic(path: &Path, payload: &str) -> Result<(), StatusIoError> {
    let parent = normalized_parent(path);
    let file_name = path.file_name().ok_or(StatusIoError::InvalidPath)?;
    fs::create_dir_all(parent)?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    let temporary = temporary_path(parent, file_name, nonce);
    let result: Result<(), io::Error> = (|| {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o640);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(payload.as_bytes())?;
        if !payload.ends_with('\n') {
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        #[cfg(unix)]
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map_err(StatusIoError::from)
}

fn normalized_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn temporary_path(parent: &Path, file_name: &std::ffi::OsStr, nonce: u128) -> PathBuf {
    let mut name = file_name.to_os_string();
    name.push(format!(".{}.{}.tmp", std::process::id(), nonce));
    parent.join(name)
}

#[cfg(test)]
mod tests {
    use super::normalized_parent;
    use std::path::Path;

    #[test]
    fn bare_file_name_uses_current_directory() {
        assert_eq!(normalized_parent(Path::new("status.json")), Path::new("."));
    }
}
