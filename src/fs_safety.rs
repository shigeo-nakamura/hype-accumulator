//! Shared symlink / hard-link file-safety helpers.
//!
//! These guard every security-relevant file open in the runtime, ledger
//! anchor, and workflow head stores. They are deliberately tiny, dependency
//! free, and byte-identical to the per-module copies they replaced so the
//! existing rejection tests in `runtime` and `workflow` remain the
//! behavioral contract.

use std::{
    fs::{self, File},
    io::{self, ErrorKind},
    path::{Component, Path},
};

/// Returns `true` only for an absolute path made of plain components with a
/// file name: no `.`, `..`, or trailing separator.
pub(crate) fn normal_absolute_path(path: &Path) -> bool {
    path.is_absolute()
        && path.file_name().is_some()
        && path.components().all(|component| {
            matches!(
                component,
                Component::Prefix(_) | Component::RootDir | Component::Normal(_)
            )
        })
}

/// Rejects an existing symbolic link at `path`. A missing entry is accepted so
/// the caller can create it with `create_new`.
pub(crate) fn reject_linked_file(path: &Path) -> Result<(), io::Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(io::Error::new(
            ErrorKind::PermissionDenied,
            "symbolic links are forbidden",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Rejects an open file whose inode has more than one hard link (Unix only;
/// a no-op elsewhere).
pub(crate) fn reject_multiple_links(file: &File) -> Result<(), io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if file.metadata()?.nlink() != 1 {
            return Err(io::Error::new(
                ErrorKind::PermissionDenied,
                "multiple hard links are forbidden",
            ));
        }
    }
    Ok(())
}
