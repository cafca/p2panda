use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use tracing::{info, warn};

/// Version of the on-disk data layout.
///
/// Bump this when the persisted p2panda formats change incompatibly (operation header layout,
/// wire codec, store schema). Stores written by a different version are wiped on startup: all
/// shares, downloads, contacts and synced state are removed. Only the node identity
/// (`node.key`) and app settings (`settings.json`) survive an upgrade.
pub const DATA_SCHEMA_VERSION: u32 = 2;

const SCHEMA_VERSION_FILE: &str = "schema.version";

/// Store files owned by the app which are wiped on schema mismatch.
const STORE_FILES: &[&str] = &[
    "profile-store.sqlite3",
    "profile-store.sqlite3-wal",
    "profile-store.sqlite3-shm",
    "state.sqlite3",
    "state.sqlite3-wal",
    "state.sqlite3-shm",
    "contacts.json",
];

/// Store directories owned by the app which are wiped on schema mismatch.
const STORE_DIRS: &[&str] = &["blobs", "contact-record-cache"];

/// Ensures the data directory matches the current data schema version.
///
/// Existing stores are treated as disposable: when the version on disk differs from
/// [`DATA_SCHEMA_VERSION`] all local stores, shares and sync state are deleted before any of
/// them are opened. Must run before other startup code touches the data directory.
pub fn ensure_data_schema(data_dir: &Path) -> Result<()> {
    fs::create_dir_all(data_dir).with_context(|| {
        format!(
            "failed to create data directory {} for schema check",
            data_dir.display()
        )
    })?;

    let version_path = data_dir.join(SCHEMA_VERSION_FILE);
    let stored_version = fs::read_to_string(&version_path)
        .ok()
        .and_then(|contents| contents.trim().parse::<u32>().ok());

    if stored_version == Some(DATA_SCHEMA_VERSION) {
        return Ok(());
    }

    if has_store_artifacts(data_dir) {
        warn!(
            stored_version = stored_version.map(|v| v.to_string()).as_deref().unwrap_or("none"),
            current_version = DATA_SCHEMA_VERSION,
            "incompatible data schema; wiping local stores, shares and syncs"
        );
        wipe_stores(data_dir)?;
        info!("local stores wiped; shares and contacts must be re-created");
    }

    fs::write(&version_path, format!("{DATA_SCHEMA_VERSION}\n")).with_context(|| {
        format!(
            "failed to write data schema version file {}",
            version_path.display()
        )
    })?;
    Ok(())
}

fn has_store_artifacts(data_dir: &Path) -> bool {
    STORE_FILES
        .iter()
        .chain(STORE_DIRS.iter())
        .any(|name| data_dir.join(name).exists())
}

fn wipe_stores(data_dir: &Path) -> Result<()> {
    for name in STORE_FILES {
        let path = data_dir.join(name);
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("failed to remove store file {}", path.display()))?;
        }
    }
    for name in STORE_DIRS {
        let path = data_dir.join(name);
        if path.exists() {
            fs::remove_dir_all(&path)
                .with_context(|| format!("failed to remove store directory {}", path.display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_directory_is_stamped_without_wiping() -> Result<()> {
        let dir = tempfile::tempdir()?;
        ensure_data_schema(dir.path())?;
        assert_eq!(
            fs::read_to_string(dir.path().join(SCHEMA_VERSION_FILE))?.trim(),
            DATA_SCHEMA_VERSION.to_string()
        );
        Ok(())
    }

    #[test]
    fn matching_version_leaves_stores_alone() -> Result<()> {
        let dir = tempfile::tempdir()?;
        ensure_data_schema(dir.path())?;
        fs::write(dir.path().join("state.sqlite3"), b"data")?;
        ensure_data_schema(dir.path())?;
        assert!(dir.path().join("state.sqlite3").exists());
        Ok(())
    }

    #[test]
    fn version_mismatch_wipes_stores_but_keeps_identity_and_settings() -> Result<()> {
        let dir = tempfile::tempdir()?;
        fs::write(dir.path().join("node.key"), b"identity")?;
        fs::write(dir.path().join("settings.json"), b"{}")?;
        fs::write(dir.path().join("state.sqlite3"), b"data")?;
        fs::write(dir.path().join("profile-store.sqlite3"), b"data")?;
        fs::write(dir.path().join("contacts.json"), b"[]")?;
        fs::create_dir_all(dir.path().join("blobs"))?;
        fs::write(dir.path().join("blobs/blob"), b"data")?;
        fs::create_dir_all(dir.path().join("contact-record-cache"))?;

        // No version file on disk marks a pre-versioning install.
        ensure_data_schema(dir.path())?;

        assert!(dir.path().join("node.key").exists());
        assert!(dir.path().join("settings.json").exists());
        assert!(!dir.path().join("state.sqlite3").exists());
        assert!(!dir.path().join("profile-store.sqlite3").exists());
        assert!(!dir.path().join("contacts.json").exists());
        assert!(!dir.path().join("blobs").exists());
        assert!(!dir.path().join("contact-record-cache").exists());
        assert_eq!(
            fs::read_to_string(dir.path().join(SCHEMA_VERSION_FILE))?.trim(),
            DATA_SCHEMA_VERSION.to_string()
        );
        Ok(())
    }
}
