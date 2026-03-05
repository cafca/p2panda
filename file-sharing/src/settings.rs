use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

const SETTINGS_FILE_NAME: &str = "settings.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AppSettings {
    #[serde(default)]
    pub default_download_dir: Option<PathBuf>,
}

#[derive(Debug, bevy::prelude::Resource)]
pub struct SettingsStore {
    path: PathBuf,
    settings: AppSettings,
}

impl SettingsStore {
    pub fn load(data_dir: impl AsRef<Path>) -> Result<Self> {
        let path = settings_file_path(data_dir.as_ref());
        let settings = load_from_path(&path)?;
        Ok(Self { path, settings })
    }

    pub fn settings(&self) -> &AppSettings {
        &self.settings
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn set_default_download_dir(&mut self, path: Option<PathBuf>) -> Result<()> {
        if self.settings.default_download_dir != path {
            self.settings.default_download_dir = path;
            self.save()?;
        }
        Ok(())
    }

    fn save(&self) -> Result<()> {
        write_atomic(&self.path, &self.settings)
    }
}

pub fn load_settings(data_dir: impl AsRef<Path>) -> Result<AppSettings> {
    load_from_path(&settings_file_path(data_dir.as_ref()))
}

fn settings_file_path(data_dir: &Path) -> PathBuf {
    data_dir.join(SETTINGS_FILE_NAME)
}

fn load_from_path(path: &Path) -> Result<AppSettings> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse settings at {}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(AppSettings::default()),
        Err(err) => {
            Err(err).with_context(|| format!("failed to read settings at {}", path.display()))
        }
    }
}

fn write_atomic(path: &Path, settings: &AppSettings) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create settings directory {}", parent.display()))?;
    }

    let bytes = serde_json::to_vec_pretty(settings).context("failed to serialize settings")?;
    let tmp_path = path.with_extension("json.tmp");

    fs::write(&tmp_path, bytes).with_context(|| {
        format!(
            "failed to write temporary settings file {}",
            tmp_path.display()
        )
    })?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to atomically move {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn missing_settings_file_loads_defaults() -> Result<()> {
        let dir = tempdir()?;
        let settings = load_settings(dir.path())?;
        assert_eq!(settings, AppSettings::default());
        Ok(())
    }

    #[test]
    fn changing_default_download_dir_persists_to_settings_file() -> Result<()> {
        let dir = tempdir()?;
        let mut store = SettingsStore::load(dir.path())?;
        let selected = dir.path().join("downloads");

        store.set_default_download_dir(Some(selected.clone()))?;

        let settings_path = dir.path().join(SETTINGS_FILE_NAME);
        assert!(settings_path.is_file());

        let loaded = load_settings(dir.path())?;
        assert_eq!(loaded.default_download_dir, Some(selected));

        store.set_default_download_dir(None)?;
        let loaded = load_settings(dir.path())?;
        assert_eq!(loaded.default_download_dir, None);

        Ok(())
    }
}
