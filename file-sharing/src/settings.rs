use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use p2panda_net::iroh_endpoint::RelayUrl;
use serde::{Deserialize, Serialize};

const SETTINGS_FILE_NAME: &str = "settings.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UpdateChannel {
    #[default]
    Stable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RelayMode {
    #[default]
    TestingRelay,
    Relay,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSettings {
    #[serde(default)]
    pub default_download_dir: Option<PathBuf>,
    #[serde(default = "default_mdns_enabled")]
    pub mdns_enabled: bool,
    #[serde(default)]
    pub relay_mode: RelayMode,
    #[serde(default)]
    pub custom_relay_url: Option<String>,
    #[serde(default = "default_auto_update_checks")]
    pub auto_update_checks: bool,
    #[serde(default)]
    pub update_channel: UpdateChannel,
    #[serde(default)]
    pub update_last_checked_unix_secs: Option<u64>,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            default_download_dir: None,
            mdns_enabled: default_mdns_enabled(),
            relay_mode: RelayMode::TestingRelay,
            custom_relay_url: None,
            auto_update_checks: default_auto_update_checks(),
            update_channel: UpdateChannel::Stable,
            update_last_checked_unix_secs: None,
        }
    }
}

impl AppSettings {
    pub fn relay_url_for_node(&self) -> Result<Option<RelayUrl>> {
        match self.relay_mode {
            RelayMode::TestingRelay => Ok(Some(
                format!("https://{}", iroh::defaults::prod::EU_RELAY_HOSTNAME).parse()?,
            )),
            RelayMode::Relay => {
                let relay_url = normalize_custom_relay_url(self.custom_relay_url.as_deref())
                    .ok_or_else(|| anyhow::anyhow!("Relay mode requires a custom relay URL"))?;
                Ok(Some(parse_https_relay_url(relay_url)?))
            }
            RelayMode::Disabled => Ok(None),
        }
    }
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

    pub fn set_mdns_enabled(&mut self, enabled: bool) -> Result<bool> {
        let changed = self.settings.mdns_enabled != enabled;
        if changed {
            self.settings.mdns_enabled = enabled;
            self.save()?;
        }
        Ok(changed)
    }

    pub fn set_relay_config(
        &mut self,
        relay_mode: RelayMode,
        custom_relay_url: Option<String>,
    ) -> Result<bool> {
        let normalized_custom_relay_url =
            normalize_custom_relay_url(custom_relay_url.as_deref()).map(str::to_owned);

        if matches!(relay_mode, RelayMode::Relay) {
            let relay_url = normalized_custom_relay_url.as_deref().ok_or_else(|| {
                anyhow::anyhow!("Relay URL is required when relay mode is 'Relay'")
            })?;
            parse_https_relay_url(relay_url)?;
        }

        let changed = self.settings.relay_mode != relay_mode
            || self.settings.custom_relay_url != normalized_custom_relay_url;

        if changed {
            self.settings.relay_mode = relay_mode;
            self.settings.custom_relay_url = normalized_custom_relay_url;
            self.save()?;
        }

        Ok(changed)
    }

    pub fn set_auto_update_checks(&mut self, enabled: bool) -> Result<()> {
        if self.settings.auto_update_checks != enabled {
            self.settings.auto_update_checks = enabled;
            self.save()?;
        }
        Ok(())
    }

    pub fn set_update_last_checked(&mut self, timestamp: Option<u64>) -> Result<()> {
        if self.settings.update_last_checked_unix_secs != timestamp {
            self.settings.update_last_checked_unix_secs = timestamp;
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

fn normalize_custom_relay_url(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

const fn default_auto_update_checks() -> bool {
    true
}

const fn default_mdns_enabled() -> bool {
    true
}

fn parse_https_relay_url(value: &str) -> Result<RelayUrl> {
    if !value.to_ascii_lowercase().starts_with("https://") {
        anyhow::bail!("Relay URL must use https://");
    }
    value.parse().context("Invalid relay URL")
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
        assert!(settings.mdns_enabled);
        assert_eq!(settings.relay_mode, RelayMode::TestingRelay);
        assert!(settings.auto_update_checks);
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

    #[test]
    fn relay_settings_persist_and_validate_https_custom_url() -> Result<()> {
        let dir = tempdir()?;
        let mut store = SettingsStore::load(dir.path())?;

        assert!(store
            .set_relay_config(RelayMode::Relay, Some("http://relay.example.com".into()))
            .is_err());

        assert!(store.set_relay_config(RelayMode::Relay, Some("https://relay.example.com".into()))?);
        let loaded = load_settings(dir.path())?;
        assert_eq!(loaded.relay_mode, RelayMode::Relay);
        assert_eq!(
            loaded.custom_relay_url.as_deref(),
            Some("https://relay.example.com")
        );

        assert!(store.set_relay_config(RelayMode::Disabled, loaded.custom_relay_url.clone())?);
        let loaded = load_settings(dir.path())?;
        assert_eq!(loaded.relay_mode, RelayMode::Disabled);

        Ok(())
    }

    #[test]
    fn relay_url_for_node_matches_mode() -> Result<()> {
        let mut settings = AppSettings::default();
        let relay_url = settings
            .relay_url_for_node()?
            .expect("testing relay should be configured");
        assert!(relay_url.to_string().contains(iroh::defaults::prod::EU_RELAY_HOSTNAME.trim_end_matches('.')));

        settings.relay_mode = RelayMode::Disabled;
        assert!(settings.relay_url_for_node()?.is_none());

        settings.relay_mode = RelayMode::Relay;
        settings.custom_relay_url = Some("https://relay.example.com".into());
        assert_eq!(
            settings.relay_url_for_node()?.unwrap().to_string(),
            "https://relay.example.com/"
        );

        settings.custom_relay_url = Some("http://relay.example.com".into());
        assert!(settings.relay_url_for_node().is_err());

        Ok(())
    }

    #[test]
    fn update_preferences_persist_to_settings_file() -> Result<()> {
        let dir = tempdir()?;
        let mut store = SettingsStore::load(dir.path())?;

        assert!(store.set_mdns_enabled(false)?);
        store.set_auto_update_checks(false)?;
        store.set_update_last_checked(Some(1234))?;

        let loaded = load_settings(dir.path())?;
        assert!(!loaded.mdns_enabled);
        assert!(!loaded.auto_update_checks);
        assert_eq!(loaded.update_channel, UpdateChannel::Stable);
        assert_eq!(loaded.update_last_checked_unix_secs, Some(1234));

        Ok(())
    }
}
