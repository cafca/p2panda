use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use semver::Version;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::settings::{AppSettings, SettingsStore, UpdateChannel};

const UPDATE_REPO_OWNER: &str = "p2panda";
const UPDATE_REPO_NAME: &str = "p2panda";
const UPDATE_CHECK_INTERVAL_SECS: u64 = 6 * 60 * 60;
const USER_AGENT: &str = "p2panda-file-sharing-updater";
const BIN_NAME: &str = "p2panda-file-sharing-gui";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailableUpdate {
    pub version: Version,
    pub release_notes_url: String,
    pub release_notes: String,
    pub asset_name: String,
    pub asset_url: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedUpdate {
    pub version: Version,
    pub extracted_binary_path: PathBuf,
    pub staging_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedUpdate {
    pub version: Version,
    pub relaunched_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckOutcome {
    UpToDate,
    UpdateAvailable(AvailableUpdate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateStatus {
    Idle,
    Checking {
        automatic: bool,
    },
    UpToDate,
    Available {
        update: AvailableUpdate,
    },
    Downloading {
        update: AvailableUpdate,
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    },
    ReadyToInstall {
        update: AvailableUpdate,
        prepared: PreparedUpdate,
    },
    Installing {
        update: AvailableUpdate,
    },
    Error(String),
}

impl UpdateStatus {
    pub fn is_busy(&self) -> bool {
        matches!(
            self,
            Self::Checking { .. } | Self::Downloading { .. } | Self::Installing { .. }
        )
    }
}

enum WorkerMessage {
    CheckFinished {
        automatic: bool,
        checked_at_unix_secs: u64,
        result: Result<CheckOutcome, String>,
    },
    DownloadProgress {
        downloaded_bytes: u64,
        total_bytes: Option<u64>,
    },
    DownloadFinished {
        result: Result<PreparedUpdate, String>,
    },
    ApplyFinished {
        result: Result<AppliedUpdate, String>,
    },
}

trait UpdateBackend: Send + Sync {
    fn check_for_update(
        &self,
        current_version: &Version,
        channel: UpdateChannel,
    ) -> Result<CheckOutcome>;

    fn download_update(
        &self,
        update: &AvailableUpdate,
        sender: flume::Sender<WorkerMessage>,
    ) -> Result<PreparedUpdate>;

    fn apply_update(&self, prepared: &PreparedUpdate) -> Result<AppliedUpdate>;
}

#[derive(bevy::prelude::Resource)]
pub struct UpdateController {
    backend: Arc<dyn UpdateBackend>,
    sender: flume::Sender<WorkerMessage>,
    receiver: flume::Receiver<WorkerMessage>,
    current_version: Version,
    auto_check_enabled: bool,
    channel: UpdateChannel,
    last_checked_unix_secs: Option<u64>,
    status: UpdateStatus,
    snoozed_version: Option<Version>,
    pending_exit: bool,
}

impl UpdateController {
    pub fn new(data_dir: PathBuf, settings: &AppSettings) -> Result<Self> {
        Self::with_backend(
            Arc::new(GitHubReleaseBackend::new(data_dir)?),
            settings,
            env!("CARGO_PKG_VERSION"),
        )
    }

    fn with_backend(
        backend: Arc<dyn UpdateBackend>,
        settings: &AppSettings,
        current_version: &str,
    ) -> Result<Self> {
        let (sender, receiver) = flume::unbounded();
        Ok(Self {
            backend,
            sender,
            receiver,
            current_version: Version::parse(current_version)
                .with_context(|| format!("invalid current version {current_version}"))?,
            auto_check_enabled: settings.auto_update_checks,
            channel: settings.update_channel,
            last_checked_unix_secs: settings.update_last_checked_unix_secs,
            status: UpdateStatus::Idle,
            snoozed_version: None,
            pending_exit: false,
        })
    }

    pub fn status(&self) -> &UpdateStatus {
        &self.status
    }

    pub fn auto_check_enabled(&self) -> bool {
        self.auto_check_enabled
    }

    pub fn set_auto_check_enabled(&mut self, enabled: bool) {
        self.auto_check_enabled = enabled;
    }

    pub fn channel(&self) -> UpdateChannel {
        self.channel
    }

    pub fn current_version(&self) -> &Version {
        &self.current_version
    }

    pub fn last_checked_unix_secs(&self) -> Option<u64> {
        self.last_checked_unix_secs
    }

    pub fn request_manual_check(&mut self) -> bool {
        self.snoozed_version = None;
        self.request_check(false)
    }

    pub fn request_download(&mut self) -> bool {
        let update = match &self.status {
            UpdateStatus::Available { update } => update.clone(),
            _ => return false,
        };
        let backend = Arc::clone(&self.backend);
        let sender = self.sender.clone();
        self.status = UpdateStatus::Downloading {
            update: update.clone(),
            downloaded_bytes: 0,
            total_bytes: None,
        };
        std::thread::spawn(move || {
            let result = backend.download_update(&update, sender.clone());
            let _ = sender.send(WorkerMessage::DownloadFinished {
                result: result.map_err(|err| err.to_string()),
            });
        });
        true
    }

    pub fn defer_available_update(&mut self) {
        if let UpdateStatus::Available { update } = &self.status {
            self.snoozed_version = Some(update.version.clone());
            self.status = UpdateStatus::Idle;
        }
    }

    pub fn request_apply(&mut self) -> bool {
        let (update, prepared) = match &self.status {
            UpdateStatus::ReadyToInstall { update, prepared } => (update.clone(), prepared.clone()),
            _ => return false,
        };
        let backend = Arc::clone(&self.backend);
        let sender = self.sender.clone();
        self.status = UpdateStatus::Installing { update };
        std::thread::spawn(move || {
            let result = backend.apply_update(&prepared);
            let _ = sender.send(WorkerMessage::ApplyFinished {
                result: result.map_err(|err| err.to_string()),
            });
        });
        true
    }

    pub fn take_pending_exit(&mut self) -> bool {
        std::mem::take(&mut self.pending_exit)
    }

    pub fn poll(&mut self, settings_store: &mut SettingsStore) {
        self.maybe_request_automatic_check();

        while let Ok(message) = self.receiver.try_recv() {
            match message {
                WorkerMessage::CheckFinished {
                    automatic,
                    checked_at_unix_secs,
                    result,
                } => {
                    self.last_checked_unix_secs = Some(checked_at_unix_secs);
                    if let Err(err) =
                        settings_store.set_update_last_checked(Some(checked_at_unix_secs))
                    {
                        self.status = UpdateStatus::Error(err.to_string());
                        continue;
                    }

                    match result {
                        Ok(CheckOutcome::UpToDate) => {
                            self.snoozed_version = None;
                            self.status = UpdateStatus::UpToDate;
                        }
                        Ok(CheckOutcome::UpdateAvailable(update)) => {
                            let snoozed = automatic
                                && self
                                    .snoozed_version
                                    .as_ref()
                                    .map(|version| version == &update.version)
                                    .unwrap_or(false);
                            if snoozed {
                                self.status = UpdateStatus::Idle;
                            } else {
                                self.status = UpdateStatus::Available { update };
                            }
                        }
                        Err(error) => {
                            self.status = UpdateStatus::Error(error);
                        }
                    }
                }
                WorkerMessage::DownloadProgress {
                    downloaded_bytes,
                    total_bytes,
                } => {
                    if let UpdateStatus::Downloading { update, .. } = &self.status {
                        self.status = UpdateStatus::Downloading {
                            update: update.clone(),
                            downloaded_bytes,
                            total_bytes,
                        };
                    }
                }
                WorkerMessage::DownloadFinished { result } => match result {
                    Ok(prepared) => {
                        let update = AvailableUpdate {
                            version: prepared.version.clone(),
                            release_notes_url: String::new(),
                            release_notes: String::new(),
                            asset_name: prepared
                                .staging_dir
                                .file_name()
                                .and_then(|name| name.to_str())
                                .unwrap_or_default()
                                .to_owned(),
                            asset_url: String::new(),
                            sha256: String::new(),
                        };
                        let update = match &self.status {
                            UpdateStatus::Downloading { update, .. } => update.clone(),
                            _ => update,
                        };
                        self.status = UpdateStatus::ReadyToInstall { update, prepared };
                    }
                    Err(error) => {
                        self.status = UpdateStatus::Error(error);
                    }
                },
                WorkerMessage::ApplyFinished { result } => match result {
                    Ok(applied) => {
                        self.pending_exit = true;
                        self.status = UpdateStatus::UpToDate;
                        self.last_checked_unix_secs = Some(now_unix_secs());
                        self.snoozed_version = None;
                        tracing::info!("relaunched app into version {}", applied.version);
                    }
                    Err(error) => {
                        self.status = UpdateStatus::Error(error);
                    }
                },
            }
        }
    }

    fn maybe_request_automatic_check(&mut self) {
        if !self.auto_check_enabled || self.status.is_busy() {
            return;
        }

        let now = now_unix_secs();
        let due = self
            .last_checked_unix_secs
            .map(|last_checked| now.saturating_sub(last_checked) >= UPDATE_CHECK_INTERVAL_SECS)
            .unwrap_or(true);
        if due {
            let _ = self.request_check(true);
        }
    }

    fn request_check(&mut self, automatic: bool) -> bool {
        if self.status.is_busy() {
            return false;
        }
        let backend = Arc::clone(&self.backend);
        let sender = self.sender.clone();
        let current_version = self.current_version.clone();
        let channel = self.channel;
        self.status = UpdateStatus::Checking { automatic };
        std::thread::spawn(move || {
            let checked_at_unix_secs = now_unix_secs();
            let result = backend
                .check_for_update(&current_version, channel)
                .map_err(|err| err.to_string());
            let _ = sender.send(WorkerMessage::CheckFinished {
                automatic,
                checked_at_unix_secs,
                result,
            });
        });
        true
    }
}

struct GitHubReleaseBackend {
    client: Client,
    staging_root: PathBuf,
    api_base_url: String,
}

impl GitHubReleaseBackend {
    fn new(data_dir: PathBuf) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .user_agent(USER_AGENT)
                .build()
                .context("failed to construct updater http client")?,
            staging_root: data_dir.join("updates"),
            api_base_url: "https://api.github.com".to_owned(),
        })
    }

    fn latest_release_url(&self, channel: UpdateChannel) -> String {
        let api_base_url = self.api_base_url.trim_end_matches('/');
        match channel {
            UpdateChannel::Stable => format!(
                "{api_base_url}/repos/{UPDATE_REPO_OWNER}/{UPDATE_REPO_NAME}/releases/latest"
            ),
        }
    }
}

impl UpdateBackend for GitHubReleaseBackend {
    fn check_for_update(
        &self,
        current_version: &Version,
        channel: UpdateChannel,
    ) -> Result<CheckOutcome> {
        let release: GitHubRelease = self
            .client
            .get(self.latest_release_url(channel))
            .send()
            .context("failed to query latest release")?
            .error_for_status()
            .context("release server returned an error")?
            .json()
            .context("failed to decode release metadata")?;

        let version = parse_release_version(&release.tag_name)?;
        if version <= *current_version {
            return Ok(CheckOutcome::UpToDate);
        }

        let (asset, sha256) = select_platform_asset(&release.assets)
            .context("failed to find a matching update asset for this platform")?;

        Ok(CheckOutcome::UpdateAvailable(AvailableUpdate {
            version,
            release_notes_url: release.html_url,
            release_notes: release.body,
            asset_name: asset.name,
            asset_url: asset.browser_download_url,
            sha256,
        }))
    }

    fn download_update(
        &self,
        update: &AvailableUpdate,
        sender: flume::Sender<WorkerMessage>,
    ) -> Result<PreparedUpdate> {
        let stage_dir = self.staging_root.join(format!("v{}", update.version));
        if stage_dir.exists() {
            fs::remove_dir_all(&stage_dir).with_context(|| {
                format!(
                    "failed to clean previous update staging dir {}",
                    stage_dir.display()
                )
            })?;
        }
        fs::create_dir_all(&stage_dir).with_context(|| {
            format!(
                "failed to create update staging dir {}",
                stage_dir.display()
            )
        })?;

        let archive_path = stage_dir.join(&update.asset_name);
        let mut response = self
            .client
            .get(&update.asset_url)
            .send()
            .context("failed to download update archive")?
            .error_for_status()
            .context("update server returned an error for archive download")?;
        let total_bytes = response.content_length();
        let mut archive_file = File::create(&archive_path)
            .with_context(|| format!("failed to create {}", archive_path.display()))?;
        let mut hasher = Sha256::new();
        let mut downloaded_bytes = 0u64;
        let mut buffer = [0u8; 64 * 1024];

        loop {
            let read = response
                .read(&mut buffer)
                .context("failed while reading update download")?;
            if read == 0 {
                break;
            }
            archive_file
                .write_all(&buffer[..read])
                .context("failed while writing update archive")?;
            hasher.update(&buffer[..read]);
            downloaded_bytes = downloaded_bytes.saturating_add(read as u64);
            let _ = sender.send(WorkerMessage::DownloadProgress {
                downloaded_bytes,
                total_bytes,
            });
        }

        let actual_sha256 = format!("{:x}", hasher.finalize());
        if actual_sha256 != update.sha256 {
            anyhow::bail!(
                "update integrity verification failed: expected {}, got {}",
                update.sha256,
                actual_sha256
            );
        }

        let extracted_dir = stage_dir.join("extracted");
        if extracted_dir.exists() {
            fs::remove_dir_all(&extracted_dir).with_context(|| {
                format!(
                    "failed to clean extracted update dir {}",
                    extracted_dir.display()
                )
            })?;
        }
        fs::create_dir_all(&extracted_dir)
            .with_context(|| format!("failed to create {}", extracted_dir.display()))?;
        extract_archive(&archive_path, &extracted_dir)
            .with_context(|| format!("failed to unpack {}", archive_path.display()))?;

        let binary_name = platform_binary_name();
        let extracted_binary_path =
            find_named_file(&extracted_dir, binary_name).with_context(|| {
                format!(
                    "failed to locate extracted binary {binary_name} in {}",
                    extracted_dir.display()
                )
            })?;

        Ok(PreparedUpdate {
            version: update.version.clone(),
            extracted_binary_path,
            staging_dir: stage_dir,
        })
    }

    fn apply_update(&self, prepared: &PreparedUpdate) -> Result<AppliedUpdate> {
        let current_exe = std::env::current_exe().context("failed to locate current executable")?;
        self_replace::self_replace(&prepared.extracted_binary_path)
            .context("failed to replace current executable with downloaded update")?;
        Command::new(&current_exe)
            .spawn()
            .with_context(|| format!("failed to relaunch {}", current_exe.display()))?;
        Ok(AppliedUpdate {
            version: prepared.version.clone(),
            relaunched_path: current_exe,
        })
    }
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
    html_url: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    assets: Vec<GitHubAsset>,
}

#[derive(Debug, Clone, Deserialize)]
struct GitHubAsset {
    name: String,
    browser_download_url: String,
    #[serde(default)]
    digest: Option<String>,
}

fn parse_release_version(tag_name: &str) -> Result<Version> {
    let trimmed = tag_name.trim_start_matches('v');
    Version::parse(trimmed).with_context(|| format!("invalid release version tag {tag_name}"))
}

fn select_platform_asset(assets: &[GitHubAsset]) -> Result<(GitHubAsset, String)> {
    let archive_extension = platform_archive_extension();
    let checksum_extension = platform_checksum_extension();

    let archive = assets
        .iter()
        .find(|asset| asset.name.ends_with(archive_extension))
        .cloned()
        .with_context(|| format!("no asset ending in {archive_extension}"))?;

    if let Some(digest) = archive.digest.clone() {
        return Ok((archive, parse_sha256_digest(&digest)?));
    }

    let checksum_asset = assets
        .iter()
        .find(|asset| asset.name.ends_with(checksum_extension))
        .cloned()
        .with_context(|| format!("no checksum asset ending in {checksum_extension}"))?;

    let client = Client::builder()
        .user_agent(USER_AGENT)
        .build()
        .context("failed to create checksum lookup client")?;
    let body = client
        .get(&checksum_asset.browser_download_url)
        .send()
        .context("failed to download checksum asset")?
        .error_for_status()
        .context("checksum asset download returned an error")?
        .text()
        .context("failed to read checksum asset body")?;
    let sha256 = parse_sha256_file_line(&body, &archive.name)
        .with_context(|| format!("failed to parse checksum for {}", archive.name))?;

    Ok((archive, sha256))
}

fn parse_sha256_digest(digest: &str) -> Result<String> {
    let (algorithm, value) = digest
        .split_once(':')
        .with_context(|| format!("unsupported digest format {digest}"))?;
    if algorithm != "sha256" {
        anyhow::bail!("unsupported digest algorithm {algorithm}");
    }
    Ok(value.to_ascii_lowercase())
}

fn parse_sha256_file_line(contents: &str, asset_name: &str) -> Result<String> {
    for line in contents.lines() {
        let mut parts = line.split_whitespace();
        let hash = parts.next();
        let name = parts.next();
        if let (Some(hash), Some(name)) = (hash, name) {
            let file_name = Path::new(name)
                .file_name()
                .and_then(|entry| entry.to_str())
                .unwrap_or(name);
            if file_name == asset_name {
                return Ok(hash.to_ascii_lowercase());
            }
        }
    }
    anyhow::bail!("checksum entry for {asset_name} not found")
}

fn extract_archive(archive_path: &Path, destination: &Path) -> Result<()> {
    if archive_path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
    {
        let file = File::open(archive_path)
            .with_context(|| format!("failed to open {}", archive_path.display()))?;
        let mut archive = zip::ZipArchive::new(file)
            .with_context(|| format!("failed to open zip {}", archive_path.display()))?;
        archive
            .extract(destination)
            .with_context(|| format!("failed to extract {}", archive_path.display()))?;
        return Ok(());
    }

    let file = File::open(archive_path)
        .with_context(|| format!("failed to open {}", archive_path.display()))?;
    let decoder = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);
    archive
        .unpack(destination)
        .with_context(|| format!("failed to extract {}", archive_path.display()))?;
    Ok(())
}

fn find_named_file(root: &Path, file_name: &str) -> Result<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in
            fs::read_dir(&path).with_context(|| format!("failed to read {}", path.display()))?
        {
            let entry =
                entry.with_context(|| format!("failed to read entry in {}", path.display()))?;
            let entry_path = entry.path();
            if entry.file_type()?.is_dir() {
                pending.push(entry_path);
            } else if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name == file_name)
            {
                return Ok(entry_path);
            }
        }
    }
    anyhow::bail!("file {file_name} not found under {}", root.display())
}

fn platform_binary_name() -> &'static str {
    #[cfg(target_os = "windows")]
    {
        "p2panda-file-sharing-gui.exe"
    }
    #[cfg(not(target_os = "windows"))]
    {
        BIN_NAME
    }
}

fn platform_archive_extension() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "-macos.zip"
    }
    #[cfg(target_os = "windows")]
    {
        "-windows-x86_64.zip"
    }
    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    {
        "-linux-x86_64.tar.gz"
    }
}

fn platform_checksum_extension() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "-macos-zip.sha256"
    }
    #[cfg(target_os = "windows")]
    {
        "-windows-zip.sha256"
    }
    #[cfg(all(not(target_os = "macos"), not(target_os = "windows")))]
    {
        "-linux-tar.sha256"
    }
}

pub fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn format_last_checked(timestamp: Option<u64>) -> String {
    timestamp
        .map(|timestamp| format!("{timestamp}"))
        .unwrap_or_else(|| "Never".to_owned())
}

pub fn release_notes_preview(notes: &str) -> String {
    const MAX_LINES: usize = 6;
    let lines = BufReader::new(io::Cursor::new(notes))
        .lines()
        .take(MAX_LINES)
        .filter_map(|line| line.ok())
        .collect::<Vec<_>>();
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use anyhow::Result;
    use serde_json::json;

    use super::*;

    #[derive(Clone, Default)]
    struct FakeBackend {
        state: Arc<Mutex<FakeBackendState>>,
    }

    #[derive(Default)]
    struct FakeBackendState {
        check_result: Option<Result<CheckOutcome, String>>,
        download_result: Option<Result<PreparedUpdate, String>>,
        apply_result: Option<Result<AppliedUpdate, String>>,
        progress_steps: Vec<(u64, Option<u64>)>,
        check_calls: usize,
        download_calls: usize,
        apply_calls: usize,
    }

    impl FakeBackend {
        fn with_check_result(self, result: Result<CheckOutcome, String>) -> Self {
            self.state.lock().unwrap().check_result = Some(result);
            self
        }

        fn with_download_result(
            self,
            result: Result<PreparedUpdate, String>,
            progress_steps: Vec<(u64, Option<u64>)>,
        ) -> Self {
            let mut state = self.state.lock().unwrap();
            state.download_result = Some(result);
            state.progress_steps = progress_steps;
            drop(state);
            self
        }

        fn with_apply_result(self, result: Result<AppliedUpdate, String>) -> Self {
            self.state.lock().unwrap().apply_result = Some(result);
            self
        }
    }

    impl UpdateBackend for FakeBackend {
        fn check_for_update(
            &self,
            _current_version: &Version,
            _channel: UpdateChannel,
        ) -> Result<CheckOutcome> {
            let mut state = self.state.lock().unwrap();
            state.check_calls += 1;
            match state
                .check_result
                .clone()
                .unwrap_or(Ok(CheckOutcome::UpToDate))
            {
                Ok(result) => Ok(result),
                Err(error) => anyhow::bail!(error),
            }
        }

        fn download_update(
            &self,
            _update: &AvailableUpdate,
            sender: flume::Sender<WorkerMessage>,
        ) -> Result<PreparedUpdate> {
            let mut state = self.state.lock().unwrap();
            state.download_calls += 1;
            let progress_steps = state.progress_steps.clone();
            let result = state.download_result.clone().unwrap();
            drop(state);

            for (downloaded_bytes, total_bytes) in progress_steps {
                sender
                    .send(WorkerMessage::DownloadProgress {
                        downloaded_bytes,
                        total_bytes,
                    })
                    .unwrap();
            }

            match result {
                Ok(prepared) => Ok(prepared),
                Err(error) => anyhow::bail!(error),
            }
        }

        fn apply_update(&self, _prepared: &PreparedUpdate) -> Result<AppliedUpdate> {
            let mut state = self.state.lock().unwrap();
            state.apply_calls += 1;
            match state.apply_result.clone().unwrap() {
                Ok(applied) => Ok(applied),
                Err(error) => anyhow::bail!(error),
            }
        }
    }

    fn wait_until(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if predicate() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("timed out waiting for predicate");
    }

    fn test_settings_store() -> Result<(tempfile::TempDir, SettingsStore)> {
        let dir = tempfile::tempdir()?;
        let store = SettingsStore::load(dir.path())?;
        Ok((dir, store))
    }

    fn available_update(version: &str) -> AvailableUpdate {
        AvailableUpdate {
            version: Version::parse(version).unwrap(),
            release_notes_url: "https://example.com/release".into(),
            release_notes: "line1\nline2\nline3".into(),
            asset_name: "p2panda-file-sharing-0.2.0-linux-x86_64.tar.gz".into(),
            asset_url: "https://example.com/download".into(),
            sha256: "abc123".into(),
        }
    }

    struct TestHttpServer {
        base_url: String,
        handle: Option<std::thread::JoinHandle<Result<()>>>,
    }

    impl TestHttpServer {
        fn spawn(
            build_routes: impl FnOnce(&str) -> HashMap<String, Vec<u8>> + Send + 'static,
        ) -> Result<Self> {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let base_url = format!("http://{}", listener.local_addr()?);
            let routes = build_routes(&base_url);
            let expected_requests = routes.len();
            let handle = std::thread::spawn(move || -> Result<()> {
                for _ in 0..expected_requests {
                    let (mut stream, _) = listener.accept()?;
                    let mut request = Vec::new();
                    let mut buffer = [0u8; 1024];
                    loop {
                        let read = stream.read(&mut buffer)?;
                        if read == 0 {
                            break;
                        }
                        request.extend_from_slice(&buffer[..read]);
                        if request.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }

                    let request_line = String::from_utf8_lossy(&request)
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .to_owned();
                    let path = request_line
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or("/")
                        .to_owned();

                    if let Some(body) = routes.get(&path) {
                        write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        )?;
                        stream.write_all(body)?;
                    } else {
                        stream.write_all(
                            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )?;
                    }
                }

                Ok(())
            });

            Ok(Self {
                base_url,
                handle: Some(handle),
            })
        }

        fn finish(mut self) -> Result<()> {
            if let Some(handle) = self.handle.take() {
                handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("test http server panicked"))??;
            }
            Ok(())
        }
    }

    fn prepared_update(version: &str) -> PreparedUpdate {
        PreparedUpdate {
            version: Version::parse(version).unwrap(),
            extracted_binary_path: PathBuf::from("/tmp/p2panda-file-sharing-gui"),
            staging_dir: PathBuf::from("/tmp/p2panda-update"),
        }
    }

    #[test]
    fn manual_check_surfaces_new_version_and_persists_last_checked() -> Result<()> {
        let backend = FakeBackend::default()
            .with_check_result(Ok(CheckOutcome::UpdateAvailable(available_update("0.2.0"))));
        let (_dir, mut settings_store) = test_settings_store()?;
        let mut controller =
            UpdateController::with_backend(Arc::new(backend), settings_store.settings(), "0.1.0")?;

        assert!(controller.request_manual_check());
        wait_until(|| {
            controller.poll(&mut settings_store);
            matches!(controller.status(), UpdateStatus::Available { .. })
        });

        assert!(matches!(
            controller.status(),
            UpdateStatus::Available { .. }
        ));
        assert!(controller.last_checked_unix_secs().is_some());
        let reloaded = crate::settings::load_settings(settings_store.path().parent().unwrap())?;
        assert!(reloaded.update_last_checked_unix_secs.is_some());

        Ok(())
    }

    #[test]
    fn download_progress_transitions_to_ready_to_install() -> Result<()> {
        let backend = FakeBackend::default()
            .with_download_result(
                Ok(prepared_update("0.2.0")),
                vec![(128, Some(256)), (256, Some(256))],
            )
            .with_apply_result(Ok(AppliedUpdate {
                version: Version::parse("0.2.0").unwrap(),
                relaunched_path: PathBuf::from("/tmp/p2panda-file-sharing-gui"),
            }));
        let (_dir, mut settings_store) = test_settings_store()?;
        let mut controller =
            UpdateController::with_backend(Arc::new(backend), settings_store.settings(), "0.1.0")?;
        controller.status = UpdateStatus::Available {
            update: available_update("0.2.0"),
        };

        assert!(controller.request_download());
        wait_until(|| {
            controller.poll(&mut settings_store);
            matches!(controller.status(), UpdateStatus::ReadyToInstall { .. })
        });

        match controller.status() {
            UpdateStatus::ReadyToInstall { prepared, .. } => {
                assert_eq!(prepared.version, Version::parse("0.2.0").unwrap());
            }
            other => panic!("unexpected status: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn successful_apply_requests_process_exit() -> Result<()> {
        let backend = FakeBackend::default().with_apply_result(Ok(AppliedUpdate {
            version: Version::parse("0.2.0").unwrap(),
            relaunched_path: PathBuf::from("/tmp/p2panda-file-sharing-gui"),
        }));
        let (_dir, mut settings_store) = test_settings_store()?;
        let mut controller =
            UpdateController::with_backend(Arc::new(backend), settings_store.settings(), "0.1.0")?;
        controller.status = UpdateStatus::ReadyToInstall {
            update: available_update("0.2.0"),
            prepared: prepared_update("0.2.0"),
        };

        assert!(controller.request_apply());
        wait_until(|| {
            controller.poll(&mut settings_store);
            controller.take_pending_exit()
        });

        Ok(())
    }

    #[test]
    fn failed_apply_keeps_current_install_running() -> Result<()> {
        let backend = FakeBackend::default().with_apply_result(Err("apply failed".into()));
        let (_dir, mut settings_store) = test_settings_store()?;
        let mut controller =
            UpdateController::with_backend(Arc::new(backend), settings_store.settings(), "0.1.0")?;
        controller.status = UpdateStatus::ReadyToInstall {
            update: available_update("0.2.0"),
            prepared: prepared_update("0.2.0"),
        };

        assert!(controller.request_apply());
        wait_until(|| {
            controller.poll(&mut settings_store);
            matches!(controller.status(), UpdateStatus::Error(_))
        });
        assert!(!controller.take_pending_exit());

        Ok(())
    }

    #[test]
    fn automatic_checks_fire_when_due() -> Result<()> {
        let backend = FakeBackend::default().with_check_result(Ok(CheckOutcome::UpToDate));
        let (_dir, mut settings_store) = test_settings_store()?;
        settings_store.set_update_last_checked(Some(1))?;
        let settings = settings_store.settings().clone();
        let mut controller =
            UpdateController::with_backend(Arc::new(backend.clone()), &settings, "0.1.0")?;
        controller.last_checked_unix_secs = Some(0);

        wait_until(|| {
            controller.poll(&mut settings_store);
            matches!(controller.status(), UpdateStatus::UpToDate)
        });

        assert_eq!(backend.state.lock().unwrap().check_calls, 1);
        Ok(())
    }

    #[test]
    fn github_release_backend_detects_newer_release_from_release_feed() -> Result<()> {
        let asset_name = format!("p2panda-file-sharing-0.2.0{}", platform_archive_extension());
        let checksum_name = format!(
            "p2panda-file-sharing-0.2.0{}",
            platform_checksum_extension()
        );
        let release_path = format!("/repos/{UPDATE_REPO_OWNER}/{UPDATE_REPO_NAME}/releases/latest");
        let asset_path = format!("/downloads/{asset_name}");
        let checksum_path = format!("/downloads/{checksum_name}");
        let checksum_body = format!("cafebabe  {asset_name}\nfeedface  unrelated-file.tar.gz\n");

        let server = TestHttpServer::spawn(move |base_url| {
            HashMap::from([
                (
                    release_path.clone(),
                    serde_json::to_vec(&json!({
                        "tag_name": "v0.2.0",
                        "html_url": "https://example.com/releases/v0.2.0",
                        "body": "Bug fixes",
                        "assets": [
                            {
                                "name": asset_name,
                                "browser_download_url": format!("{base_url}{asset_path}")
                            },
                            {
                                "name": checksum_name,
                                "browser_download_url": format!("{base_url}{checksum_path}")
                            }
                        ]
                    }))
                    .expect("release payload should serialize"),
                ),
                (checksum_path.clone(), checksum_body.into_bytes()),
            ])
        })?;

        let backend = GitHubReleaseBackend {
            client: Client::builder().user_agent(USER_AGENT).build()?,
            staging_root: tempfile::tempdir()?.path().join("updates"),
            api_base_url: server.base_url.clone(),
        };

        let update = backend.check_for_update(&Version::parse("0.1.0")?, UpdateChannel::Stable)?;
        let available = match update {
            CheckOutcome::UpdateAvailable(update) => update,
            other => panic!("expected available update, got {other:?}"),
        };

        assert_eq!(available.version, Version::parse("0.2.0")?);
        assert_eq!(available.sha256, "cafebabe");
        assert_eq!(
            available.release_notes_url,
            "https://example.com/releases/v0.2.0"
        );

        server.finish()?;
        Ok(())
    }

    #[test]
    fn github_release_backend_rejects_downloads_with_sha256_mismatch() -> Result<()> {
        let asset_name = format!("p2panda-file-sharing-0.2.0{}", platform_archive_extension());
        let asset_path = format!("/downloads/{asset_name}");
        let served_asset_path = asset_path.clone();
        let server = TestHttpServer::spawn(move |_| {
            HashMap::from([(
                served_asset_path.clone(),
                b"definitely-not-a-real-release-archive".to_vec(),
            )])
        })?;
        let staging_root = tempfile::tempdir()?;
        let backend = GitHubReleaseBackend {
            client: Client::builder().user_agent(USER_AGENT).build()?,
            staging_root: staging_root.path().join("updates"),
            api_base_url: server.base_url.clone(),
        };
        let (sender, _receiver) = flume::unbounded();

        let error = backend
            .download_update(
                &AvailableUpdate {
                    version: Version::parse("0.2.0")?,
                    release_notes_url: String::new(),
                    release_notes: String::new(),
                    asset_name: asset_name.clone(),
                    asset_url: format!("{}{asset_path}", server.base_url),
                    sha256: "0000000000000000000000000000000000000000000000000000000000000000"
                        .to_owned(),
                },
                sender,
            )
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("update integrity verification failed"));
        server.finish()?;
        Ok(())
    }

    #[test]
    fn sha256_digest_and_file_lines_are_parsed() -> Result<()> {
        assert_eq!(parse_sha256_digest("sha256:ABCDEF")?, "abcdef".to_owned());
        assert_eq!(
            parse_sha256_file_line(
                "cafebabe  p2panda-file-sharing-0.2.0-linux-x86_64.tar.gz\n",
                "p2panda-file-sharing-0.2.0-linux-x86_64.tar.gz"
            )?,
            "cafebabe"
        );
        Ok(())
    }

    #[test]
    fn release_notes_preview_is_bounded() {
        let preview = release_notes_preview("1\n2\n3\n4\n5\n6\n7\n8");
        assert_eq!(preview.lines().count(), 6);
    }
}
