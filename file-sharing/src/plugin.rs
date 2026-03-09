use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bevy::app::{AppExit, Plugin};
use bevy::prelude::{App, EventWriter, IntoScheduleConfigs, Query, Res, ResMut, Update, With};
use bevy::window::{PrimaryWindow, Window};
use bevy_egui::{egui, EguiContexts};
use directories::ProjectDirs;
use flume::TryRecvError;

use crate::bridge::{AsyncBridge, NetworkCommand, NetworkEvent};
use crate::contacts::ContactsStore;
use crate::node::NodeOptions;
use crate::notifications::NotificationState;
use crate::profile::ProfileStore;
use crate::settings::{AppSettings, SettingsStore};
use crate::state::{Direction, FileProgress, Transfer, TransferRegistry, TransferStatus};
use crate::ui::{default_download_directory, ui_system, UiState};
use crate::updater::UpdateController;

pub struct FileSharingPlugin;
const INSECURE_SKIP_RELAY_CERT_VERIFY_ENV: &str =
    "P2PANDA_FILE_SHARING_INSECURE_SKIP_RELAY_CERT_VERIFY";
const DIAGNOSTIC_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(bevy::prelude::Resource)]
pub(crate) struct DiagnosticsPollState {
    last_requested_at: Instant,
}

#[derive(bevy::prelude::Resource)]
struct PluginStartupError {
    message: String,
}

struct PluginResources {
    bridge: AsyncBridge,
    contacts_store: ContactsStore,
    profile_store: ProfileStore,
    settings_store: SettingsStore,
    ui_state: UiState,
    updater: UpdateController,
}

impl Default for DiagnosticsPollState {
    fn default() -> Self {
        Self {
            last_requested_at: Instant::now() - DIAGNOSTIC_POLL_INTERVAL,
        }
    }
}

impl Plugin for FileSharingPlugin {
    fn build(&self, app: &mut App) {
        match initialize_plugin_resources() {
            Ok(resources) => {
                app.insert_resource(resources.bridge);
                app.insert_resource(TransferRegistry::default());
                app.insert_resource(resources.contacts_store);
                app.insert_resource(resources.profile_store);
                app.insert_resource(resources.settings_store);
                app.insert_resource(resources.ui_state);
                app.insert_resource(resources.updater);
                app.insert_resource(NotificationState::default());
                app.insert_resource(DiagnosticsPollState::default());
                app.add_systems(
                    Update,
                    (poll_update_state, poll_network_events, ui_system).chain(),
                );
            }
            Err(err) => {
                tracing::error!("failed to initialize file-sharing plugin: {err:#}");
                app.insert_resource(PluginStartupError {
                    message: err.to_string(),
                });
                app.add_systems(Update, render_startup_error_ui);
            }
        }
    }
}

fn initialize_plugin_resources() -> Result<PluginResources> {
    let data_dir = resolve_data_dir().context("failed to resolve file-sharing data directory")?;
    let settings_store =
        SettingsStore::load(&data_dir).context("failed to load app settings from disk")?;
    let settings = settings_store.settings().clone();
    let node_options =
        resolve_node_options(&settings).context("failed to resolve node options from settings")?;
    let bridge = AsyncBridge::spawn_with_data_dir(node_options, data_dir.clone())
        .context("failed to initialize async bridge")?;
    let contacts_store =
        ContactsStore::load(&data_dir).context("failed to load contacts from disk")?;
    let profile_store =
        ProfileStore::load_or_create(&data_dir).context("failed to load profile")?;
    let ui_state = UiState::with_settings(
        settings_store
            .settings()
            .default_download_dir
            .clone()
            .unwrap_or_else(default_download_directory),
        settings.mdns_enabled,
        settings.relay_mode,
        settings.custom_relay_url.clone(),
        profile_store.profile().display_name.clone(),
    );
    let updater = UpdateController::new(data_dir.clone(), &settings)
        .context("failed to initialize updater")?;

    Ok(PluginResources {
        bridge,
        contacts_store,
        profile_store,
        settings_store,
        ui_state,
        updater,
    })
}

pub(crate) fn resolve_data_dir() -> Result<std::path::PathBuf> {
    const APP_NAME: &str = "p2panda-file-sharing";
    ProjectDirs::from("", "", APP_NAME)
        .map(|dirs| dirs.data_dir().to_path_buf())
        .with_context(|| format!("failed to resolve app data directory for {APP_NAME}"))
}

fn resolve_node_options(settings: &AppSettings) -> Result<NodeOptions> {
    let relay_url = settings.relay_url_for_node()?;

    let insecure_skip_relay_cert_verify = std::env::var(INSECURE_SKIP_RELAY_CERT_VERIFY_ENV)
        .ok()
        .map(|value| parse_bool_env_var(&value, INSECURE_SKIP_RELAY_CERT_VERIFY_ENV))
        .transpose()?
        .unwrap_or(false);

    Ok(NodeOptions {
        relay_url,
        mdns_enabled: settings.mdns_enabled,
        insecure_skip_relay_cert_verify,
    })
}

fn parse_bool_env_var(value: &str, name: &str) -> Result<bool> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => anyhow::bail!(
            "invalid boolean value for {name}: {value} (expected true/false, 1/0, yes/no, on/off)"
        ),
    }
}

fn render_startup_error_ui(
    mut egui_contexts: EguiContexts,
    startup_error: Res<PluginStartupError>,
) {
    let ctx = egui_contexts.ctx_mut();
    egui::CentralPanel::default().show(ctx, |ui| {
        ui.heading("Startup Error");
        ui.separator();
        ui.label("The file-sharing runtime failed to initialize.");
        ui.label("Check logs for details, then restart the app after fixing the issue.");
        ui.add_space(8.0);
        ui.monospace(&startup_error.message);
    });
}

fn poll_update_state(
    mut updater: ResMut<UpdateController>,
    mut settings_store: ResMut<SettingsStore>,
    mut exit_events: EventWriter<AppExit>,
) {
    updater.poll(&mut settings_store);
    if updater.take_pending_exit() {
        exit_events.write(AppExit::Success);
    }
}

pub(crate) fn poll_network_events(
    bridge: Res<AsyncBridge>,
    mut transfers: ResMut<TransferRegistry>,
    mut ui_state: ResMut<UiState>,
    mut notifications: ResMut<NotificationState>,
    diagnostics_poll_state: Option<ResMut<DiagnosticsPollState>>,
    primary_window: Query<&Window, With<PrimaryWindow>>,
) {
    let app_focused = primary_window
        .iter()
        .next()
        .map(|window| window.focused)
        .unwrap_or(true);
    let now = Instant::now();
    if let Some(mut diagnostics_poll_state) = diagnostics_poll_state {
        if now.duration_since(diagnostics_poll_state.last_requested_at) >= DIAGNOSTIC_POLL_INTERVAL
            && bridge.send(NetworkCommand::RequestDiagnostics).is_ok()
        {
            diagnostics_poll_state.last_requested_at = now;
        }
    }
    let mut saw_progress = false;

    loop {
        match bridge.try_recv() {
            Ok(Some(event)) => {
                saw_progress |= apply_network_event(&mut transfers, &mut ui_state, event.clone());
                notifications.on_event(&event, &transfers, app_focused, now);
            }
            Ok(None) | Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => break,
        }
    }

    notifications.flush_due(app_focused, now);

    if saw_progress {
        for transfer in transfers.transfers_mut() {
            if transfer.start_time.elapsed() >= Duration::from_millis(500) {
                transfer.update_bandwidth();
            }
        }
    }
}

fn apply_network_event(
    transfers: &mut TransferRegistry,
    ui_state: &mut UiState,
    event: NetworkEvent,
) -> bool {
    match event {
        NetworkEvent::DiagnosticsSnapshot { snapshot } => {
            ui_state.diagnostics_snapshot = snapshot;
            false
        }
        NetworkEvent::ShareReady {
            transfer_id,
            directory_name,
            share_code,
            collection_hash,
            total_bytes,
            file_count,
        } => {
            let transfer = get_or_insert_transfer(transfers, transfer_id, || {
                Transfer::new(transfer_id, &directory_name, Direction::Upload)
            });
            transfer.name = directory_name;
            transfer.share_code = Some(share_code);
            transfer.collection_hash = Some(collection_hash);
            transfer.total_bytes = total_bytes;
            transfer.downloaded_bytes = total_bytes;
            if transfer.files.is_empty() && file_count > 0 {
                transfer.files = (0..file_count)
                    .map(|index| {
                        let mut fp = FileProgress::new(format!("file-{index}"), 0);
                        fp.mark_completed();
                        fp
                    })
                    .collect();
            }
            transfer.status = TransferStatus::Completed;
            ui_state.record_uploaded_bytes(transfer_id, total_bytes);
            false
        }
        NetworkEvent::DownloadStarted {
            transfer_id,
            directory_name,
            collection_hash,
            total_bytes,
            file_count,
        } => {
            let transfer = get_or_insert_transfer(transfers, transfer_id, || {
                Transfer::new(transfer_id, &directory_name, Direction::Download)
            });
            transfer.name = directory_name;
            transfer.collection_hash = Some(collection_hash);
            transfer.total_bytes = total_bytes;
            transfer.status = TransferStatus::Active;
            transfer.files = (0..file_count)
                .map(|index| FileProgress::new(format!("file-{index}"), 0))
                .collect();
            transfer.refresh_downloaded_bytes();
            false
        }
        NetworkEvent::FileDownloadProgress {
            transfer_id,
            file_index,
            bytes_downloaded,
        } => {
            if let Some(transfer) = transfers.get_mut(transfer_id) {
                ensure_file_slot(transfer, file_index);
                if let Some(file) = transfer.files.get_mut(file_index) {
                    file.downloaded_bytes = bytes_downloaded;
                    if file.total_size < bytes_downloaded {
                        file.total_size = bytes_downloaded;
                    }
                }
                transfer.refresh_downloaded_bytes();
                ui_state.record_downloaded_bytes(transfer_id, file_index, bytes_downloaded);
                return true;
            }
            false
        }
        NetworkEvent::DownloadProviderUpdate {
            transfer_id,
            provider_id,
            target,
            status,
        } => {
            ui_state.record_download_provider_event(transfer_id, provider_id, target, status);
            false
        }
        NetworkEvent::FileCompleted {
            transfer_id,
            file_index,
        } => {
            if let Some(transfer) = transfers.get_mut(transfer_id) {
                ensure_file_slot(transfer, file_index);
                if let Some(file) = transfer.files.get_mut(file_index) {
                    file.mark_completed();
                }
                transfer.refresh_downloaded_bytes();
                return true;
            }
            false
        }
        NetworkEvent::FileVerificationFailed {
            transfer_id,
            file_index,
            error_message,
        } => {
            if let Some(transfer) = transfers.get_mut(transfer_id) {
                ensure_file_slot(transfer, file_index);
                if let Some(file) = transfer.files.get_mut(file_index) {
                    file.mark_failed_verification(error_message);
                }
            }
            false
        }
        NetworkEvent::TransferCompleted { transfer_id } => {
            let transfer = get_or_insert_transfer(transfers, transfer_id, || {
                Transfer::new(transfer_id, "Recovered transfer", Direction::Download)
            });
            transfer.status = TransferStatus::Completed;
            false
        }
        NetworkEvent::TransferCancelled { transfer_id } => {
            if let Some(transfer) = transfers.get_mut(transfer_id) {
                transfer.status = TransferStatus::Cancelled;
            }
            false
        }
        NetworkEvent::TransferPaused { transfer_id } => {
            if let Some(transfer) = transfers.get_mut(transfer_id) {
                transfer.status = TransferStatus::Paused;
            }
            false
        }
        NetworkEvent::TransferResumed { transfer_id } => {
            if let Some(transfer) = transfers.get_mut(transfer_id) {
                transfer.status = TransferStatus::Active;
            }
            false
        }
        NetworkEvent::Error {
            transfer_id,
            error_message,
        } => {
            let transfer = get_or_insert_transfer(transfers, transfer_id, || {
                Transfer::new(transfer_id, "Transfer", Direction::Download)
            });
            transfer.status = TransferStatus::Error(error_message);
            ui_state.prune_download_progress(transfer_id);
            false
        }
        NetworkEvent::GlobalPauseChanged { paused } => {
            ui_state.global_paused = paused;
            false
        }
    }
}

fn get_or_insert_transfer<F>(
    transfers: &mut TransferRegistry,
    transfer_id: u64,
    make_transfer: F,
) -> &mut Transfer
where
    F: FnOnce() -> Transfer,
{
    let existing_index = {
        let transfers_ref = transfers.transfers();
        transfers_ref
            .iter()
            .position(|transfer| transfer.id == transfer_id)
    };
    if let Some(index) = existing_index {
        return &mut transfers.transfers_mut()[index];
    }

    let mut transfer = make_transfer();
    transfer.id = transfer_id;
    transfers.push(transfer);
    let index = transfers.transfers().len().saturating_sub(1);
    &mut transfers.transfers_mut()[index]
}

fn ensure_file_slot(transfer: &mut crate::state::Transfer, file_index: usize) {
    while transfer.files.len() <= file_index {
        let slot = transfer.files.len();
        transfer
            .files
            .push(FileProgress::new(format!("file-{slot}"), 0));
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use anyhow::Result;
    use bevy::prelude::App;
    use flume::Sender;

    use super::*;
    use crate::bridge::{NetworkCommand, NetworkEvent};
    use crate::diagnostics::DownloadProviderStatus;
    use crate::settings::{AppSettings, RelayMode};
    use crate::state::{Direction, FileVerification, Transfer};

    type BoxFuture<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'static>>;

    fn spawn_test_bridge<Worker>(worker: Worker) -> AsyncBridge
    where
        Worker: Fn(Arc<()>, NetworkCommand, Sender<NetworkEvent>) -> BoxFuture<Result<()>>
            + Send
            + Sync
            + 'static,
    {
        AsyncBridge::spawn_with_worker(|| Box::pin(async { Ok(()) }), worker).unwrap()
    }

    fn run_poll(app: &mut App) {
        app.add_systems(Update, poll_network_events);
        app.update();
    }

    #[test]
    fn progress_events_update_the_transfer_registry() {
        let bridge = spawn_test_bridge(|_, command, events| {
            Box::pin(async move {
                let transfer_id = match command {
                    NetworkCommand::StartDownload { transfer_id, .. } => transfer_id,
                    other => panic!("unexpected command: {other:?}"),
                };

                events
                    .send_async(NetworkEvent::DownloadStarted {
                        transfer_id,
                        directory_name: "photos".into(),
                        collection_hash:
                            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                                .into(),
                        total_bytes: 10,
                        file_count: 2,
                    })
                    .await?;
                events
                    .send_async(NetworkEvent::FileDownloadProgress {
                        transfer_id,
                        file_index: 0,
                        bytes_downloaded: 4,
                    })
                    .await?;
                events
                    .send_async(NetworkEvent::FileDownloadProgress {
                        transfer_id,
                        file_index: 1,
                        bytes_downloaded: 3,
                    })
                    .await?;
                Ok(())
            })
        });

        bridge
            .send(NetworkCommand::StartDownload {
                transfer_id: 41,
                share_code: "p2p-TEST".into(),
                output_directory: PathBuf::from("/tmp/out"),
                source_contact: None,
            })
            .unwrap();

        let mut registry = TransferRegistry::default();
        let mut transfer = Transfer::new(41, "pending", Direction::Download);
        transfer.start_time = Instant::now() - Duration::from_secs(1);
        registry.push(transfer);

        let mut app = App::new();
        app.insert_resource(bridge);
        app.insert_resource(registry);
        app.insert_resource(UiState::default());
        app.insert_resource(NotificationState::default());

        std::thread::sleep(Duration::from_millis(50));
        run_poll(&mut app);

        let world = app.world();
        let registry = world.resource::<TransferRegistry>();
        let transfer = registry.get(41).unwrap();
        assert_eq!(transfer.name, "photos");
        assert_eq!(transfer.status, TransferStatus::Active);
        assert_eq!(transfer.total_bytes, 10);
        assert_eq!(
            transfer.collection_hash.as_deref(),
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
        assert_eq!(transfer.downloaded_bytes, 7);
        assert_eq!(transfer.files.len(), 2);
        assert_eq!(transfer.files[0].downloaded_bytes, 4);
        assert_eq!(transfer.files[1].downloaded_bytes, 3);
    }

    #[test]
    fn bandwidth_values_stay_reasonable_after_progress_updates() {
        let bridge = spawn_test_bridge(|_, command, events| {
            Box::pin(async move {
                let transfer_id = match command {
                    NetworkCommand::StartDownload { transfer_id, .. } => transfer_id,
                    other => panic!("unexpected command: {other:?}"),
                };

                events
                    .send_async(NetworkEvent::FileDownloadProgress {
                        transfer_id,
                        file_index: 0,
                        bytes_downloaded: 2_048,
                    })
                    .await?;
                Ok(())
            })
        });

        bridge
            .send(NetworkCommand::StartDownload {
                transfer_id: 7,
                share_code: "p2p-BANDWIDTH".into(),
                output_directory: PathBuf::from("/tmp/out"),
                source_contact: None,
            })
            .unwrap();

        let mut registry = TransferRegistry::default();
        let mut transfer = Transfer::new(7, "download", Direction::Download);
        transfer.start_time = Instant::now() - Duration::from_secs(2);
        registry.push(transfer);

        let mut app = App::new();
        app.insert_resource(bridge);
        app.insert_resource(registry);
        app.insert_resource(UiState::default());
        app.insert_resource(NotificationState::default());

        std::thread::sleep(Duration::from_millis(50));
        run_poll(&mut app);

        let world = app.world();
        let transfer = world.resource::<TransferRegistry>().get(7).unwrap().clone();
        assert!(transfer.inbound_bytes_per_sec.is_finite());
        assert!(transfer.inbound_bytes_per_sec > 0.0);
        assert_eq!(transfer.outbound_bytes_per_sec, 0.0);
    }

    #[test]
    fn all_network_event_variants_are_handled() {
        let bridge = spawn_test_bridge(|_, command, events| {
            Box::pin(async move {
                let transfer_id = match command {
                    NetworkCommand::ShareDirectory { transfer_id, .. } => transfer_id,
                    other => panic!("unexpected command: {other:?}"),
                };

                events
                    .send_async(NetworkEvent::ShareReady {
                        transfer_id,
                        directory_name: "photos".into(),
                        share_code: "p2p-CODE".into(),
                        collection_hash:
                            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                                .into(),
                        total_bytes: 5,
                        file_count: 1,
                    })
                    .await?;
                events
                    .send_async(NetworkEvent::FileDownloadProgress {
                        transfer_id,
                        file_index: 0,
                        bytes_downloaded: 5,
                    })
                    .await?;
                events
                    .send_async(NetworkEvent::DownloadProviderUpdate {
                        transfer_id,
                        provider_id: "peer-alpha".into(),
                        target: "file file-0".into(),
                        status: DownloadProviderStatus::Trying,
                    })
                    .await?;
                events
                    .send_async(NetworkEvent::FileCompleted {
                        transfer_id,
                        file_index: 0,
                    })
                    .await?;
                events
                    .send_async(NetworkEvent::TransferCompleted { transfer_id })
                    .await?;
                events
                    .send_async(NetworkEvent::TransferCancelled { transfer_id })
                    .await?;
                events
                    .send_async(NetworkEvent::FileVerificationFailed {
                        transfer_id,
                        file_index: 0,
                        error_message: "verification failed for file-0".into(),
                    })
                    .await?;
                events
                    .send_async(NetworkEvent::Error {
                        transfer_id,
                        error_message: "late error".into(),
                    })
                    .await?;
                Ok(())
            })
        });

        bridge
            .send(NetworkCommand::ShareDirectory {
                transfer_id: 99,
                directory_path: PathBuf::from("/tmp/share"),
            })
            .unwrap();

        let mut registry = TransferRegistry::default();
        let mut transfer = Transfer::new(99, "share", Direction::Upload);
        transfer.start_time = Instant::now() - Duration::from_secs(2);
        registry.push(transfer);

        let mut app = App::new();
        app.insert_resource(bridge);
        app.insert_resource(registry);
        app.insert_resource(UiState::default());
        app.insert_resource(NotificationState::default());

        std::thread::sleep(Duration::from_millis(50));
        run_poll(&mut app);

        let world = app.world();
        let transfer = world.resource::<TransferRegistry>().get(99).unwrap();
        let ui_state = world.resource::<UiState>();
        assert_eq!(transfer.share_code.as_deref(), Some("p2p-CODE"));
        assert_eq!(
            transfer.collection_hash.as_deref(),
            Some("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
        );
        assert_eq!(transfer.status, TransferStatus::Error("late error".into()));
        assert!(matches!(
            transfer.files[0].verification,
            FileVerification::Failed(_)
        ));
        assert_eq!(transfer.completed_file_count(), 0);
        assert_eq!(transfer.downloaded_bytes, 5);
        assert!(transfer.outbound_bytes_per_sec.is_finite());
        assert!(transfer.outbound_bytes_per_sec >= 0.0);
        assert_eq!(ui_state.download_provider_history.len(), 1);
        assert_eq!(
            ui_state.download_provider_history[0].provider_id,
            "peer-alpha"
        );
    }

    #[test]
    fn recovered_share_ready_uses_persisted_name_and_counts() {
        let bridge = spawn_test_bridge(|_, command, events| {
            Box::pin(async move {
                let transfer_id = match command {
                    NetworkCommand::ShareDirectory { transfer_id, .. } => transfer_id,
                    other => panic!("unexpected command: {other:?}"),
                };

                events
                    .send_async(NetworkEvent::ShareReady {
                        transfer_id,
                        directory_name: "restored-share".into(),
                        share_code: "p2p-RESTORE".into(),
                        collection_hash:
                            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                                .into(),
                        total_bytes: 77,
                        file_count: 3,
                    })
                    .await?;
                Ok(())
            })
        });

        bridge
            .send(NetworkCommand::ShareDirectory {
                transfer_id: 555,
                directory_path: PathBuf::from("/tmp/share"),
            })
            .unwrap();

        let mut app = App::new();
        app.insert_resource(bridge);
        app.insert_resource(TransferRegistry::default());
        app.insert_resource(UiState::default());
        app.insert_resource(NotificationState::default());

        std::thread::sleep(Duration::from_millis(50));
        run_poll(&mut app);

        let world = app.world();
        let transfer = world.resource::<TransferRegistry>().get(555).unwrap();
        assert_eq!(transfer.name, "restored-share");
        assert_eq!(transfer.completed_file_count(), 3);
        assert_eq!(transfer.file_count(), 3);
        assert_eq!(transfer.downloaded_bytes, 77);
        assert_eq!(transfer.total_bytes, 77);
        assert_eq!(
            transfer.collection_hash.as_deref(),
            Some("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
        );
        assert_eq!(transfer.status, TransferStatus::Completed);
    }

    #[test]
    fn resolve_node_options_uses_testing_relay_by_default() -> Result<()> {
        let options = resolve_node_options(&AppSettings::default())?;
        let relay = options
            .relay_url
            .expect("testing relay should be configured");
        assert!(relay.to_string().contains(iroh::defaults::prod::EU_RELAY_HOSTNAME.trim_end_matches('.')));
        assert!(options.mdns_enabled);
        Ok(())
    }

    #[test]
    fn resolve_node_options_uses_custom_relay_when_selected() -> Result<()> {
        let settings = AppSettings {
            relay_mode: RelayMode::Relay,
            custom_relay_url: Some("https://relay.example.com".into()),
            ..Default::default()
        };
        let options = resolve_node_options(&settings)?;
        assert_eq!(
            options.relay_url.unwrap().to_string(),
            "https://relay.example.com/"
        );
        Ok(())
    }

    #[test]
    fn resolve_node_options_disables_relay_when_disabled_mode_selected() -> Result<()> {
        let settings = AppSettings {
            relay_mode: RelayMode::Disabled,
            custom_relay_url: Some("https://relay.example.com".into()),
            ..Default::default()
        };
        let options = resolve_node_options(&settings)?;
        assert!(options.relay_url.is_none());
        Ok(())
    }

    #[test]
    fn resolve_node_options_uses_mdns_setting() -> Result<()> {
        let settings = AppSettings {
            mdns_enabled: false,
            ..Default::default()
        };
        let options = resolve_node_options(&settings)?;
        assert!(!options.mdns_enabled);
        Ok(())
    }
}
