use std::path::PathBuf;

use bevy::prelude::{EventReader, Res, ResMut, Resource};
use bevy::window::FileDragAndDrop;
use bevy_egui::{egui, EguiContexts};

use crate::bridge::{AsyncBridge, NetworkCommand};
use crate::settings::{RelayMode, SettingsStore};
use crate::state::{Direction, Transfer, TransferRegistry, TransferStatus};

/// Which file dialog is currently open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingDialog {
    Share,
    DownloadDestination,
    SettingsDefaultDownloadDirectory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferAction {
    Cancel { transfer_id: u64 },
    Pause { transfer_id: u64 },
    Resume { transfer_id: u64 },
}

#[derive(Debug, Resource)]
pub struct UiState {
    pub global_paused: bool,
    pub show_settings_view: bool,
    pub download_dialog_open: bool,
    pub download_share_code_input: String,
    pub pending_share_path: Option<PathBuf>,
    pub selected_download_directory: PathBuf,
    pub relay_mode: RelayMode,
    pub custom_relay_url_input: String,
    pub relay_restart_required: bool,
    pub settings_error: Option<String>,
    pub drag_drop_active: bool,
    pub drag_drop_error: Option<String>,
    folder_dialog: Option<(PendingDialog, flume::Receiver<Option<PathBuf>>)>,
}

impl Default for UiState {
    fn default() -> Self {
        Self::with_default_download_directory(default_download_directory())
    }
}

impl UiState {
    pub fn with_default_download_directory(default_download_directory: PathBuf) -> Self {
        Self {
            global_paused: false,
            show_settings_view: false,
            download_dialog_open: false,
            download_share_code_input: String::new(),
            pending_share_path: None,
            selected_download_directory: default_download_directory,
            relay_mode: RelayMode::TestingRelay,
            custom_relay_url_input: String::new(),
            relay_restart_required: false,
            settings_error: None,
            drag_drop_active: false,
            drag_drop_error: None,
            folder_dialog: None,
        }
    }

    pub fn with_settings(
        default_download_directory: PathBuf,
        relay_mode: RelayMode,
        custom_relay_url: Option<String>,
    ) -> Self {
        let mut state = Self::with_default_download_directory(default_download_directory);
        state.relay_mode = relay_mode;
        state.custom_relay_url_input = custom_relay_url.unwrap_or_default();
        state
    }
}

/// Spawn a non-blocking folder picker on a background thread, returning a
/// receiver that will eventually deliver the chosen path (or `None` if the
/// user cancelled).
fn open_folder_dialog() -> flume::Receiver<Option<PathBuf>> {
    let (tx, rx) = flume::bounded(1);
    std::thread::spawn(move || {
        let result = rfd::FileDialog::new().pick_folder();
        let _ = tx.send(result);
    });
    rx
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DragDropUiEvent {
    Hovered,
    Dropped(PathBuf),
    HoveredCanceled,
}

fn handle_drag_drop_events(
    ui_state: &mut UiState,
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
    events: impl IntoIterator<Item = DragDropUiEvent>,
) {
    let mut rejected = 0usize;
    let mut started_share = false;

    for event in events {
        match event {
            DragDropUiEvent::Hovered => {
                ui_state.drag_drop_active = true;
            }
            DragDropUiEvent::HoveredCanceled => {
                ui_state.drag_drop_active = false;
            }
            DragDropUiEvent::Dropped(path) => {
                ui_state.drag_drop_active = false;
                if matches!(
                    std::fs::metadata(&path).map(|metadata| metadata.is_dir()),
                    Ok(true)
                ) {
                    ui_state.pending_share_path = Some(path.clone());
                    start_share_transfer(transfers, bridge, path);
                    started_share = true;
                } else {
                    rejected = rejected.saturating_add(1);
                }
            }
        }
    }

    if started_share {
        ui_state.drag_drop_error = None;
    } else if rejected > 0 {
        ui_state.drag_drop_error = Some(if rejected == 1 {
            "Only directories can be shared".to_owned()
        } else {
            format!("Only directories can be shared ({rejected} files ignored)")
        });
    }
}

pub fn ui_system(
    mut egui_contexts: EguiContexts,
    mut ui_state: ResMut<UiState>,
    mut transfers: ResMut<TransferRegistry>,
    bridge: Res<AsyncBridge>,
    mut settings_store: ResMut<SettingsStore>,
    mut drag_and_drop_events: EventReader<FileDragAndDrop>,
) {
    let drag_drop_events = drag_and_drop_events
        .read()
        .map(|event| match event {
            FileDragAndDrop::HoveredFile { .. } => DragDropUiEvent::Hovered,
            FileDragAndDrop::HoveredFileCanceled { .. } => DragDropUiEvent::HoveredCanceled,
            FileDragAndDrop::DroppedFile { path_buf, .. } => {
                DragDropUiEvent::Dropped(path_buf.clone())
            }
        })
        .collect::<Vec<_>>();
    handle_drag_drop_events(&mut ui_state, &mut transfers, &bridge, drag_drop_events);

    // Check if a pending folder dialog has completed.
    if let Some((purpose, rx)) = &ui_state.folder_dialog {
        if let Ok(result) = rx.try_recv() {
            let purpose = *purpose;
            ui_state.folder_dialog = None;
            if let Some(path) = result {
                match purpose {
                    PendingDialog::Share => {
                        ui_state.pending_share_path = Some(path.clone());
                        start_share_transfer(&mut transfers, &bridge, path);
                    }
                    PendingDialog::DownloadDestination => {
                        ui_state.selected_download_directory = path;
                    }
                    PendingDialog::SettingsDefaultDownloadDirectory => {
                        ui_state.selected_download_directory = path.clone();
                        if let Err(err) = settings_store.set_default_download_dir(Some(path)) {
                            ui_state.settings_error = Some(err.to_string());
                        } else {
                            ui_state.settings_error = None;
                        }
                    }
                }
            }
        }
    }

    let dialog_busy = ui_state.folder_dialog.is_some();

    let ctx = egui_contexts.ctx_mut();

    egui::CentralPanel::default().show(ctx, |ui| {
        ui.heading("p2panda File Sharing");

        ui.horizontal(|ui| {
            if ui
                .add_enabled(
                    !dialog_busy && !ui_state.global_paused,
                    egui::Button::new("Share Directory..."),
                )
                .clicked()
            {
                let rx = open_folder_dialog();
                ui_state.folder_dialog = Some((PendingDialog::Share, rx));
            }

            if ui
                .add_enabled(!ui_state.global_paused, egui::Button::new("Download"))
                .clicked()
            {
                ui_state.download_dialog_open = true;
            }

            let toggle_label = if ui_state.global_paused {
                "Resume All"
            } else {
                "Pause All"
            };
            if ui.button(toggle_label).clicked() {
                let command = if ui_state.global_paused {
                    NetworkCommand::ResumeAll
                } else {
                    NetworkCommand::PauseAll
                };
                if bridge.send(command).is_ok() {
                    ui_state.global_paused = !ui_state.global_paused;
                }
            }

            ui.separator();
            if ui
                .selectable_label(ui_state.show_settings_view, "Settings")
                .clicked()
            {
                ui_state.show_settings_view = !ui_state.show_settings_view;
            }
        });

        ui.separator();
        if let Some(error) = &ui_state.drag_drop_error {
            ui.colored_label(egui::Color32::RED, error);
            ui.separator();
        }

        if ui_state.show_settings_view {
            render_settings_view(ui, &mut ui_state, &mut settings_store, dialog_busy);
        } else {
            egui::ScrollArea::vertical().show(ui, |ui| {
                if transfers.transfers().is_empty() {
                    ui.label("No active transfers");
                }

                let mut pending_actions = Vec::new();
                for transfer in transfers.transfers() {
                    if let Some(action) = render_transfer_row(ui, transfer, ui_state.global_paused)
                    {
                        pending_actions.push(action);
                    }
                }

                for action in pending_actions {
                    apply_transfer_action(&mut transfers, &bridge, action);
                }
            });
        }
    });

    if ui_state.download_dialog_open {
        render_download_dialog(ctx, &mut ui_state, &mut transfers, &bridge);
    }

    if ui_state.drag_drop_active {
        render_drag_drop_overlay(ctx);
    }
}

pub fn render_transfer_ui(
    egui_contexts: EguiContexts,
    ui_state: ResMut<UiState>,
    transfers: ResMut<TransferRegistry>,
    bridge: Res<AsyncBridge>,
    settings_store: ResMut<SettingsStore>,
    drag_and_drop_events: EventReader<FileDragAndDrop>,
) {
    ui_system(
        egui_contexts,
        ui_state,
        transfers,
        bridge,
        settings_store,
        drag_and_drop_events,
    );
}

fn render_download_dialog(
    ctx: &egui::Context,
    ui_state: &mut UiState,
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
) {
    let mut is_open = ui_state.download_dialog_open;
    let mut should_start = false;
    let dialog_busy = ui_state.folder_dialog.is_some();

    egui::Window::new("Start Download")
        .collapsible(false)
        .resizable(false)
        .open(&mut is_open)
        .show(ctx, |ui| {
            ui.label("Share code");
            ui.horizontal(|ui| {
                ui.add(
                    egui::TextEdit::singleline(&mut ui_state.download_share_code_input)
                        .desired_width(360.0),
                );
                if ui.button("Paste").clicked() {
                    paste_into_text(&mut ui_state.download_share_code_input);
                }
            });

            ui.separator();
            ui.horizontal(|ui| {
                ui.label("Destination:");
                ui.label(ui_state.selected_download_directory.display().to_string());
                if ui
                    .add_enabled(!dialog_busy, egui::Button::new("Download to..."))
                    .clicked()
                {
                    let rx = open_folder_dialog();
                    ui_state.folder_dialog = Some((PendingDialog::DownloadDestination, rx));
                }
            });

            ui.separator();
            let can_start = !ui_state.download_share_code_input.trim().is_empty();
            if ui
                .add_enabled(can_start, egui::Button::new("Start"))
                .clicked()
            {
                should_start = true;
            }
        });

    if should_start {
        let share_code = ui_state.download_share_code_input.trim().to_owned();
        let output_directory = ui_state.selected_download_directory.clone();
        start_download_transfer(transfers, bridge, share_code, output_directory);
        ui_state.download_share_code_input.clear();
        is_open = false;
    }

    ui_state.download_dialog_open = is_open;
}

fn render_drag_drop_overlay(ctx: &egui::Context) {
    let rect = ctx.screen_rect();
    let painter = ctx.layer_painter(egui::LayerId::new(
        egui::Order::Foreground,
        egui::Id::new("drag_drop_overlay"),
    ));

    painter.rect_filled(rect, 0.0, egui::Color32::from_black_alpha(140));
    painter.rect_stroke(
        rect.shrink(18.0),
        8.0,
        egui::Stroke::new(2.0, egui::Color32::from_rgb(120, 220, 180)),
        egui::StrokeKind::Middle,
    );
    painter.text(
        rect.center(),
        egui::Align2::CENTER_CENTER,
        "Drop folder to share",
        egui::TextStyle::Heading.resolve(&ctx.style()),
        egui::Color32::WHITE,
    );
}

fn render_settings_view(
    ui: &mut egui::Ui,
    ui_state: &mut UiState,
    settings_store: &mut SettingsStore,
    dialog_busy: bool,
) {
    ui.heading("Settings");
    ui.label("Default download directory");
    ui.horizontal(|ui| {
        ui.label(ui_state.selected_download_directory.display().to_string());
        if ui
            .add_enabled(!dialog_busy, egui::Button::new("Browse..."))
            .clicked()
        {
            let rx = open_folder_dialog();
            ui_state.folder_dialog = Some((PendingDialog::SettingsDefaultDownloadDirectory, rx));
        }
        if ui.button("Reset to default").clicked() {
            ui_state.selected_download_directory = default_download_directory();
            if let Err(err) = settings_store.set_default_download_dir(None) {
                ui_state.settings_error = Some(err.to_string());
            } else {
                ui_state.settings_error = None;
            }
        }
    });

    if let Some(error) = &ui_state.settings_error {
        ui.colored_label(
            egui::Color32::RED,
            format!("Failed to save settings: {error}"),
        );
    }

    ui.separator();
    ui.heading("Relay Server");
    ui.label("Relay servers help connect peers who cannot reach each other directly. Data is encrypted end-to-end.");

    let mut relay_changed = false;
    relay_changed |= ui
        .radio_value(
            &mut ui_state.relay_mode,
            RelayMode::TestingRelay,
            "Testing Relay (iroh)",
        )
        .changed();
    relay_changed |= ui
        .radio_value(&mut ui_state.relay_mode, RelayMode::Relay, "Relay")
        .changed();
    relay_changed |= ui
        .radio_value(&mut ui_state.relay_mode, RelayMode::Disabled, "Disabled")
        .changed();

    if matches!(ui_state.relay_mode, RelayMode::TestingRelay) {
        ui.small("Public testing server from iroh; not intended for production use.");
    }

    ui.horizontal(|ui| {
        ui.label("Relay URL");
        let text_edit =
            egui::TextEdit::singleline(&mut ui_state.custom_relay_url_input).desired_width(360.0);
        let response = ui.add_enabled(matches!(ui_state.relay_mode, RelayMode::Relay), text_edit);
        relay_changed |= response.changed();
    });

    if relay_changed {
        match settings_store.set_relay_config(
            ui_state.relay_mode,
            Some(ui_state.custom_relay_url_input.clone()),
        ) {
            Ok(changed) => {
                if changed {
                    ui_state.relay_restart_required = true;
                }
                ui_state.settings_error = None;
            }
            Err(err) => {
                ui_state.settings_error = Some(err.to_string());
            }
        }
    }

    if ui_state.relay_restart_required {
        ui.colored_label(
            egui::Color32::YELLOW,
            "Restart required to apply relay changes",
        );
    }
}

fn render_transfer_row(
    ui: &mut egui::Ui,
    transfer: &Transfer,
    globally_paused: bool,
) -> Option<TransferAction> {
    let mut action = None;

    ui.group(|ui| {
        ui.horizontal(|ui| {
            let icon = match transfer.direction {
                Direction::Upload => "↑",
                Direction::Download => "↓",
            };
            ui.label(format!("{icon} {}", transfer.name));

            let files_total = transfer.file_count();
            let files_done = transfer.completed_file_count();
            ui.label(format!("{files_done}/{files_total} files"));

            let bytes_per_sec = match transfer.direction {
                Direction::Upload => transfer.outbound_bytes_per_sec,
                Direction::Download => transfer.inbound_bytes_per_sec,
            };
            ui.label(format!(
                "{} /s",
                format_bytes(bytes_per_sec.max(0.0) as u64)
            ));

            match transfer.status {
                TransferStatus::Paused => {
                    if ui.button("Resume").clicked() {
                        action = Some(TransferAction::Resume {
                            transfer_id: transfer.id,
                        });
                    }
                }
                TransferStatus::Error(_) | TransferStatus::Cancelled => {
                    ui.add_enabled(false, egui::Button::new("Pause"));
                }
                _ => {
                    if can_pause_transfer(transfer) && ui.button("Pause").clicked() {
                        action = Some(TransferAction::Pause {
                            transfer_id: transfer.id,
                        });
                    }
                }
            }

            if can_cancel_transfer(transfer) && ui.button("Cancel").clicked() {
                action = Some(TransferAction::Cancel {
                    transfer_id: transfer.id,
                });
            }
        });

        match &transfer.status {
            TransferStatus::Pending => {
                ui.label(status_label(transfer, globally_paused));
            }
            TransferStatus::Active
            | TransferStatus::Paused
            | TransferStatus::Completed
            | TransferStatus::Cancelled => {
                ui.label(status_label(transfer, globally_paused));
                if should_render_progress_bar(transfer, globally_paused) {
                    let (fraction, downloaded_bytes) = match transfer.direction {
                        Direction::Upload => (1.0, transfer.total_bytes),
                        Direction::Download => {
                            (transfer.progress_fraction(), transfer.downloaded_bytes)
                        }
                    };
                    render_progress_bar(ui, fraction, downloaded_bytes, transfer.total_bytes);
                }
            }
            TransferStatus::Error(message) => {
                ui.colored_label(egui::Color32::RED, status_label(transfer, globally_paused));
                ui.colored_label(egui::Color32::RED, message);
            }
        }

        if matches!(transfer.direction, Direction::Upload) {
            if let Some(code) = &transfer.share_code {
                ui.horizontal(|ui| {
                    ui.label("Share code:");
                    let mut share_code_text = code.clone();
                    ui.add(egui::TextEdit::singleline(&mut share_code_text).desired_width(280.0));
                    if ui.button("Copy").clicked() {
                        copy_text(ui.ctx(), code);
                    }
                });
            }
        }
    });

    action
}

fn render_progress_bar(ui: &mut egui::Ui, fraction: f32, downloaded_bytes: u64, total_bytes: u64) {
    ui.add(egui::ProgressBar::new(fraction).text(format!(
        "{:.1}% ({}/{})",
        fraction * 100.0,
        format_bytes(downloaded_bytes),
        format_bytes(total_bytes),
    )));
}

fn status_label(transfer: &Transfer, globally_paused: bool) -> &'static str {
    if globally_paused && matches!(transfer.status, TransferStatus::Active) {
        return "Paused";
    }

    match (&transfer.direction, &transfer.status) {
        (Direction::Upload, TransferStatus::Pending) => "Importing...",
        (Direction::Upload, TransferStatus::Active | TransferStatus::Completed) => "Seeding",
        (Direction::Upload, TransferStatus::Paused) => "Paused",
        (Direction::Upload, TransferStatus::Cancelled) => "Cancelled",
        (Direction::Upload, TransferStatus::Error(_)) => "Failed",
        (Direction::Download, TransferStatus::Pending) => "Waiting...",
        (Direction::Download, TransferStatus::Active) => "Downloading",
        (Direction::Download, TransferStatus::Paused) => "Paused",
        (Direction::Download, TransferStatus::Completed) => "Done",
        (Direction::Download, TransferStatus::Cancelled) => "Cancelled",
        (Direction::Download, TransferStatus::Error(_)) => "Failed",
    }
}

fn should_render_progress_bar(transfer: &Transfer, globally_paused: bool) -> bool {
    if globally_paused && matches!(transfer.status, TransferStatus::Active) {
        return true;
    }

    matches!(
        (&transfer.direction, &transfer.status),
        (
            Direction::Upload,
            TransferStatus::Active | TransferStatus::Paused | TransferStatus::Completed
        ) | (
            Direction::Download,
            TransferStatus::Active | TransferStatus::Paused
        )
    )
}

fn can_pause_transfer(transfer: &Transfer) -> bool {
    match (&transfer.direction, &transfer.status) {
        (_, TransferStatus::Paused | TransferStatus::Cancelled | TransferStatus::Error(_)) => false,
        (
            Direction::Upload,
            TransferStatus::Pending | TransferStatus::Active | TransferStatus::Completed,
        ) => true,
        (Direction::Download, TransferStatus::Pending | TransferStatus::Active) => true,
        (Direction::Download, TransferStatus::Completed) => false,
    }
}

fn can_cancel_transfer(transfer: &Transfer) -> bool {
    matches!(
        transfer.status,
        TransferStatus::Pending | TransferStatus::Active
    )
}

fn apply_transfer_action(
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
    action: TransferAction,
) {
    let (transfer_id, command) = match action {
        TransferAction::Cancel { transfer_id } => {
            (transfer_id, NetworkCommand::CancelTransfer { transfer_id })
        }
        TransferAction::Pause { transfer_id } => {
            (transfer_id, NetworkCommand::PauseTransfer { transfer_id })
        }
        TransferAction::Resume { transfer_id } => {
            (transfer_id, NetworkCommand::ResumeTransfer { transfer_id })
        }
    };

    if let Err(err) = bridge.send(command) {
        if let Some(transfer) = transfers.get_mut(transfer_id) {
            transfer.status = TransferStatus::Error(err.to_string());
        }
        return;
    }

    if let Some(transfer) = transfers.get_mut(transfer_id) {
        transfer.status = match action {
            TransferAction::Cancel { .. } => TransferStatus::Cancelled,
            TransferAction::Pause { .. } => TransferStatus::Paused,
            TransferAction::Resume { .. } => TransferStatus::Active,
        };
    }
}

fn start_share_transfer(
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
    directory_path: PathBuf,
) -> u64 {
    let transfer_id = transfers.allocate_id();
    let name = transfer_name_from_path(&directory_path, "Shared directory");
    transfers.push(Transfer::new(transfer_id, name, Direction::Upload));

    if let Err(err) = bridge.send(NetworkCommand::ShareDirectory {
        transfer_id,
        directory_path,
    }) {
        if let Some(transfer) = transfers.get_mut(transfer_id) {
            transfer.status = TransferStatus::Error(err.to_string());
        }
    }

    transfer_id
}

fn start_download_transfer(
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
    share_code: String,
    output_directory: PathBuf,
) -> u64 {
    let transfer_id = transfers.allocate_id();
    transfers.push(Transfer::new(transfer_id, "Download", Direction::Download));

    if let Err(err) = bridge.send(NetworkCommand::StartDownload {
        transfer_id,
        share_code,
        output_directory,
    }) {
        if let Some(transfer) = transfers.get_mut(transfer_id) {
            transfer.status = TransferStatus::Error(err.to_string());
        }
    }

    transfer_id
}

fn transfer_name_from_path(path: &std::path::Path, fallback: &str) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(fallback)
        .to_owned()
}

pub fn default_download_directory() -> PathBuf {
    let base = directories::UserDirs::new()
        .and_then(|dirs| dirs.download_dir().map(ToOwned::to_owned))
        .unwrap_or_else(|| PathBuf::from("Downloads"));

    base.join("p2panda")
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    match bytes {
        0..=1023 => format!("{bytes} B"),
        1024..=1_048_575 => format!("{:.1} KB", bytes as f64 / KIB),
        1_048_576..=1_073_741_823 => format!("{:.1} MB", bytes as f64 / MIB),
        _ => format!("{:.1} GB", bytes as f64 / GIB),
    }
}

fn copy_text(ctx: &egui::Context, text: &str) {
    ctx.copy_text(text.to_owned());
    if let Ok(mut clipboard) = arboard::Clipboard::new() {
        let _ = clipboard.set_text(text.to_owned());
    }
}

fn paste_into_text(target: &mut String) {
    if let Ok(mut clipboard) = arboard::Clipboard::new() {
        if let Ok(contents) = clipboard.get_text() {
            *target = contents.trim().to_owned();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Result;
    use flume::Sender;

    use super::*;
    use crate::bridge::{NetworkCommand, NetworkEvent};

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

    fn wait_for_event(bridge: &AsyncBridge) -> NetworkEvent {
        let started = std::time::Instant::now();
        loop {
            if let Some(event) = bridge.try_recv().unwrap() {
                return event;
            }

            assert!(
                started.elapsed() < Duration::from_secs(5),
                "timed out waiting for bridge event"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn bytes_are_formatted_as_human_readable_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(999), "999 B");
        assert_eq!(format_bytes(1_024), "1.0 KB");
        assert_eq!(format_bytes(1_048_576), "1.0 MB");
        assert_eq!(format_bytes(1_073_741_824), "1.0 GB");
    }

    #[test]
    fn share_transfer_allocates_upload_and_sends_bridge_command() {
        let bridge = spawn_test_bridge(|_, command, events| {
            Box::pin(async move {
                match command {
                    NetworkCommand::ShareDirectory { transfer_id, .. } => {
                        events
                            .send_async(NetworkEvent::TransferCompleted { transfer_id })
                            .await?;
                    }
                    other => panic!("unexpected command: {other:?}"),
                }
                Ok(())
            })
        });

        let mut registry = TransferRegistry::default();
        let transfer_id =
            start_share_transfer(&mut registry, &bridge, PathBuf::from("/tmp/photos"));

        let transfer = registry.get(transfer_id).unwrap();
        assert_eq!(transfer.direction, Direction::Upload);
        assert_eq!(transfer.status, TransferStatus::Pending);
        assert_eq!(transfer.name, "photos");

        assert_eq!(
            wait_for_event(&bridge),
            NetworkEvent::TransferCompleted { transfer_id }
        );
    }

    #[test]
    fn download_transfer_allocates_download_and_sends_bridge_command() {
        let bridge = spawn_test_bridge(|_, command, events| {
            Box::pin(async move {
                match command {
                    NetworkCommand::StartDownload { transfer_id, .. } => {
                        events
                            .send_async(NetworkEvent::DownloadStarted {
                                transfer_id,
                                directory_name: "album".into(),
                                total_bytes: 5,
                                file_count: 1,
                            })
                            .await?;
                    }
                    other => panic!("unexpected command: {other:?}"),
                }
                Ok(())
            })
        });

        let mut registry = TransferRegistry::default();
        let transfer_id = start_download_transfer(
            &mut registry,
            &bridge,
            "p2p-TEST".to_owned(),
            PathBuf::from("/tmp/output"),
        );

        let transfer = registry.get(transfer_id).unwrap();
        assert_eq!(transfer.direction, Direction::Download);
        assert_eq!(transfer.status, TransferStatus::Pending);

        assert_eq!(
            wait_for_event(&bridge),
            NetworkEvent::DownloadStarted {
                transfer_id,
                directory_name: "album".into(),
                total_bytes: 5,
                file_count: 1,
            }
        );
    }

    #[test]
    fn default_download_directory_appends_p2panda_folder() {
        assert!(default_download_directory().ends_with("p2panda"));
    }

    #[test]
    fn ui_state_can_start_with_persisted_default_download_directory() {
        let path = PathBuf::from("/tmp/p2panda-settings-downloads");
        let state = UiState::with_default_download_directory(path.clone());
        assert_eq!(state.selected_download_directory, path);
    }

    #[test]
    fn status_labels_match_transfer_direction_and_state() {
        let mut upload = Transfer::new(1, "upload", Direction::Upload);
        upload.status = TransferStatus::Active;
        assert_eq!(status_label(&upload, false), "Seeding");
        upload.status = TransferStatus::Completed;
        assert_eq!(status_label(&upload, false), "Seeding");
        upload.status = TransferStatus::Paused;
        assert_eq!(status_label(&upload, false), "Paused");
        upload.status = TransferStatus::Cancelled;
        assert_eq!(status_label(&upload, false), "Cancelled");

        let mut download = Transfer::new(2, "download", Direction::Download);
        download.status = TransferStatus::Active;
        assert_eq!(status_label(&download, false), "Downloading");
        download.status = TransferStatus::Paused;
        assert_eq!(status_label(&download, false), "Paused");
        download.status = TransferStatus::Completed;
        assert_eq!(status_label(&download, false), "Done");
        download.status = TransferStatus::Cancelled;
        assert_eq!(status_label(&download, false), "Cancelled");
        download.status = TransferStatus::Error("boom".into());
        assert_eq!(status_label(&download, false), "Failed");
    }

    #[test]
    fn progress_bar_rules_match_task_20_requirements() {
        let mut upload = Transfer::new(1, "upload", Direction::Upload);
        upload.status = TransferStatus::Active;
        assert!(should_render_progress_bar(&upload, false));
        upload.status = TransferStatus::Completed;
        assert!(should_render_progress_bar(&upload, false));
        upload.status = TransferStatus::Paused;
        assert!(should_render_progress_bar(&upload, false));

        let mut download = Transfer::new(2, "download", Direction::Download);
        download.status = TransferStatus::Active;
        assert!(should_render_progress_bar(&download, false));
        download.status = TransferStatus::Paused;
        assert!(should_render_progress_bar(&download, false));
        download.status = TransferStatus::Completed;
        assert!(!should_render_progress_bar(&download, false));
        download.status = TransferStatus::Cancelled;
        assert!(!should_render_progress_bar(&download, false));
    }

    #[test]
    fn global_pause_overrides_active_status_label() {
        let mut transfer = Transfer::new(42, "sync", Direction::Download);
        transfer.status = TransferStatus::Active;
        assert_eq!(status_label(&transfer, false), "Downloading");
        assert_eq!(status_label(&transfer, true), "Paused");
        assert!(should_render_progress_bar(&transfer, true));
    }

    #[test]
    fn cancel_and_pause_button_rules_match_transfer_status() {
        let mut pending = Transfer::new(1, "pending", Direction::Download);
        pending.status = TransferStatus::Pending;
        assert!(can_cancel_transfer(&pending));
        assert!(can_pause_transfer(&pending));

        let mut active = Transfer::new(2, "active", Direction::Download);
        active.status = TransferStatus::Active;
        assert!(can_cancel_transfer(&active));
        assert!(can_pause_transfer(&active));

        let mut completed = Transfer::new(3, "done", Direction::Download);
        completed.status = TransferStatus::Completed;
        assert!(!can_cancel_transfer(&completed));
        assert!(!can_pause_transfer(&completed));

        let mut paused = Transfer::new(4, "paused", Direction::Download);
        paused.status = TransferStatus::Paused;
        assert!(!can_cancel_transfer(&paused));
        assert!(!can_pause_transfer(&paused));

        let mut cancelled = Transfer::new(5, "cancelled", Direction::Download);
        cancelled.status = TransferStatus::Cancelled;
        assert!(!can_cancel_transfer(&cancelled));
        assert!(!can_pause_transfer(&cancelled));

        let mut errored = Transfer::new(6, "errored", Direction::Download);
        errored.status = TransferStatus::Error("boom".into());
        assert!(!can_cancel_transfer(&errored));
        assert!(!can_pause_transfer(&errored));
    }

    #[test]
    fn dropping_directories_starts_share_and_ignores_files() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let folder = temp.path().join("photos");
        let file = temp.path().join("notes.txt");
        std::fs::create_dir(&folder)?;
        std::fs::write(&file, b"not a directory")?;

        let bridge = spawn_test_bridge(|_, command, _| {
            Box::pin(async move {
                match command {
                    NetworkCommand::ShareDirectory { .. } => Ok(()),
                    other => panic!("unexpected command: {other:?}"),
                }
            })
        });
        let mut state = UiState::default();
        let mut registry = TransferRegistry::default();

        handle_drag_drop_events(
            &mut state,
            &mut registry,
            &bridge,
            [
                DragDropUiEvent::Hovered,
                DragDropUiEvent::Dropped(folder.clone()),
                DragDropUiEvent::Dropped(file),
            ],
        );

        assert!(!state.drag_drop_active);
        assert_eq!(state.pending_share_path, Some(folder));
        assert!(state.drag_drop_error.is_none());
        assert_eq!(registry.transfers().len(), 1);
        assert_eq!(registry.transfers()[0].direction, Direction::Upload);
        Ok(())
    }

    #[test]
    fn dropping_only_files_sets_error_and_starts_no_transfer() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let file = temp.path().join("notes.txt");
        std::fs::write(&file, b"not a directory")?;

        let bridge = spawn_test_bridge(|_, command, _| {
            Box::pin(async move {
                panic!("unexpected command: {command:?}");
            })
        });
        let mut state = UiState::default();
        let mut registry = TransferRegistry::default();

        handle_drag_drop_events(
            &mut state,
            &mut registry,
            &bridge,
            [DragDropUiEvent::Dropped(file)],
        );

        assert_eq!(
            state.drag_drop_error.as_deref(),
            Some("Only directories can be shared")
        );
        assert_eq!(registry.transfers().len(), 0);
        Ok(())
    }

    #[test]
    fn hover_cancel_turns_off_drag_overlay() {
        let bridge = spawn_test_bridge(|_, command, _| {
            Box::pin(async move {
                panic!("unexpected command: {command:?}");
            })
        });
        let mut state = UiState::default();
        let mut registry = TransferRegistry::default();

        handle_drag_drop_events(
            &mut state,
            &mut registry,
            &bridge,
            [DragDropUiEvent::Hovered, DragDropUiEvent::HoveredCanceled],
        );

        assert!(!state.drag_drop_active);
    }
}
