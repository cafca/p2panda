use std::path::PathBuf;

use bevy::prelude::{Res, ResMut, Resource};
use bevy_egui::{egui, EguiContexts};

use crate::bridge::{AsyncBridge, NetworkCommand};
use crate::state::{Direction, Transfer, TransferRegistry, TransferStatus};

#[derive(Debug, Resource)]
pub struct UiState {
    pub download_dialog_open: bool,
    pub download_share_code_input: String,
    pub pending_share_path: Option<PathBuf>,
    pub selected_download_directory: PathBuf,
}

impl Default for UiState {
    fn default() -> Self {
        Self {
            download_dialog_open: false,
            download_share_code_input: String::new(),
            pending_share_path: None,
            selected_download_directory: default_download_directory(),
        }
    }
}

pub fn render_transfer_ui(
    mut egui_contexts: EguiContexts,
    mut ui_state: ResMut<UiState>,
    mut transfers: ResMut<TransferRegistry>,
    bridge: Res<AsyncBridge>,
) {
    let ctx = egui_contexts.ctx_mut();

    egui::CentralPanel::default().show(ctx, |ui| {
        ui.heading("p2panda File Sharing");

        ui.horizontal(|ui| {
            if ui.button("Share Directory...").clicked() {
                if let Some(path) = rfd::FileDialog::new().pick_folder() {
                    ui_state.pending_share_path = Some(path.clone());
                    start_share_transfer(&mut transfers, &bridge, path);
                }
            }

            if ui.button("Download").clicked() {
                ui_state.download_dialog_open = true;
            }
        });

        ui.separator();

        egui::ScrollArea::vertical().show(ui, |ui| {
            if transfers.transfers().is_empty() {
                ui.label("No active transfers");
            }

            for transfer in transfers.transfers() {
                render_transfer_row(ui, transfer);
            }
        });
    });

    if ui_state.download_dialog_open {
        render_download_dialog(ctx, &mut ui_state, &mut transfers, &bridge);
    }
}

fn render_download_dialog(
    ctx: &egui::Context,
    ui_state: &mut UiState,
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
) {
    let mut is_open = ui_state.download_dialog_open;
    let mut should_start = false;

    egui::Window::new("Start Download")
        .collapsible(false)
        .resizable(false)
        .open(&mut is_open)
        .show(ctx, |ui| {
            ui.label("Share code");
            ui.text_edit_singleline(&mut ui_state.download_share_code_input);

            ui.separator();
            ui.horizontal(|ui| {
                ui.label("Destination:");
                ui.label(ui_state.selected_download_directory.display().to_string());
                if ui.button("Choose...").clicked() {
                    if let Some(path) = rfd::FileDialog::new().pick_folder() {
                        ui_state.selected_download_directory = path;
                    }
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

fn render_transfer_row(ui: &mut egui::Ui, transfer: &Transfer) {
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
        });

        match &transfer.status {
            TransferStatus::Pending => {
                let label = match transfer.direction {
                    Direction::Upload => "Importing...",
                    Direction::Download => "Waiting...",
                };
                ui.label(label);
            }
            TransferStatus::Active => {
                let fraction = transfer.progress_fraction();
                ui.add(egui::ProgressBar::new(fraction).text(format!(
                    "{:.1}% ({}/{})",
                    fraction * 100.0,
                    format_bytes(transfer.downloaded_bytes),
                    format_bytes(transfer.total_bytes),
                )));
            }
            TransferStatus::Completed => {
                ui.label("Done");
            }
            TransferStatus::Error(message) => {
                ui.colored_label(egui::Color32::RED, message);
            }
        }

        if matches!(transfer.direction, Direction::Upload) {
            if let Some(code) = &transfer.share_code {
                ui.horizontal(|ui| {
                    ui.label("Share code:");
                    let mut share_code_text = code.clone();
                    ui.add(
                        egui::TextEdit::singleline(&mut share_code_text)
                            .interactive(false)
                            .desired_width(280.0),
                    );
                    if ui.button("Copy").clicked() {
                        ui.ctx().copy_text(code.clone());
                    }
                });
            }
        }
    });
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

fn default_download_directory() -> PathBuf {
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
}
