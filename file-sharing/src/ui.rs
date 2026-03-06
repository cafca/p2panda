use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use bevy::ecs::system::SystemParam;
use bevy::prelude::{EventReader, Res, ResMut, Resource};
use bevy::window::FileDragAndDrop;
use bevy_egui::{egui, EguiContexts};

use crate::bridge::{AsyncBridge, ContactDownloadSource, NetworkCommand};
use crate::contacts::{ContactsStore, DiscoveredProfile, DiscoverySort};
use crate::diagnostics::{
    now_unix_ms, DiagnosticsSnapshot, PeerConnectionState, PeerDiscoveryMethod,
};
use crate::profile::ProfileStore;
use crate::settings::{RelayMode, SettingsStore};
use crate::state::{Direction, FileVerification, Transfer, TransferRegistry, TransferStatus};
use crate::updater::{format_last_checked, release_notes_preview, UpdateController, UpdateStatus};

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
    Remove { transfer_id: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContactsView {
    Followed,
    Discovery,
}

#[derive(Debug, Resource)]
pub struct UiState {
    pub global_paused: bool,
    pub show_contacts_view: bool,
    pub show_settings_view: bool,
    pub show_diagnostics_view: bool,
    pub download_dialog_open: bool,
    pub download_share_code_input: String,
    pub contact_profile_id_input: String,
    pub contact_nickname_input: String,
    pub selected_contact_profile_id: Option<String>,
    pub selected_discovered_profile_id: Option<String>,
    pub contact_error: Option<String>,
    contacts_view: ContactsView,
    pub discovery_search_input: String,
    pub discovery_show_followed: bool,
    pub discovery_has_shares_only: bool,
    pub discovery_sort: DiscoverySort,
    pub pending_share_path: Option<PathBuf>,
    pub selected_download_directory: PathBuf,
    pub mdns_enabled: bool,
    pub relay_mode: RelayMode,
    pub custom_relay_url_input: String,
    pub network_restart_required: bool,
    pub profile_display_name_input: String,
    pub profile_error: Option<String>,
    pub settings_error: Option<String>,
    pub drag_drop_active: bool,
    pub drag_drop_error: Option<String>,
    pub diagnostics_snapshot: DiagnosticsSnapshot,
    pub diagnostics_total_bytes_received: u64,
    pub diagnostics_total_bytes_sent: u64,
    next_contact_refresh_unix_secs: u64,
    upload_byte_counted_transfers: HashSet<u64>,
    download_progress_watermark: HashMap<(u64, usize), u64>,
    pending_share_removal: Option<u64>,
    confirm_clear_completed: bool,
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
            show_contacts_view: false,
            show_settings_view: false,
            show_diagnostics_view: false,
            download_dialog_open: false,
            download_share_code_input: String::new(),
            contact_profile_id_input: String::new(),
            contact_nickname_input: String::new(),
            selected_contact_profile_id: None,
            selected_discovered_profile_id: None,
            contact_error: None,
            contacts_view: ContactsView::Followed,
            discovery_search_input: String::new(),
            discovery_show_followed: false,
            discovery_has_shares_only: false,
            discovery_sort: DiscoverySort::MutualCount,
            pending_share_path: None,
            selected_download_directory: default_download_directory,
            mdns_enabled: true,
            relay_mode: RelayMode::TestingRelay,
            custom_relay_url_input: String::new(),
            network_restart_required: false,
            profile_display_name_input: String::new(),
            profile_error: None,
            settings_error: None,
            drag_drop_active: false,
            drag_drop_error: None,
            diagnostics_snapshot: DiagnosticsSnapshot::default(),
            diagnostics_total_bytes_received: 0,
            diagnostics_total_bytes_sent: 0,
            next_contact_refresh_unix_secs: 0,
            upload_byte_counted_transfers: HashSet::new(),
            download_progress_watermark: HashMap::new(),
            pending_share_removal: None,
            confirm_clear_completed: false,
            folder_dialog: None,
        }
    }

    pub fn with_settings(
        default_download_directory: PathBuf,
        mdns_enabled: bool,
        relay_mode: RelayMode,
        custom_relay_url: Option<String>,
        profile_display_name: String,
    ) -> Self {
        let mut state = Self::with_default_download_directory(default_download_directory);
        state.mdns_enabled = mdns_enabled;
        state.relay_mode = relay_mode;
        state.custom_relay_url_input = custom_relay_url.unwrap_or_default();
        state.profile_display_name_input = profile_display_name;
        state
    }

    pub fn record_uploaded_bytes(&mut self, transfer_id: u64, total_bytes: u64) {
        if self.upload_byte_counted_transfers.insert(transfer_id) {
            self.diagnostics_total_bytes_sent = self
                .diagnostics_total_bytes_sent
                .saturating_add(total_bytes);
        }
    }

    pub fn record_downloaded_bytes(
        &mut self,
        transfer_id: u64,
        file_index: usize,
        bytes_downloaded: u64,
    ) {
        let key = (transfer_id, file_index);
        let previous = self
            .download_progress_watermark
            .insert(key, bytes_downloaded);
        if let Some(previous) = previous {
            if bytes_downloaded > previous {
                self.diagnostics_total_bytes_received = self
                    .diagnostics_total_bytes_received
                    .saturating_add(bytes_downloaded - previous);
            }
        } else {
            self.diagnostics_total_bytes_received = self
                .diagnostics_total_bytes_received
                .saturating_add(bytes_downloaded);
        }
    }

    pub fn prune_download_progress(&mut self, transfer_id: u64) {
        self.download_progress_watermark
            .retain(|(tracked_transfer_id, _), _| *tracked_transfer_id != transfer_id);
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

#[derive(SystemParam)]
pub struct UiSystemParams<'w, 's> {
    egui_contexts: EguiContexts<'w, 's>,
    ui_state: ResMut<'w, UiState>,
    transfers: ResMut<'w, TransferRegistry>,
    bridge: Res<'w, AsyncBridge>,
    contacts_store: ResMut<'w, ContactsStore>,
    profile_store: ResMut<'w, ProfileStore>,
    settings_store: ResMut<'w, SettingsStore>,
    updater: ResMut<'w, UpdateController>,
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

pub fn ui_system(params: UiSystemParams, mut drag_and_drop_events: EventReader<FileDragAndDrop>) {
    let UiSystemParams {
        mut egui_contexts,
        mut ui_state,
        mut transfers,
        bridge,
        mut contacts_store,
        mut profile_store,
        mut settings_store,
        mut updater,
    } = params;

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
    let now_unix_secs = now_unix_ms() / 1_000;
    if ui_state.show_contacts_view && now_unix_secs >= ui_state.next_contact_refresh_unix_secs {
        if let Err(err) = contacts_store.refresh_due_contacts(now_unix_secs) {
            ui_state.contact_error = Some(err.to_string());
        }
        ui_state.next_contact_refresh_unix_secs = now_unix_secs.saturating_add(5);
    }

    let ctx = egui_contexts.ctx_mut();
    apply_app_theme(ctx);

    egui::CentralPanel::default()
        .frame(egui::Frame::new().inner_margin(egui::Margin::same(18)))
        .show(ctx, |ui| {
            egui::Frame::new()
                .fill(egui::Color32::from_rgb(27, 31, 38))
                .corner_radius(egui::CornerRadius::same(12))
                .inner_margin(egui::Margin::symmetric(14, 12))
                .show(ui, |ui| {
                    ui.horizontal_wrapped(|ui| {
                        ui.heading("p2panda File Sharing");
                        ui.add_space(8.0);
                        if ui_state.global_paused {
                            ui.colored_label(
                                egui::Color32::from_rgb(242, 190, 85),
                                "All transfers paused",
                            );
                        } else {
                            ui.colored_label(egui::Color32::from_rgb(107, 206, 168), "Live");
                        }
                    });
                    ui.add_space(8.0);

                    ui.horizontal_wrapped(|ui| {
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

                        let has_completed = transfers
                            .transfers()
                            .iter()
                            .any(|transfer| matches!(transfer.status, TransferStatus::Completed));
                        if ui
                            .add_enabled(has_completed, egui::Button::new("Clear completed"))
                            .clicked()
                        {
                            let has_completed_upload =
                                transfers.transfers().iter().any(is_serving_share);
                            if has_completed_upload {
                                ui_state.confirm_clear_completed = true;
                            } else {
                                clear_completed_transfers(&mut transfers);
                            }
                        }

                        ui.separator();
                        if ui
                            .selectable_label(ui_state.show_contacts_view, "Contacts")
                            .clicked()
                        {
                            ui_state.show_contacts_view = !ui_state.show_contacts_view;
                            if ui_state.show_contacts_view {
                                ui_state.show_settings_view = false;
                                ui_state.show_diagnostics_view = false;
                            }
                        }

                        if ui
                            .selectable_label(ui_state.show_settings_view, "Settings")
                            .clicked()
                        {
                            ui_state.show_settings_view = !ui_state.show_settings_view;
                            if ui_state.show_settings_view {
                                ui_state.show_contacts_view = false;
                                ui_state.show_diagnostics_view = false;
                            }
                        }

                        if ui
                            .selectable_label(ui_state.show_diagnostics_view, "Diagnostics")
                            .clicked()
                        {
                            ui_state.show_diagnostics_view = !ui_state.show_diagnostics_view;
                            if ui_state.show_diagnostics_view {
                                ui_state.show_contacts_view = false;
                                ui_state.show_settings_view = false;
                            }
                        }
                    });
                });

            ui.add_space(10.0);
            if let Some(error) = &ui_state.drag_drop_error {
                ui.colored_label(egui::Color32::from_rgb(240, 119, 119), error);
                ui.add_space(6.0);
            }

            if ui_state.show_contacts_view {
                render_contacts_view(
                    ui,
                    &mut ui_state,
                    &mut contacts_store,
                    &mut profile_store,
                    &mut transfers,
                    &bridge,
                );
            } else if ui_state.show_settings_view {
                render_settings_view(
                    ui,
                    &mut ui_state,
                    &mut profile_store,
                    &mut settings_store,
                    &mut updater,
                    dialog_busy,
                );
            } else if ui_state.show_diagnostics_view {
                render_diagnostics_view(ui, &ui_state, &transfers);
            } else {
                render_transfer_list_header(ui, transfers.transfers().len());
                ui.add_space(4.0);
                egui::ScrollArea::vertical()
                    .id_salt("transfer-list-scroll")
                    .show(ui, |ui| {
                        if transfers.transfers().is_empty() {
                            render_empty_state(ui);
                        }

                        let mut pending_actions = Vec::new();
                        for transfer in transfers.transfers() {
                            if let Some(action) =
                                render_transfer_row(ui, transfer, ui_state.global_paused)
                            {
                                pending_actions.push(action);
                            }
                            ui.add_space(8.0);
                        }

                        for action in pending_actions {
                            apply_transfer_action(&mut transfers, &mut ui_state, &bridge, action);
                        }
                    });
            }
        });

    if ui_state.download_dialog_open {
        render_download_dialog(ctx, &mut ui_state, &mut transfers, &bridge);
    }

    render_share_removal_confirmation(ctx, &mut ui_state, &mut transfers, &bridge);
    render_clear_completed_confirmation(ctx, &mut ui_state, &mut transfers, &bridge);

    if ui_state.drag_drop_active {
        render_drag_drop_overlay(ctx);
    }
}

pub fn render_transfer_ui(
    params: UiSystemParams,
    drag_and_drop_events: EventReader<FileDragAndDrop>,
) {
    ui_system(params, drag_and_drop_events);
}

fn apply_app_theme(ctx: &egui::Context) {
    let mut style = (*ctx.style()).clone();
    style.spacing.item_spacing = egui::vec2(10.0, 8.0);
    style.spacing.button_padding = egui::vec2(10.0, 6.0);
    style.spacing.window_margin = egui::Margin::symmetric(14, 12);
    style.visuals.panel_fill = egui::Color32::from_rgb(20, 24, 30);
    style.visuals.widgets.noninteractive.bg_fill = egui::Color32::from_rgb(24, 28, 35);
    style.visuals.widgets.inactive.bg_fill = egui::Color32::from_rgb(44, 52, 63);
    style.visuals.widgets.hovered.bg_fill = egui::Color32::from_rgb(59, 71, 86);
    style.visuals.widgets.active.bg_fill = egui::Color32::from_rgb(68, 82, 99);
    style.visuals.widgets.open.bg_fill = egui::Color32::from_rgb(54, 65, 79);
    style.visuals.selection.bg_fill = egui::Color32::from_rgb(48, 98, 132);
    style.visuals.widgets.noninteractive.corner_radius = egui::CornerRadius::same(8);
    style.visuals.widgets.inactive.corner_radius = egui::CornerRadius::same(8);
    style.visuals.widgets.hovered.corner_radius = egui::CornerRadius::same(8);
    style.visuals.widgets.active.corner_radius = egui::CornerRadius::same(8);
    style.visuals.window_corner_radius = egui::CornerRadius::same(10);
    ctx.set_style(style);
}

fn render_transfer_list_header(ui: &mut egui::Ui, total: usize) {
    ui.horizontal(|ui| {
        ui.heading("Transfers");
        ui.add_space(8.0);
        ui.label(format!("{total} total"));
    });
    ui.label("Share and download activity, integrity state, and transfer controls.");
}

fn render_empty_state(ui: &mut egui::Ui) {
    egui::Frame::new()
        .fill(egui::Color32::from_rgb(24, 29, 36))
        .corner_radius(egui::CornerRadius::same(10))
        .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(49, 59, 73)))
        .inner_margin(egui::Margin::same(14))
        .show(ui, |ui| {
            ui.heading("No transfers yet");
            ui.label(
                "Use Share Directory to publish files, or Download to fetch from a share code.",
            );
        });
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

fn render_contacts_view(
    ui: &mut egui::Ui,
    ui_state: &mut UiState,
    contacts_store: &mut ContactsStore,
    profile_store: &mut ProfileStore,
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
) {
    ui.heading("Contacts");
    ui.label(
        "Follow profile IDs, inspect published shares, and discover contacts through your network.",
    );
    ui.add_space(8.0);

    let mut should_follow = false;
    ui.horizontal_wrapped(|ui| {
        ui.label("Profile ID");
        ui.add(
            egui::TextEdit::singleline(&mut ui_state.contact_profile_id_input).desired_width(360.0),
        );
        ui.label("Nickname");
        ui.add(
            egui::TextEdit::singleline(&mut ui_state.contact_nickname_input).desired_width(180.0),
        );
        let can_follow = !ui_state.contact_profile_id_input.trim().is_empty();
        if ui
            .add_enabled(can_follow, egui::Button::new("Follow"))
            .clicked()
        {
            should_follow = true;
        }
    });

    if should_follow {
        let profile_id = ui_state.contact_profile_id_input.trim().to_owned();
        match follow_contact_from_ui(
            contacts_store,
            profile_store,
            profile_id.clone(),
            Some(ui_state.contact_nickname_input.clone()),
        ) {
            Ok(()) => {
                ui_state.selected_contact_profile_id = Some(profile_id);
                ui_state.contacts_view = ContactsView::Followed;
                ui_state.contact_profile_id_input.clear();
                ui_state.contact_nickname_input.clear();
                ui_state.contact_error = None;
            }
            Err(err) => {
                ui_state.contact_error = Some(err.to_string());
            }
        }
    }

    if let Some(error) = &ui_state.contact_error {
        ui.colored_label(egui::Color32::from_rgb(240, 119, 119), error);
        ui.add_space(6.0);
    }

    ui.horizontal(|ui| {
        ui.selectable_value(
            &mut ui_state.contacts_view,
            ContactsView::Followed,
            "Followed",
        );
        ui.selectable_value(
            &mut ui_state.contacts_view,
            ContactsView::Discovery,
            "Discovery",
        );
    });
    ui.add_space(8.0);

    match ui_state.contacts_view {
        ContactsView::Followed => render_followed_contacts_view(
            ui,
            ui_state,
            contacts_store,
            profile_store,
            transfers,
            bridge,
        ),
        ContactsView::Discovery => render_discovery_view(
            ui,
            ui_state,
            contacts_store,
            profile_store,
            transfers,
            bridge,
        ),
    }
}

fn render_followed_contacts_view(
    ui: &mut egui::Ui,
    ui_state: &mut UiState,
    contacts_store: &mut ContactsStore,
    profile_store: &mut ProfileStore,
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
) {
    if ui_state.selected_contact_profile_id.is_none() {
        ui_state.selected_contact_profile_id = contacts_store
            .contacts()
            .first()
            .map(|contact| contact.profile_id.clone());
    }

    if contacts_store.contacts().is_empty() {
        render_empty_contacts_state(ui);
        return;
    }

    let mut refresh_profile_id = None;
    let mut remove_profile_id = None;
    let mut download_request = None;

    ui.columns(2, |columns| {
        columns[0].heading("Followed");
        columns[0].add_space(4.0);
        for contact in contacts_store.contacts() {
            columns[0].group(|ui| {
                ui.horizontal(|ui| {
                    let selected = ui_state.selected_contact_profile_id.as_deref()
                        == Some(contact.profile_id.as_str());
                    if ui.selectable_label(selected, contact.label()).clicked() {
                        ui_state.selected_contact_profile_id = Some(contact.profile_id.clone());
                    }
                    if ui.button("Remove").clicked() {
                        remove_profile_id = Some(contact.profile_id.clone());
                    }
                });
                ui.small(truncate_hash(&contact.profile_id));
                if let Some(display_name) = contact.display_name() {
                    ui.small(format!("Name: {display_name}"));
                }
                let status = contact.last_error.as_deref().unwrap_or("Ready");
                ui.small(status);
            });
            columns[0].add_space(6.0);
        }

        columns[1].heading("Detail");
        columns[1].add_space(4.0);
        if let Some(profile_id) = ui_state.selected_contact_profile_id.as_deref() {
            if let Some(contact) = contacts_store.get(profile_id) {
                columns[1].horizontal(|ui| {
                    ui.heading(contact.label());
                    if ui.button("Copy ID").clicked() {
                        copy_text(ui.ctx(), &contact.profile_id);
                    }
                    if ui.button("Refresh").clicked() {
                        refresh_profile_id = Some(contact.profile_id.clone());
                    }
                });
                columns[1].monospace(&contact.profile_id);
                if let Some(display_name) = contact.display_name() {
                    columns[1].label(format!("Display name: {display_name}"));
                }
                columns[1].label(format!(
                    "Followed: {}",
                    format_unix_date(contact.followed_at)
                ));
                if let Some(last_error) = &contact.last_error {
                    columns[1].colored_label(egui::Color32::from_rgb(240, 119, 119), last_error);
                }

                columns[1].separator();
                columns[1].label("Published shares");
                if contact.cached_shares.is_empty() {
                    columns[1].small("No published shares discovered yet.");
                } else {
                    for share in &contact.cached_shares {
                        columns[1].group(|ui| {
                            ui.label(&share.share_name);
                            ui.horizontal_wrapped(|ui| {
                                ui.monospace(&share.share_code);
                                if ui.button("Copy").clicked() {
                                    copy_text(ui.ctx(), &share.share_code);
                                }
                                if ui.button("Download").clicked() {
                                    download_request = Some((
                                        share.share_code.clone(),
                                        ContactDownloadSource {
                                            profile_id: contact.profile_id.clone(),
                                            display_name: contact
                                                .display_name()
                                                .unwrap_or(contact.label())
                                                .to_owned(),
                                        },
                                    ));
                                }
                            });
                        });
                        columns[1].add_space(6.0);
                    }
                }
            }
        }
    });

    if let Some(profile_id) = refresh_profile_id {
        if let Err(err) = contacts_store.refresh_contact(&profile_id) {
            ui_state.contact_error = Some(err.to_string());
        } else {
            ui_state.contact_error = None;
        }
    }

    if let Some(profile_id) = remove_profile_id {
        match remove_contact_from_ui(contacts_store, profile_store, &profile_id) {
            Ok(()) => {
                if ui_state.selected_contact_profile_id.as_deref() == Some(profile_id.as_str()) {
                    ui_state.selected_contact_profile_id = contacts_store
                        .contacts()
                        .first()
                        .map(|contact| contact.profile_id.clone());
                }
                ui_state.contact_error = None;
            }
            Err(err) => {
                ui_state.contact_error = Some(err.to_string());
            }
        }
    }

    if let Some((share_code, source_contact)) = download_request {
        start_download_transfer_with_source(
            transfers,
            bridge,
            share_code,
            ui_state.selected_download_directory.clone(),
            Some(source_contact),
        );
    }
}

fn render_discovery_view(
    ui: &mut egui::Ui,
    ui_state: &mut UiState,
    contacts_store: &mut ContactsStore,
    profile_store: &mut ProfileStore,
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
) {
    ui.horizontal_wrapped(|ui| {
        ui.label("Search");
        ui.add(
            egui::TextEdit::singleline(&mut ui_state.discovery_search_input).desired_width(220.0),
        );
        ui.checkbox(&mut ui_state.discovery_has_shares_only, "Has shares only");
        ui.checkbox(&mut ui_state.discovery_show_followed, "Show followed");
    });

    ui.horizontal_wrapped(|ui| {
        ui.label("Sort");
        ui.selectable_value(
            &mut ui_state.discovery_sort,
            DiscoverySort::MutualCount,
            "Mutuals",
        );
        ui.selectable_value(
            &mut ui_state.discovery_sort,
            DiscoverySort::RecentlySeen,
            "Recent",
        );
        ui.selectable_value(
            &mut ui_state.discovery_sort,
            DiscoverySort::HasShares,
            "Has shares",
        );
    });
    ui.add_space(8.0);

    let discovered = contacts_store.discover_second_degree_profiles(
        ui_state.discovery_show_followed,
        ui_state.discovery_has_shares_only,
        &ui_state.discovery_search_input,
        ui_state.discovery_sort,
    );
    sync_selected_discovered_profile(ui_state, &discovered);

    if discovered.is_empty() {
        render_empty_discovery_state(ui);
        return;
    }

    let mut follow_request = None;
    let mut download_request = None;

    ui.columns(2, |columns| {
        columns[0].heading("Discovered");
        columns[0].add_space(4.0);
        for profile in &discovered {
            columns[0].group(|ui| {
                let selected = ui_state.selected_discovered_profile_id.as_deref()
                    == Some(profile.profile_id.as_str());
                if ui.selectable_label(selected, profile.label()).clicked() {
                    ui_state.selected_discovered_profile_id = Some(profile.profile_id.clone());
                }
                ui.small(truncate_hash(&profile.profile_id));
                ui.small(format!("Seen via {} contact(s)", profile.mutual_count));
                if profile.already_followed {
                    ui.small("Already followed");
                }
                ui.horizontal_wrapped(|ui| {
                    for source in &profile.source_contacts {
                        ui.small(format!("followed by {}", source.label));
                    }
                });
            });
            columns[0].add_space(6.0);
        }

        columns[1].heading("Profile");
        columns[1].add_space(4.0);
        if let Some(profile_id) = ui_state.selected_discovered_profile_id.as_deref() {
            if let Some(profile) = discovered
                .iter()
                .find(|profile| profile.profile_id == profile_id)
            {
                columns[1].horizontal(|ui| {
                    ui.heading(profile.label());
                    if ui.button("Copy ID").clicked() {
                        copy_text(ui.ctx(), &profile.profile_id);
                    }
                    if !profile.already_followed && ui.button("Follow").clicked() {
                        follow_request = Some(profile.profile_id.clone());
                    }
                });
                columns[1].monospace(&profile.profile_id);
                if let Some(display_name) = profile.display_name() {
                    columns[1].label(format!("Display name: {display_name}"));
                }
                if let Some(last_seen_at) = profile.last_seen_at {
                    columns[1].label(format!("Recently seen: {}", format_unix_date(last_seen_at)));
                }
                columns[1].horizontal_wrapped(|ui| {
                    ui.label("Context:");
                    for source in &profile.source_contacts {
                        ui.small(format!("followed by {}", source.label));
                    }
                });
                if let Some(last_error) = &profile.last_error {
                    columns[1].colored_label(egui::Color32::from_rgb(240, 119, 119), last_error);
                }

                columns[1].separator();
                columns[1].label("Published shares");
                render_discovered_profile_shares(&mut columns[1], profile, &mut download_request);
            }
        }
    });

    if let Some(profile_id) = follow_request {
        match follow_contact_from_ui(contacts_store, profile_store, profile_id.clone(), None) {
            Ok(()) => {
                ui_state.selected_contact_profile_id = Some(profile_id);
                ui_state.contacts_view = ContactsView::Followed;
                ui_state.contact_error = None;
            }
            Err(err) => {
                ui_state.contact_error = Some(err.to_string());
            }
        }
    }

    if let Some((share_code, source_contact)) = download_request {
        start_download_transfer_with_source(
            transfers,
            bridge,
            share_code,
            ui_state.selected_download_directory.clone(),
            Some(source_contact),
        );
    }
}

fn render_discovered_profile_shares(
    ui: &mut egui::Ui,
    profile: &DiscoveredProfile,
    download_request: &mut Option<(String, ContactDownloadSource)>,
) {
    if profile.cached_shares.is_empty() {
        ui.small("No cached share records available yet.");
        return;
    }

    for share in &profile.cached_shares {
        ui.group(|ui| {
            ui.label(&share.share_name);
            ui.horizontal_wrapped(|ui| {
                ui.monospace(&share.share_code);
                if ui.button("Copy").clicked() {
                    copy_text(ui.ctx(), &share.share_code);
                }
                if ui.button("Download").clicked() {
                    *download_request = Some((
                        share.share_code.clone(),
                        ContactDownloadSource {
                            profile_id: profile.profile_id.clone(),
                            display_name: profile
                                .display_name()
                                .unwrap_or(profile.label())
                                .to_owned(),
                        },
                    ));
                }
            });
        });
        ui.add_space(6.0);
    }
}

fn follow_contact_from_ui(
    contacts_store: &mut ContactsStore,
    profile_store: &mut ProfileStore,
    profile_id: String,
    nickname: Option<String>,
) -> anyhow::Result<()> {
    contacts_store.follow_contact(profile_id.clone(), nickname.clone())?;
    if let Err(err) = profile_store.follow_contact(profile_id.clone(), nickname) {
        let _ = contacts_store.remove_contact(&profile_id);
        return Err(err);
    }
    contacts_store.refresh_contact(&profile_id)?;
    Ok(())
}

fn remove_contact_from_ui(
    contacts_store: &mut ContactsStore,
    profile_store: &mut ProfileStore,
    profile_id: &str,
) -> anyhow::Result<()> {
    if !contacts_store.remove_contact(profile_id)? {
        anyhow::bail!("unknown contact {profile_id}");
    }
    profile_store.unfollow_contact(profile_id.to_owned())?;
    Ok(())
}

fn sync_selected_discovered_profile(ui_state: &mut UiState, discovered: &[DiscoveredProfile]) {
    let selected_is_still_visible = ui_state
        .selected_discovered_profile_id
        .as_deref()
        .and_then(|selected| {
            discovered
                .iter()
                .find(|profile| profile.profile_id == selected)
        })
        .is_some();
    if !selected_is_still_visible {
        ui_state.selected_discovered_profile_id =
            discovered.first().map(|profile| profile.profile_id.clone());
    }
}

fn render_empty_contacts_state(ui: &mut egui::Ui) {
    egui::Frame::new()
        .fill(egui::Color32::from_rgb(24, 29, 36))
        .corner_radius(egui::CornerRadius::same(10))
        .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(49, 59, 73)))
        .inner_margin(egui::Margin::same(14))
        .show(ui, |ui| {
            ui.heading("No contacts yet");
            ui.label("Paste a profile ID to follow someone and browse their published shares.");
        });
}

fn render_empty_discovery_state(ui: &mut egui::Ui) {
    egui::Frame::new()
        .fill(egui::Color32::from_rgb(24, 29, 36))
        .corner_radius(egui::CornerRadius::same(10))
        .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(49, 59, 73)))
        .inner_margin(egui::Margin::same(14))
        .show(ui, |ui| {
            ui.heading("No second-degree profiles yet");
            ui.label(
                "Refresh followed contacts or follow more people to expand the discovery graph.",
            );
        });
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
    profile_store: &mut ProfileStore,
    settings_store: &mut SettingsStore,
    updater: &mut UpdateController,
    dialog_busy: bool,
) {
    ui.heading("Settings");
    ui.label("Profile");
    ui.horizontal(|ui| {
        ui.label("Profile ID");
        ui.monospace(&profile_store.profile().profile_id);
        if ui.button("Copy ID").clicked() {
            copy_text(ui.ctx(), &profile_store.profile().profile_id);
        }
    });
    ui.horizontal(|ui| {
        ui.label("Display name");
        ui.add(
            egui::TextEdit::singleline(&mut ui_state.profile_display_name_input)
                .desired_width(260.0),
        );
        let can_save_name = !ui_state.profile_display_name_input.trim().is_empty()
            && ui_state.profile_display_name_input.trim() != profile_store.profile().display_name;
        if ui
            .add_enabled(can_save_name, egui::Button::new("Save"))
            .clicked()
        {
            match profile_store.update_display_name(ui_state.profile_display_name_input.clone()) {
                Ok(_) => {
                    ui_state.profile_display_name_input =
                        profile_store.profile().display_name.clone();
                    ui_state.profile_error = None;
                }
                Err(err) => {
                    ui_state.profile_error = Some(err.to_string());
                }
            }
        }
    });
    if let Some(warning) = profile_store.load_warning() {
        ui.colored_label(egui::Color32::YELLOW, warning);
    }
    if let Some(error) = &ui_state.profile_error {
        ui.colored_label(
            egui::Color32::RED,
            format!("Failed to save profile: {error}"),
        );
    }

    ui.separator();
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
    ui.heading("Discovery");
    if ui
        .checkbox(&mut ui_state.mdns_enabled, "Enable mDNS discovery")
        .changed()
    {
        match settings_store.set_mdns_enabled(ui_state.mdns_enabled) {
            Ok(changed) => {
                if changed {
                    ui_state.network_restart_required = true;
                }
                ui_state.settings_error = None;
            }
            Err(err) => {
                ui_state.settings_error = Some(err.to_string());
                ui_state.mdns_enabled = settings_store.settings().mdns_enabled;
            }
        }
    }
    ui.small("Restart the app after changing discovery settings.");

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
                    ui_state.network_restart_required = true;
                }
                ui_state.settings_error = None;
            }
            Err(err) => {
                ui_state.settings_error = Some(err.to_string());
            }
        }
    }

    if ui_state.network_restart_required {
        ui.colored_label(
            egui::Color32::YELLOW,
            "Restart required to apply network changes",
        );
    }

    ui.separator();
    ui.heading("Updates");
    ui.horizontal(|ui| {
        ui.label(format!("Current version: v{}", updater.current_version()));
        ui.label(format!(
            "Channel: {}",
            match updater.channel() {
                crate::settings::UpdateChannel::Stable => "Stable",
            }
        ));
    });

    let mut auto_update_checks = updater.auto_check_enabled();
    if ui
        .checkbox(&mut auto_update_checks, "Automatically check for updates")
        .changed()
    {
        if let Err(err) = settings_store.set_auto_update_checks(auto_update_checks) {
            ui_state.settings_error = Some(err.to_string());
        } else {
            updater.set_auto_check_enabled(auto_update_checks);
            ui_state.settings_error = None;
        }
    }

    ui.label(format!(
        "Last checked: {}",
        format_last_checked(updater.last_checked_unix_secs())
    ));

    let check_busy = matches!(updater.status(), UpdateStatus::Checking { .. });
    if ui
        .add_enabled(!check_busy, egui::Button::new("Check for updates"))
        .clicked()
    {
        updater.request_manual_check();
    }

    match updater.status().clone() {
        UpdateStatus::Idle => {
            ui.small("Updates are idle. Automatic checks run in the background when enabled.");
        }
        UpdateStatus::Checking { automatic } => {
            if automatic {
                ui.label("Checking for updates in the background...");
            } else {
                ui.label("Checking for updates...");
            }
        }
        UpdateStatus::UpToDate => {
            ui.colored_label(egui::Color32::from_rgb(107, 206, 168), "You're up to date.");
        }
        UpdateStatus::Available { update } => {
            ui.horizontal_wrapped(|ui| {
                ui.colored_label(
                    egui::Color32::from_rgb(120, 220, 180),
                    format!("Update available: v{}", update.version),
                );
                if ui.button("Download update").clicked() {
                    updater.request_download();
                }
                if ui.button("Remind me later").clicked() {
                    updater.defer_available_update();
                }
            });
            if !update.release_notes_url.is_empty() {
                ui.hyperlink_to("Release notes", &update.release_notes_url);
            }
            if !update.release_notes.trim().is_empty() {
                ui.code(release_notes_preview(&update.release_notes));
            }
        }
        UpdateStatus::Downloading {
            update,
            downloaded_bytes,
            total_bytes,
        } => {
            ui.label(format!("Downloading v{}...", update.version));
            let fraction = total_bytes
                .map(|total| {
                    if total == 0 {
                        0.0
                    } else {
                        (downloaded_bytes as f32 / total as f32).clamp(0.0, 1.0)
                    }
                })
                .unwrap_or(0.0);
            let text = total_bytes
                .map(|total| {
                    format!(
                        "{} / {}",
                        format_bytes(downloaded_bytes),
                        format_bytes(total)
                    )
                })
                .unwrap_or_else(|| format_bytes(downloaded_bytes));
            ui.add(egui::ProgressBar::new(fraction).text(text));
        }
        UpdateStatus::ReadyToInstall { update, .. } => {
            ui.horizontal_wrapped(|ui| {
                ui.colored_label(
                    egui::Color32::from_rgb(120, 220, 180),
                    format!("v{} downloaded and verified.", update.version),
                );
                if ui.button("Apply & Restart").clicked() {
                    updater.request_apply();
                }
            });
        }
        UpdateStatus::Installing { update } => {
            ui.label(format!("Installing v{} and restarting...", update.version));
        }
        UpdateStatus::Error(error) => {
            ui.colored_label(egui::Color32::RED, format!("Update failed: {error}"));
        }
    }
}

fn render_diagnostics_view(ui: &mut egui::Ui, ui_state: &UiState, transfers: &TransferRegistry) {
    ui.heading("Diagnostics");
    ui.label("Live network and transfer state for troubleshooting.");
    ui.separator();

    let snapshot = &ui_state.diagnostics_snapshot;
    ui.heading("Node identity");
    ui.monospace(format!("Node ID: {}", snapshot.node_identity.node_id));
    ui.label(format!(
        "Relay URL: {}",
        snapshot
            .node_identity
            .relay_url
            .as_deref()
            .unwrap_or("Disabled")
    ));
    if snapshot.node_identity.local_listen_addrs.is_empty() {
        ui.label("Listening addresses: none");
    } else {
        ui.label("Listening addresses:");
        for addr in &snapshot.node_identity.local_listen_addrs {
            ui.monospace(addr);
        }
    }

    ui.separator();
    ui.heading("Peer connections");
    if snapshot.peers.is_empty() {
        ui.label("No known peers");
    } else {
        egui::Grid::new("diagnostic-peers-grid")
            .num_columns(5)
            .striped(true)
            .show(ui, |ui| {
                ui.strong("Node");
                ui.strong("State");
                ui.strong("Discovery");
                ui.strong("Last seen");
                ui.strong("RTT");
                ui.end_row();
                for peer in &snapshot.peers {
                    ui.monospace(truncate_hash(&peer.node_id));
                    ui.label(match peer.state {
                        PeerConnectionState::Connected => "connected",
                        PeerConnectionState::Known => "known",
                        PeerConnectionState::Disconnected => "disconnected",
                    });
                    ui.label(match peer.discovered_via {
                        PeerDiscoveryMethod::Manual => "manual",
                        PeerDiscoveryMethod::Relay => "relay",
                        PeerDiscoveryMethod::Mdns => "mDNS",
                        PeerDiscoveryMethod::Unknown => "unknown",
                    });
                    ui.label(
                        peer.last_seen_unix_ms
                            .map(format_ago)
                            .unwrap_or_else(|| "n/a".into()),
                    );
                    ui.label(
                        peer.rtt_ms
                            .map(|rtt_ms| format!("{rtt_ms} ms"))
                            .unwrap_or_else(|| "n/a".into()),
                    );
                    ui.end_row();
                }
            });
    }

    ui.separator();
    ui.heading("Connection history");
    if snapshot.connection_history.is_empty() {
        ui.label("No recent connection events");
    } else {
        egui::ScrollArea::vertical()
            .id_salt("diagnostics-connection-history")
            .max_height(140.0)
            .show(ui, |ui| {
                for entry in snapshot.connection_history.iter().rev() {
                    let peer = entry
                        .peer_node_id
                        .as_deref()
                        .map(truncate_hash)
                        .unwrap_or_else(|| "-".into());
                    let duration = entry
                        .establish_ms
                        .map(|ms| format!(" in {ms} ms"))
                        .unwrap_or_default();
                    ui.monospace(format!(
                        "[{}] {} {} {}{}",
                        format_ago(entry.at_unix_ms),
                        entry.event,
                        peer,
                        entry.detail,
                        duration
                    ));
                }
            });
    }

    ui.separator();
    ui.heading("Transfer stats");
    let active_transfers = transfers
        .transfers()
        .iter()
        .filter(|transfer| matches!(transfer.status, TransferStatus::Active))
        .count();
    let completed_transfers = transfers
        .transfers()
        .iter()
        .filter(|transfer| matches!(transfer.status, TransferStatus::Completed))
        .count();
    let failed_transfers = transfers
        .transfers()
        .iter()
        .filter(|transfer| matches!(transfer.status, TransferStatus::Error(_)))
        .count();
    ui.label(format!(
        "Bytes received (session): {}",
        format_bytes(ui_state.diagnostics_total_bytes_received)
    ));
    ui.label(format!(
        "Bytes sent (session): {}",
        format_bytes(ui_state.diagnostics_total_bytes_sent)
    ));
    ui.label(format!("Active transfers: {active_transfers}"));
    ui.label(format!("Completed transfers: {completed_transfers}"));
    ui.label(format!("Failed transfers: {failed_transfers}"));

    ui.separator();
    ui.heading("Error log");
    if snapshot.error_log.is_empty() {
        ui.label("No recent errors");
    } else {
        egui::ScrollArea::vertical()
            .id_salt("diagnostics-error-log")
            .max_height(120.0)
            .show(ui, |ui| {
                for entry in snapshot.error_log.iter().rev() {
                    ui.monospace(format!(
                        "[{}] {}",
                        format_ago(entry.at_unix_ms),
                        entry.message
                    ));
                }
            });
    }

    ui.separator();
    ui.heading("Gossip state");
    if snapshot.gossip_topics.is_empty() {
        ui.label("No active gossip topics");
    } else {
        for topic in &snapshot.gossip_topics {
            ui.monospace(format!(
                "{} peers={} topic={}",
                truncate_hash(&topic.topic_id),
                topic.peer_count,
                topic.topic_id
            ));
        }
    }
}

fn format_ago(at_unix_ms: u64) -> String {
    let now = now_unix_ms();
    if at_unix_ms >= now {
        return "just now".into();
    }
    let seconds = (now - at_unix_ms) / 1_000;
    if seconds < 60 {
        format!("{seconds}s ago")
    } else {
        format!("{}m ago", seconds / 60)
    }
}

fn render_transfer_row(
    ui: &mut egui::Ui,
    transfer: &Transfer,
    globally_paused: bool,
) -> Option<TransferAction> {
    let mut action = None;

    egui::Frame::new()
        .fill(egui::Color32::from_rgb(25, 32, 40))
        .corner_radius(egui::CornerRadius::same(10))
        .stroke(egui::Stroke::new(1.0, egui::Color32::from_rgb(50, 61, 75)))
        .inner_margin(egui::Margin::same(12))
        .show(ui, |ui| {
            ui.horizontal_wrapped(|ui| {
                let icon = match transfer.direction {
                    Direction::Upload => "UPLOAD",
                    Direction::Download => "DOWNLOAD",
                };
                ui.colored_label(
                    egui::Color32::from_rgb(146, 175, 205),
                    egui::RichText::new(icon).strong(),
                );
                ui.label(egui::RichText::new(&transfer.name).strong());
                ui.separator();

                let files_total = transfer.file_count();
                let files_done = transfer.completed_file_count();
                ui.label(format!("{files_done}/{files_total} files"));
                ui.separator();

                let bytes_per_sec = match transfer.direction {
                    Direction::Upload => transfer.outbound_bytes_per_sec,
                    Direction::Download => transfer.inbound_bytes_per_sec,
                };
                ui.label(format!(
                    "{} /s",
                    format_bytes(bytes_per_sec.max(0.0) as u64)
                ));
                ui.separator();
                let status = status_label(transfer, globally_paused);
                ui.colored_label(
                    status_color(transfer, globally_paused),
                    egui::RichText::new(status).strong(),
                );

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

                if can_remove_transfer(transfer) && ui.button("Remove").clicked() {
                    action = Some(TransferAction::Remove {
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

            if let Some((label, color)) = verification_badge(transfer) {
                ui.colored_label(color, label);
            }

            if matches!(transfer.direction, Direction::Download) && !transfer.files.is_empty() {
                egui::CollapsingHeader::new("Integrity details")
                    .default_open(false)
                    .show(ui, |ui| {
                        for file in &transfer.files {
                            ui.horizontal_wrapped(|ui| {
                                ui.label(file_integrity_label(file));
                                ui.label(file_verification_label(file));
                            });
                            if let FileVerification::Failed(message) = &file.verification {
                                ui.small(message);
                            }
                        }
                    });
            }

            if matches!(transfer.direction, Direction::Upload) {
                if let Some(code) = &transfer.share_code {
                    ui.horizontal(|ui| {
                        ui.label("Share code:");
                        let mut share_code_text = code.clone();
                        ui.add(
                            egui::TextEdit::singleline(&mut share_code_text).desired_width(280.0),
                        );
                        if ui.button("Copy").clicked() {
                            copy_text(ui.ctx(), code);
                        }
                    });
                }
            }
        });

    action
}

fn status_color(transfer: &Transfer, globally_paused: bool) -> egui::Color32 {
    if globally_paused && matches!(transfer.status, TransferStatus::Active) {
        return egui::Color32::from_rgb(242, 190, 85);
    }

    match transfer.status {
        TransferStatus::Error(_) => egui::Color32::from_rgb(238, 109, 109),
        TransferStatus::Completed => egui::Color32::from_rgb(107, 206, 168),
        TransferStatus::Cancelled => egui::Color32::from_rgb(173, 182, 194),
        TransferStatus::Paused => egui::Color32::from_rgb(242, 190, 85),
        TransferStatus::Pending | TransferStatus::Active => egui::Color32::from_rgb(146, 175, 205),
    }
}

fn render_progress_bar(ui: &mut egui::Ui, fraction: f32, downloaded_bytes: u64, total_bytes: u64) {
    ui.add(egui::ProgressBar::new(fraction).text(format!(
        "{:.1}% ({}/{})",
        fraction * 100.0,
        format_bytes(downloaded_bytes),
        format_bytes(total_bytes),
    )));
}

fn verification_badge(transfer: &Transfer) -> Option<(&'static str, egui::Color32)> {
    if !matches!(transfer.direction, Direction::Download) {
        return None;
    }

    let failed = transfer.failed_verification_count();
    if failed > 0 {
        return Some(("Verification failed", egui::Color32::RED));
    }

    if matches!(transfer.status, TransferStatus::Completed)
        && transfer.file_count() > 0
        && transfer.verified_file_count() == transfer.file_count()
    {
        return Some(("Verified", egui::Color32::from_rgb(75, 180, 100)));
    }

    None
}

fn file_integrity_label(file: &crate::state::FileProgress) -> String {
    format!("{} ({})", file.relative_path, format_bytes(file.total_size))
}

fn file_verification_label(file: &crate::state::FileProgress) -> &'static str {
    match &file.verification {
        FileVerification::Pending => "Pending",
        FileVerification::Verified => "Verified",
        FileVerification::Failed(_) => "Failed",
    }
}

fn truncate_hash(hash: &str) -> String {
    const PREFIX: usize = 10;
    const SUFFIX: usize = 10;
    if hash.len() <= PREFIX + SUFFIX + 3 {
        return hash.to_owned();
    }
    format!("{}...{}", &hash[..PREFIX], &hash[hash.len() - SUFFIX..])
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
    ui_state: &mut UiState,
    bridge: &AsyncBridge,
    action: TransferAction,
) {
    if let TransferAction::Remove { transfer_id } = action {
        if let Some(transfer) = transfers.get(transfer_id) {
            if matches!(transfer.direction, Direction::Upload) {
                ui_state.pending_share_removal = Some(transfer_id);
                return;
            }
        }
        let _ = transfers.remove(transfer_id);
        return;
    }

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
        TransferAction::Remove { .. } => unreachable!("handled above"),
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
            TransferAction::Remove { .. } => unreachable!("handled above"),
        };
    }
}

fn render_share_removal_confirmation(
    ctx: &egui::Context,
    ui_state: &mut UiState,
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
) {
    let Some(transfer_id) = ui_state.pending_share_removal else {
        return;
    };

    let transfer_name = transfers
        .get(transfer_id)
        .map(|transfer| transfer.name.clone())
        .unwrap_or_else(|| "this share".to_owned());

    let mut keep_open = true;
    let mut confirm = false;
    let mut cancel = false;

    egui::Window::new("Remove share?")
        .collapsible(false)
        .resizable(false)
        .open(&mut keep_open)
        .show(ctx, |ui| {
            ui.label(format!(
                "Removing '{transfer_name}' will stop serving this share, invalidate its share code, and delete associated blob data."
            ));
            ui.label("The original source directory is not modified.");
            ui.horizontal(|ui| {
                if ui.button("Cancel").clicked() {
                    cancel = true;
                }
                if ui.button("Remove share").clicked() {
                    confirm = true;
                }
            });
        });

    if confirm {
        remove_share_transfer(transfers, bridge, transfer_id);
        ui_state.pending_share_removal = None;
        return;
    }

    if cancel || !keep_open {
        ui_state.pending_share_removal = None;
    }
}

fn render_clear_completed_confirmation(
    ctx: &egui::Context,
    ui_state: &mut UiState,
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
) {
    if !ui_state.confirm_clear_completed {
        return;
    }

    let completed_uploads = transfers
        .transfers()
        .iter()
        .filter(|transfer| is_serving_share(transfer))
        .count();
    if completed_uploads == 0 {
        clear_completed_transfers(transfers);
        ui_state.confirm_clear_completed = false;
        return;
    }

    let mut keep_open = true;
    let mut confirm = false;
    let mut cancel = false;
    egui::Window::new("Clear completed transfers?")
        .collapsible(false)
        .resizable(false)
        .open(&mut keep_open)
        .show(ctx, |ui| {
            ui.label(format!(
                "This will remove completed downloads and stop serving {completed_uploads} completed share(s)."
            ));
            ui.horizontal(|ui| {
                if ui.button("Cancel").clicked() {
                    cancel = true;
                }
                if ui.button("Clear completed").clicked() {
                    confirm = true;
                }
            });
        });

    if confirm {
        clear_completed_transfers_with_share_cleanup(transfers, bridge);
        ui_state.confirm_clear_completed = false;
        return;
    }

    if cancel || !keep_open {
        ui_state.confirm_clear_completed = false;
    }
}

fn clear_completed_transfers(transfers: &mut TransferRegistry) {
    transfers.retain(|transfer| !matches!(transfer.status, TransferStatus::Completed));
}

fn clear_completed_transfers_with_share_cleanup(
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
) {
    let completed_ids: Vec<u64> = transfers
        .transfers()
        .iter()
        .filter(|transfer| matches!(transfer.status, TransferStatus::Completed))
        .map(|transfer| transfer.id)
        .collect();

    for transfer_id in completed_ids {
        let Some(transfer) = transfers.get(transfer_id).cloned() else {
            continue;
        };
        if is_serving_share(&transfer) {
            remove_share_transfer(transfers, bridge, transfer_id);
        } else {
            let _ = transfers.remove(transfer_id);
        }
    }
}

fn remove_share_transfer(transfers: &mut TransferRegistry, bridge: &AsyncBridge, transfer_id: u64) {
    if let Err(err) = bridge.send(NetworkCommand::RemoveShare { transfer_id }) {
        if let Some(transfer) = transfers.get_mut(transfer_id) {
            transfer.status = TransferStatus::Error(err.to_string());
        }
        return;
    }
    let _ = transfers.remove(transfer_id);
}

fn can_remove_transfer(transfer: &Transfer) -> bool {
    if matches!(transfer.direction, Direction::Upload) {
        return true;
    }
    matches!(
        transfer.status,
        TransferStatus::Completed | TransferStatus::Cancelled | TransferStatus::Error(_)
    )
}

fn is_serving_share(transfer: &Transfer) -> bool {
    matches!(
        (&transfer.direction, &transfer.status),
        (Direction::Upload, TransferStatus::Completed)
    )
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
    start_download_transfer_with_source(transfers, bridge, share_code, output_directory, None)
}

fn start_download_transfer_with_source(
    transfers: &mut TransferRegistry,
    bridge: &AsyncBridge,
    share_code: String,
    output_directory: PathBuf,
    source_contact: Option<ContactDownloadSource>,
) -> u64 {
    let transfer_id = transfers.allocate_id();
    transfers.push(Transfer::new(transfer_id, "Download", Direction::Download));

    if let Err(err) = bridge.send(NetworkCommand::StartDownload {
        transfer_id,
        share_code,
        output_directory,
        source_contact,
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

fn format_unix_date(timestamp: u64) -> String {
    format!("{timestamp}")
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
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Result;
    use flume::Sender;
    use p2panda_core::PrivateKey;
    use tempfile::tempdir;

    use super::*;
    use crate::bridge::{NetworkCommand, NetworkEvent};
    use crate::contacts::ContactsStore;
    use crate::profile::ProfileStore;
    use crate::state::FileProgress;

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

    fn write_node_key(data_dir: &std::path::Path, private_key: &PrivateKey) -> Result<()> {
        fs::create_dir_all(data_dir)?;
        fs::write(data_dir.join("node.key"), private_key.as_bytes())?;
        Ok(())
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
    fn verification_badge_reports_verified_and_failed_downloads() {
        let mut verified = Transfer::new(1, "ok", Direction::Download);
        verified.status = TransferStatus::Completed;
        verified.files = vec![FileProgress {
            relative_path: "ok".into(),
            total_size: 1,
            downloaded_bytes: 1,
            completed: true,
            verification: FileVerification::Verified,
        }];
        assert!(matches!(
            verification_badge(&verified),
            Some(("Verified", _))
        ));

        let mut failed = Transfer::new(2, "bad", Direction::Download);
        failed.status = TransferStatus::Error("verification failed".into());
        failed.files = vec![FileProgress {
            relative_path: "bad".into(),
            total_size: 1,
            downloaded_bytes: 1,
            completed: false,
            verification: FileVerification::Failed("hash mismatch".into()),
        }];
        assert!(matches!(
            verification_badge(&failed),
            Some(("Verification failed", _))
        ));
    }

    #[test]
    fn hash_truncation_keeps_prefix_and_suffix() {
        let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert_eq!(truncate_hash(hash), "0123456789...6789abcdef");
    }

    #[test]
    fn integrity_labels_include_file_name_and_size() {
        let file = FileProgress {
            relative_path: "nested/photo.jpg".into(),
            total_size: 2_048,
            downloaded_bytes: 2_048,
            completed: true,
            verification: FileVerification::Verified,
        };

        assert_eq!(file_integrity_label(&file), "nested/photo.jpg (2.0 KB)");
        assert_eq!(file_verification_label(&file), "Verified");
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
                                collection_hash:
                                    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                                        .into(),
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
                collection_hash: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                    .into(),
                total_bytes: 5,
                file_count: 1,
            }
        );
    }

    #[test]
    fn contact_download_transfer_sends_contact_metadata() {
        let expected_source = ContactDownloadSource {
            profile_id: "z6Mktestcontact".into(),
            display_name: "Alice".into(),
        };
        let expected_source_for_assert = expected_source.clone();
        let bridge = spawn_test_bridge(move |_, command, events| {
            let expected_source = expected_source_for_assert.clone();
            Box::pin(async move {
                match command {
                    NetworkCommand::StartDownload {
                        transfer_id,
                        source_contact,
                        ..
                    } => {
                        assert_eq!(source_contact, Some(expected_source));
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
        let transfer_id = start_download_transfer_with_source(
            &mut registry,
            &bridge,
            "p2p-CONTACT".to_owned(),
            PathBuf::from("/tmp/contact-download"),
            Some(expected_source),
        );

        let transfer = registry.get(transfer_id).unwrap();
        assert_eq!(transfer.direction, Direction::Download);
        assert_eq!(transfer.status, TransferStatus::Pending);
        assert_eq!(
            wait_for_event(&bridge),
            NetworkEvent::TransferCompleted { transfer_id }
        );
    }

    #[test]
    fn discovery_follow_action_updates_local_contacts_and_profile_graph() -> Result<()> {
        let data_dir = tempdir()?;
        let local_key = PrivateKey::new();
        let discovered_key = PrivateKey::new();
        write_node_key(data_dir.path(), &local_key)?;

        let mut contacts = ContactsStore::load(data_dir.path())?;
        let mut profile_store = ProfileStore::load_or_create(data_dir.path())?;

        follow_contact_from_ui(
            &mut contacts,
            &mut profile_store,
            discovered_key.public_key().to_string(),
            None,
        )?;

        assert!(contacts
            .get(&discovered_key.public_key().to_string())
            .is_some());
        assert!(profile_store
            .contact_follow_records()?
            .iter()
            .any(|record| {
                record.followed_profile_id == discovered_key.public_key().to_string()
                    && record.active
            }));

        Ok(())
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
    fn remove_button_rules_match_transfer_status() {
        let mut pending_download = Transfer::new(1, "pending", Direction::Download);
        pending_download.status = TransferStatus::Pending;
        assert!(!can_remove_transfer(&pending_download));

        let mut pending_upload = Transfer::new(2, "pending-share", Direction::Upload);
        pending_upload.status = TransferStatus::Pending;
        assert!(can_remove_transfer(&pending_upload));

        let mut active_upload = Transfer::new(3, "active-share", Direction::Upload);
        active_upload.status = TransferStatus::Active;
        assert!(can_remove_transfer(&active_upload));

        let mut paused_upload = Transfer::new(4, "paused-share", Direction::Upload);
        paused_upload.status = TransferStatus::Paused;
        assert!(can_remove_transfer(&paused_upload));

        let mut completed_download = Transfer::new(5, "done", Direction::Download);
        completed_download.status = TransferStatus::Completed;
        assert!(can_remove_transfer(&completed_download));

        let mut completed_upload = Transfer::new(6, "seed", Direction::Upload);
        completed_upload.status = TransferStatus::Completed;
        assert!(can_remove_transfer(&completed_upload));
        assert!(is_serving_share(&completed_upload));

        let mut cancelled = Transfer::new(7, "cancelled", Direction::Download);
        cancelled.status = TransferStatus::Cancelled;
        assert!(can_remove_transfer(&cancelled));

        let mut errored = Transfer::new(8, "errored", Direction::Download);
        errored.status = TransferStatus::Error("boom".into());
        assert!(can_remove_transfer(&errored));
    }

    #[test]
    fn clear_completed_removes_only_completed_transfers() {
        let mut registry = TransferRegistry::default();
        let mut completed = Transfer::new(1, "done", Direction::Download);
        completed.status = TransferStatus::Completed;
        let mut active = Transfer::new(2, "active", Direction::Download);
        active.status = TransferStatus::Active;
        let mut errored = Transfer::new(3, "error", Direction::Download);
        errored.status = TransferStatus::Error("boom".into());
        registry.push(completed);
        registry.push(active);
        registry.push(errored);

        clear_completed_transfers(&mut registry);

        assert!(registry.get(1).is_none());
        assert!(registry.get(2).is_some());
        assert!(registry.get(3).is_some());
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
