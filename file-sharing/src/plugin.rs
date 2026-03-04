use std::time::Duration;

use anyhow::{Context, Result};
use bevy::app::Plugin;
use bevy::prelude::{App, IntoSystemConfigs, Res, ResMut, Update};
use directories::ProjectDirs;
use flume::TryRecvError;

use crate::bridge::{AsyncBridge, NetworkEvent};
use crate::node::NodeOptions;
use crate::state::{Direction, FileProgress, Transfer, TransferRegistry, TransferStatus};
use crate::ui::{ui_system, UiState};

pub struct FileSharingPlugin;

impl Plugin for FileSharingPlugin {
    fn build(&self, app: &mut App) {
        let bridge = AsyncBridge::spawn_with_data_dir(
            NodeOptions::default(),
            resolve_data_dir().expect("failed to resolve file-sharing data directory"),
        )
        .expect("failed to initialize async bridge");

        app.insert_resource(bridge);
        app.insert_resource(TransferRegistry::default());
        app.insert_resource(UiState::default());
        app.add_systems(Update, (poll_network_events, ui_system).chain());
    }
}

pub(crate) fn resolve_data_dir() -> Result<std::path::PathBuf> {
    const APP_NAME: &str = "p2panda-file-sharing";
    ProjectDirs::from("", "", APP_NAME)
        .map(|dirs| dirs.data_dir().to_path_buf())
        .with_context(|| format!("failed to resolve app data directory for {APP_NAME}"))
}

pub fn poll_network_events(bridge: Res<AsyncBridge>, mut transfers: ResMut<TransferRegistry>) {
    let mut saw_progress = false;

    loop {
        match bridge.try_recv() {
            Ok(Some(event)) => {
                saw_progress |= apply_network_event(&mut transfers, event);
            }
            Ok(None) | Err(TryRecvError::Disconnected) => break,
            Err(TryRecvError::Empty) => break,
        }
    }

    if saw_progress {
        for transfer in transfers.transfers_mut() {
            if transfer.start_time.elapsed() >= Duration::from_millis(500) {
                transfer.update_bandwidth();
            }
        }
    }
}

fn apply_network_event(transfers: &mut TransferRegistry, event: NetworkEvent) -> bool {
    match event {
        NetworkEvent::ShareReady {
            transfer_id,
            share_code,
            total_bytes,
            file_count,
        } => {
            let transfer = get_or_insert_transfer(transfers, transfer_id, || {
                Transfer::new(transfer_id, "Recovered share", Direction::Upload)
            });
            transfer.share_code = Some(share_code);
            transfer.total_bytes = total_bytes;
            if transfer.files.is_empty() && file_count > 0 {
                transfer.files = (0..file_count)
                    .map(|index| FileProgress::new(format!("file-{index}"), 0))
                    .collect();
            }
            transfer.status = TransferStatus::Active;
            false
        }
        NetworkEvent::DownloadStarted {
            transfer_id,
            directory_name,
            total_bytes,
            file_count,
        } => {
            let transfer = get_or_insert_transfer(transfers, transfer_id, || {
                Transfer::new(transfer_id, &directory_name, Direction::Download)
            });
            transfer.name = directory_name;
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
                return true;
            }
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
        NetworkEvent::TransferCompleted { transfer_id } => {
            let transfer = get_or_insert_transfer(transfers, transfer_id, || {
                Transfer::new(transfer_id, "Recovered transfer", Direction::Download)
            });
            transfer.status = TransferStatus::Completed;
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
    if transfers.get(transfer_id).is_none() {
        transfers.push(make_transfer());
    }
    transfers
        .get_mut(transfer_id)
        .expect("transfer must exist after insertion")
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
    use crate::state::{Direction, Transfer};

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
            })
            .unwrap();

        let mut registry = TransferRegistry::default();
        let mut transfer = Transfer::new(41, "pending", Direction::Download);
        transfer.start_time = Instant::now() - Duration::from_secs(1);
        registry.push(transfer);

        let mut app = App::new();
        app.insert_resource(bridge);
        app.insert_resource(registry);

        std::thread::sleep(Duration::from_millis(50));
        run_poll(&mut app);

        let world = app.world();
        let registry = world.resource::<TransferRegistry>();
        let transfer = registry.get(41).unwrap();
        assert_eq!(transfer.name, "photos");
        assert_eq!(transfer.status, TransferStatus::Active);
        assert_eq!(transfer.total_bytes, 10);
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
            })
            .unwrap();

        let mut registry = TransferRegistry::default();
        let mut transfer = Transfer::new(7, "download", Direction::Download);
        transfer.start_time = Instant::now() - Duration::from_secs(2);
        registry.push(transfer);

        let mut app = App::new();
        app.insert_resource(bridge);
        app.insert_resource(registry);

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
                        share_code: "p2p-CODE".into(),
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
                    .send_async(NetworkEvent::FileCompleted {
                        transfer_id,
                        file_index: 0,
                    })
                    .await?;
                events
                    .send_async(NetworkEvent::TransferCompleted { transfer_id })
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

        std::thread::sleep(Duration::from_millis(50));
        run_poll(&mut app);

        let world = app.world();
        let transfer = world.resource::<TransferRegistry>().get(99).unwrap();
        assert_eq!(transfer.share_code.as_deref(), Some("p2p-CODE"));
        assert_eq!(transfer.status, TransferStatus::Error("late error".into()));
        assert_eq!(transfer.completed_file_count(), 1);
        assert_eq!(transfer.downloaded_bytes, 5);
        assert!(transfer.outbound_bytes_per_sec.is_finite());
        assert!(transfer.outbound_bytes_per_sec >= 0.0);
    }
}
