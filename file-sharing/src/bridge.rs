use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::{self, JoinHandle};

use anyhow::{Context, Result};
use bevy::prelude::Resource;
use flume::{Receiver, Sender, TryRecvError};

use crate::node::{AppNode, NodeOptions};

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkCommand {
    ShareDirectory {
        transfer_id: u64,
        directory_path: PathBuf,
    },
    StartDownload {
        transfer_id: u64,
        share_code: String,
        output_directory: PathBuf,
    },
    CancelTransfer {
        transfer_id: u64,
    },
}

impl NetworkCommand {
    fn transfer_id(&self) -> u64 {
        match self {
            Self::ShareDirectory { transfer_id, .. }
            | Self::StartDownload { transfer_id, .. }
            | Self::CancelTransfer { transfer_id } => *transfer_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkEvent {
    ShareReady {
        transfer_id: u64,
        share_code: String,
        total_bytes: u64,
        file_count: usize,
    },
    DownloadStarted {
        transfer_id: u64,
        directory_name: String,
        total_bytes: u64,
        file_count: usize,
    },
    FileDownloadProgress {
        transfer_id: u64,
        file_index: usize,
        bytes_downloaded: u64,
    },
    FileCompleted {
        transfer_id: u64,
        file_index: usize,
    },
    TransferCompleted {
        transfer_id: u64,
    },
    Error {
        transfer_id: u64,
        error_message: String,
    },
}

#[derive(Resource)]
pub struct AsyncBridge {
    command_tx: Sender<NetworkCommand>,
    event_rx: Receiver<NetworkEvent>,
    runtime_thread: Mutex<Option<JoinHandle<()>>>,
}

impl AsyncBridge {
    pub fn spawn(node_options: NodeOptions) -> Result<Self> {
        Self::spawn_with_worker(
            move || Box::pin(async move { AppNode::new(node_options).await }),
            |_node, command, events| Box::pin(default_handle_command(command, events)),
        )
    }

    pub(crate) fn spawn_with_worker<State, Init, Worker>(init: Init, worker: Worker) -> Result<Self>
    where
        State: Send + Sync + 'static,
        Init: FnOnce() -> BoxFuture<Result<State>> + Send + 'static,
        Worker: Fn(Arc<State>, NetworkCommand, Sender<NetworkEvent>) -> BoxFuture<Result<()>>
            + Send
            + Sync
            + 'static,
    {
        let (command_tx, command_rx) = flume::unbounded();
        let (event_tx, event_rx) = flume::unbounded();
        let (init_tx, init_rx) = std::sync::mpsc::sync_channel(1);
        let worker = Arc::new(worker);

        let runtime_thread = thread::Builder::new()
            .name("file-sharing-network".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        let _ = init_tx.send(Err(anyhow::Error::new(err)));
                        return;
                    }
                };

                runtime.block_on(async move {
                    let state = match init().await {
                        Ok(state) => {
                            let state = Arc::new(state);
                            let _ = init_tx.send(Ok(()));
                            state
                        }
                        Err(err) => {
                            let _ = init_tx.send(Err(err));
                            return;
                        }
                    };

                    run_network_loop(state, command_rx, event_tx, worker).await;
                });
            })
            .context("failed to spawn network bridge thread")?;

        init_rx
            .recv()
            .context("network bridge startup channel closed before initialization completed")??;

        Ok(Self {
            command_tx,
            event_rx,
            runtime_thread: Mutex::new(Some(runtime_thread)),
        })
    }

    pub fn send(&self, command: NetworkCommand) -> Result<()> {
        self.command_tx
            .send(command)
            .context("failed to send command to network thread")
    }

    pub fn try_recv(&self) -> Result<Option<NetworkEvent>, TryRecvError> {
        match self.event_rx.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn drain_events(&self) -> Vec<NetworkEvent> {
        let mut events = Vec::new();
        while let Ok(event) = self.event_rx.try_recv() {
            events.push(event);
        }
        events
    }
}

impl Drop for AsyncBridge {
    fn drop(&mut self) {
        let (replacement_tx, replacement_rx) = flume::unbounded();
        let old_tx = std::mem::replace(&mut self.command_tx, replacement_tx);
        drop(replacement_rx);
        drop(old_tx);

        if let Some(runtime_thread) = self.runtime_thread.lock().unwrap().take() {
            let _ = runtime_thread.join();
        }
    }
}

async fn run_network_loop<State, Worker>(
    state: Arc<State>,
    command_rx: Receiver<NetworkCommand>,
    event_tx: Sender<NetworkEvent>,
    worker: Arc<Worker>,
) where
    State: Send + Sync + 'static,
    Worker: Fn(Arc<State>, NetworkCommand, Sender<NetworkEvent>) -> BoxFuture<Result<()>>
        + Send
        + Sync
        + 'static,
{
    while let Ok(command) = command_rx.recv_async().await {
        let state = Arc::clone(&state);
        let event_tx = event_tx.clone();
        let worker = Arc::clone(&worker);

        tokio::spawn(async move {
            let transfer_id = command.transfer_id();
            if let Err(err) = worker(state, command, event_tx.clone()).await {
                let _ = event_tx.send(NetworkEvent::Error {
                    transfer_id,
                    error_message: err.to_string(),
                });
            }
        });
    }
}

async fn default_handle_command(
    command: NetworkCommand,
    event_tx: Sender<NetworkEvent>,
) -> Result<()> {
    let transfer_id = command.transfer_id();
    let action = match command {
        NetworkCommand::ShareDirectory { .. } => "share",
        NetworkCommand::StartDownload { .. } => "download",
        NetworkCommand::CancelTransfer { .. } => "cancel",
    };

    event_tx
        .send_async(NetworkEvent::Error {
            transfer_id,
            error_message: format!("{action} flow is not implemented yet"),
        })
        .await
        .context("failed to send placeholder bridge error event")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Result;

    use super::*;

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
    fn commands_sent_from_bevy_thread_are_received_by_network_thread() {
        let bridge = spawn_test_bridge(|_, command, events| {
            Box::pin(async move {
                match command {
                    NetworkCommand::ShareDirectory { transfer_id, .. } => {
                        events
                            .send_async(NetworkEvent::ShareReady {
                                transfer_id,
                                share_code: "p2p-TEST".into(),
                                total_bytes: 128,
                                file_count: 3,
                            })
                            .await?;
                    }
                    other => panic!("unexpected command: {other:?}"),
                }
                Ok(())
            })
        });

        bridge
            .send(NetworkCommand::ShareDirectory {
                transfer_id: 7,
                directory_path: PathBuf::from("/tmp/example"),
            })
            .unwrap();

        assert_eq!(
            wait_for_event(&bridge),
            NetworkEvent::ShareReady {
                transfer_id: 7,
                share_code: "p2p-TEST".into(),
                total_bytes: 128,
                file_count: 3,
            }
        );
    }

    #[test]
    fn events_are_receivable_via_try_recv_and_drain_without_blocking() {
        let bridge = spawn_test_bridge(|_, command, events| {
            Box::pin(async move {
                let transfer_id = command.transfer_id();
                events
                    .send_async(NetworkEvent::TransferCompleted { transfer_id })
                    .await?;
                Ok(())
            })
        });

        assert_eq!(bridge.try_recv().unwrap(), None);

        bridge
            .send(NetworkCommand::CancelTransfer { transfer_id: 11 })
            .unwrap();

        let events = {
            let started = std::time::Instant::now();
            loop {
                let events = bridge.drain_events();
                if !events.is_empty() {
                    break events;
                }

                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "timed out waiting to drain bridge events"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        };

        assert_eq!(
            events,
            vec![NetworkEvent::TransferCompleted { transfer_id: 11 }]
        );
        assert_eq!(bridge.try_recv().unwrap(), None);
    }

    #[test]
    fn multiple_commands_run_concurrently() {
        let bridge = spawn_test_bridge(|_, command, events| {
            Box::pin(async move {
                let (transfer_id, delay_ms) = match command {
                    NetworkCommand::ShareDirectory { transfer_id, .. } => (transfer_id, 200),
                    NetworkCommand::StartDownload { transfer_id, .. } => (transfer_id, 20),
                    NetworkCommand::CancelTransfer { transfer_id } => (transfer_id, 0),
                };

                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                events
                    .send_async(NetworkEvent::TransferCompleted { transfer_id })
                    .await?;
                Ok(())
            })
        });

        bridge
            .send(NetworkCommand::ShareDirectory {
                transfer_id: 1,
                directory_path: PathBuf::from("/tmp/slow"),
            })
            .unwrap();
        bridge
            .send(NetworkCommand::StartDownload {
                transfer_id: 2,
                share_code: "p2p-FAST".into(),
                output_directory: PathBuf::from("/tmp/fast"),
            })
            .unwrap();

        let first = wait_for_event(&bridge);
        let second = wait_for_event(&bridge);

        assert_eq!(first, NetworkEvent::TransferCompleted { transfer_id: 2 });
        assert_eq!(second, NetworkEvent::TransferCompleted { transfer_id: 1 });
    }
}
