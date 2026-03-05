use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::{self, JoinHandle};
use tokio::task::JoinHandle as TokioJoinHandle;

use anyhow::{Context, Result};
use bevy::prelude::Resource;
use flume::{Receiver, Sender, TryRecvError};
use iroh_blobs::hashseq::HashSeq;
use p2panda_blobs::Hash as BlobHash;
use tracing::warn;

use crate::download::{download_share_with_progress, DownloadEvent};
use crate::node::{AppNode, NodeOptions};
use crate::persist::{
    resume_active_transfers, resume_share_record, DownloadRecord, RecoveredShare, ShareRecord,
    StateStore,
};
use crate::settings::load_settings;
use crate::share::{share_directory, ShareSession};
use crate::share_code::decode_share_code;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkCommand {
    RecoverStartup,
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
    PauseTransfer {
        transfer_id: u64,
    },
    ResumeTransfer {
        transfer_id: u64,
    },
    PauseAll,
    ResumeAll,
}

impl NetworkCommand {
    fn transfer_id(&self) -> u64 {
        match self {
            Self::RecoverStartup => u64::MAX,
            Self::PauseAll => u64::MAX - 1,
            Self::ResumeAll => u64::MAX - 2,
            Self::ShareDirectory { transfer_id, .. }
            | Self::StartDownload { transfer_id, .. }
            | Self::CancelTransfer { transfer_id }
            | Self::PauseTransfer { transfer_id }
            | Self::ResumeTransfer { transfer_id } => *transfer_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkEvent {
    ShareReady {
        transfer_id: u64,
        directory_name: String,
        share_code: String,
        collection_hash: String,
        total_bytes: u64,
        file_count: usize,
    },
    DownloadStarted {
        transfer_id: u64,
        directory_name: String,
        collection_hash: String,
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
    FileVerificationFailed {
        transfer_id: u64,
        file_index: usize,
        error_message: String,
    },
    TransferCompleted {
        transfer_id: u64,
    },
    TransferCancelled {
        transfer_id: u64,
    },
    Error {
        transfer_id: u64,
        error_message: String,
    },
    TransferPaused {
        transfer_id: u64,
    },
    TransferResumed {
        transfer_id: u64,
    },
    GlobalPauseChanged {
        paused: bool,
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
        let data_dir = super::plugin::resolve_data_dir()?;
        Self::spawn_with_data_dir(node_options, data_dir)
    }

    pub fn spawn_with_data_dir(node_options: NodeOptions, data_dir: PathBuf) -> Result<Self> {
        let bridge = Self::spawn_with_worker(
            move || Box::pin(async move { RuntimeState::new(data_dir, node_options).await }),
            |state, command, events| Box::pin(default_handle_command(state, command, events)),
        )?;
        bridge.send(NetworkCommand::RecoverStartup)?;
        Ok(bridge)
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
    let active_transfer_commands: Arc<
        tokio::sync::Mutex<HashMap<u64, tokio::task::JoinHandle<()>>>,
    > = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    while let Ok(command) = command_rx.recv_async().await {
        if let NetworkCommand::CancelTransfer { transfer_id } = &command {
            if let Some(handle) = active_transfer_commands.lock().await.remove(transfer_id) {
                handle.abort();
            }
        }

        let state = Arc::clone(&state);
        let event_tx = event_tx.clone();
        let worker = Arc::clone(&worker);
        let active_transfer_commands = Arc::clone(&active_transfer_commands);
        let active_transfer_commands_for_task = Arc::clone(&active_transfer_commands);
        let transfer_id = command.transfer_id();
        let track_command = matches!(
            command,
            NetworkCommand::ShareDirectory { .. } | NetworkCommand::StartDownload { .. }
        );

        let handle = tokio::spawn(async move {
            if let Err(err) = worker(state, command, event_tx.clone()).await {
                let _ = event_tx.send(NetworkEvent::Error {
                    transfer_id,
                    error_message: err.to_string(),
                });
            }

            if track_command {
                active_transfer_commands_for_task
                    .lock()
                    .await
                    .remove(&transfer_id);
            }
        });

        if track_command {
            active_transfer_commands
                .lock()
                .await
                .insert(transfer_id, handle);
        }
    }
}

struct RuntimeState {
    node: AppNode,
    inner: tokio::sync::Mutex<RuntimeInner>,
    _settings: crate::settings::AppSettings,
    next_recovery_transfer_id: AtomicU64,
}

struct RuntimeInner {
    store: StateStore,
    global_paused: bool,
    live_shares: HashMap<u64, ShareSession>,
    recovered_shares: HashMap<u64, RecoveredShare>,
    globally_paused_shares: HashMap<u64, ShareRecord>,
    paused_shares: HashMap<u64, ShareRecord>,
    active_downloads: HashMap<u64, ActiveDownload>,
    globally_paused_downloads: HashMap<u64, DownloadRecord>,
    paused_downloads: HashMap<u64, DownloadRecord>,
}

struct ActiveDownload {
    record: DownloadRecord,
    handle: TokioJoinHandle<()>,
}

impl RuntimeState {
    async fn new(data_dir: PathBuf, node_options: NodeOptions) -> Result<Self> {
        let settings = load_settings(&data_dir)?;
        let node = AppNode::with_data_dir(data_dir, node_options).await?;
        let store = StateStore::load(&node.data_dir)?;
        Ok(Self {
            node,
            inner: tokio::sync::Mutex::new(RuntimeInner {
                store,
                global_paused: false,
                live_shares: HashMap::new(),
                recovered_shares: HashMap::new(),
                globally_paused_shares: HashMap::new(),
                paused_shares: HashMap::new(),
                active_downloads: HashMap::new(),
                globally_paused_downloads: HashMap::new(),
                paused_downloads: HashMap::new(),
            }),
            _settings: settings,
            next_recovery_transfer_id: AtomicU64::new(1_000_000),
        })
    }

    fn next_recovery_transfer_id(&self) -> u64 {
        self.next_recovery_transfer_id
            .fetch_add(1, Ordering::Relaxed)
    }
}

async fn default_handle_command(
    state: Arc<RuntimeState>,
    command: NetworkCommand,
    event_tx: Sender<NetworkEvent>,
) -> Result<()> {
    match command {
        NetworkCommand::RecoverStartup => recover_startup_state(&state, &event_tx).await,
        NetworkCommand::ShareDirectory {
            transfer_id,
            directory_path,
        } => {
            let session = share_directory(&state.node, directory_path).await?;
            event_tx
                .send(NetworkEvent::ShareReady {
                    transfer_id,
                    directory_name: session
                        .source_dir
                        .file_name()
                        .and_then(|name| name.to_str())
                        .filter(|name| !name.is_empty())
                        .unwrap_or("Shared directory")
                        .to_owned(),
                    share_code: session.share_code.clone(),
                    collection_hash: session.collection_hash.to_string(),
                    total_bytes: session.total_bytes,
                    file_count: session.file_count(),
                })
                .context("failed to send ShareReady event")?;

            let mut runtime = state.inner.lock().await;
            runtime.store.add_share(ShareRecord::from(&session))?;
            runtime.live_shares.insert(transfer_id, session);
            Ok(())
        }
        NetworkCommand::StartDownload {
            transfer_id,
            share_code,
            output_directory,
        } => {
            let decoded = decode_share_code(&share_code)?;
            let record =
                DownloadRecord::new(share_code, output_directory, decoded.collection_hash());
            start_download_task(state, transfer_id, record, event_tx).await
        }
        NetworkCommand::CancelTransfer { transfer_id } => {
            let mut runtime = state.inner.lock().await;

            if let Some(session) = runtime.live_shares.remove(&transfer_id) {
                state
                    .node
                    .blobs
                    .block_serving_hashes(share_hashes_for_session(&session));
                runtime.store.remove_share_by_code(&session.share_code)?;
            } else if let Some(session) = runtime.recovered_shares.remove(&transfer_id) {
                let hashes = share_hashes_for_record(&state.node, &session.record).await;
                state.node.blobs.block_serving_hashes(hashes);
                runtime
                    .store
                    .remove_share_by_code(&session.record.share_code)?;
            } else if let Some(active) = runtime.active_downloads.remove(&transfer_id) {
                active.handle.abort();
                runtime.store.remove_download_by_code(
                    &active.record.share_code,
                    &active.record.output_dir,
                )?;
            } else if let Some(record) = runtime.paused_shares.remove(&transfer_id) {
                runtime.store.remove_share_by_code(&record.share_code)?;
            } else if let Some(record) = runtime.globally_paused_shares.remove(&transfer_id) {
                runtime.store.remove_share_by_code(&record.share_code)?;
            } else if let Some(record) = runtime.paused_downloads.remove(&transfer_id) {
                runtime
                    .store
                    .remove_download_by_code(&record.share_code, &record.output_dir)?;
            } else if let Some(record) = runtime.globally_paused_downloads.remove(&transfer_id) {
                runtime
                    .store
                    .remove_download_by_code(&record.share_code, &record.output_dir)?;
            }
            drop(runtime);
            event_tx
                .send(NetworkEvent::TransferCancelled { transfer_id })
                .context("failed to send TransferCancelled event")?;
            Ok(())
        }
        NetworkCommand::PauseTransfer { transfer_id } => {
            pause_transfer(state, transfer_id, event_tx).await
        }
        NetworkCommand::ResumeTransfer { transfer_id } => {
            resume_transfer(state, transfer_id, event_tx).await
        }
        NetworkCommand::PauseAll => pause_all_transfers(state, event_tx).await,
        NetworkCommand::ResumeAll => resume_all_transfers(state, event_tx).await,
    }
}

async fn start_download_task(
    state: Arc<RuntimeState>,
    transfer_id: u64,
    record: DownloadRecord,
    event_tx: Sender<NetworkEvent>,
) -> Result<()> {
    {
        let mut runtime = state.inner.lock().await;
        runtime.store.add_download(record.clone())?;
    }

    let task_state = Arc::clone(&state);
    let task_record = record.clone();
    let task_event_tx = event_tx.clone();
    let handle = tokio::spawn(async move {
        let result = download_share_with_progress(
            &task_state.node,
            &task_record.share_code,
            &task_record.output_dir,
            |event| emit_download_event(&task_event_tx, transfer_id, event),
        )
        .await;

        let mut runtime = task_state.inner.lock().await;
        runtime.active_downloads.remove(&transfer_id);

        match result {
            Ok(_) => {
                let _ = runtime
                    .store
                    .remove_download_by_code(&task_record.share_code, &task_record.output_dir);
                runtime.paused_downloads.remove(&transfer_id);
            }
            Err(err) => {
                let _ = task_event_tx.send(NetworkEvent::Error {
                    transfer_id,
                    error_message: err.to_string(),
                });
            }
        }
    });

    let mut runtime = state.inner.lock().await;
    runtime
        .active_downloads
        .insert(transfer_id, ActiveDownload { record, handle });

    Ok(())
}

async fn pause_transfer(
    state: Arc<RuntimeState>,
    transfer_id: u64,
    event_tx: Sender<NetworkEvent>,
) -> Result<()> {
    let mut runtime = state.inner.lock().await;

    if let Some(session) = runtime.live_shares.remove(&transfer_id) {
        let blocked_hashes = share_hashes_for_session(&session);
        state.node.blobs.block_serving_hashes(blocked_hashes);
        let mut record = ShareRecord::from(&session);
        record.paused = true;
        runtime.store.add_share(record.clone())?;
        runtime.paused_shares.insert(transfer_id, record);
        event_tx
            .send(NetworkEvent::TransferPaused { transfer_id })
            .context("failed to send TransferPaused event")?;
        return Ok(());
    }

    if let Some(recovered) = runtime.recovered_shares.remove(&transfer_id) {
        let mut record = recovered.record;
        let blocked_hashes = share_hashes_for_record(&state.node, &record).await;
        state.node.blobs.block_serving_hashes(blocked_hashes);
        record.paused = true;
        runtime.store.add_share(record.clone())?;
        runtime.paused_shares.insert(transfer_id, record);
        event_tx
            .send(NetworkEvent::TransferPaused { transfer_id })
            .context("failed to send TransferPaused event")?;
        return Ok(());
    }

    if let Some(active) = runtime.active_downloads.remove(&transfer_id) {
        active.handle.abort();
        let mut record = active.record;
        record.paused = true;
        runtime.store.add_download(record.clone())?;
        runtime.paused_downloads.insert(transfer_id, record);
        event_tx
            .send(NetworkEvent::TransferPaused { transfer_id })
            .context("failed to send TransferPaused event")?;
        return Ok(());
    }

    if runtime.paused_shares.contains_key(&transfer_id)
        || runtime.paused_downloads.contains_key(&transfer_id)
    {
        return Ok(());
    }

    if let Some(mut record) = runtime.globally_paused_shares.remove(&transfer_id) {
        record.paused = true;
        runtime.store.add_share(record.clone())?;
        runtime.paused_shares.insert(transfer_id, record);
        event_tx
            .send(NetworkEvent::TransferPaused { transfer_id })
            .context("failed to send TransferPaused event")?;
        return Ok(());
    }

    if let Some(mut record) = runtime.globally_paused_downloads.remove(&transfer_id) {
        record.paused = true;
        runtime.store.add_download(record.clone())?;
        runtime.paused_downloads.insert(transfer_id, record);
        event_tx
            .send(NetworkEvent::TransferPaused { transfer_id })
            .context("failed to send TransferPaused event")?;
        return Ok(());
    }

    Err(anyhow::anyhow!("unknown transfer id {transfer_id}"))
}

async fn resume_transfer(
    state: Arc<RuntimeState>,
    transfer_id: u64,
    event_tx: Sender<NetworkEvent>,
) -> Result<()> {
    if let Some(mut record) = {
        let mut runtime = state.inner.lock().await;
        runtime.paused_shares.remove(&transfer_id)
    } {
        let blocked_hashes = share_hashes_for_record(&state.node, &record).await;
        let globally_paused = {
            let runtime = state.inner.lock().await;
            runtime.global_paused
        };
        record.paused = false;
        if globally_paused {
            let mut runtime = state.inner.lock().await;
            runtime.store.add_share(record.clone())?;
            runtime.globally_paused_shares.insert(transfer_id, record);
            event_tx
                .send(NetworkEvent::TransferResumed { transfer_id })
                .context("failed to send TransferResumed event")?;
            return Ok(());
        }
        state
            .node
            .blobs
            .unblock_serving_hashes(blocked_hashes.iter().copied());
        let recovered = match resume_share_record(&state.node, &record).await {
            Ok(recovered) => recovered,
            Err(err) => {
                state.node.blobs.block_serving_hashes(blocked_hashes);
                return Err(err);
            }
        };
        {
            let mut runtime = state.inner.lock().await;
            runtime.store.add_share(record.clone())?;
            runtime.recovered_shares.insert(transfer_id, recovered);
        }
        event_tx
            .send(NetworkEvent::TransferResumed { transfer_id })
            .context("failed to send TransferResumed event")?;
        event_tx
            .send(NetworkEvent::ShareReady {
                transfer_id,
                directory_name: record.directory_name,
                share_code: record.share_code,
                collection_hash: record.collection_hash,
                total_bytes: record.total_bytes,
                file_count: record.file_count,
            })
            .context("failed to send ShareReady event for resumed share")?;
        return Ok(());
    }

    if let Some(mut record) = {
        let mut runtime = state.inner.lock().await;
        runtime.paused_downloads.remove(&transfer_id)
    } {
        record.paused = false;
        let globally_paused = {
            let runtime = state.inner.lock().await;
            runtime.global_paused
        };
        if globally_paused {
            let mut runtime = state.inner.lock().await;
            runtime.store.add_download(record.clone())?;
            runtime
                .globally_paused_downloads
                .insert(transfer_id, record);
            event_tx
                .send(NetworkEvent::TransferResumed { transfer_id })
                .context("failed to send TransferResumed event")?;
            return Ok(());
        }
        start_download_task(Arc::clone(&state), transfer_id, record, event_tx.clone()).await?;
        event_tx
            .send(NetworkEvent::TransferResumed { transfer_id })
            .context("failed to send TransferResumed event")?;
        return Ok(());
    }

    let runtime = state.inner.lock().await;
    if runtime.active_downloads.contains_key(&transfer_id)
        || runtime.live_shares.contains_key(&transfer_id)
        || runtime.recovered_shares.contains_key(&transfer_id)
        || runtime.globally_paused_shares.contains_key(&transfer_id)
        || runtime.globally_paused_downloads.contains_key(&transfer_id)
    {
        return Ok(());
    }

    Err(anyhow::anyhow!("unknown transfer id {transfer_id}"))
}

async fn pause_all_transfers(
    state: Arc<RuntimeState>,
    event_tx: Sender<NetworkEvent>,
) -> Result<()> {
    let mut runtime = state.inner.lock().await;

    if runtime.global_paused {
        event_tx
            .send(NetworkEvent::GlobalPauseChanged { paused: true })
            .context("failed to send GlobalPauseChanged event")?;
        return Ok(());
    }

    runtime.global_paused = true;
    runtime.store.set_global_paused(true)?;

    let live_share_ids: Vec<u64> = runtime.live_shares.keys().copied().collect();
    for transfer_id in live_share_ids {
        if let Some(session) = runtime.live_shares.remove(&transfer_id) {
            let blocked_hashes = share_hashes_for_session(&session);
            state.node.blobs.block_serving_hashes(blocked_hashes);
            let mut record = ShareRecord::from(&session);
            record.paused = false;
            runtime.store.add_share(record.clone())?;
            runtime.globally_paused_shares.insert(transfer_id, record);
            event_tx
                .send(NetworkEvent::TransferPaused { transfer_id })
                .context("failed to send TransferPaused event")?;
        }
    }

    let recovered_share_ids: Vec<u64> = runtime.recovered_shares.keys().copied().collect();
    for transfer_id in recovered_share_ids {
        if let Some(recovered) = runtime.recovered_shares.remove(&transfer_id) {
            let mut record = recovered.record;
            let blocked_hashes = share_hashes_for_record(&state.node, &record).await;
            state.node.blobs.block_serving_hashes(blocked_hashes);
            record.paused = false;
            runtime.store.add_share(record.clone())?;
            runtime.globally_paused_shares.insert(transfer_id, record);
            event_tx
                .send(NetworkEvent::TransferPaused { transfer_id })
                .context("failed to send TransferPaused event")?;
        }
    }

    let active_download_ids: Vec<u64> = runtime.active_downloads.keys().copied().collect();
    for transfer_id in active_download_ids {
        if let Some(active) = runtime.active_downloads.remove(&transfer_id) {
            active.handle.abort();
            let mut record = active.record;
            record.paused = false;
            runtime.store.add_download(record.clone())?;
            runtime
                .globally_paused_downloads
                .insert(transfer_id, record);
            event_tx
                .send(NetworkEvent::TransferPaused { transfer_id })
                .context("failed to send TransferPaused event")?;
        }
    }

    event_tx
        .send(NetworkEvent::GlobalPauseChanged { paused: true })
        .context("failed to send GlobalPauseChanged event")?;
    Ok(())
}

async fn resume_all_transfers(
    state: Arc<RuntimeState>,
    event_tx: Sender<NetworkEvent>,
) -> Result<()> {
    let mut runtime = state.inner.lock().await;

    if !runtime.global_paused {
        event_tx
            .send(NetworkEvent::GlobalPauseChanged { paused: false })
            .context("failed to send GlobalPauseChanged event")?;
        return Ok(());
    }

    runtime.global_paused = false;
    runtime.store.set_global_paused(false)?;

    let globally_paused_share_ids: Vec<u64> =
        runtime.globally_paused_shares.keys().copied().collect();
    for transfer_id in globally_paused_share_ids {
        if let Some(mut record) = runtime.globally_paused_shares.remove(&transfer_id) {
            if record.paused {
                runtime.paused_shares.insert(transfer_id, record);
                continue;
            }

            let blocked_hashes = share_hashes_for_record(&state.node, &record).await;
            state
                .node
                .blobs
                .unblock_serving_hashes(blocked_hashes.iter().copied());
            let recovered = match resume_share_record(&state.node, &record).await {
                Ok(recovered) => recovered,
                Err(err) => {
                    state.node.blobs.block_serving_hashes(blocked_hashes);
                    return Err(err);
                }
            };
            record.paused = false;
            runtime.store.add_share(record.clone())?;
            runtime.recovered_shares.insert(transfer_id, recovered);
            event_tx
                .send(NetworkEvent::TransferResumed { transfer_id })
                .context("failed to send TransferResumed event")?;
            event_tx
                .send(NetworkEvent::ShareReady {
                    transfer_id,
                    directory_name: record.directory_name,
                    share_code: record.share_code,
                    collection_hash: record.collection_hash,
                    total_bytes: record.total_bytes,
                    file_count: record.file_count,
                })
                .context("failed to send ShareReady event for resumed share")?;
        }
    }

    let globally_paused_download_ids: Vec<u64> =
        runtime.globally_paused_downloads.keys().copied().collect();
    for transfer_id in globally_paused_download_ids {
        if let Some(mut record) = runtime.globally_paused_downloads.remove(&transfer_id) {
            if record.paused {
                runtime.paused_downloads.insert(transfer_id, record);
                continue;
            }

            record.paused = false;
            start_download_task(Arc::clone(&state), transfer_id, record, event_tx.clone()).await?;
            event_tx
                .send(NetworkEvent::TransferResumed { transfer_id })
                .context("failed to send TransferResumed event")?;
        }
    }

    event_tx
        .send(NetworkEvent::GlobalPauseChanged { paused: false })
        .context("failed to send GlobalPauseChanged event")?;
    Ok(())
}

fn emit_download_event(event_tx: &Sender<NetworkEvent>, transfer_id: u64, event: DownloadEvent) {
    let network_event = match event {
        DownloadEvent::DownloadStarted {
            directory_name,
            collection_hash,
            total_bytes,
            file_count,
        } => NetworkEvent::DownloadStarted {
            transfer_id,
            directory_name,
            collection_hash,
            total_bytes,
            file_count,
        },
        DownloadEvent::FileDownloadProgress {
            file_index,
            bytes_downloaded,
        } => NetworkEvent::FileDownloadProgress {
            transfer_id,
            file_index,
            bytes_downloaded,
        },
        DownloadEvent::FileCompleted { file_index } => NetworkEvent::FileCompleted {
            transfer_id,
            file_index,
        },
        DownloadEvent::FileError {
            file_index,
            error_message,
        } => NetworkEvent::FileVerificationFailed {
            transfer_id,
            file_index,
            error_message,
        },
        DownloadEvent::TransferCompleted => NetworkEvent::TransferCompleted { transfer_id },
    };

    let _ = event_tx.send(network_event);
}

async fn recover_startup_state(
    state: &RuntimeState,
    event_tx: &Sender<NetworkEvent>,
) -> Result<()> {
    let persisted_state = {
        let runtime = state.inner.lock().await;
        runtime.store.state().clone()
    };

    {
        let mut runtime = state.inner.lock().await;
        runtime.global_paused = persisted_state.global_paused;
    }

    event_tx
        .send(NetworkEvent::GlobalPauseChanged {
            paused: persisted_state.global_paused,
        })
        .context("failed to send GlobalPauseChanged recovery event")?;

    if persisted_state.global_paused {
        let mut runtime = state.inner.lock().await;
        for share in &persisted_state.active_shares {
            let blocked_hashes = share_hashes_for_record(&state.node, share).await;
            state.node.blobs.block_serving_hashes(blocked_hashes);
            let transfer_id = state.next_recovery_transfer_id();
            event_tx
                .send(NetworkEvent::ShareReady {
                    transfer_id,
                    directory_name: share.directory_name.clone(),
                    share_code: share.share_code.clone(),
                    collection_hash: share.collection_hash.clone(),
                    total_bytes: share.total_bytes,
                    file_count: share.file_count,
                })
                .context("failed to send globally paused ShareReady event")?;
            event_tx
                .send(NetworkEvent::TransferPaused { transfer_id })
                .context("failed to send globally paused TransferPaused event")?;
            if share.paused {
                runtime.paused_shares.insert(transfer_id, share.clone());
            } else {
                runtime
                    .globally_paused_shares
                    .insert(transfer_id, share.clone());
            }
        }

        for download in &persisted_state.active_downloads {
            let transfer_id = state.next_recovery_transfer_id();
            event_tx
                .send(NetworkEvent::DownloadStarted {
                    transfer_id,
                    directory_name: "Download".into(),
                    collection_hash: download.collection_hash.clone(),
                    total_bytes: 0,
                    file_count: 0,
                })
                .context("failed to send globally paused DownloadStarted event")?;
            event_tx
                .send(NetworkEvent::TransferPaused { transfer_id })
                .context("failed to send globally paused download TransferPaused event")?;
            if download.paused {
                runtime
                    .paused_downloads
                    .insert(transfer_id, download.clone());
            } else {
                runtime
                    .globally_paused_downloads
                    .insert(transfer_id, download.clone());
            }
        }

        return Ok(());
    }

    let recovered = resume_active_transfers(&state.node, &persisted_state).await?;
    let mut runtime = state.inner.lock().await;

    for share in recovered.shares {
        let transfer_id = state.next_recovery_transfer_id();
        event_tx
            .send(NetworkEvent::ShareReady {
                transfer_id,
                directory_name: share.record.directory_name.clone(),
                share_code: share.record.share_code.clone(),
                collection_hash: share.record.collection_hash.clone(),
                total_bytes: share.record.total_bytes,
                file_count: share.record.file_count,
            })
            .context("failed to send recovered ShareReady event")?;
        runtime.recovered_shares.insert(transfer_id, share);
    }

    for share in persisted_state
        .active_shares
        .iter()
        .filter(|record| record.paused)
    {
        let blocked_hashes = share_hashes_for_record(&state.node, share).await;
        state.node.blobs.block_serving_hashes(blocked_hashes);
        let transfer_id = state.next_recovery_transfer_id();
        event_tx
            .send(NetworkEvent::ShareReady {
                transfer_id,
                directory_name: share.directory_name.clone(),
                share_code: share.share_code.clone(),
                collection_hash: share.collection_hash.clone(),
                total_bytes: share.total_bytes,
                file_count: share.file_count,
            })
            .context("failed to send paused ShareReady event")?;
        event_tx
            .send(NetworkEvent::TransferPaused { transfer_id })
            .context("failed to send paused TransferPaused event")?;
        runtime.paused_shares.insert(transfer_id, share.clone());
    }

    for download in recovered.downloads {
        let transfer_id = state.next_recovery_transfer_id();
        event_tx
            .send(NetworkEvent::DownloadStarted {
                transfer_id,
                directory_name: download.directory_name.clone(),
                collection_hash: download.collection_hash.to_string(),
                total_bytes: download.total_bytes,
                file_count: download.files.len(),
            })
            .context("failed to send recovered DownloadStarted event")?;

        for (file_index, file) in download.files.iter().enumerate() {
            event_tx
                .send(NetworkEvent::FileDownloadProgress {
                    transfer_id,
                    file_index,
                    bytes_downloaded: file.size,
                })
                .context("failed to send recovered FileDownloadProgress event")?;
            event_tx
                .send(NetworkEvent::FileCompleted {
                    transfer_id,
                    file_index,
                })
                .context("failed to send recovered FileCompleted event")?;
        }

        event_tx
            .send(NetworkEvent::TransferCompleted { transfer_id })
            .context("failed to send recovered TransferCompleted event")?;

        runtime.store.remove_download_by_code(
            &download.share_code.encode()?,
            download
                .output_root
                .parent()
                .unwrap_or(&download.output_root),
        )?;
    }

    for download in persisted_state
        .active_downloads
        .iter()
        .filter(|record| record.paused)
    {
        let transfer_id = state.next_recovery_transfer_id();
        event_tx
            .send(NetworkEvent::DownloadStarted {
                transfer_id,
                directory_name: "Download".into(),
                collection_hash: download.collection_hash.clone(),
                total_bytes: 0,
                file_count: 0,
            })
            .context("failed to send paused DownloadStarted event")?;
        event_tx
            .send(NetworkEvent::TransferPaused { transfer_id })
            .context("failed to send paused download TransferPaused event")?;
        runtime
            .paused_downloads
            .insert(transfer_id, download.clone());
    }

    Ok(())
}

fn share_hashes_for_session(session: &ShareSession) -> Vec<BlobHash> {
    let mut hashes = Vec::with_capacity(session.files.len() + 2);
    hashes.push(session.collection_hash);
    hashes.push(session.manifest_hash);
    hashes.extend(session.files.iter().map(|file| file.hash));
    hashes
}

async fn share_hashes_for_record(node: &AppNode, record: &ShareRecord) -> Vec<BlobHash> {
    let collection_hash = match record.collection_hash() {
        Ok(hash) => hash,
        Err(err) => {
            warn!(
                "failed to parse collection hash {} from persisted share record: {err}",
                record.collection_hash
            );
            return Vec::new();
        }
    };
    let mut hashes = Vec::new();
    hashes.push(collection_hash);

    match node.blobs.get_bytes(collection_hash).await {
        Ok(collection_bytes) => match HashSeq::new(collection_bytes) {
            Some(hash_seq) => hashes.extend(hash_seq.into_iter()),
            None => warn!(
                "collection blob {} is not a valid hash sequence while resolving paused share hashes",
                record.collection_hash
            ),
        },
        Err(err) => warn!(
            "failed to load collection blob {} for paused share hash resolution: {err}",
            record.collection_hash
        ),
    }

    hashes
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
                                directory_name: "example".into(),
                                share_code: "p2p-TEST".into(),
                                collection_hash:
                                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                                        .into(),
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
                directory_name: "example".into(),
                share_code: "p2p-TEST".into(),
                collection_hash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .into(),
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
                    NetworkCommand::RecoverStartup => (u64::MAX, 0),
                    NetworkCommand::PauseAll => (u64::MAX - 1, 0),
                    NetworkCommand::ResumeAll => (u64::MAX - 2, 0),
                    NetworkCommand::ShareDirectory { transfer_id, .. } => (transfer_id, 200),
                    NetworkCommand::StartDownload { transfer_id, .. } => (transfer_id, 20),
                    NetworkCommand::CancelTransfer { transfer_id } => (transfer_id, 0),
                    NetworkCommand::PauseTransfer { transfer_id } => (transfer_id, 0),
                    NetworkCommand::ResumeTransfer { transfer_id } => (transfer_id, 0),
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

    #[test]
    fn cancel_aborts_in_flight_transfer_command() {
        let bridge = spawn_test_bridge(|_, command, events| {
            Box::pin(async move {
                match command {
                    NetworkCommand::ShareDirectory { transfer_id, .. } => {
                        tokio::time::sleep(Duration::from_millis(400)).await;
                        events
                            .send_async(NetworkEvent::TransferCompleted { transfer_id })
                            .await?;
                    }
                    NetworkCommand::CancelTransfer { transfer_id } => {
                        events
                            .send_async(NetworkEvent::TransferCancelled { transfer_id })
                            .await?;
                    }
                    _ => {}
                }
                Ok(())
            })
        });

        bridge
            .send(NetworkCommand::ShareDirectory {
                transfer_id: 42,
                directory_path: PathBuf::from("/tmp/slow-share"),
            })
            .unwrap();
        std::thread::sleep(Duration::from_millis(25));
        bridge
            .send(NetworkCommand::CancelTransfer { transfer_id: 42 })
            .unwrap();

        let first = wait_for_event(&bridge);
        assert_eq!(first, NetworkEvent::TransferCancelled { transfer_id: 42 });

        std::thread::sleep(Duration::from_millis(500));
        let remaining = bridge.drain_events();
        assert!(
            remaining.is_empty(),
            "unexpected extra events: {remaining:?}"
        );
    }

    #[test]
    fn canceling_one_transfer_does_not_stop_another() {
        let bridge = spawn_test_bridge(|_, command, events| {
            Box::pin(async move {
                match command {
                    NetworkCommand::ShareDirectory { transfer_id, .. } => {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        events
                            .send_async(NetworkEvent::TransferCompleted { transfer_id })
                            .await?;
                    }
                    NetworkCommand::CancelTransfer { transfer_id } => {
                        events
                            .send_async(NetworkEvent::TransferCancelled { transfer_id })
                            .await?;
                    }
                    _ => {}
                }
                Ok(())
            })
        });

        bridge
            .send(NetworkCommand::ShareDirectory {
                transfer_id: 10,
                directory_path: PathBuf::from("/tmp/one"),
            })
            .unwrap();
        bridge
            .send(NetworkCommand::ShareDirectory {
                transfer_id: 11,
                directory_path: PathBuf::from("/tmp/two"),
            })
            .unwrap();
        std::thread::sleep(Duration::from_millis(20));
        bridge
            .send(NetworkCommand::CancelTransfer { transfer_id: 10 })
            .unwrap();

        let first = wait_for_event(&bridge);
        let second = wait_for_event(&bridge);
        assert_eq!(first, NetworkEvent::TransferCancelled { transfer_id: 10 });
        assert_eq!(second, NetworkEvent::TransferCompleted { transfer_id: 11 });
    }
}
