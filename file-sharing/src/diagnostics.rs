use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

pub const CONNECTION_HISTORY_LIMIT: usize = 100;
pub const ERROR_LOG_LIMIT: usize = 50;
pub const DOWNLOAD_PROVIDER_HISTORY_LIMIT: usize = 50;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DiagnosticsSnapshot {
    pub captured_at_unix_ms: u64,
    pub node_identity: NodeIdentitySnapshot,
    pub peers: Vec<PeerSnapshot>,
    pub connection_history: Vec<ConnectionHistoryEntry>,
    pub error_log: Vec<DiagnosticErrorEntry>,
    pub gossip_topics: Vec<GossipTopicSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NodeIdentitySnapshot {
    pub node_id: String,
    pub relay_url: Option<String>,
    pub local_listen_addrs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerSnapshot {
    pub node_id: String,
    pub state: PeerConnectionState,
    pub discovered_via: PeerDiscoveryMethod,
    pub last_seen_unix_ms: Option<u64>,
    pub rtt_ms: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerConnectionState {
    Connected,
    Known,
    Disconnected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerDiscoveryMethod {
    Manual,
    Relay,
    Mdns,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionHistoryEntry {
    pub at_unix_ms: u64,
    pub peer_node_id: Option<String>,
    pub event: String,
    pub detail: String,
    pub establish_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiagnosticErrorEntry {
    pub at_unix_ms: u64,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipTopicSnapshot {
    pub topic_id: String,
    pub peer_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadProviderStatus {
    Trying,
    Failed,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadProviderDiagnosticEntry {
    pub at_unix_ms: u64,
    pub transfer_id: u64,
    pub provider_id: String,
    pub target: String,
    pub status: DownloadProviderStatus,
}

pub fn push_bounded_download_provider_history(
    history: &mut VecDeque<DownloadProviderDiagnosticEntry>,
    entry: DownloadProviderDiagnosticEntry,
) {
    if history.len() == DOWNLOAD_PROVIDER_HISTORY_LIMIT {
        history.pop_front();
    }
    history.push_back(entry);
}

pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_unix_ms_is_non_zero() {
        assert!(now_unix_ms() > 0);
    }

    #[test]
    fn download_provider_history_is_bounded() {
        let mut history = VecDeque::new();
        for transfer_id in 0..(DOWNLOAD_PROVIDER_HISTORY_LIMIT as u64 + 2) {
            push_bounded_download_provider_history(
                &mut history,
                DownloadProviderDiagnosticEntry {
                    at_unix_ms: transfer_id,
                    transfer_id,
                    provider_id: format!("provider-{transfer_id}"),
                    target: "file example.txt".into(),
                    status: DownloadProviderStatus::Trying,
                },
            );
        }

        assert_eq!(history.len(), DOWNLOAD_PROVIDER_HISTORY_LIMIT);
        assert_eq!(history.front().map(|entry| entry.transfer_id), Some(2));
        assert_eq!(
            history.back().map(|entry| entry.transfer_id),
            Some(DOWNLOAD_PROVIDER_HISTORY_LIMIT as u64 + 1)
        );
    }
}
