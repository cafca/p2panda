use std::time::{SystemTime, UNIX_EPOCH};

pub const CONNECTION_HISTORY_LIMIT: usize = 100;
pub const ERROR_LOG_LIMIT: usize = 50;

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
}
