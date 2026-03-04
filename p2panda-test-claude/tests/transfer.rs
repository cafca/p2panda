// Integration tests: in-process multi-node file transfer via gossip + blobs.
//
// test_blob_announcement_and_download: two nodes, Node A announces a blob on a
// topic, Node B receives and downloads it.
//
// test_topic_isolation: three nodes, Node A announces on topic alpha, Node B
// (subscribed to alpha) receives it, Node C (subscribed to beta) does not.
//
// test_relay_bootstrap_transfer / test_peer_flag_passive_mdns_transfer: two
// nodes with passive mDNS connect through an in-process iroh relay using only
// a relay endpoint address (node id + relay URL), then transfer a blob.

use std::time::Duration;

use futures_util::StreamExt;
use iroh::test_utils::run_relay_server;
use p2panda_blobs::{Blobs, MemStore};
use p2panda_core::Hash;
use p2panda_file_sharing::node::{FileSharingNode, NodeOptions};
use p2panda_file_sharing::protocol::BlobAnnouncement;
use p2panda_net::addrs::NodeInfo;
use p2panda_net::iroh_endpoint::{from_public_key, EndpointAddr, RelayUrl};
use p2panda_net::test_utils::{setup_logging, TestNode};
use p2panda_net::TopicId;
use tokio::time::timeout;

fn relay_bootstrap_node_info(node_id: p2panda_net::NodeId, relay_url: RelayUrl) -> NodeInfo {
    let endpoint_addr = EndpointAddr::new(from_public_key(node_id)).with_relay_url(relay_url);
    NodeInfo::from(endpoint_addr).bootstrap()
}

async fn spawn_relay_node(topic: &str, relay_url: RelayUrl) -> anyhow::Result<FileSharingNode> {
    FileSharingNode::new(
        topic,
        NodeOptions {
            relay_url: Some(relay_url),
            insecure_skip_relay_cert_verify: true,
            passive_mdns: true,
        },
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_blob_announcement_and_download() -> anyhow::Result<()> {
    setup_logging();

    let topic_id: TopicId = Hash::new(b"test-transfer").into();

    // --- Node A (sender) ---
    let mut node_a = TestNode::spawn([1; 32]).await;
    let store_a = MemStore::new();
    let blobs_a = Blobs::new(&*store_a, &node_a.endpoint, &node_a.address_book)
        .await
        .unwrap();

    // Import file content as a blob.
    let content = b"Hello from Node A!";
    let tag_info = blobs_a.add_slice(content).await.unwrap();
    let hash = tag_info.hash;

    // --- Node B (receiver) ---
    let node_b = TestNode::spawn([2; 32]).await;
    let store_b = MemStore::new();
    let blobs_b = Blobs::new(&*store_b, &node_b.endpoint, &node_b.address_book)
        .await
        .unwrap();

    // B adds A to its address book so it can resolve A's endpoint for blob download.
    node_b
        .address_book
        .insert_node_info(node_a.node_info())
        .await
        .unwrap();

    // B subscribes to gossip topic first.
    let handle_b = node_b.gossip.stream(topic_id).await.unwrap();
    let mut sub_b = handle_b.subscribe();

    // A joins the same topic and announces the blob.
    let handle_a = node_a.gossip.stream(topic_id).await.unwrap();
    let announcement = BlobAnnouncement::new(hash, "hello.txt".to_string());
    handle_a.publish(announcement.encode()).await.unwrap();

    // B waits for the announcement (with timeout).
    let received_ann = timeout(Duration::from_secs(15), async {
        while let Some(Ok(bytes)) = sub_b.next().await {
            if let Ok(ann) = BlobAnnouncement::decode(&bytes) {
                return Some(ann);
            }
        }
        None
    })
    .await
    .expect("timed out waiting for announcement");

    let received_ann = received_ann.expect("no announcement received");
    assert_eq!(received_ann.hash, hash);
    assert_eq!(received_ann.filename, "hello.txt");

    // B downloads the blob from A.
    blobs_b.download(received_ann.hash).await.unwrap();

    // Verify content.
    let downloaded = blobs_b.get_bytes(received_ann.hash).await.unwrap();
    assert_eq!(downloaded.as_ref(), content);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_topic_isolation() -> anyhow::Result<()> {
    setup_logging();

    let topic_alpha: TopicId = Hash::new(b"topic-alpha").into();
    let topic_beta: TopicId = Hash::new(b"topic-beta").into();

    // --- Node A (sender) ---
    let mut node_a = TestNode::spawn([10; 32]).await;
    let store_a = MemStore::new();
    let blobs_a = Blobs::new(&*store_a, &node_a.endpoint, &node_a.address_book)
        .await
        .unwrap();

    // Import content as a blob.
    let content = b"Isolated content for alpha only";
    let tag_info = blobs_a.add_slice(content).await.unwrap();
    let hash = tag_info.hash;

    // --- Node B: subscribes to alpha, knows about A ---
    let node_b = TestNode::spawn([20; 32]).await;
    let store_b = MemStore::new();
    let blobs_b = Blobs::new(&*store_b, &node_b.endpoint, &node_b.address_book)
        .await
        .unwrap();
    node_b
        .address_book
        .insert_node_info(node_a.node_info())
        .await
        .unwrap();
    let handle_b = node_b.gossip.stream(topic_alpha).await.unwrap();
    let mut sub_b = handle_b.subscribe();

    // --- Node C: subscribes to beta only (different topic) ---
    let node_c = TestNode::spawn([30; 32]).await;
    let handle_c = node_c.gossip.stream(topic_beta).await.unwrap();
    let mut sub_c = handle_c.subscribe();

    // A announces on alpha only.
    let handle_a = node_a.gossip.stream(topic_alpha).await.unwrap();
    let announcement = BlobAnnouncement::new(hash, "isolated.txt".to_string());
    handle_a.publish(announcement.encode()).await.unwrap();

    // B should receive the announcement on alpha.
    let received_b = timeout(Duration::from_secs(15), async {
        while let Some(Ok(bytes)) = sub_b.next().await {
            if let Ok(ann) = BlobAnnouncement::decode(&bytes) {
                return Some(ann);
            }
        }
        None
    })
    .await
    .expect("timed out waiting for Node B announcement");

    let received_b = received_b.expect("Node B did not receive announcement on alpha");
    assert_eq!(received_b.hash, hash);
    assert_eq!(received_b.filename, "isolated.txt");

    // C should NOT receive any announcement on beta within a short window.
    let received_c = timeout(Duration::from_secs(3), async {
        while let Some(Ok(bytes)) = sub_c.next().await {
            if BlobAnnouncement::decode(&bytes).is_ok() {
                return true; // unexpected
            }
        }
        false
    })
    .await;
    // timeout = Ok(false) means nothing arrived; Err(_) means timed out — both correct.
    assert!(
        received_c.unwrap_or(false) == false,
        "Node C on topic beta should NOT receive announcement published on topic alpha"
    );

    // B downloads the blob from A to confirm end-to-end correctness.
    blobs_b.download(received_b.hash).await.unwrap();
    let downloaded = blobs_b.get_bytes(received_b.hash).await.unwrap();
    assert_eq!(downloaded.as_ref(), content);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_relay_bootstrap_transfer() -> anyhow::Result<()> {
    setup_logging();

    let (_relay_map, relay_url, _relay_server) = run_relay_server().await?;
    let node_a = spawn_relay_node("test-relay-bootstrap", relay_url.clone()).await?;
    let node_b = spawn_relay_node("test-relay-bootstrap", relay_url.clone()).await?;

    let content = b"Hello via relay bootstrap!";
    let tag_info = node_a.blobs.add_slice(content).await.unwrap();
    let hash = tag_info.hash;

    node_b
        .address_book
        .insert_node_info(relay_bootstrap_node_info(
            node_a.endpoint.node_id(),
            relay_url.clone(),
        ))
        .await
        .unwrap();

    // Give the sender a moment to establish its home-relay registration.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let handle_b = node_b.gossip.stream(node_b.topic_id).await.unwrap();
    let mut sub_b = handle_b.subscribe();

    let handle_a = node_a.gossip.stream(node_a.topic_id).await.unwrap();
    let announcement = BlobAnnouncement::new(hash, "relay-bootstrap.txt".to_string());
    handle_a.publish(announcement.encode()).await.unwrap();

    let received_ann = timeout(Duration::from_secs(20), async {
        while let Some(Ok(bytes)) = sub_b.next().await {
            if let Ok(ann) = BlobAnnouncement::decode(&bytes) {
                return Some(ann);
            }
        }
        None
    })
    .await
    .expect("timed out waiting for relay-bootstrap announcement");

    let received_ann = received_ann.expect("no announcement received via relay bootstrap");
    assert_eq!(received_ann.hash, hash);
    assert_eq!(received_ann.filename, "relay-bootstrap.txt");

    node_b.blobs.download(received_ann.hash).await.unwrap();
    let downloaded = node_b.blobs.get_bytes(received_ann.hash).await.unwrap();
    assert_eq!(downloaded.as_ref(), content);

    Ok(())
}

/// PRD task 5: receiver connects to sender via --peer flag using relay transport.
///
/// Both nodes use passive mDNS so no automatic discovery occurs. Node B registers only
/// Node A's node id plus relay URL in its address book, matching the CLI flow where
/// `--peer` and `--relay-url` bootstrap a connection through the relay.
#[tokio::test(flavor = "multi_thread")]
async fn test_peer_flag_passive_mdns_transfer() -> anyhow::Result<()> {
    setup_logging();

    let (_relay_map, relay_url, _relay_server) = run_relay_server().await?;
    let node_a = spawn_relay_node("test-peer-flag", relay_url.clone()).await?;
    let node_b = spawn_relay_node("test-peer-flag", relay_url.clone()).await?;

    let content = b"Hello via --peer flag!";
    let tag_info = node_a.blobs.add_slice(content).await.unwrap();
    let hash = tag_info.hash;

    node_b
        .address_book
        .insert_node_info(relay_bootstrap_node_info(
            node_a.endpoint.node_id(),
            relay_url.clone(),
        ))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    let handle_b = node_b.gossip.stream(node_b.topic_id).await.unwrap();
    let mut sub_b = handle_b.subscribe();

    let handle_a = node_a.gossip.stream(node_a.topic_id).await.unwrap();
    let announcement = BlobAnnouncement::new(hash, "peer-flag.txt".to_string());
    handle_a.publish(announcement.encode()).await.unwrap();

    let received_ann = timeout(Duration::from_secs(20), async {
        while let Some(Ok(bytes)) = sub_b.next().await {
            if let Ok(ann) = BlobAnnouncement::decode(&bytes) {
                return Some(ann);
            }
        }
        None
    })
    .await
    .expect("timed out waiting for --peer announcement");

    let received_ann = received_ann.expect("no announcement received via --peer");
    assert_eq!(received_ann.hash, hash);
    assert_eq!(received_ann.filename, "peer-flag.txt");

    node_b.blobs.download(received_ann.hash).await.unwrap();
    let downloaded = node_b.blobs.get_bytes(received_ann.hash).await.unwrap();
    assert_eq!(downloaded.as_ref(), content);

    Ok(())
}

/// PRD task 6: receiver discovers sender via mDNS on the same network without --peer.
///
/// Both nodes use active mDNS (the CLI default when --peer is absent) and no
/// address-book bootstrapping.  After mDNS populates each node's address book,
/// gossip connects them, the announcement propagates, and the blob download
/// succeeds — all without any manual peer exchange.
#[tokio::test(flavor = "multi_thread")]
async fn test_mdns_discovery_and_transfer() -> anyhow::Result<()> {
    setup_logging();

    // Default NodeOptions: active mDNS, no relay, no explicit peer.
    let node_a = FileSharingNode::new("test-mdns-e2e", NodeOptions::default()).await?;
    let node_b = FileSharingNode::new("test-mdns-e2e", NodeOptions::default()).await?;

    // Import file content as a blob on Node A.
    let content = b"Hello via mDNS discovery!";
    let tag_info = node_a.blobs.add_slice(content).await.unwrap();
    let hash = tag_info.hash;

    // B subscribes to the gossip topic first.
    let handle_b = node_b.gossip.stream(node_b.topic_id).await.unwrap();
    let mut sub_b = handle_b.subscribe();

    // Give mDNS time to discover both nodes and populate the address books,
    // then give gossip time to connect them on the shared topic.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // A joins the topic and announces the blob.
    let handle_a = node_a.gossip.stream(node_a.topic_id).await.unwrap();
    let announcement = BlobAnnouncement::new(hash, "mdns-discovered.txt".to_string());
    handle_a.publish(announcement.encode()).await.unwrap();

    // B waits for the announcement (generous timeout to account for mDNS timing).
    let received_ann = timeout(Duration::from_secs(30), async {
        while let Some(Ok(bytes)) = sub_b.next().await {
            if let Ok(ann) = BlobAnnouncement::decode(&bytes) {
                return Some(ann);
            }
        }
        None
    })
    .await
    .expect("timed out waiting for mDNS-discovered announcement");

    let received_ann = received_ann.expect("no announcement received via mDNS");
    assert_eq!(received_ann.hash, hash);
    assert_eq!(received_ann.filename, "mdns-discovered.txt");

    // B downloads the blob from A.  The address book was populated by mDNS discovery,
    // so no manual peer insertion was needed.
    node_b.blobs.download(received_ann.hash).await.unwrap();

    // Verify content.
    let downloaded = node_b.blobs.get_bytes(received_ann.hash).await.unwrap();
    assert_eq!(downloaded.as_ref(), content);

    Ok(())
}
