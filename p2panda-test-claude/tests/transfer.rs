// Integration test: two in-process nodes transfer a file via mDNS (gossip + blobs).
//
// Node A imports a blob and publishes a BlobAnnouncement on a gossip topic.
// Node B subscribes to the same topic, receives the announcement, downloads
// the blob, and verifies the content matches.

use std::time::Duration;

use futures_util::StreamExt;
use p2panda_blobs::{Blobs, MemStore};
use p2panda_core::Hash;
use p2panda_file_sharing::protocol::BlobAnnouncement;
use p2panda_net::test_utils::{TestNode, setup_logging};
use p2panda_net::TopicId;
use tokio::time::timeout;

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
