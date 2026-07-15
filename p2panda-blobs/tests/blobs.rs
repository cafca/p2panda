// SPDX-License-Identifier: MIT OR Apache-2.0

use std::time::Duration;

use p2panda_blobs::{BlobServePolicy, Blobs, BoxFuture, Hash, MemStore, ServeDecision};
use p2panda_core::test_utils::setup_logging;
use p2panda_net::NodeId;
use p2panda_net::test_utils::TestNode;

/// Two-node integration test: Penguin stores a blob, Panda downloads it.
///
/// Exercises the full path: `Blobs::new` (protocol registration), `add_slice`,
/// `AddressBook::insert_node_info` (peer address resolution), `Blobs::download`,
/// and blob read-back via `get_bytes`.
#[tokio::test]
async fn blob_download_from_peer() {
    setup_logging();

    // ฅ՞•ﻌ•՞ฅ <- Penguin (provider)
    let mut penguin = TestNode::spawn([99; 32], None).await;
    let penguin_mem_store = MemStore::new();
    let penguin_blobs = Blobs::new(&penguin_mem_store, &penguin.endpoint, &penguin.address_book)
        .await
        .unwrap();

    // Penguin stores a blob.
    let tag_info = penguin_blobs.add_slice(b"Hello, Panda!").await.unwrap();
    let hash = tag_info.hash;

    // ฅ՞•ﻌ•՞ฅ <- Panda (downloader)
    let panda = TestNode::spawn([98; 32], None).await;
    let panda_mem_store = MemStore::new();
    let panda_blobs = Blobs::new(&panda_mem_store, &panda.endpoint, &panda.address_book)
        .await
        .unwrap();

    // Panda adds Penguin to its address book so the iroh endpoint can resolve the peer address.
    panda
        .address_book
        .insert_node_info(penguin.node_info())
        .await
        .unwrap();

    // Panda downloads the blob from Penguin.
    panda_blobs.download(hash).await.unwrap();

    // Panda reads the blob back and verifies its contents.
    let bytes = panda_blobs.get_bytes(hash).await.unwrap();
    assert_eq!(bytes.as_ref(), b"Hello, Panda!");
}

/// `download_with_progress` returns the downloader handle without losing the progress stream API.
#[tokio::test]
async fn blob_download_with_progress_from_peer() {
    setup_logging();

    let mut penguin = TestNode::spawn([97; 32], None).await;
    let penguin_mem_store = MemStore::new();
    let penguin_blobs = Blobs::new(&penguin_mem_store, &penguin.endpoint, &penguin.address_book)
        .await
        .unwrap();

    let tag_info = penguin_blobs
        .add_slice(b"Hello with progress!")
        .await
        .unwrap();
    let hash = tag_info.hash;

    let panda = TestNode::spawn([96; 32], None).await;
    let panda_mem_store = MemStore::new();
    let panda_blobs = Blobs::new(&panda_mem_store, &panda.endpoint, &panda.address_book)
        .await
        .unwrap();

    panda
        .address_book
        .insert_node_info(penguin.node_info())
        .await
        .unwrap();

    let download = panda_blobs.download_with_progress(hash).await.unwrap();
    download.await.unwrap();

    let bytes = panda_blobs.get_bytes(hash).await.unwrap();
    assert_eq!(bytes.as_ref(), b"Hello with progress!");
}

/// A provider policy that refuses every request stops peers from downloading.
///
/// Exercises the `on_request` hook: Penguin serves under a policy that denies
/// all requests, so Panda — reachable and told the hash — still can't fetch it.
#[tokio::test]
async fn policy_on_request_denies_download() {
    setup_logging();

    struct DenyRequests;
    impl BlobServePolicy for DenyRequests {
        fn on_connect(&self, _peer: NodeId) -> BoxFuture<'_, ServeDecision> {
            Box::pin(async { ServeDecision::Allow })
        }
        fn on_request(&self, _peer: NodeId, _hash: Hash) -> BoxFuture<'_, ServeDecision> {
            Box::pin(async { ServeDecision::Deny })
        }
    }

    let mut penguin = TestNode::spawn([95; 32], None).await;
    let penguin_mem_store = MemStore::new();
    let penguin_blobs = Blobs::with_policy(
        &penguin_mem_store,
        &penguin.endpoint,
        &penguin.address_book,
        std::sync::Arc::new(DenyRequests),
    )
    .await
    .unwrap();

    let tag_info = penguin_blobs.add_slice(b"secret").await.unwrap();
    let hash = tag_info.hash;

    let panda = TestNode::spawn([94; 32], None).await;
    let panda_mem_store = MemStore::new();
    let panda_blobs = Blobs::new(&panda_mem_store, &panda.endpoint, &panda.address_book)
        .await
        .unwrap();
    panda
        .address_book
        .insert_node_info(penguin.node_info())
        .await
        .unwrap();

    // The download must not succeed: it either errors out or makes no progress.
    let result = tokio::time::timeout(Duration::from_secs(5), panda_blobs.download(hash)).await;
    assert!(
        matches!(result, Err(_) | Ok(Err(_))),
        "download should not succeed against a deny-all provider policy, got {result:?}",
    );
}

/// A provider policy that rejects a specific peer at connect time blocks it.
///
/// Exercises the `on_connect` hook: Penguin denies Panda's node id, so the
/// connection is refused before any request is served.
#[tokio::test]
async fn policy_on_connect_denies_peer() {
    setup_logging();

    struct DenyPeer(NodeId);
    impl BlobServePolicy for DenyPeer {
        fn on_connect(&self, peer: NodeId) -> BoxFuture<'_, ServeDecision> {
            let blocked = self.0;
            Box::pin(async move { ServeDecision::allow_if(peer != blocked) })
        }
        fn on_request(&self, _peer: NodeId, _hash: Hash) -> BoxFuture<'_, ServeDecision> {
            Box::pin(async { ServeDecision::Allow })
        }
    }

    let panda = TestNode::spawn([92; 32], None).await;
    let panda_mem_store = MemStore::new();
    let panda_blobs = Blobs::new(&panda_mem_store, &panda.endpoint, &panda.address_book)
        .await
        .unwrap();

    let mut penguin = TestNode::spawn([93; 32], None).await;
    let penguin_mem_store = MemStore::new();
    let penguin_blobs = Blobs::with_policy(
        &penguin_mem_store,
        &penguin.endpoint,
        &penguin.address_book,
        std::sync::Arc::new(DenyPeer(panda.node_id())),
    )
    .await
    .unwrap();

    let tag_info = penguin_blobs.add_slice(b"not for panda").await.unwrap();
    let hash = tag_info.hash;

    panda
        .address_book
        .insert_node_info(penguin.node_info())
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(5), panda_blobs.download(hash)).await;
    assert!(
        matches!(result, Err(_) | Ok(Err(_))),
        "download should not succeed when the provider rejects the peer, got {result:?}",
    );
}
