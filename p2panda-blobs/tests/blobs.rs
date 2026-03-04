// SPDX-License-Identifier: MIT OR Apache-2.0

use p2panda_blobs::{Blobs, MemStore};
use p2panda_net::test_utils::{TestNode, setup_logging};

/// Two-node integration test: Penguin stores a blob, Panda downloads it.
///
/// Exercises the full path: `Blobs::new` (protocol registration), `add_slice`,
/// `AddressBook::insert_node_info` (peer address resolution), `Blobs::download`,
/// and blob read-back via `get_bytes`.
#[tokio::test]
async fn blob_download_from_peer() {
    setup_logging();

    // ฅ՞•ﻌ•՞ฅ <- Penguin (provider)
    let mut penguin = TestNode::spawn([99; 32]).await;
    let penguin_mem_store = MemStore::new();
    let penguin_blobs = Blobs::new(&penguin_mem_store, &penguin.endpoint, &penguin.address_book)
        .await
        .unwrap();

    // Penguin stores a blob.
    let tag_info = penguin_blobs.add_slice(b"Hello, Panda!").await.unwrap();
    let hash = tag_info.hash;

    // ฅ՞•ﻌ•՞ฅ <- Panda (downloader)
    let panda = TestNode::spawn([98; 32]).await;
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

    let mut penguin = TestNode::spawn([97; 32]).await;
    let penguin_mem_store = MemStore::new();
    let penguin_blobs = Blobs::new(&penguin_mem_store, &penguin.endpoint, &penguin.address_book)
        .await
        .unwrap();

    let tag_info = penguin_blobs
        .add_slice(b"Hello with progress!")
        .await
        .unwrap();
    let hash = tag_info.hash;

    let panda = TestNode::spawn([96; 32]).await;
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
