use std::fs;
use std::path::Path;

use anyhow::Result;
use p2panda_core::SigningKey;
use p2panda_file_sharing_gui::contacts::{write_contact_cache, ContactsStore, DiscoverySort};
use p2panda_file_sharing_gui::download::download_share;
use p2panda_file_sharing_gui::node::{AppNode, NodeOptions};
use p2panda_file_sharing_gui::operation_domain::{ReducedProfileState, ReducedShareState};
use p2panda_file_sharing_gui::profile::{DownloadedShareInput, ProfileStore};
use p2panda_file_sharing_gui::share::{publish_share_metadata, share_directory};
use p2panda_net::addrs::NodeInfo;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread")]
async fn discover_second_degree_profile_browse_shares_and_download() -> Result<()> {
    let bob_dir = tempdir()?;
    let charlie_dir = tempdir()?;
    let alice_dir = tempdir()?;
    let dana_dir = tempdir()?;
    let source_dir = tempdir()?;
    let output_dir = tempdir()?;

    write_node_key(alice_dir.path(), &SigningKey::generate())?;
    write_node_key(dana_dir.path(), &SigningKey::generate())?;

    let source_root = source_dir.path().join("photos");
    fs::create_dir_all(source_root.join("nested"))?;
    fs::write(source_root.join("hello.txt"), b"hello discovery")?;
    fs::write(
        source_root.join("nested").join("data.bin"),
        vec![3_u8; 4096],
    )?;

    let bob = AppNode::with_data_dir(bob_dir.path(), NodeOptions::default()).await?;
    let charlie = AppNode::with_data_dir(charlie_dir.path(), NodeOptions::default()).await?;
    charlie
        .address_book
        .insert_node_info(NodeInfo::from(bob.endpoint.endpoint().await?.addr()).bootstrap())
        .await?;

    let share = share_directory(&bob, &source_root).await?;
    let _publisher = publish_share_metadata(&bob, &share).await?;

    let mut bob_profile = ProfileStore::load_or_create(bob_dir.path())?;
    let mut alice_profile = ProfileStore::load_or_create(alice_dir.path())?;
    let mut dana_profile = ProfileStore::load_or_create(dana_dir.path())?;

    alice_profile.follow_contact(bob_profile.profile().profile_id.clone())?;
    dana_profile.follow_contact(bob_profile.profile().profile_id.clone())?;
    bob_profile.ensure_downloaded_share_record(DownloadedShareInput {
        profile_id: bob_profile.profile().profile_id.clone(),
        share_code: share.share_code.clone(),
        collection_hash: share.collection_hash.to_string(),
        manifest_bytes: share.manifest_bytes.clone(),
        source_dir: share.source_dir.clone(),
        source_contact_profile_id: None,
        source_contact_display_name: None,
    })?;

    let mut contacts = ContactsStore::load(charlie_dir.path())?;
    contacts.follow_contact(alice_profile.profile().profile_id.clone())?;
    contacts.follow_contact(dana_profile.profile().profile_id.clone())?;
    contacts.refresh_contact(&alice_profile.profile().profile_id)?;
    contacts.refresh_contact(&dana_profile.profile().profile_id)?;

    let bob_share = bob_profile
        .share_ownership_records()?
        .into_iter()
        .find(|record| record.share_code == share.share_code)
        .expect("bob share persisted");
    for state in [
        ReducedProfileState {
            profile_id: alice_profile.profile().profile_id.clone(),
            display_name: Some(alice_profile.profile().display_name.clone()),
            shares: Vec::new(),
            followed_profile_ids: vec![bob_profile.profile().profile_id.clone()],
        },
        ReducedProfileState {
            profile_id: dana_profile.profile().profile_id.clone(),
            display_name: Some(dana_profile.profile().display_name.clone()),
            shares: Vec::new(),
            followed_profile_ids: vec![bob_profile.profile().profile_id.clone()],
        },
        ReducedProfileState {
            profile_id: bob_profile.profile().profile_id.clone(),
            display_name: Some(bob_profile.profile().display_name.clone()),
            shares: vec![ReducedShareState {
                profile_id: bob_profile.profile().profile_id.clone(),
                collection_hash: bob_share.collection_hash.clone(),
                share_code: bob_share.share_code.clone(),
                manifest_bytes: bob_share.manifest_bytes.clone(),
                source_dir: bob_share.source_dir.clone(),
                recorded_at: bob_share.recorded_at,
                source_contact_profile_id: bob_share.source_contact_profile_id.clone(),
                source_contact_display_name: bob_share.source_contact_display_name.clone(),
            }],
            followed_profile_ids: Vec::new(),
        },
    ] {
        write_contact_cache(charlie_dir.path(), &state.profile_id, &state)?;
    }

    let discovered =
        contacts.discover_second_degree_profiles(false, true, "", DiscoverySort::MutualCount);
    assert_eq!(discovered.len(), 1);
    let bob = &discovered[0];
    assert_eq!(bob.mutual_count, 2);
    assert_eq!(bob.cached_shares.len(), 1);
    assert_eq!(bob.cached_shares[0].share_code, share.share_code);

    let session = download_share(
        &charlie,
        &bob.cached_shares[0].share_code,
        output_dir.path(),
    )
    .await?;
    assert_tree_matches(
        &source_root,
        &output_dir.path().join(session.directory_name.clone()),
    )?;

    Ok(())
}

fn assert_tree_matches(expected_root: &Path, actual_root: &Path) -> Result<()> {
    assert_tree_matches_recursive(expected_root, expected_root, actual_root)
}

fn write_node_key(data_dir: &Path, private_key: &SigningKey) -> Result<()> {
    fs::create_dir_all(data_dir)?;
    fs::write(data_dir.join("node.key"), private_key.as_bytes())?;
    Ok(())
}

fn assert_tree_matches_recursive(
    expected_root: &Path,
    current: &Path,
    actual_root: &Path,
) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            assert_tree_matches_recursive(expected_root, &path, actual_root)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }

        let relative = path.strip_prefix(expected_root)?;
        let expected = fs::read(&path)?;
        let actual = fs::read(actual_root.join(relative))?;
        assert_eq!(actual, expected, "mismatch for {}", relative.display());
    }

    Ok(())
}
