use std::fs;
use std::path::Path;

use anyhow::Result;
use p2panda_net::addrs::NodeInfo;
use tempfile::tempdir;

use p2panda_file_sharing_gui::contacts::{contact_records_cache_path, ContactsStore};
use p2panda_file_sharing_gui::download::download_share;
use p2panda_file_sharing_gui::node::{AppNode, NodeOptions};
use p2panda_file_sharing_gui::persist::ShareRecord;
use p2panda_file_sharing_gui::profile::{profile_records_path, ProfileStore};
use p2panda_file_sharing_gui::share::share_directory;

#[tokio::test(flavor = "multi_thread")]
async fn follow_contact_browse_share_and_record_download_provenance() -> Result<()> {
    let sharer_dir = tempdir()?;
    let downloader_dir = tempdir()?;
    let source_dir = tempdir()?;
    let output_dir = tempdir()?;

    let source_root = source_dir.path().join("photos");
    fs::create_dir_all(source_root.join("nested"))?;
    fs::write(source_root.join("hello.txt"), b"hello contacts")?;
    fs::write(
        source_root.join("nested").join("data.bin"),
        vec![7_u8; 2048],
    )?;

    let sharer = AppNode::with_data_dir(sharer_dir.path(), NodeOptions::default()).await?;
    let downloader = AppNode::with_data_dir(downloader_dir.path(), NodeOptions::default()).await?;
    downloader
        .address_book
        .insert_node_info(NodeInfo::from(sharer.endpoint.endpoint().await?.addr()).bootstrap())
        .await?;

    let share = share_directory(&sharer, &source_root).await?;
    let mut sharer_profile = ProfileStore::load_or_create(sharer_dir.path())?;
    let mut share_record = ShareRecord::from(&share);
    share_record.owner_profile_id = Some(sharer_profile.profile().profile_id.clone());
    sharer_profile.ensure_share_ownership_record(&share_record)?;

    let sharer_profile_id = sharer_profile.profile().profile_id.clone();
    let sharer_display_name = sharer_profile.profile().display_name.clone();
    let cache_path = contact_records_cache_path(downloader_dir.path(), &sharer_profile_id);
    fs::create_dir_all(cache_path.parent().unwrap())?;
    fs::copy(profile_records_path(sharer_dir.path()), &cache_path)?;

    let mut contacts = ContactsStore::load(downloader_dir.path())?;
    contacts.follow_contact(sharer_profile_id.clone(), Some("Alice".into()))?;
    contacts.refresh_contact(&sharer_profile_id)?;

    let contact = contacts.get(&sharer_profile_id).unwrap();
    assert_eq!(contact.cached_shares.len(), 1);
    assert_eq!(contact.cached_shares[0].share_code, share.share_code);

    let session = download_share(
        &downloader,
        &contact.cached_shares[0].share_code,
        output_dir.path(),
    )
    .await?;

    assert_tree_matches(
        &source_root,
        &output_dir.path().join(session.directory_name.clone()),
    )?;

    let mut downloader_profile = ProfileStore::load_or_create(downloader_dir.path())?;
    let local_profile_id = downloader_profile.profile().profile_id.clone();
    downloader_profile.ensure_downloaded_share_record(
        &local_profile_id,
        session.share_code.encode()?,
        session.collection_hash.to_string(),
        session.output_root.clone(),
        Some(sharer_profile_id.clone()),
        Some(sharer_display_name.clone()),
    )?;

    let ownerships = downloader_profile.share_ownership_records()?;
    assert!(ownerships.iter().any(|record| {
        record.profile_id == local_profile_id
            && record.share_code == session.share_code.encode().unwrap()
            && record.source_contact_profile_id.as_deref() == Some(sharer_profile_id.as_str())
            && record.source_contact_display_name.as_deref() == Some(sharer_display_name.as_str())
    }));

    Ok(())
}

fn assert_tree_matches(expected_root: &Path, actual_root: &Path) -> Result<()> {
    assert_tree_matches_recursive(expected_root, expected_root, actual_root)
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
