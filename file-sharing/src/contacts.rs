use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use p2panda_core::PublicKey;
use serde::{Deserialize, Serialize};

use crate::profile::{
    active_follow_records_for_profile, load_profile_records_from_path, ProfileMetadataRecord,
    ProfileRecord, ShareOwnershipRecord,
};

const CONTACTS_FILE_NAME: &str = "contacts.json";
const CONTACT_CACHE_DIR_NAME: &str = "contact-record-cache";
const CONTACT_REFRESH_INTERVAL_SECS: u64 = 5;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactShare {
    pub share_code: String,
    pub collection_hash: String,
    pub share_name: String,
    pub recorded_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Contact {
    pub profile_id: String,
    pub followed_at: u64,
    #[serde(default)]
    pub cached_display_name: Option<String>,
    #[serde(default)]
    pub cached_shares: Vec<ContactShare>,
    #[serde(default)]
    pub last_refreshed_at: Option<u64>,
    #[serde(default)]
    pub last_error: Option<String>,
}

impl Contact {
    pub fn label(&self) -> String {
        self.display_name()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| truncate_profile_id(&self.profile_id))
    }

    pub fn display_name(&self) -> Option<&str> {
        self.cached_display_name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
    }

    pub fn is_refresh_due(&self, now_unix_secs: u64) -> bool {
        self.last_refreshed_at
            .map(|last| now_unix_secs.saturating_sub(last) >= CONTACT_REFRESH_INTERVAL_SECS)
            .unwrap_or(true)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoverySource {
    pub profile_id: String,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredProfile {
    pub profile_id: String,
    pub cached_display_name: Option<String>,
    pub cached_shares: Vec<ContactShare>,
    pub source_contacts: Vec<DiscoverySource>,
    pub mutual_count: usize,
    pub last_seen_at: Option<u64>,
    pub already_followed: bool,
    pub last_error: Option<String>,
}

impl DiscoveredProfile {
    pub fn label(&self) -> String {
        self.cached_display_name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| truncate_profile_id(&self.profile_id))
    }

    pub fn display_name(&self) -> Option<&str> {
        self.cached_display_name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
    }

    pub fn has_shares(&self) -> bool {
        !self.cached_shares.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoverySort {
    MutualCount,
    RecentlySeen,
    HasShares,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
struct PersistedContacts {
    #[serde(default)]
    followed_contacts: Vec<Contact>,
}

#[derive(Debug, bevy::prelude::Resource)]
pub struct ContactsStore {
    path: PathBuf,
    cache_dir: PathBuf,
    state: PersistedContacts,
}

impl ContactsStore {
    pub fn load(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        let path = data_dir.join(CONTACTS_FILE_NAME);
        let cache_dir = data_dir.join(CONTACT_CACHE_DIR_NAME);
        let state = load_from_path(&path)?;
        Ok(Self {
            path,
            cache_dir,
            state,
        })
    }

    pub fn contacts(&self) -> &[Contact] {
        &self.state.followed_contacts
    }

    pub fn get(&self, profile_id: &str) -> Option<&Contact> {
        self.state
            .followed_contacts
            .iter()
            .find(|contact| contact.profile_id == profile_id)
    }

    pub fn follow_contact(&mut self, profile_id: impl Into<String>) -> Result<()> {
        let profile_id = normalize_profile_id(profile_id.into())?;
        if self
            .state
            .followed_contacts
            .iter()
            .any(|contact| contact.profile_id == profile_id)
        {
            anyhow::bail!("contact is already followed");
        }

        self.state.followed_contacts.push(Contact {
            profile_id,
            followed_at: now_unix_secs(),
            cached_display_name: None,
            cached_shares: Vec::new(),
            last_refreshed_at: None,
            last_error: None,
        });
        self.state
            .followed_contacts
            .sort_by(|left, right| left.followed_at.cmp(&right.followed_at));
        self.save()
    }

    pub fn remove_contact(&mut self, profile_id: &str) -> Result<bool> {
        let before = self.state.followed_contacts.len();
        self.state
            .followed_contacts
            .retain(|contact| contact.profile_id != profile_id);
        let changed = self.state.followed_contacts.len() != before;
        if changed {
            self.save()?;
        }
        Ok(changed)
    }

    pub fn refresh_contact(&mut self, profile_id: &str) -> Result<()> {
        let now = now_unix_secs();
        let cache_path = self.cache_path(profile_id);
        let Some(contact) = self
            .state
            .followed_contacts
            .iter_mut()
            .find(|contact| contact.profile_id == profile_id)
        else {
            anyhow::bail!("unknown contact {profile_id}");
        };

        match load_profile_records_from_path(&cache_path) {
            Ok(records) => {
                let snapshot = ContactSnapshot::from_records(profile_id, &records)?;
                contact.cached_display_name = snapshot.display_name;
                contact.cached_shares = snapshot.shares;
                contact.last_refreshed_at = Some(now);
                contact.last_error = None;
            }
            Err(err) => {
                contact.last_refreshed_at = Some(now);
                contact.last_error = Some(format!(
                    "Contact records are unavailable for {profile_id}: {err}"
                ));
            }
        }

        self.save()
    }

    pub fn refresh_due_contacts(&mut self, now_unix_secs: u64) -> Result<()> {
        let due_profile_ids = self
            .state
            .followed_contacts
            .iter()
            .filter(|contact| contact.is_refresh_due(now_unix_secs))
            .map(|contact| contact.profile_id.clone())
            .collect::<Vec<_>>();

        for profile_id in due_profile_ids {
            self.refresh_contact(&profile_id)?;
        }

        Ok(())
    }

    pub fn cache_path(&self, profile_id: &str) -> PathBuf {
        self.cache_dir.join(format!("{profile_id}.json"))
    }

    pub fn discover_second_degree_profiles(
        &self,
        include_followed: bool,
        require_shares: bool,
        search_term: &str,
        sort: DiscoverySort,
    ) -> Vec<DiscoveredProfile> {
        let followed_ids = self
            .state
            .followed_contacts
            .iter()
            .map(|contact| contact.profile_id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let mut discovered = std::collections::HashMap::<String, DiscoveredProfile>::new();

        for source_contact in &self.state.followed_contacts {
            let source_records =
                match load_profile_records_from_path(self.cache_path(&source_contact.profile_id)) {
                    Ok(records) => records,
                    Err(err) => {
                        tracing::warn!(
                            "failed to load cached profile records for {}: {err}",
                            source_contact.profile_id
                        );
                        continue;
                    }
                };

            let source_label = latest_profile_metadata(&source_contact.profile_id, &source_records)
                .map(|record| record.display_name)
                .unwrap_or_else(|| source_contact.label());
            for follow_record in
                active_follow_records_for_profile(&source_contact.profile_id, &source_records)
            {
                if follow_record.followed_profile_id == source_contact.profile_id {
                    continue;
                }

                let entry = discovered
                    .entry(follow_record.followed_profile_id.clone())
                    .or_insert_with(|| DiscoveredProfile {
                        profile_id: follow_record.followed_profile_id.clone(),
                        cached_display_name: None,
                        cached_shares: Vec::new(),
                        source_contacts: Vec::new(),
                        mutual_count: 0,
                        last_seen_at: None,
                        already_followed: false,
                        last_error: None,
                    });

                if !entry
                    .source_contacts
                    .iter()
                    .any(|source| source.profile_id == source_contact.profile_id)
                {
                    entry.source_contacts.push(DiscoverySource {
                        profile_id: source_contact.profile_id.clone(),
                        label: source_label.clone(),
                    });
                    entry.mutual_count = entry.source_contacts.len();
                }
                entry.last_seen_at = Some(
                    entry
                        .last_seen_at
                        .map(|timestamp| timestamp.max(follow_record.recorded_at))
                        .unwrap_or(follow_record.recorded_at),
                );
            }
        }

        for entry in discovered.values_mut() {
            entry.already_followed = followed_ids.contains(entry.profile_id.as_str());
            entry
                .source_contacts
                .sort_by(|left, right| left.label.cmp(&right.label));
            match load_profile_records_from_path(self.cache_path(&entry.profile_id)) {
                Ok(records) => match ContactSnapshot::from_records(&entry.profile_id, &records) {
                    Ok(snapshot) => {
                        entry.cached_display_name = snapshot.display_name;
                        entry.cached_shares = snapshot.shares;
                        entry.last_error = None;
                    }
                    Err(err) => {
                        entry.last_error = Some(err.to_string());
                    }
                },
                Err(err) => {
                    entry.last_error = Some(format!(
                        "Cached profile data is unavailable for {}: {err}",
                        entry.profile_id
                    ));
                }
            }
        }

        let search_term = search_term.trim().to_lowercase();
        let mut discovered = discovered
            .into_values()
            .filter(|profile| include_followed || !profile.already_followed)
            .filter(|profile| !require_shares || profile.has_shares())
            .filter(|profile| {
                if search_term.is_empty() {
                    return true;
                }
                let mut haystacks = vec![profile.profile_id.to_lowercase()];
                if let Some(display_name) = profile.display_name() {
                    haystacks.push(display_name.to_lowercase());
                }
                haystacks.extend(
                    profile
                        .source_contacts
                        .iter()
                        .map(|source| source.label.to_lowercase()),
                );
                haystacks
                    .into_iter()
                    .any(|value| value.contains(&search_term))
            })
            .collect::<Vec<_>>();

        discovered.sort_by(|left, right| match sort {
            DiscoverySort::MutualCount => right
                .mutual_count
                .cmp(&left.mutual_count)
                .then_with(|| right.has_shares().cmp(&left.has_shares()))
                .then_with(|| right.last_seen_at.cmp(&left.last_seen_at))
                .then_with(|| left.label().cmp(&right.label())),
            DiscoverySort::RecentlySeen => right
                .last_seen_at
                .cmp(&left.last_seen_at)
                .then_with(|| right.mutual_count.cmp(&left.mutual_count))
                .then_with(|| right.has_shares().cmp(&left.has_shares()))
                .then_with(|| left.label().cmp(&right.label())),
            DiscoverySort::HasShares => right
                .has_shares()
                .cmp(&left.has_shares())
                .then_with(|| right.mutual_count.cmp(&left.mutual_count))
                .then_with(|| right.last_seen_at.cmp(&left.last_seen_at))
                .then_with(|| left.label().cmp(&right.label())),
        });
        discovered
    }

    fn save(&self) -> Result<()> {
        write_atomic(&self.path, &self.state)
    }
}

pub fn contact_records_cache_path(data_dir: impl AsRef<Path>, profile_id: &str) -> PathBuf {
    data_dir
        .as_ref()
        .join(CONTACT_CACHE_DIR_NAME)
        .join(format!("{profile_id}.json"))
}

#[derive(Debug)]
struct ContactSnapshot {
    display_name: Option<String>,
    shares: Vec<ContactShare>,
}

impl ContactSnapshot {
    fn from_records(profile_id: &str, records: &[ProfileRecord]) -> Result<Self> {
        let display_name =
            latest_profile_metadata(profile_id, records).map(|record| record.display_name);
        let mut shares = share_records_for_profile(profile_id, records)
            .into_iter()
            .map(|record| ContactShare {
                share_code: record.share_code,
                collection_hash: record.collection_hash,
                share_name: share_name_from_record(&record.source_dir),
                recorded_at: record.recorded_at,
            })
            .collect::<Vec<_>>();
        shares.sort_by(|left, right| {
            right
                .recorded_at
                .cmp(&left.recorded_at)
                .then_with(|| left.share_name.cmp(&right.share_name))
        });

        if display_name.is_none() && shares.is_empty() {
            anyhow::bail!("no metadata or shares found for profile {profile_id}");
        }

        Ok(Self {
            display_name,
            shares,
        })
    }
}

fn latest_profile_metadata(
    profile_id: &str,
    records: &[ProfileRecord],
) -> Option<ProfileMetadataRecord> {
    records
        .iter()
        .filter_map(|record| match record {
            ProfileRecord::Metadata(record) if record.profile_id == profile_id => {
                Some(record.clone())
            }
            ProfileRecord::Metadata(_)
            | ProfileRecord::ShareOwnership(_)
            | ProfileRecord::ContactFollow(_) => None,
        })
        .max_by(|left, right| {
            left.updated_at
                .cmp(&right.updated_at)
                .then_with(|| left.created_at.cmp(&right.created_at))
        })
}

fn share_records_for_profile(
    profile_id: &str,
    records: &[ProfileRecord],
) -> Vec<ShareOwnershipRecord> {
    records
        .iter()
        .filter_map(|record| match record {
            ProfileRecord::ShareOwnership(record) if record.profile_id == profile_id => {
                Some(record.clone())
            }
            ProfileRecord::Metadata(_)
            | ProfileRecord::ShareOwnership(_)
            | ProfileRecord::ContactFollow(_) => None,
        })
        .collect()
}

fn share_name_from_record(source_dir: &Path) -> String {
    source_dir
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("Shared directory")
        .to_owned()
}

fn normalize_profile_id(profile_id: String) -> Result<String> {
    let profile_id = profile_id.trim().to_owned();
    if profile_id.is_empty() {
        anyhow::bail!("profile ID cannot be empty");
    }
    let _: PublicKey = profile_id
        .parse()
        .with_context(|| format!("invalid profile ID {profile_id}"))?;
    Ok(profile_id)
}

fn truncate_profile_id(profile_id: &str) -> String {
    profile_id.chars().take(8).collect()
}

fn load_from_path(path: &Path) -> Result<PersistedContacts> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse contacts at {}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(PersistedContacts::default()),
        Err(err) => {
            Err(err).with_context(|| format!("failed to read contacts at {}", path.display()))
        }
    }
}

fn write_atomic(path: &Path, state: &PersistedContacts) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create contacts directory {}", parent.display()))?;
    }

    let bytes = serde_json::to_vec_pretty(state).context("failed to serialize contacts")?;
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, bytes).with_context(|| {
        format!(
            "failed to write temporary contacts file {}",
            tmp_path.display()
        )
    })?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to atomically move {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use p2panda_blobs::Hash as BlobHash;
    use p2panda_core::PrivateKey;
    use tempfile::tempdir;

    use super::*;
    use crate::persist::ShareRecord;
    use crate::profile::ProfileStore;

    fn write_node_key(data_dir: &Path, private_key: &PrivateKey) -> Result<()> {
        fs::create_dir_all(data_dir)?;
        fs::write(data_dir.join("node.key"), private_key.as_bytes())?;
        Ok(())
    }

    fn sample_share_record(profile_id: &str) -> ShareRecord {
        let collection_hash = BlobHash::new(b"contact-share");
        let mut record = ShareRecord::new(
            PathBuf::from("/tmp/contact-share"),
            "p2p-CONTACT".to_owned(),
            collection_hash,
            "contact-share".to_owned(),
            1,
            64,
        );
        record.owner_profile_id = Some(profile_id.to_owned());
        record
    }

    #[test]
    fn follow_persists_and_rejects_invalid_ids_or_duplicates() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        let profile_id = private_key.public_key().to_string();
        let mut store = ContactsStore::load(dir.path())?;

        assert!(store.follow_contact("not-a-key").is_err());

        store.follow_contact(profile_id.clone())?;
        assert_eq!(store.contacts().len(), 1);
        assert_eq!(
            store.contacts()[0].label(),
            truncate_profile_id(&profile_id)
        );

        let reloaded = ContactsStore::load(dir.path())?;
        assert_eq!(reloaded.contacts().len(), 1);
        assert!(store.follow_contact(profile_id).is_err());

        Ok(())
    }

    #[test]
    fn refresh_reads_cached_profile_records_and_lists_shares() -> Result<()> {
        let sharer_dir = tempdir()?;
        let follower_dir = tempdir()?;
        let sharer_key = PrivateKey::new();
        let follower_key = PrivateKey::new();
        write_node_key(sharer_dir.path(), &sharer_key)?;
        write_node_key(follower_dir.path(), &follower_key)?;

        let mut sharer_store = ProfileStore::load_or_create(sharer_dir.path())?;
        let share = sample_share_record(&sharer_store.profile().profile_id);
        sharer_store.ensure_share_ownership_record(&share)?;

        let cache_path =
            contact_records_cache_path(follower_dir.path(), &sharer_store.profile().profile_id);
        fs::create_dir_all(cache_path.parent().unwrap())?;
        fs::copy(
            crate::profile::profile_records_path(sharer_dir.path()),
            &cache_path,
        )?;

        let mut contacts = ContactsStore::load(follower_dir.path())?;
        contacts.follow_contact(sharer_store.profile().profile_id.clone())?;
        contacts.refresh_contact(&sharer_store.profile().profile_id)?;

        let contact = contacts.get(&sharer_store.profile().profile_id).unwrap();
        assert_eq!(
            contact.display_name(),
            Some(sharer_store.profile().display_name.as_str())
        );
        assert_eq!(contact.cached_shares.len(), 1);
        assert_eq!(contact.cached_shares[0].share_code, "p2p-CONTACT");
        assert_eq!(contact.cached_shares[0].share_name, "contact-share");
        assert!(contact.last_error.is_none());

        Ok(())
    }

    #[test]
    fn refresh_reports_offline_contacts_clearly() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        let profile_id = private_key.public_key().to_string();
        let mut store = ContactsStore::load(dir.path())?;
        store.follow_contact(profile_id.clone())?;

        store.refresh_contact(&profile_id)?;

        let contact = store.get(&profile_id).unwrap();
        assert!(contact.cached_shares.is_empty());
        assert!(contact
            .last_error
            .as_deref()
            .unwrap_or_default()
            .contains("Contact records are unavailable"));

        Ok(())
    }

    #[test]
    fn removing_contact_does_not_touch_downloaded_files() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        let profile_id = private_key.public_key().to_string();
        let downloaded_file = dir.path().join("downloads").join("kept.txt");
        fs::create_dir_all(downloaded_file.parent().unwrap())?;
        fs::write(&downloaded_file, b"keep me")?;

        let mut store = ContactsStore::load(dir.path())?;
        store.follow_contact(profile_id.clone())?;
        assert!(store.remove_contact(&profile_id)?);
        assert!(store.get(&profile_id).is_none());
        assert_eq!(fs::read(&downloaded_file)?, b"keep me");

        Ok(())
    }

    #[test]
    fn second_degree_discovery_deduplicates_context_and_filters_followed_profiles() -> Result<()> {
        let viewer_dir = tempdir()?;
        let alice_dir = tempdir()?;
        let dana_dir = tempdir()?;
        let bob_dir = tempdir()?;
        let erin_dir = tempdir()?;

        let viewer_key = PrivateKey::new();
        let alice_key = PrivateKey::new();
        let dana_key = PrivateKey::new();
        let bob_key = PrivateKey::new();
        let erin_key = PrivateKey::new();

        write_node_key(viewer_dir.path(), &viewer_key)?;
        write_node_key(alice_dir.path(), &alice_key)?;
        write_node_key(dana_dir.path(), &dana_key)?;
        write_node_key(bob_dir.path(), &bob_key)?;
        write_node_key(erin_dir.path(), &erin_key)?;

        let mut alice_profile = ProfileStore::load_or_create(alice_dir.path())?;
        let mut dana_profile = ProfileStore::load_or_create(dana_dir.path())?;
        let mut bob_profile = ProfileStore::load_or_create(bob_dir.path())?;
        let erin_profile = ProfileStore::load_or_create(erin_dir.path())?;

        alice_profile.follow_contact(bob_profile.profile().profile_id.clone())?;
        dana_profile.follow_contact(bob_profile.profile().profile_id.clone())?;
        dana_profile.follow_contact(erin_profile.profile().profile_id.clone())?;

        let bob_share = sample_share_record(&bob_profile.profile().profile_id);
        bob_profile.ensure_share_ownership_record(&bob_share)?;

        let mut contacts = ContactsStore::load(viewer_dir.path())?;
        contacts.follow_contact(alice_profile.profile().profile_id.clone())?;
        contacts.follow_contact(dana_profile.profile().profile_id.clone())?;

        for profile_id in [
            alice_profile.profile().profile_id.clone(),
            dana_profile.profile().profile_id.clone(),
            bob_profile.profile().profile_id.clone(),
            erin_profile.profile().profile_id.clone(),
        ] {
            let source_path = if profile_id == alice_profile.profile().profile_id {
                crate::profile::profile_records_path(alice_dir.path())
            } else if profile_id == dana_profile.profile().profile_id {
                crate::profile::profile_records_path(dana_dir.path())
            } else if profile_id == bob_profile.profile().profile_id {
                crate::profile::profile_records_path(bob_dir.path())
            } else {
                crate::profile::profile_records_path(erin_dir.path())
            };
            let cache_path = contact_records_cache_path(viewer_dir.path(), &profile_id);
            fs::create_dir_all(cache_path.parent().unwrap())?;
            fs::copy(source_path, cache_path)?;
        }

        contacts.refresh_contact(&alice_profile.profile().profile_id)?;
        contacts.refresh_contact(&dana_profile.profile().profile_id)?;

        let discovered =
            contacts.discover_second_degree_profiles(false, false, "", DiscoverySort::MutualCount);
        assert_eq!(discovered.len(), 2);
        assert_eq!(discovered[0].profile_id, bob_profile.profile().profile_id);
        assert_eq!(discovered[0].mutual_count, 2);
        let mut source_labels = discovered[0]
            .source_contacts
            .iter()
            .map(|source| source.label.as_str())
            .collect::<Vec<_>>();
        let mut expected_labels = vec![
            alice_profile.profile().display_name.as_str(),
            dana_profile.profile().display_name.as_str(),
        ];
        source_labels.sort_unstable();
        expected_labels.sort_unstable();
        assert_eq!(source_labels, expected_labels);
        assert_eq!(discovered[0].cached_shares.len(), 1);
        assert_eq!(discovered[1].profile_id, erin_profile.profile().profile_id);

        contacts.follow_contact(bob_profile.profile().profile_id.clone())?;
        let filtered =
            contacts.discover_second_degree_profiles(false, false, "", DiscoverySort::MutualCount);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].profile_id, erin_profile.profile().profile_id);

        let included =
            contacts.discover_second_degree_profiles(true, true, "", DiscoverySort::HasShares);
        assert_eq!(included.len(), 1);
        assert!(included[0].already_followed);
        assert_eq!(included[0].profile_id, bob_profile.profile().profile_id);

        Ok(())
    }

    #[test]
    fn malformed_second_degree_graph_data_is_ignored_without_crashing() -> Result<()> {
        let viewer_dir = tempdir()?;
        let source_dir = tempdir()?;
        let viewer_key = PrivateKey::new();
        let source_key = PrivateKey::new();
        write_node_key(viewer_dir.path(), &viewer_key)?;
        write_node_key(source_dir.path(), &source_key)?;

        let source_profile = ProfileStore::load_or_create(source_dir.path())?;
        let mut contacts = ContactsStore::load(viewer_dir.path())?;
        contacts.follow_contact(source_profile.profile().profile_id.clone())?;

        let cache_path =
            contact_records_cache_path(viewer_dir.path(), &source_profile.profile().profile_id);
        fs::create_dir_all(cache_path.parent().unwrap())?;
        fs::write(&cache_path, b"{not valid json")?;

        let discovered =
            contacts.discover_second_degree_profiles(false, false, "", DiscoverySort::MutualCount);
        assert!(discovered.is_empty());

        Ok(())
    }
}
