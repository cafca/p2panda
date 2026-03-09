use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use p2panda_net::timestamp::Timestamp;
use p2panda_store::sqlite::store::Pool;

use anyhow::{Context, Result};
use p2panda_core::cbor::{decode_cbor, encode_cbor};
use p2panda_core::identity::PRIVATE_KEY_LEN;
use p2panda_core::{
    validate_operation, Body, Hash, Header, Operation, PrivateKey, PublicKey, RawOperation,
};
use serde::{Deserialize, Serialize};

use crate::persist::ShareRecord;
use crate::profile_data::{load_json_key, open_profile_data_store, write_json_key};

#[cfg(test)]
const PROFILE_FILE_NAME: &str = "profile.json";
#[cfg(test)]
const PROFILE_RECORDS_FILE_NAME: &str = "profile-records.json";
const NODE_KEY_FILE_NAME: &str = "node.key";
const PROFILE_STORE_PROFILE_KEY: &str = "user_profile";
const PROFILE_STORE_RECORDS_KEY: &str = "profile_records";
const PROFILE_VERSION: u8 = 1;
const PROFILE_RECORDS_VERSION: u8 = 1;

const ADJECTIVES: &[&str] = &[
    "Amber", "Brisk", "Calm", "Daring", "Gentle", "Mellow", "Nimble", "Quiet", "Rustic", "Solar",
    "Tidy", "Velvet",
];

const FRUITS: &[&str] = &[
    "Apple",
    "Apricot",
    "Berry",
    "Clementine",
    "Fig",
    "Kiwi",
    "Lemon",
    "Mango",
    "Melon",
    "Olive",
    "Pear",
    "Plum",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserProfile {
    #[serde(default = "profile_version")]
    pub version: u8,
    pub profile_id: String,
    pub display_name: String,
    pub created_at: u64,
    pub updated_at: u64,
}

impl UserProfile {
    fn new(public_key: PublicKey) -> Self {
        let timestamp = u64::from(Timestamp::now());
        let profile_id = public_key.to_string();
        Self {
            version: PROFILE_VERSION,
            display_name: default_display_name(&profile_id),
            profile_id,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfileMetadataRecord {
    pub author: PublicKey,
    pub profile_id: String,
    pub display_name: String,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareOwnershipRecord {
    pub author: PublicKey,
    pub profile_id: String,
    pub collection_hash: String,
    pub share_code: String,
    pub source_dir: PathBuf,
    pub recorded_at: u64,
    pub source_contact_profile_id: Option<String>,
    pub source_contact_display_name: Option<String>,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContactFollowRecord {
    pub author: PublicKey,
    pub profile_id: String,
    pub followed_profile_id: String,
    pub recorded_at: u64,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileRecord {
    Metadata(ProfileMetadataRecord),
    ShareOwnership(ShareOwnershipRecord),
    ContactFollow(ContactFollowRecord),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShareOwnershipUpdate {
    profile_id: String,
    collection_hash: String,
    share_code: String,
    source_dir: PathBuf,
    source_contact_profile_id: Option<String>,
    source_contact_display_name: Option<String>,
    active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ProfileRecordBody {
    Metadata {
        profile_id: String,
        display_name: String,
        created_at: u64,
        updated_at: u64,
    },
    ShareOwnership {
        profile_id: String,
        collection_hash: String,
        share_code: String,
        source_dir: PathBuf,
        recorded_at: u64,
        #[serde(default)]
        source_contact_profile_id: Option<String>,
        #[serde(default)]
        source_contact_display_name: Option<String>,
        #[serde(default = "share_record_active_default")]
        active: bool,
    },
    ContactFollow {
        profile_id: String,
        followed_profile_id: String,
        recorded_at: u64,
        active: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredOperation {
    header: Header<()>,
    #[serde(with = "serde_bytes")]
    body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
struct PersistedProfileRecords {
    #[serde(default = "profile_records_version")]
    version: u8,
    #[serde(default)]
    operations: Vec<StoredOperation>,
}

#[derive(Debug, bevy::prelude::Resource)]
pub struct ProfileStore {
    pool: Pool,
    profile: UserProfile,
    records: PersistedProfileRecords,
    load_warning: Option<String>,
    private_key: PrivateKey,
}

impl ProfileStore {
    pub fn load_or_create(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        fs::create_dir_all(data_dir).with_context(|| {
            format!("failed to create profile directory {}", data_dir.display())
        })?;

        let private_key = load_private_key(data_dir)?;
        let public_key = private_key.public_key();
        let store_data = open_profile_data_store(data_dir)?;
        let profile = load_or_create_profile(&store_data.pool, public_key)?;
        let records = load_or_create_records(&store_data.pool)?;
        let load_warning = store_data.load_warning;
        if let Some(message) = &load_warning {
            tracing::warn!("{message}");
        }

        let mut store = Self {
            pool: store_data.pool,
            profile,
            records,
            load_warning,
            private_key,
        };

        store.validate_profile_id()?;
        store.ensure_profile_metadata_record()?;

        Ok(store)
    }

    pub fn profile(&self) -> &UserProfile {
        &self.profile
    }

    pub fn load_warning(&self) -> Option<&str> {
        self.load_warning.as_deref()
    }

    pub fn set_profile_id(&mut self, profile_id: impl Into<String>) -> Result<bool> {
        let profile_id = normalize_profile_id(profile_id.into())?;
        if self.profile.profile_id == profile_id {
            return Ok(false);
        }

        self.profile.profile_id = profile_id;
        if self.profile.display_name.trim().is_empty() {
            self.profile.display_name = default_display_name(&self.profile.profile_id);
        }
        self.profile.updated_at = u64::from(Timestamp::now());
        self.save_profile()?;
        self.ensure_profile_metadata_record()?;
        Ok(true)
    }

    pub fn update_display_name(&mut self, display_name: impl Into<String>) -> Result<bool> {
        let display_name = display_name.into().trim().to_owned();
        if display_name.is_empty() {
            anyhow::bail!("display name cannot be empty");
        }

        if self.profile.display_name == display_name {
            return Ok(false);
        }

        self.profile.display_name = display_name;
        self.profile.updated_at = u64::from(Timestamp::now());
        self.save_profile()?;
        self.append_record(ProfileRecordBody::Metadata {
            profile_id: self.profile.profile_id.clone(),
            display_name: self.profile.display_name.clone(),
            created_at: self.profile.created_at,
            updated_at: self.profile.updated_at,
        })?;
        Ok(true)
    }

    pub fn records(&self) -> Result<Vec<ProfileRecord>> {
        self.records
            .operations
            .iter()
            .map(decode_stored_operation)
            .collect()
    }

    pub fn metadata_records(&self) -> Result<Vec<ProfileMetadataRecord>> {
        Ok(self
            .records()?
            .into_iter()
            .filter_map(|record| match record {
                ProfileRecord::Metadata(record) => Some(record),
                ProfileRecord::ShareOwnership(_) | ProfileRecord::ContactFollow(_) => None,
            })
            .collect())
    }

    pub fn share_ownership_records(&self) -> Result<Vec<ShareOwnershipRecord>> {
        Ok(self
            .records()?
            .into_iter()
            .filter_map(|record| match record {
                ProfileRecord::Metadata(_) => None,
                ProfileRecord::ShareOwnership(record) => Some(record),
                ProfileRecord::ContactFollow(_) => None,
            })
            .collect())
    }

    pub fn contact_follow_records(&self) -> Result<Vec<ContactFollowRecord>> {
        Ok(self
            .records()?
            .into_iter()
            .filter_map(|record| match record {
                ProfileRecord::ContactFollow(record) => Some(record),
                ProfileRecord::Metadata(_) | ProfileRecord::ShareOwnership(_) => None,
            })
            .collect())
    }

    pub fn follow_contact(&mut self, followed_profile_id: impl Into<String>) -> Result<bool> {
        self.set_contact_follow_state(followed_profile_id.into(), true)
    }

    pub fn unfollow_contact(&mut self, followed_profile_id: impl Into<String>) -> Result<bool> {
        self.set_contact_follow_state(followed_profile_id.into(), false)
    }

    pub fn ensure_share_ownership_record(&mut self, share: &ShareRecord) -> Result<bool> {
        let owner_profile_id = share
            .owner_profile_id
            .as_deref()
            .unwrap_or(&self.profile.profile_id);

        self.set_share_ownership_state(ShareOwnershipUpdate {
            profile_id: owner_profile_id.to_owned(),
            collection_hash: share.collection_hash.clone(),
            share_code: share.share_code.clone(),
            source_dir: share.source_dir.clone(),
            source_contact_profile_id: None,
            source_contact_display_name: None,
            active: true,
        })
    }

    pub fn ensure_downloaded_share_record(
        &mut self,
        profile_id: &str,
        share_code: impl Into<String>,
        collection_hash: impl Into<String>,
        source_dir: impl Into<PathBuf>,
        source_contact_profile_id: Option<String>,
        source_contact_display_name: Option<String>,
    ) -> Result<bool> {
        self.set_share_ownership_state(ShareOwnershipUpdate {
            profile_id: profile_id.to_owned(),
            collection_hash: collection_hash.into(),
            share_code: share_code.into(),
            source_dir: source_dir.into(),
            source_contact_profile_id,
            source_contact_display_name,
            active: true,
        })
    }

    pub fn remove_share_ownership_record(&mut self, share: &ShareRecord) -> Result<bool> {
        let owner_profile_id = share
            .owner_profile_id
            .as_deref()
            .unwrap_or(&self.profile.profile_id);

        self.set_share_ownership_state(ShareOwnershipUpdate {
            profile_id: owner_profile_id.to_owned(),
            collection_hash: share.collection_hash.clone(),
            share_code: share.share_code.clone(),
            source_dir: share.source_dir.clone(),
            source_contact_profile_id: None,
            source_contact_display_name: None,
            active: false,
        })
    }

    pub fn ensure_share_ownership_records<'a>(
        &mut self,
        shares: impl IntoIterator<Item = &'a ShareRecord>,
    ) -> Result<usize> {
        let mut appended = 0usize;
        for share in shares {
            if self.ensure_share_ownership_record(share)? {
                appended += 1;
            }
        }
        Ok(appended)
    }

    fn validate_profile_id(&mut self) -> Result<()> {
        self.profile.profile_id = normalize_profile_id(self.profile.profile_id.clone())?;
        if self.profile.display_name.trim().is_empty() {
            self.profile.display_name = default_display_name(&self.profile.profile_id);
        }
        self.save_profile()?;
        Ok(())
    }

    fn ensure_profile_metadata_record(&mut self) -> Result<()> {
        let already_present = self.metadata_records()?.into_iter().any(|record| {
            record.profile_id == self.profile.profile_id
                && record.display_name == self.profile.display_name
                && record.updated_at == self.profile.updated_at
        });
        if already_present {
            return Ok(());
        }

        self.append_record(ProfileRecordBody::Metadata {
            profile_id: self.profile.profile_id.clone(),
            display_name: self.profile.display_name.clone(),
            created_at: self.profile.created_at,
            updated_at: self.profile.updated_at,
        })?;
        Ok(())
    }

    fn set_share_ownership_state(&mut self, update: ShareOwnershipUpdate) -> Result<bool> {
        let share_records = self.share_ownership_records()?;
        let latest = latest_share_ownership_record(
            &update.profile_id,
            &update.collection_hash,
            &update.share_code,
            &share_records,
        );
        if let Some(latest) = latest {
            if latest.active == update.active {
                return Ok(false);
            }
        }

        self.append_record(ProfileRecordBody::ShareOwnership {
            profile_id: update.profile_id,
            collection_hash: update.collection_hash,
            share_code: update.share_code,
            source_dir: update.source_dir,
            recorded_at: u64::from(Timestamp::now()),
            source_contact_profile_id: update.source_contact_profile_id,
            source_contact_display_name: update.source_contact_display_name,
            active: update.active,
        })?;
        Ok(true)
    }

    fn set_contact_follow_state(
        &mut self,
        followed_profile_id: String,
        active: bool,
    ) -> Result<bool> {
        let followed_profile_id = normalize_profile_id(followed_profile_id)?;
        let follow_records = self.contact_follow_records()?;
        let latest = latest_contact_follow_record(
            &self.profile.profile_id,
            &followed_profile_id,
            &follow_records,
        );
        if let Some(latest) = latest {
            if latest.active == active {
                return Ok(false);
            }
        }

        self.append_record(ProfileRecordBody::ContactFollow {
            profile_id: self.profile.profile_id.clone(),
            followed_profile_id,
            recorded_at: u64::from(Timestamp::now()),
            active,
        })?;
        Ok(true)
    }

    fn append_record(&mut self, body: ProfileRecordBody) -> Result<()> {
        let body_bytes = encode_cbor(&body).context("failed to encode profile record body")?;
        let body = Body::from(body_bytes.clone());
        let previous_hash = self
            .records
            .operations
            .iter()
            .rev()
            .find(|operation| operation.header.public_key == self.private_key.public_key())
            .map(|operation| operation.header.hash());
        let seq_num = self
            .records
            .operations
            .iter()
            .filter(|operation| operation.header.public_key == self.private_key.public_key())
            .count() as u64;
        let timestamp = u64::from(Timestamp::now());
        let mut header = Header {
            version: 1,
            public_key: self.private_key.public_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            timestamp,
            seq_num,
            backlink: previous_hash,
            previous: previous_hash.into_iter().collect(),
            extensions: (),
        };
        header.sign(&self.private_key);

        self.records.operations.push(StoredOperation {
            header,
            body: body_bytes,
        });
        self.save_records()
    }

    fn save_profile(&self) -> Result<()> {
        write_json_key(&self.pool, PROFILE_STORE_PROFILE_KEY, &self.profile)
    }

    fn save_records(&self) -> Result<()> {
        write_json_key(&self.pool, PROFILE_STORE_RECORDS_KEY, &self.records)
    }
}

pub fn load_profile_records(data_dir: impl AsRef<Path>) -> Result<Vec<ProfileRecord>> {
    let store = open_profile_data_store(data_dir.as_ref())?;
    let records: PersistedProfileRecords =
        load_json_key(&store.pool, PROFILE_STORE_RECORDS_KEY)?.unwrap_or_default();
    records
        .operations
        .iter()
        .map(decode_stored_operation)
        .collect()
}

pub fn load_raw_profile_operations(data_dir: impl AsRef<Path>) -> Result<Vec<RawOperation>> {
    let store = open_profile_data_store(data_dir.as_ref())?;
    let records: PersistedProfileRecords =
        load_json_key(&store.pool, PROFILE_STORE_RECORDS_KEY)?.unwrap_or_default();
    Ok(records
        .operations
        .into_iter()
        .map(|operation| (operation.header.to_bytes(), Some(operation.body)))
        .collect())
}

pub fn write_raw_profile_operations(
    data_dir: impl AsRef<Path>,
    operations: impl IntoIterator<Item = RawOperation>,
) -> Result<()> {
    let operations = operations
        .into_iter()
        .map(|(header_bytes, body)| {
            let header = ciborium::de::from_reader::<Header<()>, _>(&header_bytes[..])
                .context("failed to decode raw profile operation header")?;
            let body = body.context("profile sync operation is missing a body")?;
            Ok(StoredOperation { header, body })
        })
        .collect::<Result<Vec<_>>>()?;

    let records = PersistedProfileRecords {
        version: PROFILE_RECORDS_VERSION,
        operations,
    };
    let store = open_profile_data_store(data_dir.as_ref())?;
    write_json_key(&store.pool, PROFILE_STORE_RECORDS_KEY, &records)
}

pub(crate) fn load_private_key_from_data_dir(data_dir: &Path) -> Result<PrivateKey> {
    load_private_key(data_dir)
}

pub fn active_follow_records_for_profile(
    profile_id: &str,
    records: &[ProfileRecord],
) -> Vec<ContactFollowRecord> {
    let mut latest_records = HashMap::<String, ContactFollowRecord>::new();
    for record in records {
        let ProfileRecord::ContactFollow(record) = record else {
            continue;
        };
        if record.profile_id != profile_id {
            continue;
        }
        latest_records.insert(record.followed_profile_id.clone(), record.clone());
    }

    let mut active_records = latest_records
        .into_values()
        .filter(|record| record.active)
        .collect::<Vec<_>>();
    active_records.sort_by(|left, right| {
        right
            .recorded_at
            .cmp(&left.recorded_at)
            .then_with(|| left.followed_profile_id.cmp(&right.followed_profile_id))
    });
    active_records
}

fn decode_stored_operation(operation: &StoredOperation) -> Result<ProfileRecord> {
    let body = Body::from(operation.body.clone());
    let validated = Operation {
        hash: operation.header.hash(),
        header: operation.header.clone(),
        body: Some(body),
    };
    validate_operation(&validated).context("profile record operation validation failed")?;

    let author = operation.header.public_key;
    let body: ProfileRecordBody =
        decode_cbor(operation.body.as_slice()).context("failed to decode profile record body")?;
    Ok(match body {
        ProfileRecordBody::Metadata {
            profile_id,
            display_name,
            created_at,
            updated_at,
        } => ProfileRecord::Metadata(ProfileMetadataRecord {
            author,
            profile_id,
            display_name,
            created_at,
            updated_at,
        }),
        ProfileRecordBody::ShareOwnership {
            profile_id,
            collection_hash,
            share_code,
            source_dir,
            recorded_at,
            source_contact_profile_id,
            source_contact_display_name,
            active,
        } => ProfileRecord::ShareOwnership(ShareOwnershipRecord {
            author,
            profile_id,
            collection_hash,
            share_code,
            source_dir,
            recorded_at,
            source_contact_profile_id,
            source_contact_display_name,
            active,
        }),
        ProfileRecordBody::ContactFollow {
            profile_id,
            followed_profile_id,
            recorded_at,
            active,
        } => ProfileRecord::ContactFollow(ContactFollowRecord {
            author,
            profile_id,
            followed_profile_id,
            recorded_at,
            active,
        }),
    })
}

fn load_or_create_profile(pool: &Pool, public_key: PublicKey) -> Result<UserProfile> {
    if let Some(profile) = load_json_key(pool, PROFILE_STORE_PROFILE_KEY)? {
        return Ok(profile);
    }

    let profile = UserProfile::new(public_key);
    write_json_key(pool, PROFILE_STORE_PROFILE_KEY, &profile)?;
    Ok(profile)
}

fn load_or_create_records(pool: &Pool) -> Result<PersistedProfileRecords> {
    Ok(load_json_key(pool, PROFILE_STORE_RECORDS_KEY)?.unwrap_or_default())
}

fn latest_contact_follow_record<'a>(
    profile_id: &str,
    followed_profile_id: &str,
    records: &'a [ContactFollowRecord],
) -> Option<&'a ContactFollowRecord> {
    records.iter().rev().find(|record| {
        record.profile_id == profile_id && record.followed_profile_id == followed_profile_id
    })
}

fn latest_share_ownership_record<'a>(
    profile_id: &str,
    collection_hash: &str,
    share_code: &str,
    records: &'a [ShareOwnershipRecord],
) -> Option<&'a ShareOwnershipRecord> {
    records.iter().rev().find(|record| {
        record.profile_id == profile_id
            && record.collection_hash == collection_hash
            && record.share_code == share_code
    })
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

fn load_private_key(data_dir: &Path) -> Result<PrivateKey> {
    let key_path = data_dir.join(NODE_KEY_FILE_NAME);
    let bytes = fs::read(&key_path)
        .with_context(|| format!("failed to read node private key {}", key_path.display()))?;
    let byte_len = bytes.len();
    let key_bytes: [u8; PRIVATE_KEY_LEN] = bytes.try_into().map_err(|_| {
        anyhow::anyhow!(
            "invalid private key length in {}: expected {} bytes, got {}",
            key_path.display(),
            PRIVATE_KEY_LEN,
            byte_len
        )
    })?;
    Ok(PrivateKey::from_bytes(&key_bytes))
}

fn default_display_name(profile_id: &str) -> String {
    let seed = Hash::new(profile_id.as_bytes());
    let bytes = seed.as_bytes();
    let adjective = ADJECTIVES[bytes[0] as usize % ADJECTIVES.len()];
    let fruit = FRUITS[bytes[1] as usize % FRUITS.len()];
    format!("{adjective} {fruit}")
}

const fn profile_version() -> u8 {
    PROFILE_VERSION
}

const fn profile_records_version() -> u8 {
    PROFILE_RECORDS_VERSION
}

const fn share_record_active_default() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use p2panda_blobs::Hash as BlobHash;
    use tempfile::tempdir;

    use super::*;

    fn write_node_key(data_dir: &Path, private_key: &PrivateKey) -> Result<()> {
        fs::create_dir_all(data_dir)?;
        fs::write(data_dir.join(NODE_KEY_FILE_NAME), private_key.as_bytes())?;
        Ok(())
    }

    fn sample_share_record(profile_id: &str) -> ShareRecord {
        let collection_hash = BlobHash::new(b"profile-share");
        let mut record = ShareRecord::new(
            PathBuf::from("/tmp/source"),
            "p2p-SHARE".to_owned(),
            collection_hash,
            "source".to_owned(),
            1,
            3,
        );
        record.owner_profile_id = Some(profile_id.to_owned());
        record
    }

    #[test]
    fn first_load_creates_stable_profile_and_metadata_record() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        write_node_key(dir.path(), &private_key)?;

        let store = ProfileStore::load_or_create(dir.path())?;
        assert_eq!(
            store.profile().profile_id,
            private_key.public_key().to_string()
        );
        assert!(store.profile().display_name.contains(' '));

        let reloaded = ProfileStore::load_or_create(dir.path())?;
        assert_eq!(reloaded.profile(), store.profile());

        let metadata = reloaded.metadata_records()?;
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].profile_id, store.profile().profile_id);
        assert_eq!(metadata[0].author, private_key.public_key());

        Ok(())
    }

    #[test]
    fn display_name_updates_persist_and_append_metadata_record() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        write_node_key(dir.path(), &private_key)?;

        let mut store = ProfileStore::load_or_create(dir.path())?;
        assert!(store.update_display_name("Whimsical Watermelon")?);

        let reloaded = ProfileStore::load_or_create(dir.path())?;
        assert_eq!(reloaded.profile().display_name, "Whimsical Watermelon");

        let metadata = reloaded.metadata_records()?;
        assert_eq!(metadata.len(), 2);
        assert_eq!(
            metadata.last().unwrap().display_name,
            "Whimsical Watermelon"
        );
        assert_eq!(metadata.last().unwrap().author, private_key.public_key());

        Ok(())
    }

    #[test]
    fn corrupt_profile_file_recovers_from_node_identity() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        write_node_key(dir.path(), &private_key)?;
        fs::write(
            dir.path().join("profile-store.sqlite3"),
            b"definitely not sqlite",
        )?;

        let store = ProfileStore::load_or_create(dir.path())?;

        assert_eq!(
            store.profile().profile_id,
            private_key.public_key().to_string()
        );
        assert!(store.load_warning().is_some());
        assert!(fs::read_dir(dir.path())?
            .filter_map(Result::ok)
            .any(|entry| entry.file_name().to_string_lossy().contains("corrupt")));

        Ok(())
    }

    #[test]
    fn share_ownership_records_are_queryable_from_p2panda_operations() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        write_node_key(dir.path(), &private_key)?;

        let mut store = ProfileStore::load_or_create(dir.path())?;
        let share = sample_share_record(&store.profile().profile_id);
        assert!(store.ensure_share_ownership_record(&share)?);

        let ownerships = store.share_ownership_records()?;
        assert_eq!(ownerships.len(), 1);
        assert_eq!(ownerships[0].profile_id, store.profile().profile_id);
        assert_eq!(ownerships[0].share_code, "p2p-SHARE");
        assert_eq!(ownerships[0].author, private_key.public_key());
        assert!(ownerships[0].active);
        assert!(ownerships[0].source_contact_profile_id.is_none());

        Ok(())
    }

    #[test]
    fn downloaded_share_records_can_store_contact_provenance() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        write_node_key(dir.path(), &private_key)?;

        let mut store = ProfileStore::load_or_create(dir.path())?;
        let local_profile_id = store.profile().profile_id.clone();
        assert!(store.ensure_downloaded_share_record(
            &local_profile_id,
            "p2p-DOWNLOAD",
            BlobHash::new(b"downloaded-share").to_string(),
            PathBuf::from("/tmp/downloads/shared"),
            Some("contact-profile".into()),
            Some("Alice".into()),
        )?);

        let ownerships = store.share_ownership_records()?;
        assert!(ownerships.iter().any(|record| {
            record.profile_id == local_profile_id
                && record.share_code == "p2p-DOWNLOAD"
                && record.active
                && record.source_contact_profile_id.as_deref() == Some("contact-profile")
                && record.source_contact_display_name.as_deref() == Some("Alice")
        }));

        Ok(())
    }

    #[test]
    fn share_removal_records_append_tombstone_and_allow_republish() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        write_node_key(dir.path(), &private_key)?;

        let mut store = ProfileStore::load_or_create(dir.path())?;
        let share = sample_share_record(&store.profile().profile_id);
        assert!(store.ensure_share_ownership_record(&share)?);
        assert!(store.remove_share_ownership_record(&share)?);
        assert!(store.ensure_share_ownership_record(&share)?);

        let ownerships = store.share_ownership_records()?;
        assert_eq!(ownerships.len(), 3);
        assert!(ownerships[0].active);
        assert!(!ownerships[1].active);
        assert!(ownerships[2].active);

        Ok(())
    }

    #[test]
    fn follow_records_roundtrip_and_track_latest_active_state() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        let followed_key = PrivateKey::new();
        write_node_key(dir.path(), &private_key)?;

        let mut store = ProfileStore::load_or_create(dir.path())?;
        assert!(store.follow_contact(followed_key.public_key().to_string())?);
        assert!(store.unfollow_contact(followed_key.public_key().to_string())?);

        let records = store.contact_follow_records()?;
        assert_eq!(records.len(), 2);
        assert!(records[0].active);
        assert!(!records[1].active);

        let active =
            active_follow_records_for_profile(&store.profile().profile_id, &store.records()?);
        assert!(active.is_empty());

        Ok(())
    }

    #[test]
    fn shared_profile_id_can_be_adopted_and_persisted() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        let shared_profile_id = PrivateKey::new().public_key().to_string();
        write_node_key(dir.path(), &private_key)?;

        let mut store = ProfileStore::load_or_create(dir.path())?;
        assert!(store.set_profile_id(shared_profile_id.clone())?);
        assert_eq!(store.profile().profile_id, shared_profile_id);

        let reloaded = ProfileStore::load_or_create(dir.path())?;
        assert_eq!(reloaded.profile().profile_id, shared_profile_id);
        assert_eq!(
            reloaded.profile().display_name,
            store.profile().display_name
        );
        assert_ne!(
            reloaded.profile().profile_id,
            private_key.public_key().to_string()
        );

        Ok(())
    }

    #[test]
    fn local_profile_state_survives_restart_without_legacy_json_files() -> Result<()> {
        let dir = tempdir()?;
        let private_key = PrivateKey::new();
        let followed_key = PrivateKey::new();
        write_node_key(dir.path(), &private_key)?;

        let share = {
            let mut store = ProfileStore::load_or_create(dir.path())?;
            store.update_display_name("Restart Persisted")?;
            let share = sample_share_record(&store.profile().profile_id);
            assert!(store.ensure_share_ownership_record(&share)?);
            assert!(store.follow_contact(followed_key.public_key().to_string())?);
            share
        };

        let reloaded = ProfileStore::load_or_create(dir.path())?;
        assert_eq!(reloaded.profile().display_name, "Restart Persisted");
        assert_eq!(reloaded.share_ownership_records()?.len(), 1);
        assert!(reloaded
            .share_ownership_records()?
            .iter()
            .any(|record| { record.share_code == share.share_code && record.active }));
        assert_eq!(
            active_follow_records_for_profile(&reloaded.profile().profile_id, &reloaded.records()?)
                .len(),
            1
        );
        assert!(!dir.path().join(PROFILE_FILE_NAME).exists());
        assert!(!dir.path().join(PROFILE_RECORDS_FILE_NAME).exists());

        Ok(())
    }
}
