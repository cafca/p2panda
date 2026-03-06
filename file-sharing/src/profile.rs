use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use p2panda_core::cbor::{decode_cbor, encode_cbor};
use p2panda_core::identity::PRIVATE_KEY_LEN;
use p2panda_core::{validate_operation, Body, Hash, Header, Operation, PrivateKey, PublicKey};
use serde::{Deserialize, Serialize};

use crate::persist::ShareRecord;

const PROFILE_FILE_NAME: &str = "profile.json";
const PROFILE_RECORDS_FILE_NAME: &str = "profile-records.json";
const NODE_KEY_FILE_NAME: &str = "node.key";
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
        let timestamp = now_unix_secs();
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileRecord {
    Metadata(ProfileMetadataRecord),
    ShareOwnership(ShareOwnershipRecord),
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
    path: PathBuf,
    records_path: PathBuf,
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
        let path = data_dir.join(PROFILE_FILE_NAME);
        let records_path = data_dir.join(PROFILE_RECORDS_FILE_NAME);

        let (profile, profile_warning) = load_or_create_profile(&path, public_key)?;
        let (records, records_warning) = load_or_create_records(&records_path)?;

        let load_warning = profile_warning.or(records_warning);
        if let Some(message) = &load_warning {
            tracing::warn!("{message}");
        }

        let mut store = Self {
            path,
            records_path,
            profile,
            records,
            load_warning,
            private_key,
        };

        store.normalize_profile_id(public_key)?;
        store.ensure_profile_metadata_record()?;

        Ok(store)
    }

    pub fn profile(&self) -> &UserProfile {
        &self.profile
    }

    pub fn load_warning(&self) -> Option<&str> {
        self.load_warning.as_deref()
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
        self.profile.updated_at = now_unix_secs();
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
                ProfileRecord::ShareOwnership(_) => None,
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
            })
            .collect())
    }

    pub fn ensure_share_ownership_record(&mut self, share: &ShareRecord) -> Result<bool> {
        let owner_profile_id = share
            .owner_profile_id
            .as_deref()
            .unwrap_or(&self.profile.profile_id);

        if self.has_share_ownership_record(
            owner_profile_id,
            &share.collection_hash,
            &share.share_code,
        )? {
            return Ok(false);
        }

        self.append_record(ProfileRecordBody::ShareOwnership {
            profile_id: owner_profile_id.to_owned(),
            collection_hash: share.collection_hash.clone(),
            share_code: share.share_code.clone(),
            source_dir: share.source_dir.clone(),
            recorded_at: now_unix_secs(),
        })?;
        Ok(true)
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

    fn normalize_profile_id(&mut self, public_key: PublicKey) -> Result<()> {
        let expected_profile_id = public_key.to_string();
        if self.profile.profile_id == expected_profile_id {
            return Ok(());
        }

        self.profile.profile_id = expected_profile_id;
        if self.profile.display_name.trim().is_empty() {
            self.profile.display_name = default_display_name(&self.profile.profile_id);
        }
        self.profile.updated_at = now_unix_secs();
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

    fn has_share_ownership_record(
        &self,
        profile_id: &str,
        collection_hash: &str,
        share_code: &str,
    ) -> Result<bool> {
        Ok(self.share_ownership_records()?.into_iter().any(|record| {
            record.profile_id == profile_id
                && record.collection_hash == collection_hash
                && record.share_code == share_code
        }))
    }

    fn append_record(&mut self, body: ProfileRecordBody) -> Result<()> {
        let body_bytes = encode_cbor(&body).context("failed to encode profile record body")?;
        let body = Body::from(body_bytes.clone());
        let previous_hash = self
            .records
            .operations
            .last()
            .map(|operation| operation.header.hash());
        let timestamp = now_unix_secs();
        let mut header = Header {
            version: 1,
            public_key: self.private_key.public_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            timestamp,
            seq_num: self.records.operations.len() as u64,
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
        write_json_atomic(&self.path, &self.profile, "profile")
    }

    fn save_records(&self) -> Result<()> {
        write_json_atomic(&self.records_path, &self.records, "profile records")
    }
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
        } => ProfileRecord::ShareOwnership(ShareOwnershipRecord {
            author,
            profile_id,
            collection_hash,
            share_code,
            source_dir,
            recorded_at,
        }),
    })
}

fn load_or_create_profile(
    path: &Path,
    public_key: PublicKey,
) -> Result<(UserProfile, Option<String>)> {
    match fs::read(path) {
        Ok(bytes) => match serde_json::from_slice::<UserProfile>(&bytes) {
            Ok(profile) => Ok((profile, None)),
            Err(err) => {
                let recovered_path = move_corrupt_file_aside(path)?;
                let profile = UserProfile::new(public_key);
                write_json_atomic(path, &profile, "profile")?;
                Ok((
                    profile,
                    Some(format!(
                        "Recovered profile after parse failure ({err}) and moved corrupt file to {}",
                        recovered_path.display()
                    )),
                ))
            }
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let profile = UserProfile::new(public_key);
            write_json_atomic(path, &profile, "profile")?;
            Ok((profile, None))
        }
        Err(err) => {
            Err(err).with_context(|| format!("failed to read profile file {}", path.display()))
        }
    }
}

fn load_or_create_records(path: &Path) -> Result<(PersistedProfileRecords, Option<String>)> {
    match fs::read(path) {
        Ok(bytes) => match serde_json::from_slice::<PersistedProfileRecords>(&bytes) {
            Ok(records) => Ok((records, None)),
            Err(err) => {
                let recovered_path = move_corrupt_file_aside(path)?;
                let records = PersistedProfileRecords {
                    version: PROFILE_RECORDS_VERSION,
                    operations: Vec::new(),
                };
                write_json_atomic(path, &records, "profile records")?;
                Ok((
                    records,
                    Some(format!(
                        "Recovered profile records after parse failure ({err}) and moved corrupt file to {}",
                        recovered_path.display()
                    )),
                ))
            }
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok((
            PersistedProfileRecords {
                version: PROFILE_RECORDS_VERSION,
                operations: Vec::new(),
            },
            None,
        )),
        Err(err) => Err(err)
            .with_context(|| format!("failed to read profile records file {}", path.display())),
    }
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T, label: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create parent directory for {} at {}",
                label,
                parent.display()
            )
        })?;
    }

    let bytes =
        serde_json::to_vec_pretty(value).with_context(|| format!("failed to serialize {label}"))?;
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, bytes).with_context(|| {
        format!(
            "failed to write temporary {} file {}",
            label,
            tmp_path.display()
        )
    })?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to atomically move temporary {} file {} to {}",
            label,
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn move_corrupt_file_aside(path: &Path) -> Result<PathBuf> {
    let recovered_path = path.with_extension(format!("corrupt-{}.json", now_unix_secs()));
    fs::rename(path, &recovered_path).with_context(|| {
        format!(
            "failed to move corrupt file {} to {}",
            path.display(),
            recovered_path.display()
        )
    })?;
    Ok(recovered_path)
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

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

const fn profile_version() -> u8 {
    PROFILE_VERSION
}

const fn profile_records_version() -> u8 {
    PROFILE_RECORDS_VERSION
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
        fs::write(dir.path().join(PROFILE_FILE_NAME), b"{ definitely not json")?;

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

        Ok(())
    }
}
