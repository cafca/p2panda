use anyhow::{anyhow, ensure, Context, Result};
use p2panda_core::cbor::{decode_cbor, encode_cbor};
use p2panda_core::timestamp::HybridTimestamp;
use p2panda_core::{validate_operation, Body, Header, Operation, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

const HEADER_LENGTH_PREFIX_SIZE: usize = 4;
const MANIFEST_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestData {
    pub version: u8,
    pub name: String,
    pub files: Vec<ManifestFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestFile {
    pub relative_path: String,
    pub size: u64,
    pub hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestHeaderMetadata {
    pub manifest_version: u8,
    pub name: String,
    pub file_count: u64,
    pub total_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestHeaderExtensions {
    pub metadata: ManifestHeaderMetadata,
    pub ordering_timestamp: HybridTimestamp,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SignedManifest {
    pub header: Header<ManifestHeaderExtensions>,
    pub body: Body,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedManifest {
    pub verifying_key: VerifyingKey,
    pub data: ManifestData,
    pub metadata: ManifestHeaderMetadata,
    pub ordering_timestamp: HybridTimestamp,
}

impl ManifestData {
    pub fn encode(&self) -> Result<Vec<u8>> {
        ensure!(
            self.version == MANIFEST_VERSION,
            "unsupported manifest version {}",
            self.version
        );
        encode_cbor(self).context("failed to encode manifest data as CBOR")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let manifest: Self =
            decode_cbor(bytes).context("failed to decode manifest data from CBOR")?;
        ensure!(
            manifest.version == MANIFEST_VERSION,
            "unsupported manifest version {}",
            manifest.version
        );
        Ok(manifest)
    }
}

impl ManifestHeaderMetadata {
    pub fn from_manifest(data: &ManifestData) -> Self {
        Self {
            manifest_version: data.version,
            name: data.name.clone(),
            file_count: data.files.len() as u64,
            total_size: data.files.iter().map(|file| file.size).sum(),
        }
    }

    fn validate_against(&self, data: &ManifestData) -> Result<()> {
        ensure!(
            self.manifest_version == MANIFEST_VERSION,
            "unsupported manifest header metadata version {}",
            self.manifest_version
        );
        ensure!(
            self.manifest_version == data.version,
            "manifest header metadata version {} does not match body version {}",
            self.manifest_version,
            data.version
        );
        ensure!(
            self.name == data.name,
            "manifest header metadata name does not match body"
        );
        ensure!(
            self.file_count == data.files.len() as u64,
            "manifest header metadata file count does not match body"
        );
        ensure!(
            self.total_size == data.files.iter().map(|file| file.size).sum::<u64>(),
            "manifest header metadata total size does not match body"
        );
        Ok(())
    }
}

impl SignedManifest {
    pub fn new(private_key: &SigningKey, data: &ManifestData) -> Result<Self> {
        let body_bytes = data.encode()?;
        let body = Body::from(body_bytes);
        let ordering_timestamp = HybridTimestamp::now();

        let mut header = Header {
            version: 1,
            verifying_key: private_key.verifying_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            seq_num: 0,
            backlink: None,
            extensions: ManifestHeaderExtensions {
                metadata: ManifestHeaderMetadata::from_manifest(data),
                ordering_timestamp,
            },
        };
        header.sign(private_key);

        Ok(Self { header, body })
    }

    pub fn metadata(&self) -> &ManifestHeaderMetadata {
        &self.header.extensions.metadata
    }

    pub fn ordering_timestamp(&self) -> HybridTimestamp {
        self.header.extensions.ordering_timestamp
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let header_bytes = encode_cbor(&self.header).context("failed to encode manifest header")?;
        let header_len: u32 = header_bytes
            .len()
            .try_into()
            .map_err(|_| anyhow!("manifest header exceeds 4-byte length prefix"))?;

        let body_bytes = self.body.to_bytes();
        let mut bytes =
            Vec::with_capacity(HEADER_LENGTH_PREFIX_SIZE + header_bytes.len() + body_bytes.len());
        bytes.extend_from_slice(&header_len.to_be_bytes());
        bytes.extend_from_slice(&header_bytes);
        bytes.extend_from_slice(&body_bytes);
        Ok(bytes)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() >= HEADER_LENGTH_PREFIX_SIZE,
            "manifest blob too short for header length prefix"
        );

        let mut header_len_prefix = [0u8; HEADER_LENGTH_PREFIX_SIZE];
        header_len_prefix.copy_from_slice(&bytes[..HEADER_LENGTH_PREFIX_SIZE]);
        let header_len = u32::from_be_bytes(header_len_prefix) as usize;
        let header_end = HEADER_LENGTH_PREFIX_SIZE
            .checked_add(header_len)
            .ok_or_else(|| anyhow!("manifest header length overflow"))?;

        ensure!(
            bytes.len() >= header_end,
            "manifest blob shorter than declared header length"
        );

        let header: Header<ManifestHeaderExtensions> =
            decode_cbor(&bytes[HEADER_LENGTH_PREFIX_SIZE..header_end])
                .context("failed to decode manifest header")?;
        let body = Body::from(bytes[header_end..].to_vec());
        Ok(Self { header, body })
    }

    pub fn verify(&self) -> Result<VerifiedManifest> {
        let operation = Operation {
            hash: self.header.hash(),
            header: self.header.clone(),
            body: Some(self.body.clone()),
        };
        validate_operation(&operation).context("manifest operation validation failed")?;

        let data = ManifestData::decode(&self.body.to_bytes())?;
        let metadata = self.header.extensions.metadata.clone();
        metadata.validate_against(&data)?;

        let ordering_timestamp = self.header.extensions.ordering_timestamp;

        Ok(VerifiedManifest {
            verifying_key: self.header.verifying_key,
            data,
            metadata,
            ordering_timestamp,
        })
    }
}

pub fn encode_manifest_data(data: &ManifestData) -> Result<Vec<u8>> {
    data.encode()
}

pub fn decode_manifest_data(bytes: &[u8]) -> Result<ManifestData> {
    ManifestData::decode(bytes)
}

pub fn sign_manifest(private_key: &SigningKey, data: &ManifestData) -> Result<SignedManifest> {
    SignedManifest::new(private_key, data)
}

pub fn serialize_manifest(manifest: &SignedManifest) -> Result<Vec<u8>> {
    manifest.serialize()
}

pub fn deserialize_manifest(bytes: &[u8]) -> Result<SignedManifest> {
    SignedManifest::deserialize(bytes)
}

pub fn verify_manifest(bytes: &[u8]) -> Result<VerifiedManifest> {
    SignedManifest::deserialize(bytes)?.verify()
}

#[cfg(test)]
mod tests {
    use super::*;
    use p2panda_core::cbor::encode_cbor;

    fn sample_manifest() -> ManifestData {
        ManifestData {
            version: MANIFEST_VERSION,
            name: "shared-dir".to_string(),
            files: vec![
                ManifestFile {
                    relative_path: "photos/2026/cat.png".to_string(),
                    size: 42,
                    hash: [1; 32],
                },
                ManifestFile {
                    relative_path: "docs/readme.txt".to_string(),
                    size: 7,
                    hash: [2; 32],
                },
            ],
        }
    }

    #[test]
    fn signed_manifest_roundtrips() {
        let private_key = SigningKey::generate();
        let manifest = sample_manifest();

        let signed = sign_manifest(&private_key, &manifest).unwrap();
        let bytes = serialize_manifest(&signed).unwrap();
        let verified = verify_manifest(&bytes).unwrap();

        assert_eq!(verified.verifying_key, private_key.verifying_key());
        assert_eq!(verified.data, manifest);
        assert_eq!(
            verified.metadata,
            ManifestHeaderMetadata::from_manifest(&verified.data)
        );
        assert_eq!(verified.ordering_timestamp, signed.ordering_timestamp());
    }

    #[test]
    fn tampered_body_fails_verification() {
        let private_key = SigningKey::generate();
        let manifest = sample_manifest();

        let signed = sign_manifest(&private_key, &manifest).unwrap();
        let mut bytes = serialize_manifest(&signed).unwrap();
        let last_index = bytes.len() - 1;
        bytes[last_index] ^= 0x01;

        let err = verify_manifest(&bytes).unwrap_err();
        assert!(err
            .to_string()
            .contains("manifest operation validation failed"));
    }

    #[test]
    fn nested_paths_encode_decode() {
        let manifest = sample_manifest();

        let bytes = encode_manifest_data(&manifest).unwrap();
        let decoded = decode_manifest_data(&bytes).unwrap();

        assert_eq!(
            decoded.files[0].relative_path,
            "photos/2026/cat.png".to_string()
        );
        assert_eq!(
            decoded.files[1].relative_path,
            "docs/readme.txt".to_string()
        );
        assert_eq!(decoded, manifest);
    }

    #[test]
    fn manifest_metadata_is_available_from_header_extensions() {
        let private_key = SigningKey::generate();
        let manifest = sample_manifest();

        let signed = sign_manifest(&private_key, &manifest).unwrap();
        let expected_metadata = ManifestHeaderMetadata::from_manifest(&manifest);

        assert_eq!(signed.metadata(), &expected_metadata);
        assert_eq!(
            signed.ordering_timestamp(),
            signed.header.extensions.ordering_timestamp
        );
    }

    #[test]
    fn invalid_extension_metadata_fails_verification() {
        let private_key = SigningKey::generate();
        let manifest = sample_manifest();
        let body = Body::from(manifest.encode().unwrap());
        let ordering_timestamp = HybridTimestamp::now();
        let mut header = Header {
            version: 1,
            verifying_key: private_key.verifying_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            seq_num: 0,
            backlink: None,
            extensions: ManifestHeaderExtensions {
                metadata: ManifestHeaderMetadata {
                    manifest_version: MANIFEST_VERSION,
                    name: manifest.name.clone(),
                    file_count: 999,
                    total_size: manifest.files.iter().map(|file| file.size).sum(),
                },
                ordering_timestamp,
            },
        };
        header.sign(&private_key);

        let err = SignedManifest { header, body }.verify().unwrap_err();
        assert!(err
            .to_string()
            .contains("manifest header metadata file count does not match body"));
    }

    #[test]
    fn malformed_extension_payload_is_rejected() {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        struct MalformedManifestHeaderExtensions {
            metadata: String,
            ordering_timestamp: String,
        }

        let private_key = SigningKey::generate();
        let manifest = sample_manifest();
        let body_bytes = manifest.encode().unwrap();
        let body = Body::from(body_bytes.clone());
        let mut malformed_header = Header {
            version: 1,
            verifying_key: private_key.verifying_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            seq_num: 0,
            backlink: None,
            extensions: MalformedManifestHeaderExtensions {
                metadata: "not-a-manifest-metadata-map".to_string(),
                ordering_timestamp: "not-a-hybrid-timestamp".to_string(),
            },
        };
        malformed_header.sign(&private_key);

        let header_bytes = encode_cbor(&malformed_header).unwrap();
        let header_len = u32::try_from(header_bytes.len()).unwrap();
        let mut bytes =
            Vec::with_capacity(HEADER_LENGTH_PREFIX_SIZE + header_bytes.len() + body_bytes.len());
        bytes.extend_from_slice(&header_len.to_be_bytes());
        bytes.extend_from_slice(&header_bytes);
        bytes.extend_from_slice(&body_bytes);

        let err = verify_manifest(&bytes).unwrap_err();
        assert!(err.to_string().contains("failed to decode manifest header"));
    }
}
