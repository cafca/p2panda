use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, ensure, Context, Result};
use p2panda_core::cbor::{decode_cbor, encode_cbor};
use p2panda_core::{validate_operation, Body, Header, Operation, PrivateKey, PublicKey};
use serde::{Deserialize, Serialize};

const HEADER_LENGTH_PREFIX_SIZE: usize = 4;

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

#[derive(Debug, Clone, PartialEq)]
pub struct SignedManifest {
    pub header: Header<()>,
    pub body: Body,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedManifest {
    pub public_key: PublicKey,
    pub data: ManifestData,
}

impl ManifestData {
    pub fn encode(&self) -> Result<Vec<u8>> {
        ensure!(
            self.version == 1,
            "unsupported manifest version {}",
            self.version
        );
        encode_cbor(self).context("failed to encode manifest data as CBOR")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let manifest: Self =
            decode_cbor(bytes).context("failed to decode manifest data from CBOR")?;
        ensure!(
            manifest.version == 1,
            "unsupported manifest version {}",
            manifest.version
        );
        Ok(manifest)
    }
}

impl SignedManifest {
    pub fn new(private_key: &PrivateKey, data: &ManifestData) -> Result<Self> {
        let body_bytes = data.encode()?;
        let body = Body::from(body_bytes);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_secs();

        let mut header = Header {
            version: 1,
            public_key: private_key.public_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            timestamp,
            seq_num: 0,
            backlink: None,
            previous: vec![],
            extensions: (),
        };
        header.sign(private_key);

        Ok(Self { header, body })
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

        let header: Header<()> = decode_cbor(&bytes[HEADER_LENGTH_PREFIX_SIZE..header_end])
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
        Ok(VerifiedManifest {
            public_key: self.header.public_key,
            data,
        })
    }
}

pub fn encode_manifest_data(data: &ManifestData) -> Result<Vec<u8>> {
    data.encode()
}

pub fn decode_manifest_data(bytes: &[u8]) -> Result<ManifestData> {
    ManifestData::decode(bytes)
}

pub fn sign_manifest(private_key: &PrivateKey, data: &ManifestData) -> Result<SignedManifest> {
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

    fn sample_manifest() -> ManifestData {
        ManifestData {
            version: 1,
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
        let private_key = PrivateKey::new();
        let manifest = sample_manifest();

        let signed = sign_manifest(&private_key, &manifest).unwrap();
        let bytes = serialize_manifest(&signed).unwrap();
        let verified = verify_manifest(&bytes).unwrap();

        assert_eq!(verified.public_key, private_key.public_key());
        assert_eq!(verified.data, manifest);
    }

    #[test]
    fn tampered_body_fails_verification() {
        let private_key = PrivateKey::new();
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
}
