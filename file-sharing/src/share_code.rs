use anyhow::{ensure, Context, Result};
use data_encoding::BASE32_NOPAD;
use p2panda_blobs::Hash as BlobHash;
use p2panda_core::{Hash as CoreHash, PublicKey};
use p2panda_net::TopicId;
use serde::{Deserialize, Serialize};

const SHARE_CODE_PREFIX: &str = "p2p-";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareCode {
    pub collection_hash: [u8; 32],
    pub node_id: [u8; 32],
    pub relay_url: Option<String>,
    pub owner_profile_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct EncodedShareCode {
    #[serde(with = "serde_bytes")]
    collection_hash: Vec<u8>,
    #[serde(with = "serde_bytes")]
    node_id: Vec<u8>,
    #[serde(default)]
    relay_url: Option<String>,
    #[serde(default)]
    owner_profile_id: Option<String>,
}

impl ShareCode {
    pub fn new(
        collection_hash: BlobHash,
        node_id: PublicKey,
        relay_url: Option<String>,
        owner_profile_id: Option<String>,
    ) -> Self {
        Self {
            collection_hash: *collection_hash.as_bytes(),
            node_id: *node_id.as_bytes(),
            relay_url,
            owner_profile_id,
        }
    }

    pub fn encode(&self) -> Result<String> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&EncodedShareCode::from(self), &mut bytes)
            .context("failed to encode share code as CBOR")?;
        Ok(format!(
            "{SHARE_CODE_PREFIX}{}",
            BASE32_NOPAD.encode(&bytes)
        ))
    }

    pub fn decode(encoded: &str) -> Result<Self> {
        ensure!(
            encoded.starts_with(SHARE_CODE_PREFIX),
            "share code must start with {SHARE_CODE_PREFIX}"
        );

        let payload = &encoded[SHARE_CODE_PREFIX.len()..];
        let bytes = BASE32_NOPAD
            .decode(payload.as_bytes())
            .map_err(|err| anyhow::anyhow!("invalid base32 share code: {err}"))?;

        let decoded: EncodedShareCode =
            ciborium::de::from_reader(bytes.as_slice()).context("malformed CBOR share code")?;
        decoded.try_into()
    }

    pub fn collection_hash(&self) -> BlobHash {
        BlobHash::from_bytes(self.collection_hash)
    }

    pub fn node_id(&self) -> Result<PublicKey> {
        PublicKey::from_bytes(&self.node_id).context("invalid node_id public key in share code")
    }

    pub fn owner_profile_id(&self) -> Result<String> {
        let profile_id = self
            .owner_profile_id
            .clone()
            .unwrap_or_else(|| self.node_id().map(|node_id| node_id.to_string()).unwrap());
        let _: PublicKey = profile_id
            .parse()
            .with_context(|| format!("invalid owner profile ID {profile_id} in share code"))?;
        Ok(profile_id)
    }

    pub fn topic_id(&self) -> TopicId {
        derive_topic(self.collection_hash)
    }
}

pub fn derive_topic(collection_hash: [u8; 32]) -> TopicId {
    CoreHash::new(collection_hash).into()
}

pub fn encode_share_code(
    collection_hash: BlobHash,
    node_id: PublicKey,
    relay_url: Option<String>,
    owner_profile_id: Option<String>,
) -> Result<String> {
    ShareCode::new(collection_hash, node_id, relay_url, owner_profile_id).encode()
}

pub fn decode_share_code(encoded: &str) -> Result<ShareCode> {
    ShareCode::decode(encoded)
}

impl From<&ShareCode> for EncodedShareCode {
    fn from(value: &ShareCode) -> Self {
        Self {
            collection_hash: value.collection_hash.to_vec(),
            node_id: value.node_id.to_vec(),
            relay_url: value.relay_url.clone(),
            owner_profile_id: value.owner_profile_id.clone(),
        }
    }
}

impl TryFrom<EncodedShareCode> for ShareCode {
    type Error = anyhow::Error;

    fn try_from(value: EncodedShareCode) -> Result<Self, Self::Error> {
        let collection_hash: [u8; 32] = value
            .collection_hash
            .try_into()
            .map_err(|_| anyhow::anyhow!("share code collection_hash must be 32 bytes"))?;
        let node_id: [u8; 32] = value
            .node_id
            .try_into()
            .map_err(|_| anyhow::anyhow!("share code node_id must be 32 bytes"))?;

        Ok(Self {
            collection_hash,
            node_id,
            relay_url: value.relay_url,
            owner_profile_id: value.owner_profile_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use p2panda_core::PrivateKey;

    fn sample_share_code(relay_url: Option<&str>) -> ShareCode {
        ShareCode::new(
            BlobHash::new(b"collection-root"),
            PrivateKey::from_bytes(&[7; 32]).public_key(),
            relay_url.map(str::to_owned),
            Some(PrivateKey::from_bytes(&[9; 32]).public_key().to_string()),
        )
    }

    #[test]
    fn encode_decode_roundtrip_preserves_all_fields() {
        let share_code = sample_share_code(Some("https://relay.example.com"));

        let encoded = share_code.encode().unwrap();
        let decoded = ShareCode::decode(&encoded).unwrap();

        assert_eq!(decoded, share_code);
        assert_eq!(decoded.collection_hash(), share_code.collection_hash());
        assert_eq!(decoded.node_id().unwrap(), share_code.node_id().unwrap());
        assert_eq!(
            decoded.owner_profile_id().unwrap(),
            share_code.owner_profile_id().unwrap()
        );
        assert_eq!(decoded.topic_id(), derive_topic(share_code.collection_hash));
    }

    #[test]
    fn decode_rejects_missing_prefix() {
        let err = ShareCode::decode("not-a-share-code").unwrap_err();
        assert!(err.to_string().contains("must start with p2p-"));
    }

    #[test]
    fn decode_rejects_invalid_base32_and_truncated_data() {
        let invalid_base32 = ShareCode::decode("p2p-INVALID!");
        assert!(invalid_base32
            .unwrap_err()
            .to_string()
            .contains("invalid base32"));

        let truncated = BASE32_NOPAD.encode(&[0xA1]);
        let err = ShareCode::decode(&format!("p2p-{truncated}")).unwrap_err();
        assert!(err.to_string().contains("malformed CBOR"));
    }

    #[test]
    fn share_codes_stay_compact() {
        let no_relay = sample_share_code(None).encode().unwrap();
        let with_relay = sample_share_code(Some("https://relay.example.com/path"))
            .encode()
            .unwrap();

        assert!(
            no_relay.len() < 120,
            "share code too long: {}",
            no_relay.len()
        );
        assert!(
            with_relay.len() < 180,
            "share code too long: {}",
            with_relay.len()
        );
        assert!(no_relay.len() < with_relay.len());
    }

    #[test]
    fn derived_topic_is_deterministic() {
        let share_code = sample_share_code(None);

        let expected_topic: TopicId = CoreHash::new(share_code.collection_hash).into();
        assert_eq!(share_code.topic_id(), expected_topic);
    }
}
