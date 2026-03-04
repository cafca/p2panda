use anyhow::{bail, Result};
use p2panda_blobs::Hash;

const COLLECTION_ANNOUNCEMENT_TAG: u8 = 0x01;
const COLLECTION_ANNOUNCEMENT_LEN: usize = 1 + 32;

/// Gossip message announcing a shared collection hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollectionAnnouncement {
    pub collection_hash: Hash,
}

impl CollectionAnnouncement {
    pub fn new(collection_hash: Hash) -> Self {
        Self { collection_hash }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(COLLECTION_ANNOUNCEMENT_LEN);
        bytes.push(COLLECTION_ANNOUNCEMENT_TAG);
        bytes.extend_from_slice(self.collection_hash.as_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < COLLECTION_ANNOUNCEMENT_LEN {
            bail!("collection announcement too short: {} bytes", bytes.len());
        }

        let Some(tag) = bytes.first().copied() else {
            bail!("collection announcement missing type tag");
        };

        if tag != COLLECTION_ANNOUNCEMENT_TAG {
            bail!("unsupported collection announcement type tag: 0x{tag:02x}");
        }

        let hash_bytes: [u8; 32] = bytes[1..COLLECTION_ANNOUNCEMENT_LEN].try_into()?;
        Ok(Self::new(Hash::from_bytes(hash_bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let announcement = CollectionAnnouncement::new(Hash::new(b"shared collection"));

        let encoded = announcement.encode();
        let decoded = CollectionAnnouncement::decode(&encoded).unwrap();

        assert_eq!(decoded, announcement);
        assert_eq!(encoded.len(), COLLECTION_ANNOUNCEMENT_LEN);
        assert_eq!(encoded[0], COLLECTION_ANNOUNCEMENT_TAG);
    }

    #[test]
    fn decode_rejects_wrong_type_tag() {
        let mut encoded = CollectionAnnouncement::new(Hash::new(b"shared collection")).encode();
        encoded[0] = 0x02;

        assert!(CollectionAnnouncement::decode(&encoded).is_err());
    }

    #[test]
    fn decode_rejects_short_input() {
        assert!(CollectionAnnouncement::decode(&[COLLECTION_ANNOUNCEMENT_TAG; 10]).is_err());
    }
}
