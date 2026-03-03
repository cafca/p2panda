use anyhow::{bail, Result};
use p2panda_blobs::Hash;

/// Gossip message announcing a blob available for download.
///
/// Encoding: 32 bytes hash || UTF-8 filename bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct BlobAnnouncement {
    pub hash: Hash,
    pub filename: String,
}

impl BlobAnnouncement {
    pub fn new(hash: Hash, filename: String) -> Self {
        Self { hash, filename }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32 + self.filename.len());
        buf.extend_from_slice(self.hash.as_bytes());
        buf.extend_from_slice(self.filename.as_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 32 {
            bail!("announcement too short: {} bytes", bytes.len());
        }
        let hash_bytes: [u8; 32] = bytes[..32].try_into()?;
        let hash = Hash::from_bytes(hash_bytes);
        let filename = String::from_utf8(bytes[32..].to_vec())
            .map_err(|e| anyhow::anyhow!("invalid filename: {e}"))?;
        Ok(Self { hash, filename })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let hash = Hash::new(b"test data");
        let ann = BlobAnnouncement::new(hash, "hello.txt".to_string());
        let encoded = ann.encode();
        let decoded = BlobAnnouncement::decode(&encoded).unwrap();
        assert_eq!(decoded, ann);
    }

    #[test]
    fn decode_too_short() {
        assert!(BlobAnnouncement::decode(&[0u8; 10]).is_err());
    }
}
