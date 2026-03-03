// SPDX-License-Identifier: MIT OR Apache-2.0

use anyhow::Result;
use futures_util::{Stream, StreamExt};
use iroh_blobs::api::TempTag;
use iroh_blobs::api::tags::{TagInfo, Tags};
use iroh_blobs::{BlobFormat, Hash, HashAndFormat};

/// Named pin management — a wrapper over iroh-blobs tags for GC lifecycle control.
pub struct Pins<'a> {
    tags: &'a Tags,
}

impl<'a> Pins<'a> {
    pub(crate) fn new(tags: &'a Tags) -> Self {
        Self { tags }
    }

    /// Pin a blob by name, protecting it from garbage collection.
    pub async fn set(&self, name: impl AsRef<str>, hash: Hash) -> Result<()> {
        self.tags
            .set(
                name.as_ref(),
                HashAndFormat {
                    hash,
                    format: BlobFormat::Raw,
                },
            )
            .await?;
        Ok(())
    }

    /// Get the hash for a named pin, if it exists.
    pub async fn get(&self, name: impl AsRef<str>) -> Result<Option<Hash>> {
        Ok(self
            .tags
            .get(name.as_ref())
            .await?
            .map(|i| i.hash))
    }

    /// Remove a named pin. The blob becomes eligible for GC if no other pins or temp pins hold it.
    pub async fn delete(&self, name: impl AsRef<str>) -> Result<()> {
        self.tags.delete(name.as_ref()).await?;
        Ok(())
    }

    /// List all named pins.
    pub async fn list(&self) -> Result<impl Stream<Item = Result<TagInfo>>> {
        Ok(self.tags.list().await?.map(|r| r.map_err(Into::into)))
    }

    /// List all pins with the given prefix.
    pub async fn list_prefix(&self, prefix: impl AsRef<str>) -> Result<Vec<TagInfo>> {
        let mut results = Vec::new();
        let mut stream = self.tags.list_prefix(prefix.as_ref()).await?;
        while let Some(item) = stream.next().await {
            results.push(item?);
        }
        Ok(results)
    }

    /// Delete all pins matching the given prefix.
    pub async fn delete_prefix(&self, prefix: impl AsRef<str>) -> Result<()> {
        self.tags.delete_prefix(prefix.as_ref()).await?;
        Ok(())
    }

    /// Create a temporary pin. The blob is protected from GC while the returned guard is held.
    pub async fn temp(&self, hash: Hash) -> Result<TempTag> {
        Ok(self
            .tags
            .temp_tag(HashAndFormat {
                hash,
                format: BlobFormat::Raw,
            })
            .await?)
    }
}
