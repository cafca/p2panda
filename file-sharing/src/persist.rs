use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;

use anyhow::{Context, Result};
use p2panda_blobs::Hash as BlobHash;
use p2panda_net::gossip::GossipHandle;
use p2panda_net::TopicId;
use p2panda_store::sqlite::store::{run_pending_migrations, Pool};
use serde::{Deserialize, Serialize};
use sqlx::query;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::Row;

use crate::download::{download_share, DownloadSession};
use crate::node::AppNode;
use crate::share::ShareSession;
use crate::share_code::derive_topic;

const STATE_FILE_NAME: &str = "state.json";
const STATE_DB_FILE_NAME: &str = "state.sqlite3";
const GLOBAL_PAUSED_KEY: &str = "global_paused";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedState {
    #[serde(default)]
    pub active_downloads: Vec<DownloadRecord>,
    #[serde(default)]
    pub active_shares: Vec<ShareRecord>,
    #[serde(default)]
    pub global_paused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadRecord {
    pub share_code: String,
    pub output_dir: PathBuf,
    pub collection_hash: String,
    #[serde(default)]
    pub source_contact_profile_id: Option<String>,
    #[serde(default)]
    pub source_contact_display_name: Option<String>,
    #[serde(default)]
    pub paused: bool,
}

impl DownloadRecord {
    pub fn new(
        share_code: impl Into<String>,
        output_dir: impl Into<PathBuf>,
        collection_hash: BlobHash,
    ) -> Self {
        Self {
            share_code: share_code.into(),
            output_dir: output_dir.into(),
            collection_hash: collection_hash.to_hex(),
            source_contact_profile_id: None,
            source_contact_display_name: None,
            paused: false,
        }
    }

    pub fn collection_hash(&self) -> Result<BlobHash> {
        self.collection_hash
            .parse()
            .with_context(|| format!("invalid collection hash {}", self.collection_hash))
    }

    pub fn with_source_contact(
        mut self,
        profile_id: Option<String>,
        display_name: Option<String>,
    ) -> Self {
        self.source_contact_profile_id = profile_id;
        self.source_contact_display_name = display_name;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareRecord {
    pub source_dir: PathBuf,
    pub share_code: String,
    pub collection_hash: String,
    #[serde(default)]
    pub owner_profile_id: Option<String>,
    #[serde(default)]
    pub directory_name: String,
    #[serde(default)]
    pub file_count: usize,
    #[serde(default)]
    pub total_bytes: u64,
    #[serde(default)]
    pub paused: bool,
}

impl ShareRecord {
    pub fn new(
        source_dir: impl Into<PathBuf>,
        share_code: impl Into<String>,
        collection_hash: BlobHash,
        directory_name: impl Into<String>,
        file_count: usize,
        total_bytes: u64,
    ) -> Self {
        Self {
            source_dir: source_dir.into(),
            share_code: share_code.into(),
            collection_hash: collection_hash.to_hex(),
            owner_profile_id: None,
            directory_name: directory_name.into(),
            file_count,
            total_bytes,
            paused: false,
        }
    }

    pub fn collection_hash(&self) -> Result<BlobHash> {
        self.collection_hash
            .parse()
            .with_context(|| format!("invalid collection hash {}", self.collection_hash))
    }

    pub fn with_owner_profile_id(mut self, owner_profile_id: impl Into<Option<String>>) -> Self {
        self.owner_profile_id = owner_profile_id.into();
        self
    }
}

impl From<&ShareSession> for ShareRecord {
    fn from(value: &ShareSession) -> Self {
        let directory_name = value
            .source_dir
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| !name.is_empty())
            .unwrap_or("Shared directory");
        ShareRecord::new(
            value.source_dir.clone(),
            value.share_code.clone(),
            value.collection_hash,
            directory_name,
            value.file_count(),
            value.total_bytes,
        )
        .with_owner_profile_id(value.owner_profile_id.clone())
    }
}

pub struct RecoveredShare {
    pub record: ShareRecord,
    pub topic_id: TopicId,
    pub handle: GossipHandle,
}

pub struct RecoveryState {
    pub shares: Vec<RecoveredShare>,
    pub downloads: Vec<DownloadSession>,
}

#[derive(Debug, Clone)]
pub struct StateStore {
    path: PathBuf,
    pool: Pool,
    state: PersistedState,
}

impl StateStore {
    pub fn load(data_dir: impl AsRef<Path>) -> Result<Self> {
        let path = state_db_path(data_dir.as_ref());
        let pool = open_state_pool(&path)?;
        let legacy_path = state_file_path(data_dir.as_ref());
        let mut state = load_state_from_sqlite(&pool)?;

        if state_is_empty(&state) && legacy_path.is_file() {
            let legacy_state = load_legacy_state_from_path(&legacy_path)?;
            write_state_snapshot(&pool, &legacy_state)?;
            state = load_state_from_sqlite(&pool)?;
        }

        Ok(Self { path, pool, state })
    }

    pub fn state(&self) -> &PersistedState {
        &self.state
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn save(&self) -> Result<()> {
        write_state_snapshot(&self.pool, &self.state)
    }

    pub fn set_global_paused(&mut self, paused: bool) -> Result<()> {
        if self.state.global_paused != paused {
            set_global_paused_in_sqlite(&self.pool, paused)?;
            self.state.global_paused = paused;
        }
        Ok(())
    }

    pub fn add_share(&mut self, record: ShareRecord) -> Result<()> {
        upsert_share_in_sqlite(&self.pool, &record)?;

        if let Some(existing) = self
            .state
            .active_shares
            .iter_mut()
            .find(|existing| existing.share_code == record.share_code)
        {
            *existing = record;
            return Ok(());
        }

        self.state.active_shares.push(record);
        self.state
            .active_shares
            .sort_by(|left, right| left.share_code.cmp(&right.share_code));
        Ok(())
    }

    pub fn attach_profile_to_existing_shares(&mut self, profile_id: &str) -> Result<usize> {
        let updated = attach_profile_to_shares_in_sqlite(&self.pool, profile_id)?;

        if updated > 0 {
            for record in &mut self.state.active_shares {
                if record.owner_profile_id.as_deref() != Some(profile_id) {
                    record.owner_profile_id = Some(profile_id.to_owned());
                }
            }
        }

        Ok(updated)
    }

    pub fn add_download(&mut self, record: DownloadRecord) -> Result<()> {
        upsert_download_in_sqlite(&self.pool, &record)?;

        if let Some(existing) = self.state.active_downloads.iter_mut().find(|existing| {
            existing.share_code == record.share_code && existing.output_dir == record.output_dir
        }) {
            *existing = record;
            return Ok(());
        }

        self.state.active_downloads.push(record);
        self.state.active_downloads.sort_by(|left, right| {
            left.share_code
                .cmp(&right.share_code)
                .then_with(|| left.output_dir.cmp(&right.output_dir))
        });
        Ok(())
    }

    pub fn remove_share_by_code(&mut self, share_code: &str) -> Result<bool> {
        let changed = delete_share_from_sqlite(&self.pool, share_code)?;
        if changed {
            self.state
                .active_shares
                .retain(|record| record.share_code != share_code);
        }
        Ok(changed)
    }

    pub fn remove_download_by_code(&mut self, share_code: &str, output_dir: &Path) -> Result<bool> {
        let changed = delete_download_from_sqlite(&self.pool, share_code, output_dir)?;
        if changed {
            self.state.active_downloads.retain(|record| {
                !(record.share_code == share_code && record.output_dir == output_dir)
            });
        }
        Ok(changed)
    }
}

pub fn load_state(data_dir: impl AsRef<Path>) -> Result<PersistedState> {
    StateStore::load(data_dir).map(|store| store.state().clone())
}

pub fn save_state(data_dir: impl AsRef<Path>, state: &PersistedState) -> Result<()> {
    let mut store = StateStore::load(data_dir)?;
    store.state = state.clone();
    store.save()
}

pub async fn resume_shares(node: &AppNode, state: &PersistedState) -> Result<Vec<RecoveredShare>> {
    if state.global_paused {
        return Ok(Vec::new());
    }

    let mut recovered = Vec::with_capacity(state.active_shares.len());

    for record in &state.active_shares {
        if record.paused {
            continue;
        }
        recovered.push(resume_share_record(node, record).await?);
    }

    Ok(recovered)
}

pub async fn resume_downloads(
    node: &AppNode,
    state: &PersistedState,
) -> Result<Vec<DownloadSession>> {
    if state.global_paused {
        return Ok(Vec::new());
    }

    let mut resumed = Vec::with_capacity(state.active_downloads.len());

    for record in &state.active_downloads {
        if record.paused {
            continue;
        }
        resumed.push(
            download_share(node, &record.share_code, &record.output_dir)
                .await
                .with_context(|| {
                    format!(
                        "failed to resume download for {} into {}",
                        record.share_code,
                        record.output_dir.display()
                    )
                })?,
        );
    }

    Ok(resumed)
}

pub async fn resume_active_transfers(
    node: &AppNode,
    state: &PersistedState,
) -> Result<RecoveryState> {
    let shares = resume_shares(node, state).await?;
    let downloads = resume_downloads(node, state).await?;

    Ok(RecoveryState { shares, downloads })
}

pub async fn resume_share_record(node: &AppNode, record: &ShareRecord) -> Result<RecoveredShare> {
    let collection_hash = record.collection_hash()?;
    let topic_id = derive_topic(*collection_hash.as_bytes());
    let handle = node
        .join_topic(topic_id)
        .await
        .with_context(|| format!("failed to join topic for {}", record.share_code))?;

    Ok(RecoveredShare {
        record: record.clone(),
        topic_id,
        handle,
    })
}

fn state_file_path(data_dir: &Path) -> PathBuf {
    data_dir.join(STATE_FILE_NAME)
}

fn state_db_path(data_dir: &Path) -> PathBuf {
    data_dir.join(STATE_DB_FILE_NAME)
}

fn state_is_empty(state: &PersistedState) -> bool {
    !state.global_paused && state.active_shares.is_empty() && state.active_downloads.is_empty()
}

fn open_state_pool(path: &Path) -> Result<Pool> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create state directory {}", parent.display()))?;
    }

    let db_path = path.to_path_buf();
    block_on_db(async move {
        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .with_context(|| format!("failed to open transfer database {}", db_path.display()))?;

        run_pending_migrations(&pool)
            .await
            .context("failed to run p2panda-store SQLite migrations")?;
        ensure_state_tables(&pool).await?;

        Ok(pool)
    })
}

async fn ensure_state_tables(pool: &Pool) -> Result<()> {
    query(
        "CREATE TABLE IF NOT EXISTS transfer_state_meta_v1 (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await
    .context("failed to create transfer state metadata table")?;

    query(
        "CREATE TABLE IF NOT EXISTS transfer_active_shares_v1 (
            share_code TEXT PRIMARY KEY NOT NULL,
            source_dir TEXT NOT NULL,
            collection_hash TEXT NOT NULL,
            owner_profile_id TEXT,
            directory_name TEXT NOT NULL,
            file_count TEXT NOT NULL,
            total_bytes TEXT NOT NULL,
            paused INTEGER NOT NULL
        )",
    )
    .execute(pool)
    .await
    .context("failed to create transfer share table")?;

    query(
        "CREATE INDEX IF NOT EXISTS idx_transfer_active_shares_v1_owner
         ON transfer_active_shares_v1(owner_profile_id)",
    )
    .execute(pool)
    .await
    .context("failed to create transfer share owner index")?;

    query(
        "CREATE TABLE IF NOT EXISTS transfer_active_downloads_v1 (
            share_code TEXT NOT NULL,
            output_dir TEXT NOT NULL,
            collection_hash TEXT NOT NULL,
            source_contact_profile_id TEXT,
            source_contact_display_name TEXT,
            paused INTEGER NOT NULL,
            PRIMARY KEY (share_code, output_dir)
        )",
    )
    .execute(pool)
    .await
    .context("failed to create transfer download table")?;

    query(
        "CREATE INDEX IF NOT EXISTS idx_transfer_active_downloads_v1_output
         ON transfer_active_downloads_v1(output_dir)",
    )
    .execute(pool)
    .await
    .context("failed to create transfer download output index")?;

    Ok(())
}

fn load_state_from_sqlite(pool: &Pool) -> Result<PersistedState> {
    let pool = pool.clone();
    block_on_db(async move {
        let global_paused = query("SELECT value FROM transfer_state_meta_v1 WHERE key = ?")
            .bind(GLOBAL_PAUSED_KEY)
            .fetch_optional(&pool)
            .await
            .context("failed to load transfer global pause state")?
            .map(|row| parse_sqlite_bool(&row.get::<String, _>("value")))
            .transpose()?
            .unwrap_or(false);

        let share_rows = query(
            "SELECT
                source_dir,
                share_code,
                collection_hash,
                owner_profile_id,
                directory_name,
                file_count,
                total_bytes,
                paused
             FROM transfer_active_shares_v1
             ORDER BY share_code ASC",
        )
        .fetch_all(&pool)
        .await
        .context("failed to load persisted shares from SQLite")?;
        let mut active_shares = Vec::with_capacity(share_rows.len());
        for row in share_rows {
            active_shares.push(ShareRecord {
                source_dir: PathBuf::from(row.get::<String, _>("source_dir")),
                share_code: row.get("share_code"),
                collection_hash: row.get("collection_hash"),
                owner_profile_id: row.get("owner_profile_id"),
                directory_name: row.get("directory_name"),
                file_count: row
                    .get::<String, _>("file_count")
                    .parse()
                    .context("invalid persisted share file_count")?,
                total_bytes: row
                    .get::<String, _>("total_bytes")
                    .parse()
                    .context("invalid persisted share total_bytes")?,
                paused: row.get::<i64, _>("paused") != 0,
            });
        }

        let download_rows = query(
            "SELECT
                share_code,
                output_dir,
                collection_hash,
                source_contact_profile_id,
                source_contact_display_name,
                paused
             FROM transfer_active_downloads_v1
             ORDER BY share_code ASC, output_dir ASC",
        )
        .fetch_all(&pool)
        .await
        .context("failed to load persisted downloads from SQLite")?;
        let mut active_downloads = Vec::with_capacity(download_rows.len());
        for row in download_rows {
            active_downloads.push(DownloadRecord {
                share_code: row.get("share_code"),
                output_dir: PathBuf::from(row.get::<String, _>("output_dir")),
                collection_hash: row.get("collection_hash"),
                source_contact_profile_id: row.get("source_contact_profile_id"),
                source_contact_display_name: row.get("source_contact_display_name"),
                paused: row.get::<i64, _>("paused") != 0,
            });
        }

        Ok(PersistedState {
            active_downloads,
            active_shares,
            global_paused,
        })
    })
}

fn write_state_snapshot(pool: &Pool, state: &PersistedState) -> Result<()> {
    let pool = pool.clone();
    let snapshot = state.clone();
    block_on_db(async move {
        let mut tx = pool
            .begin()
            .await
            .context("failed to begin transfer state SQLite transaction")?;

        query(
            "INSERT INTO transfer_state_meta_v1 (key, value)
             VALUES (?, ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(GLOBAL_PAUSED_KEY)
        .bind(sqlite_bool_value(snapshot.global_paused))
        .execute(&mut *tx)
        .await
        .context("failed to persist transfer global pause state")?;

        query("DELETE FROM transfer_active_shares_v1")
            .execute(&mut *tx)
            .await
            .context("failed to clear persisted shares")?;
        for record in &snapshot.active_shares {
            insert_share_query(record)
                .execute(&mut *tx)
                .await
                .with_context(|| {
                    format!("failed to persist share {} into SQLite", record.share_code)
                })?;
        }

        query("DELETE FROM transfer_active_downloads_v1")
            .execute(&mut *tx)
            .await
            .context("failed to clear persisted downloads")?;
        for record in &snapshot.active_downloads {
            insert_download_query(record)
                .execute(&mut *tx)
                .await
                .with_context(|| {
                    format!(
                        "failed to persist download {} into SQLite",
                        record.share_code
                    )
                })?;
        }

        tx.commit()
            .await
            .context("failed to commit transfer state SQLite transaction")?;
        Ok(())
    })
}

fn set_global_paused_in_sqlite(pool: &Pool, paused: bool) -> Result<()> {
    let pool = pool.clone();
    block_on_db(async move {
        query(
            "INSERT INTO transfer_state_meta_v1 (key, value)
             VALUES (?, ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(GLOBAL_PAUSED_KEY)
        .bind(sqlite_bool_value(paused))
        .execute(&pool)
        .await
        .context("failed to update transfer global pause state in SQLite")?;
        Ok(())
    })
}

fn upsert_share_in_sqlite(pool: &Pool, record: &ShareRecord) -> Result<()> {
    let pool = pool.clone();
    let record = record.clone();
    block_on_db(async move {
        query(
            "INSERT INTO transfer_active_shares_v1 (
                share_code,
                source_dir,
                collection_hash,
                owner_profile_id,
                directory_name,
                file_count,
                total_bytes,
                paused
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(share_code) DO UPDATE SET
                source_dir = excluded.source_dir,
                collection_hash = excluded.collection_hash,
                owner_profile_id = excluded.owner_profile_id,
                directory_name = excluded.directory_name,
                file_count = excluded.file_count,
                total_bytes = excluded.total_bytes,
                paused = excluded.paused",
        )
        .bind(record.share_code)
        .bind(path_to_string(&record.source_dir))
        .bind(record.collection_hash)
        .bind(record.owner_profile_id)
        .bind(record.directory_name)
        .bind(record.file_count.to_string())
        .bind(record.total_bytes.to_string())
        .bind(sqlite_bool_int(record.paused))
        .execute(&pool)
        .await
        .context("failed to upsert persisted share in SQLite")?;
        Ok(())
    })
}

fn attach_profile_to_shares_in_sqlite(pool: &Pool, profile_id: &str) -> Result<usize> {
    let pool = pool.clone();
    let profile_id = profile_id.to_owned();
    block_on_db(async move {
        let result = query(
            "UPDATE transfer_active_shares_v1
             SET owner_profile_id = ?
             WHERE owner_profile_id IS NULL OR owner_profile_id != ?",
        )
        .bind(&profile_id)
        .bind(&profile_id)
        .execute(&pool)
        .await
        .context("failed to attach owner profile to persisted shares")?;
        Ok(result.rows_affected() as usize)
    })
}

fn upsert_download_in_sqlite(pool: &Pool, record: &DownloadRecord) -> Result<()> {
    let pool = pool.clone();
    let record = record.clone();
    block_on_db(async move {
        query(
            "INSERT INTO transfer_active_downloads_v1 (
                share_code,
                output_dir,
                collection_hash,
                source_contact_profile_id,
                source_contact_display_name,
                paused
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(share_code, output_dir) DO UPDATE SET
                collection_hash = excluded.collection_hash,
                source_contact_profile_id = excluded.source_contact_profile_id,
                source_contact_display_name = excluded.source_contact_display_name,
                paused = excluded.paused",
        )
        .bind(record.share_code)
        .bind(path_to_string(&record.output_dir))
        .bind(record.collection_hash)
        .bind(record.source_contact_profile_id)
        .bind(record.source_contact_display_name)
        .bind(sqlite_bool_int(record.paused))
        .execute(&pool)
        .await
        .context("failed to upsert persisted download in SQLite")?;
        Ok(())
    })
}

fn delete_share_from_sqlite(pool: &Pool, share_code: &str) -> Result<bool> {
    let pool = pool.clone();
    let share_code = share_code.to_owned();
    block_on_db(async move {
        let result = query("DELETE FROM transfer_active_shares_v1 WHERE share_code = ?")
            .bind(share_code)
            .execute(&pool)
            .await
            .context("failed to delete persisted share from SQLite")?;
        Ok(result.rows_affected() != 0)
    })
}

fn delete_download_from_sqlite(pool: &Pool, share_code: &str, output_dir: &Path) -> Result<bool> {
    let pool = pool.clone();
    let share_code = share_code.to_owned();
    let output_dir = path_to_string(output_dir);
    block_on_db(async move {
        let result = query(
            "DELETE FROM transfer_active_downloads_v1
             WHERE share_code = ? AND output_dir = ?",
        )
        .bind(share_code)
        .bind(output_dir)
        .execute(&pool)
        .await
        .context("failed to delete persisted download from SQLite")?;
        Ok(result.rows_affected() != 0)
    })
}

fn insert_share_query<'a>(
    record: &'a ShareRecord,
) -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>> {
    query(
        "INSERT INTO transfer_active_shares_v1 (
            share_code,
            source_dir,
            collection_hash,
            owner_profile_id,
            directory_name,
            file_count,
            total_bytes,
            paused
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(record.share_code.as_str())
    .bind(path_to_string(&record.source_dir))
    .bind(record.collection_hash.as_str())
    .bind(record.owner_profile_id.as_deref())
    .bind(record.directory_name.as_str())
    .bind(record.file_count.to_string())
    .bind(record.total_bytes.to_string())
    .bind(sqlite_bool_int(record.paused))
}

fn insert_download_query<'a>(
    record: &'a DownloadRecord,
) -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>> {
    query(
        "INSERT INTO transfer_active_downloads_v1 (
            share_code,
            output_dir,
            collection_hash,
            source_contact_profile_id,
            source_contact_display_name,
            paused
        ) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(record.share_code.as_str())
    .bind(path_to_string(&record.output_dir))
    .bind(record.collection_hash.as_str())
    .bind(record.source_contact_profile_id.as_deref())
    .bind(record.source_contact_display_name.as_deref())
    .bind(sqlite_bool_int(record.paused))
}

fn load_legacy_state_from_path(path: &Path) -> Result<PersistedState> {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse persisted state at {}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(PersistedState::default()),
        Err(err) => Err(err)
            .with_context(|| format!("failed to read persisted state at {}", path.display())),
    }
}

fn block_on_db<F, T>(future: F) -> Result<T>
where
    F: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        let (tx, rx) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("failed to build SQLite helper runtime")
                .and_then(|runtime| runtime.block_on(future));
            let _ = tx.send(result);
        });

        return rx
            .recv()
            .context("SQLite helper thread terminated before returning a result")?;
    }

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build SQLite helper runtime")?
        .block_on(future)
}

fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn sqlite_bool_int(value: bool) -> i64 {
    if value {
        1
    } else {
        0
    }
}

fn sqlite_bool_value(value: bool) -> &'static str {
    if value {
        "1"
    } else {
        "0"
    }
}

fn parse_sqlite_bool(value: &str) -> Result<bool> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        other => anyhow::bail!("invalid persisted boolean value {other}"),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Result;
    use p2panda_net::addrs::NodeInfo;
    use tempfile::tempdir;

    use super::*;
    use crate::node::NodeOptions;
    use crate::share::share_directory;

    #[test]
    fn missing_state_file_loads_default_state() -> Result<()> {
        let dir = tempdir()?;
        let state = load_state(dir.path())?;

        assert!(state.active_shares.is_empty());
        assert!(state.active_downloads.is_empty());
        assert!(dir.path().join(STATE_DB_FILE_NAME).is_file());

        Ok(())
    }

    #[test]
    fn add_share_and_download_persist_in_sqlite() -> Result<()> {
        let dir = tempdir()?;
        let mut store = StateStore::load(dir.path())?;
        let hash: BlobHash = "f627847f3d5ebecf169f2e08e10c22f14e8e3a25f8b73f7f15f2f6f5ddf7c905"
            .parse()
            .unwrap();

        store.add_share(ShareRecord::new(
            dir.path().join("source"),
            "p2p-SHARE",
            hash,
            "source",
            2,
            42,
        ))?;
        store.add_download(DownloadRecord::new(
            "p2p-DOWNLOAD",
            dir.path().join("output"),
            hash,
        ))?;

        let state_db_path = dir.path().join(STATE_DB_FILE_NAME);
        assert!(state_db_path.is_file());
        assert!(!dir.path().join(STATE_FILE_NAME).exists());

        let loaded = load_state(dir.path())?;
        assert_eq!(loaded.active_shares.len(), 1);
        assert_eq!(loaded.active_downloads.len(), 1);
        assert!(!loaded.active_shares[0].paused);
        assert!(!loaded.active_downloads[0].paused);
        assert!(!loaded.global_paused);

        Ok(())
    }

    #[test]
    fn legacy_state_without_share_metadata_fields_migrates_to_sqlite() -> Result<()> {
        let dir = tempdir()?;
        let state_path = dir.path().join(STATE_FILE_NAME);
        std::fs::write(
            &state_path,
            r#"{
  "active_shares": [
    {
      "source_dir": "/tmp/source",
      "share_code": "p2p-LEGACY",
      "collection_hash": "f627847f3d5ebecf169f2e08e10c22f14e8e3a25f8b73f7f15f2f6f5ddf7c905"
    }
  ],
  "active_downloads": [],
  "global_paused": true
}"#,
        )?;

        let state = load_state(dir.path())?;
        assert_eq!(state.active_shares.len(), 1);
        let share = &state.active_shares[0];
        assert_eq!(share.directory_name, "");
        assert_eq!(share.file_count, 0);
        assert_eq!(share.total_bytes, 0);
        assert!(!share.paused);
        assert!(state.global_paused);

        fs::remove_file(&state_path)?;
        let migrated = load_state(dir.path())?;
        assert_eq!(migrated, state);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_resumes_incomplete_downloads() -> Result<()> {
        let node_a_dir = tempdir()?;
        let node_b_dir = tempdir()?;
        let source_dir = tempdir()?;
        let output_dir = tempdir()?;

        let source_root = source_dir.path().join("share-me");
        std::fs::create_dir_all(source_root.join("nested"))?;
        std::fs::write(source_root.join("alpha.txt"), vec![1_u8; 256 * 1024])?;
        std::fs::write(source_root.join("nested").join("beta.txt"), b"beta")?;

        let node_a = AppNode::with_data_dir(node_a_dir.path(), NodeOptions::default()).await?;
        let node_b = AppNode::with_data_dir(node_b_dir.path(), NodeOptions::default()).await?;

        node_b
            .address_book
            .insert_node_info(NodeInfo::from(node_a.endpoint.endpoint().await?.addr()).bootstrap())
            .await?;

        let share = share_directory(&node_a, &source_root).await?;
        let preseeded_hash = share.files[0].hash;
        node_b.blobs.download(preseeded_hash).await?;
        assert!(node_b.blobs.has(preseeded_hash).await?);

        let mut store = StateStore::load(node_b_dir.path())?;
        store.add_download(DownloadRecord::new(
            share.share_code.clone(),
            output_dir.path(),
            share.collection_hash,
        ))?;

        drop(node_b);
        tokio::time::sleep(Duration::from_millis(250)).await;

        let restarted = AppNode::with_data_dir(node_b_dir.path(), NodeOptions::default()).await?;
        restarted
            .address_book
            .insert_node_info(NodeInfo::from(node_a.endpoint.endpoint().await?.addr()).bootstrap())
            .await?;

        let state = load_state(node_b_dir.path())?;
        let sessions = resume_downloads(&restarted, &state).await?;

        assert_eq!(sessions.len(), 1);
        assert!(!sessions[0].files.is_empty());

        for relative_path in ["alpha.txt", "nested/beta.txt"] {
            let expected = std::fs::read(source_root.join(relative_path))?;
            let actual = std::fs::read(output_dir.path().join("share-me").join(relative_path))?;
            assert_eq!(actual, expected, "mismatch for {relative_path}");
        }

        Ok(())
    }
}
