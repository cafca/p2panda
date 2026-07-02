use std::time::Instant;

use bevy::prelude::Resource;

#[derive(Debug, Resource, Default)]
pub struct TransferRegistry {
    next_id: u64,
    transfers: Vec<Transfer>,
}

impl TransferRegistry {
    pub fn allocate_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        id
    }

    pub fn transfers(&self) -> &[Transfer] {
        &self.transfers
    }

    pub fn transfers_mut(&mut self) -> &mut [Transfer] {
        &mut self.transfers
    }

    pub fn push(&mut self, transfer: Transfer) {
        self.next_id = self.next_id.max(transfer.id.saturating_add(1));
        self.transfers.push(transfer);
    }

    pub fn get(&self, id: u64) -> Option<&Transfer> {
        self.transfers.iter().find(|transfer| transfer.id == id)
    }

    pub fn get_mut(&mut self, id: u64) -> Option<&mut Transfer> {
        self.transfers.iter_mut().find(|transfer| transfer.id == id)
    }

    pub fn remove(&mut self, id: u64) -> Option<Transfer> {
        let index = self
            .transfers
            .iter()
            .position(|transfer| transfer.id == id)?;
        Some(self.transfers.remove(index))
    }

    pub fn retain<F>(&mut self, mut keep: F)
    where
        F: FnMut(&Transfer) -> bool,
    {
        self.transfers.retain(|transfer| keep(transfer));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Upload,
    Download,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferStatus {
    Pending,
    Active,
    Paused,
    Cancelled,
    Completed,
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileVerification {
    Pending,
    Verified,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileProgress {
    pub relative_path: String,
    pub total_size: u64,
    pub downloaded_bytes: u64,
    pub completed: bool,
    pub verification: FileVerification,
}

impl FileProgress {
    pub fn new(relative_path: impl Into<String>, total_size: u64) -> Self {
        Self {
            relative_path: relative_path.into(),
            total_size,
            downloaded_bytes: 0,
            completed: false,
            verification: FileVerification::Pending,
        }
    }

    pub fn mark_completed(&mut self) {
        self.downloaded_bytes = self.total_size;
        self.completed = true;
        self.verification = FileVerification::Verified;
    }

    pub fn mark_failed_verification(&mut self, message: impl Into<String>) {
        self.completed = false;
        self.verification = FileVerification::Failed(message.into());
    }
}

#[derive(Debug, Clone)]
pub struct Transfer {
    pub id: u64,
    pub name: String,
    pub direction: Direction,
    pub status: TransferStatus,
    pub files: Vec<FileProgress>,
    pub share_code: Option<String>,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub start_time: Instant,
    pub inbound_bytes_per_sec: f32,
    pub outbound_bytes_per_sec: f32,
    pub collection_hash: Option<String>,
}

impl Transfer {
    pub fn new(id: u64, name: impl Into<String>, direction: Direction) -> Self {
        Self {
            id,
            name: name.into(),
            direction,
            status: TransferStatus::Pending,
            files: Vec::new(),
            share_code: None,
            total_bytes: 0,
            downloaded_bytes: 0,
            start_time: Instant::now(),
            inbound_bytes_per_sec: 0.0,
            outbound_bytes_per_sec: 0.0,
            collection_hash: None,
        }
    }

    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    pub fn completed_file_count(&self) -> usize {
        self.files.iter().filter(|file| file.completed).count()
    }

    pub fn progress_fraction(&self) -> f32 {
        if self.total_bytes == 0 {
            return 0.0;
        }

        (self.downloaded_bytes as f32 / self.total_bytes as f32).clamp(0.0, 1.0)
    }

    pub fn refresh_downloaded_bytes(&mut self) {
        self.downloaded_bytes = self
            .files
            .iter()
            .map(|file| file.downloaded_bytes.min(file.total_size))
            .sum();
    }

    pub fn update_bandwidth(&mut self) {
        let elapsed_seconds = self.start_time.elapsed().as_secs_f32();
        if elapsed_seconds <= 0.0 {
            self.inbound_bytes_per_sec = 0.0;
            self.outbound_bytes_per_sec = 0.0;
            return;
        }

        let bytes_per_sec = self.downloaded_bytes as f32 / elapsed_seconds;
        match self.direction {
            Direction::Upload => {
                self.inbound_bytes_per_sec = 0.0;
                self.outbound_bytes_per_sec = bytes_per_sec;
            }
            Direction::Download => {
                self.inbound_bytes_per_sec = bytes_per_sec;
                self.outbound_bytes_per_sec = 0.0;
            }
        }
    }

    pub fn verified_file_count(&self) -> usize {
        self.files
            .iter()
            .filter(|file| matches!(file.verification, FileVerification::Verified))
            .count()
    }

    pub fn failed_verification_count(&self) -> usize {
        self.files
            .iter()
            .filter(|file| matches!(file.verification, FileVerification::Failed(_)))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bevy::prelude::Resource;

    use super::*;

    fn assert_is_resource<T: Resource>() {}

    #[test]
    fn registry_is_a_bevy_resource() {
        assert_is_resource::<TransferRegistry>();
    }

    #[test]
    fn registry_allocates_ids_and_looks_up_transfers() {
        let mut registry = TransferRegistry::default();

        let first_id = registry.allocate_id();
        let second_id = registry.allocate_id();
        assert_eq!(first_id, 0);
        assert_eq!(second_id, 1);

        registry.push(Transfer::new(first_id, "alpha", Direction::Upload));
        registry.push(Transfer::new(second_id, "beta", Direction::Download));

        assert_eq!(registry.get(first_id).unwrap().name, "alpha");
        assert_eq!(registry.get(second_id).unwrap().name, "beta");

        registry.get_mut(second_id).unwrap().status = TransferStatus::Active;
        assert_eq!(
            registry.get(second_id).unwrap().status,
            TransferStatus::Active
        );
    }

    #[test]
    fn progress_fraction_handles_empty_partial_and_complete_transfers() {
        let mut transfer = Transfer::new(7, "download", Direction::Download);
        assert_eq!(transfer.progress_fraction(), 0.0);

        transfer.total_bytes = 100;
        transfer.downloaded_bytes = 25;
        assert_eq!(transfer.progress_fraction(), 0.25);

        transfer.downloaded_bytes = 100;
        assert_eq!(transfer.progress_fraction(), 1.0);
    }

    #[test]
    fn refresh_downloaded_bytes_and_completed_counts_follow_files() {
        let mut transfer = Transfer::new(3, "shared-dir", Direction::Download);
        transfer.files = vec![
            FileProgress {
                relative_path: "one.txt".into(),
                total_size: 10,
                downloaded_bytes: 10,
                completed: true,
                verification: FileVerification::Verified,
            },
            FileProgress {
                relative_path: "nested/two.bin".into(),
                total_size: 20,
                downloaded_bytes: 8,
                completed: false,
                verification: FileVerification::Pending,
            },
        ];

        transfer.refresh_downloaded_bytes();

        assert_eq!(transfer.file_count(), 2);
        assert_eq!(transfer.completed_file_count(), 1);
        assert_eq!(transfer.downloaded_bytes, 18);
        assert_eq!(transfer.progress_fraction(), 0.0);

        transfer.total_bytes = 30;
        assert_eq!(transfer.progress_fraction(), 0.6);
    }

    #[test]
    fn update_bandwidth_assigns_directional_rate() {
        let mut download = Transfer::new(1, "download", Direction::Download);
        download.downloaded_bytes = 4_096;
        download.start_time = Instant::now() - Duration::from_secs(2);
        download.update_bandwidth();
        assert!(download.inbound_bytes_per_sec >= 2_000.0);
        assert_eq!(download.outbound_bytes_per_sec, 0.0);

        let mut upload = Transfer::new(2, "upload", Direction::Upload);
        upload.downloaded_bytes = 8_192;
        upload.start_time = Instant::now() - Duration::from_secs(2);
        upload.update_bandwidth();
        assert!(upload.outbound_bytes_per_sec >= 4_000.0);
        assert_eq!(upload.inbound_bytes_per_sec, 0.0);
    }

    #[test]
    fn verification_counts_follow_file_verification_states() {
        let mut transfer = Transfer::new(5, "download", Direction::Download);
        transfer.files = vec![
            FileProgress {
                relative_path: "ok.bin".into(),
                total_size: 1,
                downloaded_bytes: 1,
                completed: true,
                verification: FileVerification::Verified,
            },
            FileProgress {
                relative_path: "bad.bin".into(),
                total_size: 1,
                downloaded_bytes: 1,
                completed: false,
                verification: FileVerification::Failed("hash mismatch".into()),
            },
            FileProgress {
                relative_path: "pending.bin".into(),
                total_size: 1,
                downloaded_bytes: 0,
                completed: false,
                verification: FileVerification::Pending,
            },
        ];

        assert_eq!(transfer.verified_file_count(), 1);
        assert_eq!(transfer.failed_verification_count(), 1);
    }
}
