use std::collections::HashMap;
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::Result;
use bevy::prelude::Resource;
use tracing::warn;

use crate::bridge::NetworkEvent;
use crate::state::{Direction, TransferRegistry, TransferStatus};

/// Identical notifications within this window are suppressed so a repeating failure does not
/// produce a barrage of toasts.
const NOTIFICATION_DEDUP_WINDOW: Duration = Duration::from_secs(60);

trait NotificationSender: Send + Sync {
    fn send(&self, summary: &str, body: &str) -> Result<()>;
}

struct SystemNotificationSender;

impl NotificationSender for SystemNotificationSender {
    fn send(&self, summary: &str, body: &str) -> Result<()> {
        send_system_notification(summary, body)
    }
}

fn send_system_notification(summary: &str, body: &str) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        run_notification_command("notify-send", &[summary, body])
    }
    #[cfg(target_os = "macos")]
    {
        let script = format!(
            "display notification \"{}\" with title \"{}\"",
            escape_applescript_string(body),
            escape_applescript_string(summary),
        );
        run_notification_command("osascript", &["-e", &script])
    }
    #[cfg(target_os = "windows")]
    {
        // Best-effort toast command; failures are handled by the caller.
        let script = format!(
            "try {{ \
                [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] > $null; \
                [Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime] > $null; \
                $xml = New-Object Windows.Data.Xml.Dom.XmlDocument; \
                $xml.LoadXml(\"<toast><visual><binding template='ToastGeneric'><text>{}</text><text>{}</text></binding></visual></toast>\"); \
                $toast = [Windows.UI.Notifications.ToastNotification]::new($xml); \
                $notifier = [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('p2panda File Sharing'); \
                $notifier.Show($toast); \
            }} catch {{ exit 1 }}",
            escape_xml(summary),
            escape_xml(body),
        );
        run_notification_command(
            "powershell",
            &[
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                &script,
            ],
        )
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    {
        anyhow::bail!("native notifications are not supported on this platform")
    }
}

fn run_notification_command(program: &str, args: &[&str]) -> Result<()> {
    let status = Command::new(program).args(args).status()?;
    if status.success() {
        Ok(())
    } else {
        anyhow::bail!("{program} exited with status {status}")
    }
}

#[cfg(target_os = "macos")]
fn escape_applescript_string(input: &str) -> String {
    input.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(target_os = "windows")]
fn escape_xml(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[derive(Resource)]
pub struct NotificationState {
    sender: Box<dyn NotificationSender>,
    warned_unavailable: bool,
    recently_sent: HashMap<String, Instant>,
}

impl Default for NotificationState {
    fn default() -> Self {
        Self {
            sender: Box::new(SystemNotificationSender),
            warned_unavailable: false,
            recently_sent: HashMap::new(),
        }
    }
}

impl NotificationState {
    pub fn on_event(
        &mut self,
        event: &NetworkEvent,
        transfers: &TransferRegistry,
        app_focused: bool,
        now: Instant,
    ) {
        if app_focused {
            return;
        }

        match event {
            NetworkEvent::ShareReady {
                directory_name,
                file_count,
                total_bytes,
                ..
            } => {
                let body = format!(
                    "'{}' is now available for download ({} files, {})",
                    directory_name,
                    file_count,
                    format_bytes(*total_bytes)
                );
                self.send_notification("Share ready", &body, now);
            }
            NetworkEvent::Error {
                transfer_id,
                error_message,
            } => {
                let name = transfers
                    .get(*transfer_id)
                    .map(|transfer| transfer.name.as_str())
                    .unwrap_or("Transfer");
                let body = format!("'{}' failed: {}", name, error_message);
                self.send_notification("Transfer failed", &body, now);
            }
            NetworkEvent::TransferCompleted { transfer_id } => {
                if let Some(transfer) = transfers.get(*transfer_id) {
                    if transfer.direction == Direction::Download
                        && matches!(transfer.status, TransferStatus::Completed)
                    {
                        let body = format!(
                            "'{}' finished downloading ({} files, {})",
                            transfer.name,
                            transfer.file_count(),
                            format_bytes(transfer.total_bytes)
                        );
                        self.send_notification("Download completed", &body, now);
                    }
                }
            }
            _ => {}
        }
    }

    pub fn flush_due(&mut self, _app_focused: bool, _now: Instant) {}

    fn send_notification(&mut self, summary: &str, body: &str, now: Instant) {
        let dedup_key = format!("{summary}\u{1f}{body}");
        self.recently_sent
            .retain(|_, sent_at| now.duration_since(*sent_at) < NOTIFICATION_DEDUP_WINDOW);
        if self.recently_sent.contains_key(&dedup_key) {
            return;
        }
        self.recently_sent.insert(dedup_key, now);

        if let Err(err) = self.sender.send(summary, body) {
            if !self.warned_unavailable {
                warn!("failed to send system notification: {err:#}");
                self.warned_unavailable = true;
            }
        }
    }
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    let value = bytes as f64;
    if value >= GIB {
        format!("{:.1} GiB", value / GIB)
    } else if value >= MIB {
        format!("{:.1} MiB", value / MIB)
    } else if value >= KIB {
        format!("{:.1} KiB", value / KIB)
    } else if bytes == 1 {
        "1 byte".to_owned()
    } else {
        format!("{bytes} bytes")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::state::{Direction, Transfer, TransferStatus};

    #[derive(Clone, Default)]
    struct RecordingSender {
        sent: Arc<Mutex<Vec<(String, String)>>>,
        fail: bool,
    }

    impl RecordingSender {
        fn messages(&self) -> Vec<(String, String)> {
            self.sent.lock().unwrap().clone()
        }
    }

    impl NotificationSender for RecordingSender {
        fn send(&self, summary: &str, body: &str) -> Result<()> {
            if self.fail {
                anyhow::bail!("notification backend unavailable");
            }
            self.sent
                .lock()
                .unwrap()
                .push((summary.to_owned(), body.to_owned()));
            Ok(())
        }
    }

    fn test_state(sender: RecordingSender) -> NotificationState {
        NotificationState {
            sender: Box::new(sender),
            warned_unavailable: false,
            recently_sent: HashMap::new(),
        }
    }

    #[test]
    fn does_not_notify_when_app_is_focused() {
        let sender = RecordingSender::default();
        let mut state = test_state(sender.clone());
        let transfers = TransferRegistry::default();

        state.on_event(
            &NetworkEvent::ShareReady {
                transfer_id: 1,
                directory_name: "photos".into(),
                share_code: "p2p-CODE".into(),
                collection_hash: "a".repeat(64),
                total_bytes: 42,
                file_count: 1,
            },
            &transfers,
            true,
            Instant::now(),
        );

        assert!(sender.messages().is_empty());
    }

    #[test]
    fn sends_share_ready_and_error_notifications_when_unfocused() {
        let sender = RecordingSender::default();
        let mut state = test_state(sender.clone());
        let mut transfers = TransferRegistry::default();
        let mut transfer = Transfer::new(7, "vacation", Direction::Download);
        transfer.status = TransferStatus::Active;
        transfers.push(transfer);

        state.on_event(
            &NetworkEvent::ShareReady {
                transfer_id: 1,
                directory_name: "photos".into(),
                share_code: "p2p-CODE".into(),
                collection_hash: "b".repeat(64),
                total_bytes: 2_048,
                file_count: 3,
            },
            &transfers,
            false,
            Instant::now(),
        );
        state.on_event(
            &NetworkEvent::Error {
                transfer_id: 7,
                error_message: "connection refused".into(),
            },
            &transfers,
            false,
            Instant::now(),
        );

        let messages = sender.messages();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].0, "Share ready");
        assert!(messages[0].1.contains("'photos'"));
        assert_eq!(messages[1].0, "Transfer failed");
        assert!(messages[1]
            .1
            .contains("'vacation' failed: connection refused"));
    }

    #[test]
    fn sends_one_notification_per_completed_download() {
        let sender = RecordingSender::default();
        let mut state = test_state(sender.clone());
        let now = Instant::now();

        let mut transfers = TransferRegistry::default();
        let mut first = Transfer::new(1, "alpha", Direction::Download);
        first.total_bytes = 12;
        first.status = TransferStatus::Completed;
        transfers.push(first);
        let mut second = Transfer::new(2, "beta", Direction::Download);
        second.total_bytes = 24;
        second.status = TransferStatus::Completed;
        transfers.push(second);

        state.on_event(
            &NetworkEvent::TransferCompleted { transfer_id: 1 },
            &transfers,
            false,
            now,
        );
        state.on_event(
            &NetworkEvent::TransferCompleted { transfer_id: 2 },
            &transfers,
            false,
            now + Duration::from_millis(500),
        );
        let messages = sender.messages();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].0, "Download completed");
        assert!(messages[0].1.contains("'alpha' finished downloading"));
        assert_eq!(messages[1].0, "Download completed");
        assert!(messages[1].1.contains("'beta' finished downloading"));
    }

    #[test]
    fn sends_single_download_completion_details() {
        let sender = RecordingSender::default();
        let mut state = test_state(sender.clone());
        let now = Instant::now();

        let mut transfers = TransferRegistry::default();
        let mut transfer = Transfer::new(3, "notes", Direction::Download);
        transfer.total_bytes = 1_500;
        transfer.files = vec![
            crate::state::FileProgress::new("a.txt", 500),
            crate::state::FileProgress::new("b.txt", 1_000),
        ];
        transfer.status = TransferStatus::Completed;
        transfers.push(transfer);

        state.on_event(
            &NetworkEvent::TransferCompleted { transfer_id: 3 },
            &transfers,
            false,
            now,
        );

        let messages = sender.messages();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, "Download completed");
        assert!(messages[0].1.contains("'notes' finished downloading"));
    }

    #[test]
    fn identical_notifications_are_deduplicated_within_window() {
        let sender = RecordingSender::default();
        let mut state = test_state(sender.clone());
        let transfers = TransferRegistry::default();
        let now = Instant::now();
        let error_event = NetworkEvent::Error {
            transfer_id: 9,
            error_message: "failed to access endpoint for diagnostics".into(),
        };

        // A repeating failure fires the same notification every second.
        for tick in 0..10 {
            state.on_event(
                &error_event,
                &transfers,
                false,
                now + Duration::from_secs(tick),
            );
        }
        assert_eq!(sender.messages().len(), 1);

        // After the dedup window has passed the notification may be shown again.
        state.on_event(
            &error_event,
            &transfers,
            false,
            now + NOTIFICATION_DEDUP_WINDOW + Duration::from_secs(1),
        );
        assert_eq!(sender.messages().len(), 2);
    }

    #[test]
    fn backend_failure_is_non_fatal() {
        let sender = RecordingSender {
            fail: true,
            ..Default::default()
        };
        let mut state = test_state(sender);
        let transfers = TransferRegistry::default();

        state.on_event(
            &NetworkEvent::ShareReady {
                transfer_id: 1,
                directory_name: "photos".into(),
                share_code: "p2p-CODE".into(),
                collection_hash: "c".repeat(64),
                total_bytes: 42,
                file_count: 1,
            },
            &transfers,
            false,
            Instant::now(),
        );
    }
}
