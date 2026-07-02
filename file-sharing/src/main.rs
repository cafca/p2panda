use std::path::PathBuf;

use bevy::prelude::*;
use bevy::window::{Window, WindowPlugin, WindowResolution};
use bevy_egui::EguiPlugin;
use p2panda_file_sharing_gui::plugin::FileSharingPlugin;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

fn main() {
    let _log_guard = init_logging();

    let title = format!("p2panda File Sharing v{}", env!("CARGO_PKG_VERSION"));

    App::new()
        .add_plugins(DefaultPlugins.set(WindowPlugin {
            primary_window: Some(Window {
                title,
                resolution: WindowResolution::new(900.0, 600.0),
                ..default()
            }),
            ..default()
        }))
        .add_plugins(EguiPlugin {
            enable_multipass_for_primary_context: false,
        })
        .add_plugins(FileSharingPlugin)
        .run();
}

/// Initialise a tracing subscriber that writes to a platform-appropriate log file and to stderr.
///
/// The returned guard must be kept alive for the duration of the process; dropping it flushes and
/// closes the background log writer thread.
///
/// Log levels are controlled via the `RUST_LOG` environment variable and default to `info`.
fn init_logging() -> Option<WorkerGuard> {
    let log_dir = resolve_log_dir()?;
    std::fs::create_dir_all(&log_dir).ok()?;

    let file_appender = tracing_appender::rolling::never(&log_dir, "app.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_ansi(false).with_writer(non_blocking))
        .with(fmt::layer().with_writer(std::io::stderr))
        .init();

    tracing::info!(
        log_file = %log_dir.join("app.log").display(),
        "logging initialised"
    );

    Some(guard)
}

/// Returns the platform-appropriate directory for the application log file.
///
/// | Platform | Path |
/// |----------|------|
/// | macOS    | `~/Library/Logs/p2panda-file-sharing/` |
/// | Linux    | `~/.local/share/p2panda-file-sharing/logs/` |
/// | Windows  | `%APPDATA%\p2panda-file-sharing\logs\` |
fn resolve_log_dir() -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let home = std::env::var_os("HOME")?;
        Some(
            PathBuf::from(home)
                .join("Library")
                .join("Logs")
                .join("p2panda-file-sharing"),
        )
    }

    #[cfg(not(target_os = "macos"))]
    {
        directories::ProjectDirs::from("", "", "p2panda-file-sharing")
            .map(|dirs| dirs.data_dir().join("logs"))
    }
}
