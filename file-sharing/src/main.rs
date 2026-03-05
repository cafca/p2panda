use bevy::prelude::*;
use bevy::window::{Window, WindowPlugin, WindowResolution};
use bevy_egui::EguiPlugin;
use p2panda_file_sharing_gui::plugin::FileSharingPlugin;

fn main() {
    let title = format!("p2panda File Sharing v{}", env!("CARGO_PKG_VERSION"));

    App::new()
        .add_plugins(DefaultPlugins.set(WindowPlugin {
            primary_window: Some(Window {
                title: title.into(),
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
