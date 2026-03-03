use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use p2panda_file_sharing::{node, receiver, sender};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "p2panda-file-sharing")]
#[command(about = "Share files over the p2panda network using blobs and gossip")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Import a file as a blob and announce it on a gossip topic.
    Send {
        /// Gossip topic name (determines which receivers get the file).
        #[arg(long)]
        topic: String,

        /// Path to the file to send.
        file: PathBuf,
    },
    /// Listen on a gossip topic and download announced blobs.
    Receive {
        /// Gossip topic name (must match the sender).
        #[arg(long)]
        topic: String,

        /// Directory to write received files into.
        #[arg(long, default_value = "./received")]
        output_dir: PathBuf,

        /// Node ID of a known peer to connect to (hex-encoded public key).
        #[arg(long)]
        peer: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match &cli.command {
        Command::Send { file, .. } => {
            if !file.exists() {
                anyhow::bail!("File does not exist: {}", file.display());
            }
            if !file.is_file() {
                anyhow::bail!("Path is not a file: {}", file.display());
            }
        }
        Command::Receive { output_dir, .. } => {
            if output_dir.exists() && !output_dir.is_dir() {
                anyhow::bail!(
                    "Output path exists but is not a directory: {}",
                    output_dir.display()
                );
            }
        }
    }

    match cli.command {
        Command::Send { topic, file } => {
            tracing::info!("Initializing node for topic '{}'...", topic);
            let node = node::FileSharingNode::new(&topic)
                .await
                .context("Failed to create node")?;
            sender::run(&node, &file).await?;
        }
        Command::Receive {
            topic,
            output_dir,
            peer,
        } => {
            tracing::info!("Initializing node for topic '{}'...", topic);
            let node = node::FileSharingNode::new(&topic)
                .await
                .context("Failed to create node")?;
            if let Some(peer_id) = peer {
                tracing::info!("Peer hint provided: {} (relay-based discovery)", peer_id);
            }
            receiver::run(&node, &output_dir).await?;
        }
    }

    Ok(())
}
