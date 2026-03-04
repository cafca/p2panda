use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use p2panda_file_sharing::{node, receiver, sender};
use p2panda_net::addrs::NodeInfo;
use p2panda_net::iroh_endpoint::{from_public_key, EndpointAddr};
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

        /// Relay server URL (e.g. https://use1-1.relay.iroh.network.).
        #[arg(long)]
        relay_url: Option<String>,

        /// Disable relay certificate verification for local development relays.
        #[arg(long, default_value_t = false)]
        insecure_skip_relay_cert_verify: bool,

        /// Use passive mDNS (no active announcements).
        #[arg(long, default_value_t = false)]
        passive_mdns: bool,
    },
    /// Listen on a gossip topic and download announced blobs.
    Receive {
        /// Gossip topic name (must match the sender).
        #[arg(long)]
        topic: String,

        /// Directory to write received files into.
        #[arg(long, default_value = "./received")]
        output_dir: PathBuf,

        /// Node ID of a known sender to bootstrap from (hex-encoded public key).
        /// When provided, the sender is added to the address book as a bootstrap peer.
        #[arg(long)]
        peer: Option<String>,

        /// Relay server URL (e.g. https://use1-1.relay.iroh.network.).
        #[arg(long)]
        relay_url: Option<String>,

        /// Disable relay certificate verification for local development relays.
        #[arg(long, default_value_t = false)]
        insecure_skip_relay_cert_verify: bool,

        /// Use passive mDNS (no active announcements). Implied when --peer is given.
        #[arg(long, default_value_t = false)]
        passive_mdns: bool,
    },
}

/// Parse a relay URL string into a `RelayUrl`.
fn parse_relay_url(s: &str) -> Result<p2panda_net::iroh_endpoint::RelayUrl> {
    s.parse().context("Invalid relay URL")
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
        Command::Send {
            topic,
            file,
            relay_url,
            insecure_skip_relay_cert_verify,
            passive_mdns,
        } => {
            tracing::info!("Initializing node for topic '{}'...", topic);
            let opts = node::NodeOptions {
                relay_url: relay_url.as_deref().map(parse_relay_url).transpose()?,
                insecure_skip_relay_cert_verify,
                passive_mdns,
            };
            let node = node::FileSharingNode::new(&topic, opts)
                .await
                .context("Failed to create node")?;
            sender::run(&node, &file).await?;
        }
        Command::Receive {
            topic,
            output_dir,
            peer,
            relay_url,
            insecure_skip_relay_cert_verify,
            passive_mdns,
        } => {
            tracing::info!("Initializing node for topic '{}'...", topic);
            let relay_url = relay_url.as_deref().map(parse_relay_url).transpose()?;
            if peer.is_some() && relay_url.is_none() {
                anyhow::bail!(
                    "--peer requires --relay-url because a node ID alone does not include transport addresses"
                );
            }

            // When a peer is given we default to passive mDNS to avoid mDNS taking over.
            let use_passive = passive_mdns || peer.is_some();
            let opts = node::NodeOptions {
                relay_url: relay_url.clone(),
                insecure_skip_relay_cert_verify,
                passive_mdns: use_passive,
            };
            let node = node::FileSharingNode::new(&topic, opts)
                .await
                .context("Failed to create node")?;

            if let Some(peer_hex) = peer {
                let node_id: p2panda_net::NodeId = peer_hex
                    .parse()
                    .with_context(|| format!("Invalid peer node ID: {}", peer_hex))?;
                let relay_url = relay_url.expect("validated above when --peer is used");
                let endpoint_addr =
                    EndpointAddr::new(from_public_key(node_id)).with_relay_url(relay_url);
                let node_info = NodeInfo::from(endpoint_addr).bootstrap();
                node.address_book
                    .insert_node_info(node_info)
                    .await
                    .context("Failed to insert peer into address book")?;
                tracing::info!("Bootstrap peer added: {}", peer_hex);
            }

            receiver::run(&node, &output_dir).await?;
        }
    }

    Ok(())
}
