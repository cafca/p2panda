# p2panda File Sharing Test App

This directory contains a small CLI app for testing file transfer over `p2panda-net`.

It uses:

- gossip to announce files on a topic
- `iroh-blobs` to transfer the file contents
- mDNS for LAN discovery by default
- relay bootstrap via `--peer` and `--relay-url` when testing through an Iroh relay

## Build

From the workspace root:

```bash
cargo build -p p2panda-file-sharing
```

Show CLI help:

```bash
cargo run -p p2panda-file-sharing -- --help
```

## Commands

Send a file:

```bash
cargo run -p p2panda-file-sharing -- \
  send \
  --topic mytopic \
  ./example.txt
```

Receive files:

```bash
cargo run -p p2panda-file-sharing -- \
  receive \
  --topic mytopic \
  --output-dir ./received
```

Important flags:

- `--topic <name>`: sender and receiver must use the same topic
- `--output-dir <dir>`: where received files are written, default `./received`
- `--relay-url <url>`: configures a relay/home relay for the node
- `--peer <node-id>`: bootstrap from a sender node ID through the relay
- `--passive-mdns`: disables active mDNS announcements
- `--insecure-skip-relay-cert-verify`: only for local/dev relays with self-signed certs

## LAN Test Without Relay

Receiver:

```bash
cargo run -p p2panda-file-sharing -- \
  receive \
  --topic demo \
  --output-dir ./received
```

Sender:

```bash
cargo run -p p2panda-file-sharing -- \
  send \
  --topic demo \
  ./sample.txt
```

The sender prints its node ID on startup. The receiver writes the file to the output directory using the original filename.

## Local Relay Setup

This repo includes a helper script for running a local `iroh-relay` in `--dev` mode with QUIC address discovery enabled:

```bash
bash /Users/pv/code/p2panda/p2panda-test-claude/scripts/start-dev-relay.sh
```

What the script does:

- finds the cached `iroh-relay` source in Cargo's registry
- generates self-signed localhost certificates with `openssl`
- writes relay config to `p2panda-test-claude/.local/iroh-relay/config.toml`
- starts the relay on `http://localhost:3340`
- enables QUIC address discovery on port `7824`

Because the relay uses self-signed certs for local testing, the app must be run with:

```bash
--relay-url http://localhost:3340 --insecure-skip-relay-cert-verify
```

## Relay Test Flow

Start the relay in one terminal:

```bash
bash /Users/pv/code/p2panda/p2panda-test-claude/scripts/start-dev-relay.sh
```

Start the sender in a second terminal:

```bash
cargo run -p p2panda-file-sharing -- \
  send \
  --topic relay-demo \
  --relay-url http://localhost:3340 \
  --insecure-skip-relay-cert-verify \
  --passive-mdns \
  ./sample.txt
```

Copy the sender node ID from the logs, then start the receiver in a third terminal:

```bash
cargo run -p p2panda-file-sharing -- \
  receive \
  --topic relay-demo \
  --peer <sender-node-id> \
  --relay-url http://localhost:3340 \
  --insecure-skip-relay-cert-verify \
  --output-dir ./received
```

Notes:

- `--peer` requires `--relay-url`
- when `--peer` is supplied, the receiver automatically uses passive mDNS
- a bare node ID is not enough by itself; relay bootstrap depends on the relay URL

## Test Scripts

Quick end-to-end LAN test:

```bash
bash /Users/pv/code/p2panda/p2panda-test-claude/scripts/test-transfer.sh
```

Rust tests:

```bash
cargo test -p p2panda-file-sharing
```

Relay-focused tests only:

```bash
cargo test -p p2panda-file-sharing relay -- --nocapture
```

## Files

- `src/main.rs`: CLI entry point
- `src/node.rs`: node and network setup
- `src/sender.rs`: sender flow
- `src/receiver.rs`: receiver flow
- `tests/transfer.rs`: integration tests, including relay-backed tests
- `scripts/start-dev-relay.sh`: local relay launcher
- `scripts/test-transfer.sh`: simple end-to-end process test
