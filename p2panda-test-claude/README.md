# p2panda File Transfer

A "hello world" CLI application demonstrating p2panda's core P2P capabilities for transferring files between instances on a local network.

## Features

- **Zero-config networking**: Automatic peer discovery via mDNS
- **Eventual consistency**: Files are synced using p2panda's LogSync protocol
- **Cryptographically signed**: All operations are signed with Ed25519 keys
- **Order-independent**: Sender and receiver can start in any order

## Architecture

This application uses p2panda v0.5.1 to create a decentralized file transfer system:

- **p2panda-core**: Operations with cryptographic signatures and BLAKE3 hashing
- **p2panda-net**: Networking stack (Endpoint, mDNS, Discovery, Gossip, LogSync)
- **p2panda-store**: In-memory operation storage
- **p2panda-sync**: Eventually consistent log synchronization

Files are encoded as CBOR messages in operation bodies and synced via LogSync. The receiver subscribes to a shared topic and automatically receives files as operations are published.

## Building

```bash
cargo build --release
```

## Usage

### Sending a file

```bash
./target/release/p2panda-file-transfer send <file-path>
```

Example:
```bash
./target/release/p2panda-file-transfer send ~/Documents/sample.txt
```

### Receiving files

```bash
./target/release/p2panda-file-transfer receive --output-dir <directory>
```

Example:
```bash
./target/release/p2panda-file-transfer receive --output-dir ./received
```

The receiver will wait for incoming files and save them to the specified directory. Default output directory is `./received`.

## Testing

### Unit Tests

Test protocol encoding/decoding and topic map functionality:

```bash
cargo test --lib
```

### Integration Test

Test file transfer between two in-process nodes:

```bash
cargo test --test transfer
```

### End-to-End Test

Test file transfer between two separate processes:

```bash
bash scripts/test-transfer.sh
```

This script:
1. Builds the release binary
2. Creates a temporary sample file
3. Starts a receiver process
4. Starts a sender process
5. Verifies the file is transferred and content matches
6. Reports PASS/FAIL with logs

## How It Works

1. **Initialization**: Each instance creates a p2panda node with:
   - A new Ed25519 private key
   - An in-memory operation store
   - A topic map for tracking logs
   - Network stack (mDNS, Discovery, Gossip, LogSync)

2. **Discovery**: Nodes discover each other via mDNS on the local network

3. **Sending**: The sender:
   - Reads the file from disk
   - Creates a `FileMessage` with filename and content
   - Encodes it as CBOR
   - Creates a signed p2panda operation
   - Publishes it via LogSync

4. **Receiving**: The receiver:
   - Subscribes to the shared topic
   - Receives operations via LogSync
   - Decodes the `FileMessage`
   - Writes the file to disk

5. **Sync**: LogSync provides eventual consistency, so operations are synced regardless of startup order

## Project Structure

```
p2panda-file-transfer/
├── src/
│   ├── lib.rs          # Library exports
│   ├── main.rs         # CLI entry point
│   ├── node.rs         # Network stack setup
│   ├── protocol.rs     # FileMessage encode/decode
│   ├── sender.rs       # Send mode implementation
│   └── receiver.rs     # Receive mode implementation
├── tests/
│   └── transfer.rs     # Integration test
├── scripts/
│   └── test-transfer.sh  # End-to-end test script
├── Cargo.toml
└── README.md
```

## Limitations

- Files are embedded in operation bodies, suitable for small files (<1MB)
- For larger files, p2panda-blobs would be more appropriate (when ready)
- LAN-only (no relay server configured)
- In-memory storage only (not persisted)

## References

- [p2panda.org](https://p2panda.org/) - p2panda documentation
- [p2panda reflection app](https://github.com/p2panda/reflection) - Full app example
- [p2panda chat example](https://github.com/p2panda/p2panda/blob/main/p2panda-net/examples/chat.rs) - Reference implementation
