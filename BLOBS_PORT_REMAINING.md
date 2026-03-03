# p2panda-blobs Port — Remaining Work

## Context

`p2panda-blobs` was a working thin wrapper around `iroh-blobs` that bridged iroh's blob storage/transfer with p2panda-net's networking stack. It broke at p2panda v0.5.0 when p2panda-net was completely rewritten (issue #818), removing all the types it depended on (`Network<T>`, `NetworkBuilder<T>`, `ProtocolHandler` trait). At the same time, iroh jumped from v0.34.1 to v0.96.1 and iroh-blobs (now a separate crate) underwent a complete API redesign.

The task is to port `p2panda-blobs` to work with p2panda-net v0.5.x and iroh-blobs v0.98.0.

### Key decisions

- **Provider discovery**: Use `AddressBook` as the source of peers — query all known peers when downloading. This required adding `node_ids()` to the `AddressBook` public API (wired through to `all_node_infos()` on the actor store).
- **ALPN/NetworkId**: Blobs ALPN is hashed with NetworkId like all other protocols — blobs are partitioned per network, intentionally.
- **Wrapper scope**: Middle ground — wrap p2panda-specific behavior (construction, protocol registration, download). Pass through import/export/read via `Deref<Target = iroh_blobs::api::Store>`. Add a `Pins` API over iroh's tags. Expose `store()` for advanced access.
- **LogSync integration**: Application-level — no automatic download. App calls `blobs.download(hash)` explicitly when it encounters a blob reference in an operation.

### New public API

```rust
// Construction
let blobs = Blobs::new(&store, &endpoint, &address_book).await?;

// Import/read/export — via Deref to iroh_blobs::api::Store
let tag = blobs.add_slice(b"hello").await?;
let bytes = blobs.get_bytes(hash).await?;

// Download from known peers
blobs.download(hash).await?;

// Pin management (wraps iroh tags)
blobs.pins().set("name", hash).await?;
blobs.pins().get("name").await?;
blobs.pins().delete("name").await?;
blobs.pins().list().await?;
blobs.pins().temp(hash).await?;   // returns TempTag guard

// Direct store access
let store: &iroh_blobs::api::Store = blobs.store();
```

### iroh-blobs 0.98.0 key API facts

- `BlobsProtocol::new(store, None)` — at `iroh_blobs::BlobsProtocol`, implements `iroh::protocol::ProtocolHandler`
- `iroh_blobs::ALPN` — the protocol ALPN bytes
- `Store::downloader(&iroh_ep)` — returns `Downloader` (has internal state; create once, store it)
- `Downloader::download(request, providers)` — `providers: impl ContentDiscovery`; blanket impl covers `Vec<I: Into<EndpointId>>`
- `EndpointId` = `iroh_base::PublicKey` = `iroh::EndpointId`; `from_public_key()` in p2panda-net returns this type
- `Store::tags()` returns `&Tags`
- `TagInfo` fields: `pub name: Tag`, `pub format: BlobFormat`, `pub hash: Hash`
- `Tags::delete()` and `Tags::delete_prefix()` return `RequestResult<u64>` (number deleted)
- `TempTag` is at `iroh_blobs::api::TempTag`

---

## Status

The following changes have been **completed**:

- `p2panda-net/src/address_book/actor.rs` — Added `AllNodeIds(RpcReplyPort<Vec<NodeId>>)` variant to `ToAddressBookActor` and its handler
- `p2panda-net/src/address_book/api.rs` — Added `pub async fn node_ids() -> Result<Vec<NodeId>, AddressBookError>` to `AddressBook`
- `p2panda-blobs/Cargo.toml` — Replaced old deps with `iroh-blobs = "0.98"`, `p2panda-net` path dep, `anyhow`, `thiserror`, `tracing`, `futures-util`; added `fs-store` feature
- `p2panda-blobs/src/lib.rs` — Rewritten
- `p2panda-blobs/src/blobs.rs` — Rewritten
- `p2panda-blobs/src/pins.rs` — Created
- Deleted: `download.rs`, `import.rs`, `export.rs`, `protocol.rs`, `config.rs`

---

## Remaining: Fix Compilation Errors

Run `cargo check -p p2panda-blobs` from `/Users/pv/code/p2panda` to see current errors.

---

## After Compilation Passes

- Also run `cargo check -p p2panda-net` to confirm the AddressBook changes compile
- Write a basic integration test (see original plan for test scenario)
- Update `~/code/p2panda-test-claude` example to use the new `p2panda-blobs` (currently uses manual blob transfer workaround)
