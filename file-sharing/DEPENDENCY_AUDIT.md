# Dependency Audit

This crate keeps explicit dependencies only where the crate is imported directly from app code.
The current sync-model wiring uses the following crates directly:

- `p2panda-blobs`: `src/download.rs`, `src/node.rs`, `src/persist.rs`, `src/protocol.rs`, `src/share.rs`, `src/share_code.rs`, `src/contacts.rs`, `src/bridge.rs`
- `p2panda-core`: `src/contacts.rs`, `src/download.rs`, `src/manifest.rs`, `src/node.rs`, `src/operation_domain.rs`, `src/profile.rs`, `src/profile_sync.rs`, `src/share_code.rs`
- `p2panda-net`: `src/bridge.rs`, `src/download.rs`, `src/manifest.rs`, `src/node.rs`, `src/operation_domain.rs`, `src/persist.rs`, `src/profile_sync.rs`, `src/settings.rs`, `src/share.rs`, `src/share_code.rs`
- `p2panda-store`: `src/operation_domain.rs`, `src/persist.rs`, `src/profile_sync.rs`
- `p2panda-stream`: `src/operation_domain.rs`
- `p2panda-sync`: `src/operation_domain.rs`, `src/profile_sync.rs`

Related non-p2panda direct imports that still need explicit manifest entries:

- `iroh-blobs`: `src/bridge.rs`, `src/download.rs`, `src/share.rs`

Audit result on 2026-03-08:

- No direct `p2panda-*` dependency in `file-sharing/Cargo.toml` is currently removable without changing app code.
- `p2panda-store`, `p2panda-stream`, and `p2panda-sync` remain explicit because the LogSync/operation-domain refactor imports them directly.
- Final dependency reconciliation should be re-run after any remaining Task 48 contact-sync changes land, because that work can still introduce or remove direct imports.
