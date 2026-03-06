# Development Environment

Rust verification for this repo is expected to run inside a long-lived Docker sandbox container, not through `docker sandbox run`. In this checkout, the reliable path is plain `docker exec` against a named container such as `codex-p2panda` or `claude-p2panda`.

## Baseline

The repo's Rust config currently expects:

- `gcc`
- `mold`
- glibc startup objects from `libc6-dev`
- native libraries needed by Bevy and winit (`pkg-config`, `libwayland-dev`, `libx11-dev`, `libxkbcommon-dev`, `libasound2-dev`, `libudev-dev`)

Those packages must exist inside the sandbox before running `cargo check` or `cargo test`. Without them, Rust verification fails early with linker or missing startup-object errors such as `Scrt1.o` and `crti.o`.

## Bootstrap

Prepare the sandbox with:

```bash
./scripts/bootstrap-sandbox-toolchain.sh
```

Useful variants:

```bash
./scripts/bootstrap-sandbox-toolchain.sh -p claude
./scripts/bootstrap-sandbox-toolchain.sh --sandbox codex-p2panda --template docker/sandbox-templates:codex
./scripts/bootstrap-sandbox-toolchain.sh --skip-fetch
```

What it does:

- reuses an existing named container if present
- starts a stopped container
- creates a new container from `docker/sandbox-templates:<provider>` with the repo bind-mounted at the same host path
- installs the Rust linker/runtime prerequisites inside the container as `root`
- runs `cargo fetch --locked` unless `--skip-fetch` is passed

## Verification

Run repo-local verification commands through the non-interactive entrypoint:

```bash
./scripts/verify-in-sandbox.sh -- cargo check -p p2panda-blobs
./scripts/verify-in-sandbox.sh -- cargo test -p p2panda-file-sharing-gui bridge --lib -- --nocapture
```

If the sandbox is missing or stopped, either bootstrap it first or let the verifier do both:

```bash
./scripts/verify-in-sandbox.sh --bootstrap -- cargo check -p p2panda-blobs
```

This path is intentionally explicit. It avoids the old `docker sandbox run` flow that launched the agent itself instead of a shell command, which made acceptance checks awkward or impossible to automate.

## Fork CI Validation

Task 41 has one part that cannot be proven purely with local cargo runs: actual PR and release workflow execution on the `cafca/p2panda` fork. The repo now includes a guarded helper for that path:

```bash
./scripts/release/validate-fork.sh preflight
```

Use it before any branch push or experimental tag push. It refuses unsafe remote setups and is documented in [docs/fork-validation.md](/Users/pv/code/p2panda/docs/fork-validation.md).

If your environment does not have an `ssh` client or `gh auth`, set `GITHUB_TOKEN`, `GH_TOKEN`, or `GITHUB_TOKEN_FILE`. The helper can then push to the fork over HTTPS and create the PR through the GitHub API while still enforcing the fork-only safety checks.

If the fork already has a draft PR for your branch, you can also promote it without leaving the terminal:

```bash
./scripts/release/validate-fork.sh ready-pr port-blobs-to-net-v0.5
```

For read-only verification after a push, the same helper can inspect the public fork state without `gh` auth:

```bash
./scripts/release/validate-fork.sh branch-status port-blobs-to-net-v0.5
./scripts/release/validate-fork.sh pr-status port-blobs-to-net-v0.5
./scripts/release/validate-fork.sh wait-pr port-blobs-to-net-v0.5
./scripts/release/validate-fork.sh release-status v0.1.0-experimental.1
./scripts/release/validate-fork.sh wait-release v0.1.0-experimental.1
./scripts/release/validate-fork.sh origin-status HEAD
./scripts/release/validate-fork.sh origin-status HEAD v0.1.0-experimental.1
```

`pr-status` and `wait-pr` now also verify that the fork PR head matches the commit you intend to validate (`HEAD` by default), which avoids falsely accepting a green but stale PR. The `wait-pr` and `wait-release` variants poll until GitHub finishes the relevant checks or the timeout expires, which is safer than manually re-running status commands during fork validation.

When you pass a tag to `origin-status`, it also verifies that the experimental validation tag does not exist on upstream `p2panda/p2panda`, which closes the remaining blind spot where a tag could leak upstream without sharing the same commit SHA as your fork validation run.

## Ralph Workflow

`./ralph.sh` and `./watch-ralph.sh` now use the same named Docker container model:

- `ralph.sh` ensures the container exists, then runs the selected agent with `docker exec`
- `watch-ralph.sh` tails the provider log files with `docker exec`

That keeps agent iterations and manual verification on the same filesystem mount and toolchain baseline.

## Assumptions And Limits

- The bootstrap script assumes Docker access on the host and permission to run `docker exec --user root`.
- The script can install packages inside the sandbox container, but it cannot guarantee registry availability or network stability for crates.io.
- If the sandbox image changes, keep `.cargo/config.toml` aligned with tools that are guaranteed to exist there.
- When a task depends on UI/system libraries beyond the baseline above, extend the bootstrap package list and document the reason here.
