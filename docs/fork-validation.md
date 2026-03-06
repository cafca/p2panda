# Fork CI Validation

Task 41 requires real validation on the `cafca/p2panda` fork only. Do not push PR branches, tags, or release-validation runs to `origin` (`p2panda/p2panda`).

## Preconditions

- `fork` remote points to `cafca/p2panda`
- `origin` remote points to `p2panda/p2panda`
- `gh` is installed and authenticated if you want to open the PR from the CLI
- if the `fork` remote uses SSH, the local machine must have an `ssh` client and working GitHub SSH auth

Check that baseline with:

```bash
./scripts/release/validate-fork.sh preflight
```

## PR Validation

Push the current branch to the fork:

```bash
./scripts/release/validate-fork.sh push-branch
```

Open the PR against `cafca/p2panda:main`:

```bash
./scripts/release/validate-fork.sh open-pr
```

Inspect the PR and its check suite from the public GitHub API:

```bash
./scripts/release/validate-fork.sh pr-status port-blobs-to-net-v0.5
```

You can also pass a PR number instead of a branch name. The command exits non-zero unless the PR is open, not draft, and all check runs are green.

## Release Validation

Create and push an experimental release tag from the current `HEAD`:

```bash
./scripts/release/validate-fork.sh push-tag v0.1.0-experimental.1
```

The helper refuses non-experimental tags and re-checks that only the `fork` remote is used. After the workflow finishes, confirm that:

- `release.yml` ran on `cafca/p2panda`
- macOS, Linux, and Windows jobs all passed
- the GitHub Release contains `.dmg`, `.AppImage`, `.tar.gz`, `.msi`, `.zip`, and checksum files

The status command checks all of that in one place:

```bash
./scripts/release/validate-fork.sh release-status v0.1.0-experimental.1
```

Delete the experimental tag after validation:

```bash
./scripts/release/validate-fork.sh cleanup-tag v0.1.0-experimental.1
```

If the first run fails, fix the repo issue, then retry with the next tag (`v0.1.0-experimental.2`, etc.).

## Upstream Safety Check

After a branch push or tag validation, confirm the same commit did not trigger any workflow runs on upstream `p2panda/p2panda`:

```bash
./scripts/release/validate-fork.sh origin-status HEAD
```

The command exits non-zero if it finds a matching GitHub Actions run on `origin`.
