# Local Development Safety

IcyDB is not designed to modify a developer workstation during ordinary
library use. A few maintainer and integration-test commands intentionally cross
that boundary and should be run only on hosts where that is acceptable.

## Commands With Host Or Supply-Chain Effects

- `make install-dev` is a local workstation bootstrap target. On hosts with
  `apt-get`, it may run `sudo apt-get update` and `sudo apt-get install` for
  documented system prerequisites, install Rust through the official rustup
  script when missing, install the workspace-pinned Rust toolchain and wasm
  target, install Cargo helper tools, and install npm-backed ICP CLI tools under
  `$HOME/.local`. It also configures this checkout's local `core.hooksPath` as
  `.githooks` without replacing a different existing hook path.
- `make update-dev` is a maintainer workstation updater. It refreshes the same
  workspace-pinned Rust toolchain and wasm target with `rustup`, installs or
  updates the standard Cargo helper tools and wasm tools, installs or updates
  `icp` and `ic-wasm` under `$HOME/.local` through npm, ensures the same local
  hook path, runs `cargo audit`, and refreshes `Cargo.lock` with `cargo update`.
  It does not install system packages.
- `make test` may need a PocketIC server binary. The repo test target sets
  `IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1` and a repo-local `TMPDIR`, then lets
  `ic-testkit` resolve a trusted `POCKET_IC_BIN`, cached pinned binary, or
  pinned GitHub release download.
- Crate publishing is manual maintainer work using `cargo publish`; there is no
  repo Make target or script that reads crates.io credentials.
- Tag deletion is manual maintainer work using explicit `git tag` and
  `git push --delete` commands; there is no repo wrapper for deleting remote
  tags.

## Local Canister State

`icydb canister refresh` rebuilds and reinstalls the selected ICP canister. That
clears the canister's stable memory in the chosen local or configured ICP
environment. It is destructive to that app/canister state, but it is not a host
disk wipe.

## Git Commands

The repository has one opt-in formatting-only pre-commit hook, installed by
`make install-hooks`, `make install-dev`, or `make update-dev`. A commit runs
`make fmt`; if formatting changes files, the hook aborts and leaves review and
staging to the developer. It never stages files or runs builds, tests, Clippy,
PocketIC, or release validation. `git commit --no-verify` bypasses it, and
`git push` performs no repository hook work.
