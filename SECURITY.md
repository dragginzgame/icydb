# Local Development Safety

IcyDB is not designed to modify a developer workstation during ordinary
library use. A few maintainer and integration-test commands intentionally cross
that boundary and should be run only on hosts where that is acceptable.

## Commands With Host Or Supply-Chain Effects

- `make install-dev` is a local workstation bootstrap target. On hosts with
  `apt-get`, it may run `sudo apt-get update` and `sudo apt-get install` for
  documented system prerequisites, install Rust through the official rustup
  script when missing, install the workspace-pinned Rust toolchain and wasm
  target, install Cargo helper tools, install npm-backed ICP CLI tools under
  `$HOME/.local`, and configure repository hooks.
- `make update-dev` is a maintainer workstation updater. It refreshes the same
  documented system package list, installs the workspace-pinned Rust toolchain
  and wasm target with `rustup`, installs or updates the standard Cargo helper
  tools and wasm tools, installs or updates `icp` and `ic-wasm` under
  `$HOME/.local` through npm, runs `cargo audit`, and refreshes `Cargo.lock`
  with `cargo update`.
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

## Git Hooks

Repository hooks live in `.githooks`, but they are inactive until
`make install-hooks` configures `core.hooksPath`. When enabled, the pre-commit
hook runs formatting and stages tracked formatting changes, while the pre-push
hook runs invariant checks and clippy.
