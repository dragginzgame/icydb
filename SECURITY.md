# Local Development Safety

IcyDB is not designed to modify a developer workstation during ordinary
library use. A few maintainer and integration-test commands intentionally cross
that boundary and should be run only on hosts where that is acceptable.

## Commands With Host Or Supply-Chain Effects

- Local Make targets do not install OS packages or run `sudo`. Install system
  prerequisites with your normal package manager before running repo targets.
- There is no repo bootstrap target that installs Rust, OS packages, or
  user-level GitHub release binaries. Local setup prerequisites are documented
  in `INSTALLING.md` and remain operator-owned.
- `make update-dev` is a maintainer workstation updater. It updates Rust with
  `rustup`, installs or updates the standard Cargo helper tools and wasm tools,
  installs or updates `icp` and `ic-wasm` under `$HOME/.local` through npm, runs
  `cargo audit`, and refreshes `Cargo.lock` with `cargo update`.
- `make install-canister-deps` adds the pinned Wasm Rust target and installs
  `candid-extractor`, `ic-wasm`, and `twiggy` through Cargo. It does not run
  `sudo` or install OS packages.
- `make test` may need an IC testkit server binary. It resolves a trusted
  `POCKET_IC_BIN` first, then the repo cache, and otherwise downloads the
  pinned `pocket-ic` release before passing that path into testkit.
- Direct `scripts/ci/ensure-pocket-ic-bin.sh` use downloads the test runner from
  GitHub only after `ICYDB_ALLOW_POCKET_IC_DOWNLOAD=1`. Set
  `POCKET_IC_SERVER_SHA256` when you have a trusted digest and want the script
  to verify the executable bytes.
- Crate publishing is manual maintainer work using `cargo publish`; there is no
  repo Make target or script that reads crates.io credentials.
- `scripts/dev/delete-tags.sh` deletes matching local tags and remote `origin`
  tags after an exact typed confirmation.

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
