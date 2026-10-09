# Security and Local Development Safety

IcyDB is not designed to modify a developer workstation during ordinary
library use. A few maintainer and integration-test commands intentionally cross
that boundary and should be run only on hosts where that is acceptable.

## Reporting a Vulnerability

Report suspected vulnerabilities through
[GitHub private vulnerability reporting](https://github.com/dragginzgame/icydb/security/advisories/new).
Include the affected version, reproduction steps and likely impact. Keep
sensitive details out of public issues and pull requests while the report is
being assessed.

## Commands With Host Or Supply-Chain Effects

- `make install-dev` installs documented system prerequisites through apt or
  Homebrew, prepares the pinned toolsets and selected Cargo cache, and enables
  the repository's formatting hook. Rustup must already be installed. See
  [developer prerequisites and setup](INSTALLING.md#maintainer-workstation-setup).
- `make update-dev` refreshes the selected toolsets, prepares the selected Cargo
  cache and enables the same hook. It leaves repository dependency selections
  unchanged; it does not run `cargo update` or `cargo audit`. Shared authenticated
  installers own host/IC tools rather than npm-based system installations. See
  [tooling](INSTALLING.md#maintainer-workstation-setup).
- `make test` uses prepared tools and the selected PocketIC binary or configured
  server through IC Testkit. Validation does not download a missing server or
  install the runner implicitly. See [tests](INSTALLING.md#ic-testkit-tests).
- Release and publication are explicit maintainer operations. The shared release
  workflow can validate, prepare versions, commit, tag and push; `make publish`
  uses Cargo publication and registry credentials. See
  [release and publication instructions](INSTALLING.md#publishing-crates).
- Tag maintenance has separate explicit local/remote effects and a preview mode.
  Release and publication do not invoke it. See
  [tag maintenance](INSTALLING.md#tag-maintenance).

## Local Canister State

`icydb canister refresh` rebuilds and reinstalls the selected ICP canister. That
clears the canister's stable memory in the chosen local or configured ICP
environment. It is destructive to that app/canister state, but it is not a host
disk wipe.

## Git Commands

The repository has one opt-in formatting-only pre-commit hook, installed by
`make install-hooks`, `make install-dev`, or `make update-dev`. It formats an
isolated copy of the index and refreshes only the selected staged files,
including re-staging their formatted bytes. Partial staging is rejected before
formatting; formatter failure leaves the real files and index unchanged.
Unselected working edits are preserved, and installation refuses to replace
another hook authority. See [Git hooks](INSTALLING.md#git-formatting-hook).

The hook does not run builds, tests, Clippy, PocketIC or release validation.
`git commit --no-verify` bypasses it, and `git push` performs no repository hook
work. [Formatting checks](INSTALLING.md#git-formatting-hook) remain separate
from release validation.
