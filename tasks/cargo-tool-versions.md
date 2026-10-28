# Cargo tool freshness

Apply the [common run contract](README.md). Run the canonical
`scripts/dev/check-cargo-tool-versions.pl --consumer /path/to/repository`
from Shared Tooling, using `--versions FILE` for an alternative reviewed catalog.
Use `--cargo-home DIR` for another explicit Cargo installation root and `--json`
for machine-readable output. `make check-cargo-tool-versions` checks Shared
Tooling itself. This is an online inspection, separate from offline admission.

The checker reads literal Cargo pins in the owning tool-version catalog,
checkout common-tool receipts and `$CARGO_HOME/.crates2.json` (default
`~/.cargo/.crates2.json`). It does not execute installed tools, source the catalog,
or invoke Cargo. Alternate registries, Git installations and prerelease selections
are reported as unsupported observations, never silently treated as crates.io.
Receipts describe recorded versions; they do not authenticate current binaries.
The existing `make rust-tools-check` owns byte, build and complete-set admission.

Query crates.io once per distinct package using bounded HTTPS requests. Select
the highest non-yanked stable release, excluding prereleases, and retain registry
responses and request status in the reported temporary evidence directory.
Perl core modules and curl 8.4 or newer are prerequisites. No Rust compiler or
Cargo registry/dependency fetch is needed. Exit 0 means all observed selections
are current; 1 means outdated, yanked, missing or mismatched selections; 2 means
invalid inputs or incomplete observation. Unavailable metadata is never success.

Run once per selected checkout; repeat the user-level installation inventory only
as needed. For scheduled evidence, set `TMPDIR` to the authorized run directory.
Record version differences and the reported minimum Rust version from `--json`;
an undeclared floor is not proof of compatibility. Keep shared catalog changes
with Shared Tooling and consumer-specific selections with their owners. Do not
add every user-installed package to the common toolset merely because it exists.

An authorized update uses explicit exact versions and locked Cargo builds,
preserving applicable features and prior evidence. Common tools use
`make install-rust-tools`, followed by offline `make rust-tools-check`; other
installations retain their owning setup route. Checking does not install, update
pins, raise library MSRVs, enable a schedule or edit sibling repositories.

## Optional sccache selection

Latest stable is the default update policy for governed tools. Keep exact pins
and checksums; resolve latest only during authorized maintenance, never during
builds. An older selection needs a concrete compatibility blocker, evidence and
an owning follow-up issue. A newer release is a review candidate, not permission
to silently update consumers.

Inspect the optional sccache pin through the same checker:

```bash
perl scripts/dev/check-cargo-tool-versions.pl --tool sccache --json
```

This compares the selected catalog pin and any Cargo receipt against non-yanked
stable crates.io releases. It does not infer installation from a binary download
or require sccache in the common tool bundle. Compare that result with Mozilla's
[stable release](https://github.com/mozilla/sccache/releases/latest); review the
release notes, archive asset and upstream checksum before updating the catalog.
If registry and GitHub disagree, report the observation for review rather than
changing release admission or waiting in a build.

The existing binary installer now accepts the optional catalog selection:

```bash
bash scripts/ci/install-ci-tool.sh sccache --install-dir /selected/bin
```

The reviewed binary mapping remains Linux x86-64 musl. Other supported hosts
retain their explicit qualified Cargo setup or existing owner; no new binary
matrix or wrapper is introduced. Qualify server start/stats/stop with isolated
sockets, compiler errors without retry, connection-failure fallback and actual
Rust compilation in consumers that use wrappers. Installer fixtures prove pin,
checksum and failure-retention behavior, not server or macOS compatibility.
Consumers using explicit old pins must adopt the catalog or update their owned
exception deliberately; refreshing snapshot bytes alone does not change those
callers. No manual environment exports or second installer are needed.
