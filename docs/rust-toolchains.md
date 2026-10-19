# Rust toolchain preparation

Snapshot `.github/actions/prepare-rust-toolchains/action.yml` to share explicit
Rust development/MSRV setup across native workflows. Rustup and `/bin/bash` must
already be available. The action installs the development toolchain with the
minimal profile, rustfmt and Clippy, then the minimum toolchain with the minimal
profile and the caller's required target. It does not change the default toolchain.

```yaml
- uses: ./.github/actions/prepare-rust-toolchains
  with:
    development: '1.99.0'
    minimum: '1.88.0'
    development-target: wasm32-unknown-unknown
    minimum-target: wasm32-unknown-unknown
    minimum-components: clippy,rustfmt
```

These versions illustrate the original Jobs caller, not fleet-wide pins. Each
consumer owns its versions and targets; omit `development-target` when that
workflow does not build Wasm with the development toolchain. Toolchain arguments
are passed separately, never evaluated as shell code. Missing required values,
leading options and whitespace are rejected before installation. Installation
failure stops immediately and preserves rustup's status and diagnostics.

The action provides setup only. Consumers retain validation gates, caches,
`rustc +VERSION --version --verbose` evidence, release/publication authority and
native host qualification. Optional `minimum-components` selects comma-separated
components when the minimum-toolchain lane also runs lint or formatting.

Local setup or single-toolchain CI lanes can call the same installer directly:

```bash
bash scripts/dev/prepare-rust-toolchain.sh 1.91.0 \
  --component clippy --component rustfmt --target wasm32-unknown-unknown
```

The standalone entrypoint admits the complete explicit argument list before
calling rustup. It accepts repeated component/target options, uses the minimal
profile and preserves rustup failures. Consumers retain their component policy
and version selection; checks must never call this setup entrypoint implicitly.

When policy already lives in TOML, put the prepared Mike Farah `yq` and `jq`
on PATH and select the existing file:

```bash
bash scripts/dev/prepare-rust-toolchain.sh --toolchain-file rust-toolchain.toml
bash scripts/dev/prepare-rust-toolchain.sh --workspace-manifest Cargo.toml \
  --target wasm32-unknown-unknown
```

The toolchain-file form owns the complete selection: channel, optional profile
(default minimal), components and targets. It accepts no additional options and
refuses path toolchains. The workspace form reads `workspace.package.rust-version`
and accepts the standalone form's component/target options. Parsing never installs
its prerequisites. Malformed files, missing selections and invalid arguments fail
before rustup; installation failures preserve status and diagnostics. Neither
form changes the default toolchain. These are explicit setup commands, not checks.
Optional fields default only when omitted; boolean values and line breaks in
selections are refused. Policy paths and resolved directories must contain no
CR/LF, including directory aliases.

Validation callers that need only the workspace floor can use the same read-only
projection without calling the installer:

```bash
minimum="$(bash scripts/ci/read-cargo-workspace-rust-version.sh Cargo.toml)"
```

This reader needs only prepared `yq` and `jq`, so it works before Cargo/Rust
bootstrap. `YQ` may select the prepared parser explicitly. It emits one numeric
major/minor or major/minor/patch selection only after both observations succeed;
failed parsers preserve their status without publishing a floor. It does not
validate the complete Cargo manifest or dependencies. Consumers own subsequent
compiler checks, target policy, Rustup auto-install policy and explicit overrides.
Both policy forms require exactly one JSON object from the TOML parser. Empty,
multiple or malformed observations refuse before a floor is published or Rustup
is invoked, including observations with a valid first or last document
([#270 — Reject ambiguous Rust bootstrap policy observations](https://github.com/dragginzgame/shared-tooling/issues/270)).
The setup entrypoint delegates its workspace projection to this reader; snapshot
selection automatically carries that companion.

Adopt from a reviewed, published Shared Tooling revision using the
[snapshot workflow](consuming-snapshots.md), then replace the local action and
both ordinary/recovery callers together. The offline fixture executes the actual
YAML body against a substitute rustup, checking argument parity, failure
propagation and input admission. It does not qualify real downloads or macOS;
adopters must retain their native Linux/macOS checks.
