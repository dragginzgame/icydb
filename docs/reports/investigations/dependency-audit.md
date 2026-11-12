> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Dependency update and deduplication audit

Date: 2026-09-13. IcyDB source: `818d4350e310af304fcb41b36357c292075c40a2`, workspace version `0.257.9`.

## Scope and result

Audited all 48 workspace members, all 35 directly consumed external crates, and all 369 resolved external package versions (347 distinct crate names). Checked fresh public crates.io sparse-index records for every external name, excluding prereleases and yanked releases when selecting the latest stable version. Also inspected the published manifests and local manifests/lockfiles of all three authored upstream dependencies: ic-memory, ic-testkit, and ic-timers. Their lockfiles introduce no additional external crate names outside the registry scan.

The IcyDB lockfile has **19 duplicated names / 22 excess versions**. Of 347 names, 304 already include the latest published stable version; 43 do not. That count does not imply 43 resolvable upgrades: many are incompatible lines retained by current upstream requirements. An online Cargo update dry run resolved **zero updates**. No locked IcyDB versions were yanked in the scanned registry records. This is an update/deduplication audit, not a vulnerability-advisory audit.

The selected `icydb` normal dependency graph for `wasm32-unknown-unknown`, excluding proc macros and build/dev dependencies, has 83 package names and only one duplicated name: `thiserror`. Both `sha2` and `num-bigint` are single-version. This graph is dependency reachability, not proof that all its code survives linking. Workspace-wide or all-target lockfile counts must not be presented as duplicated shipped Wasm.

Existing user changes were preserved: Cargo.lock already updated crc32fast 1.5.1 -> 1.5.2 and smallvec 1.16.0 -> 1.16.1; two 0.257 design/status files were also dirty. No workspace/package version bump was present or made.

## Recommended actions

1. **Upgrade TOML in ic-testkit first.** Owner: `/home/adam/projects/ic-testkit/Cargo.toml`, currently `toml = "0.9"`. The latest line is `1.1.6+spec-1.1.0` (MSRV 1.85, below ic-testkit's 1.88 floor). Its parser is used for Cargo manifests, lockfiles, configuration and cache-input projections in `crates/ic-testkit/src/artifacts/wasm_cache.rs`. An isolated copy of IcyDB's manifests and lockfile, with a temporary path override of the published ic-testkit source changing only its TOML requirement to `1.1`, resolved successfully and removed exactly toml 0.9.12, toml_datetime 0.7.5 and winnow 0.7.15. External-or-overridden packages fell **369 -> 366**, duplicated names **19 -> 16**, excess versions **22 -> 19**. This is a resolver-proven proposal, not a compiled or behavior-tested migration. Before releasing it, run focused manifest/configuration parsing and semantic cache-projection tests, then adopt the upstream release in IcyDB. No upstream files were changed.
2. **Treat base64 0.23.1 as a separate, small migration, not a deduplication win.** IcyDB currently resolves 0.22.1; PocketIC 16.0.0 requires 0.13.1; Hyper-util 0.1.20 requires 0.22; Reqwest 0.13.5 requires 0.23. Updating IcyDB alone leaves all three versions. It also requires updating `crates/icydb-core/src/db/cursor/string/mod.rs`: `DecodeError::InvalidLastSymbol(position, _)` became the struct variant `InvalidLastSymbol { offset, symbol, symbol_value }`. Preserve the maintained cursor alphabet, padding and trailing-bit rejection semantics, and run the existing focused cursor tests. New base64 defaults also enable `simd-unsafe`; review features rather than assuming an identical build. MSRV 1.71 is compatible. Full deduplication needs PocketIC and Hyper-util to update their requirements; these are not authored upstream crates.
3. **Keep sha2 0.10.9 until the IC stack converges.** Latest stable is 0.11.0, but ic_principal, icrc-ledger-types, ic-certification, ic-transport-types, PocketIC and authored ic-testkit still require 0.10. Updating IcyDB or ic-testkit alone introduces another SHA/digest stack. IcyDB's dependency-graph gate explicitly rejects duplicate sha2 and digest versions. The Cargo.toml comment describing this alignment is still accurate. Trigger: current releases across the relevant upstream owners accept the same SHA line. No compatibility bridges or local forks are recommended.
4. **Keep num-bigint 0.4.8 aligned with Candid.** Latest stable is 0.5.1. Candid 0.10.35, icrc-cbor 0.1.0 and icrc-ledger-types 0.2.0 still require 0.4. IcyDB passes BigUint directly into candid::Nat in `crates/icydb-schema/src/nat_big.rs:64`; a standalone 0.5 bump introduces a second type identity and breaks that direct conversion boundary. Trigger: coordinated Candid/ledger dependency updates. There is no existing num-bigint duplication to remove.
5. **Track the upstream thiserror pin.** Latest stable is 2.0.20; PocketIC 16.0.0 requires exactly `=2.0.18`, keeping IcyDB on that compatible 2.x release. Candid and ic_principal separately retain thiserror 1.0.69. Removing the 2.x exact pin and removing the 1.x duplication are different upstream tasks. ic-memory and ic-timers already resolve 2.0.20 in their own local lockfiles, but their broad `2.0` published requirements allow IcyDB to resolve 2.0.18. Updating those libraries' lockfiles or minimum requirements is not a solution to PocketIC's pin.

## Verification and limits

- Passed: locked, offline Cargo metadata for the whole workspace; online `cargo update --dry-run` (zero changes); fresh registry comparison for all 347 external names; `scripts/ci/check-dependency-graph-invariants.sh`; target-filtered normal runtime dependency inspection; isolated TOML resolver proposal.

- Passed: non-mutating, offline update previews for the three authored upstream local worktrees. Registry names all overlap the fresh IcyDB registry scan. Local upstream identities inspected: ic-memory `0dcd24d`, ic-testkit `22fea41`, ic-timers `c1d1fa0`; all were clean at inspection. Published package manifests, rather than local lockfiles, determine IcyDB's incoming requirements.

- Exact baseline graph commands: `cargo metadata --locked --offline --format-version 1`; `cargo tree --locked --offline -p icydb --target wasm32-unknown-unknown -e normal,no-proc-macro --prefix none --format '{p}'`. The latter excludes host proc-macro and build graphs, unlike a workspace-wide Wasm-target tree.

- The TOML probe used copied workspace manifests/lockfile and placeholder targets in `/tmp`, with published ic-testkit source and a temporary path override. It performed dependency resolution only, never compilation; the override is not a proposed shipped configuration.

- Skipped: source compilation, behavioral tests, full repository suites and vulnerability-advisory tooling. This audit changed no application or dependency code. Follow-up implementation needs the focused tests named above; full-suite execution remains user-owned.

- Raw Wasm bytes, IC cycles and instruction deltas: **unmeasured**. TOML's three-package reduction affects host tooling; no canister size/cycle benefit is claimed. No timing benchmarks or network lifecycle actions were performed.

- Delivery complexity: one Markdown audit report added; production file/line delta 0/0, implementation shape unchanged. Manifests, lockfiles, release metadata and upstream working trees were not edited.

Registry source: [crates.io sparse-index format](https://doc.rust-lang.org/cargo/reference/registry-index.html), read live per crate (for example [base64](https://index.crates.io/ba/se/base64), [sha2](https://index.crates.io/sh/a2/sha2), [num-bigint](https://index.crates.io/nu/m-/num-bigint), [ic-testkit](https://index.crates.io/ic/-t/ic-testkit), [toml](https://index.crates.io/to/ml/toml), [PocketIC](https://index.crates.io/po/ck/pocket-ic)). Published source manifests and base64's release notes were inspected in Cargo's local registry source cache; latest primary documentation independently confirms [base64 0.23.1](https://docs.rs/crate/base64/0.23.1), [sha2 0.11.0](https://docs.rs/crate/sha2/0.11.0), and [num-bigint 0.5.1](https://docs.rs/crate/num-bigint/0.5.1).

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/13/dependency-audit/01/report.md).
