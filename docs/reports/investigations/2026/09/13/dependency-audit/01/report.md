# Dependency update and deduplication audit

Date: 2026-09-13. IcyDB source: `818d4350e310af304fcb41b36357c292075c40a2`, workspace version `0.257.9`.

## Scope and result

Audited all 48 workspace members, all 35 directly consumed external crates, and all 369 resolved external package versions (347 distinct crate names). Checked fresh public crates.io sparse-index records for every external name, excluding prereleases and yanked releases when selecting the latest stable version. Also inspected the published manifests and local manifests/lockfiles of all three authored upstream dependencies: ic-memory, ic-testkit, and ic-timers. Their lockfiles introduce no additional external crate names outside the registry scan.

The IcyDB lockfile has **19 duplicated names / 22 excess versions**. Of 347 names, 304 already include the latest published stable version; 43 do not. That count does not imply 43 resolvable upgrades: many are incompatible lines retained by current upstream requirements. An online Cargo update dry run resolved **zero updates**. No locked IcyDB versions were yanked in the scanned registry records. This is an update/deduplication audit, not a vulnerability-advisory audit.

The selected `icydb` normal dependency graph for `wasm32-unknown-unknown`, excluding proc macros and build/dev dependencies, has 83 package names and only one duplicated name: `thiserror`. Both `sha2` and `num-bigint` are single-version. This graph is dependency reachability, not proof that all its code survives linking. Workspace-wide or all-target lockfile counts must not be presented as duplicated shipped Wasm.

Existing user changes were preserved: Cargo.lock already updated crc32fast 1.5.1 -> 1.5.2 and smallvec 1.16.0 -> 1.16.1; two 0.257 design/status files were also dirty. No workspace/package version bump was present or made.

## Recommended actions

1. **Upgrade TOML in ic-testkit first.** Owner: `/home/adam/projects/ic-testkit/Cargo.toml`, currently `toml = "0.9"`. The latest line is `1.1.6+spec-1.1.0` (MSRV 1.85, below ic-testkit's 1.88 floor). Its parser is used for Cargo manifests, lockfiles, configuration and cache-input projections in `crates/ic-testkit/src/artifacts/wasm_cache.rs`. An isolated copy of IcyDB's manifests and lockfile, with a temporary path override of the published ic-testkit source changing only its TOML requirement to `1.1`, resolved successfully and removed exactly toml 0.9.12, toml_datetime 0.7.5 and winnow 0.7.15. External-or-overridden packages fell **369 -> 366**, duplicated names **19 -> 16**, excess versions **22 -> 19**. This is a resolver-proven proposal, not a compiled or behavior-tested migration. Before releasing it, run focused manifest/configuration parsing and semantic cache-projection tests, then adopt the upstream release in IcyDB. No upstream files were changed.
2. **Treat base64 0.23.1 as a separate, small migration, not a deduplication win.** IcyDB currently resolves 0.22.1; PocketIC 16.0.0 requires 0.13.1; Hyper-util 0.1.20 requires 0.22; Reqwest 0.13.5 requires 0.23. Updating IcyDB alone leaves all three versions. It also requires updating `crates/icydb-core/src/db/cursor/string.rs`: `DecodeError::InvalidLastSymbol(position, _)` became the struct variant `InvalidLastSymbol { offset, symbol, symbol_value }`. Preserve the maintained cursor alphabet, padding and trailing-bit rejection semantics, and run the existing focused cursor tests. New base64 defaults also enable `simd-unsafe`; review features rather than assuming an identical build. MSRV 1.71 is compatible. Full deduplication needs PocketIC and Hyper-util to update their requirements; these are not authored upstream crates.
3. **Keep sha2 0.10.9 until the IC stack converges.** Latest stable is 0.11.0, but ic_principal, icrc-ledger-types, ic-certification, ic-transport-types, PocketIC and authored ic-testkit still require 0.10. Updating IcyDB or ic-testkit alone introduces another SHA/digest stack. IcyDB's dependency-graph gate explicitly rejects duplicate sha2 and digest versions. The Cargo.toml comment describing this alignment is still accurate. Trigger: current releases across the relevant upstream owners accept the same SHA line. No compatibility bridges or local forks are recommended.
4. **Keep num-bigint 0.4.8 aligned with Candid.** Latest stable is 0.5.1. Candid 0.10.35, icrc-cbor 0.1.0 and icrc-ledger-types 0.2.0 still require 0.4. IcyDB passes BigUint directly into candid::Nat in `crates/icydb-schema/src/nat_big.rs:64`; a standalone 0.5 bump introduces a second type identity and breaks that direct conversion boundary. Trigger: coordinated Candid/ledger dependency updates. There is no existing num-bigint duplication to remove.
5. **Track the upstream thiserror pin.** Latest stable is 2.0.20; PocketIC 16.0.0 requires exactly `=2.0.18`, keeping IcyDB on that compatible 2.x release. Candid and ic_principal separately retain thiserror 1.0.69. Removing the 2.x exact pin and removing the 1.x duplication are different upstream tasks. ic-memory and ic-timers already resolve 2.0.20 in their own local lockfiles, but their broad `2.0` published requirements allow IcyDB to resolve 2.0.18. Updating those libraries' lockfiles or minimum requirements is not a solution to PocketIC's pin.

## Authored upstream packages and real pins

| Owner | Published / consumed | Finding and disposition |
| --- | --- | --- |
| ic-memory | 0.13.2 / 0.13.2 | Already latest. Exact `ic-stable-structures =0.7.2` is also latest and single-version. Keep it; no current deduplication benefit from loosening. Local development lockfile can refresh toml 1.1.5 -> 1.1.6 through trybuild. |
| ic-testkit | 0.9.0 / 0.9.0 | Already latest. Its TOML requirement is the clearest authored upstream deduplication target. Retain SHA alignment. Its local lockfile has 22 compatible version updates plus one newly introduced base64 0.23.1 from Reqwest; this refresh does not update downstream IcyDB, which already resolves those newer versions. |
| ic-timers | 0.7.0 / 0.7.0 | Already latest. Exact `ic-cdk-timers =1.0.0` and `ic0 =1.1.0` are both latest and single-version. Local development lockfile can refresh smallvec 1.15.2 -> 1.16.1 and syn 3.0.4 -> 3.0.5. |
| IcyDB SQLite reference | rusqlite =0.40.2 | Already latest. This is an actual exact external pin, tied to the bundled SQLite reference identity; retain unless deliberately updating that reference. |
| IcyDB internal packages | =0.257.9 | Deliberate exact workspace alignment owned by release tooling. Not dependency-update candidates. |
| IcyDB time gate | 0.3.55 | Cargo requirement is a caret range, but the graph-invariant script requires the resolved version to equal the written workspace version. Latest is currently 0.3.55; future bumps need both authorities kept consistent. |

`base64 = "0.22.1"`, `sha2 = "0.10.9"` and `num-bigint = "0.4"` are caret requirements, not exact `=` pins. Their pre-1.0 minor ranges still exclude 0.23, 0.11 and 0.5 respectively. Raising an already-satisfied minimum version does not itself deduplicate packages. External macro/runtime pairs also use legitimate exact pins (Candid, Clap, Darling, Serde, Thiserror, Time and Wasm-bindgen); these are owned by their publishers and should not be independently overridden.

## All direct external dependencies

The resolved column lists versions reached directly from workspace members, not all older transitive copies of the same name. Latest versions link to the publisher's registry page.

| Crate | Requirement | Directly resolved | Latest stable | Disposition |
| --- | --- | --- | --- | --- |
| base64 | ^0.22.1 | 0.22.1 | [0.23.1](https://crates.io/crates/base64/0.23.1) | Small source migration; no version-count reduction. |
| blake3 | ^1.8.5 | 1.8.7 | [1.8.7](https://crates.io/crates/blake3/1.8.7) | Current. |
| candid | ^0.10 | 0.10.35 | [0.10.35](https://crates.io/crates/candid/0.10.35) | Current. |
| ciborium | ^0.2 | 0.2.2 | [0.2.2](https://crates.io/crates/ciborium/0.2.2) | Current. |
| clap | ^4.6.1 | 4.6.6 | [4.6.6](https://crates.io/crates/clap/4.6.6) | Current. |
| convert_case | ^0.12 | 0.12.0 | [0.12.0](https://crates.io/crates/convert_case/0.12.0) | Current. |
| ctor | ^1.0.0 | 1.0.13 | [1.0.13](https://crates.io/crates/ctor/1.0.13) | Current. |
| darling | ^0.24 | 0.24.1 | [0.24.1](https://crates.io/crates/darling/0.24.1) | Current. |
| derive_more | ^2.0 | 2.1.1 | [2.1.1](https://crates.io/crates/derive_more/2.1.1) | Current. |
| ethnum | ^1.5.3 | 1.5.3 | [1.5.3](https://crates.io/crates/ethnum/1.5.3) | Current. |
| getrandom | ^0.4 | 0.4.3 | [0.4.3](https://crates.io/crates/getrandom/0.4.3) | Current. |
| ic-cdk | ^0.20.2 | 0.20.2 | [0.20.2](https://crates.io/crates/ic-cdk/0.20.2) | Current. |
| ic-memory | ^0.13.2 | 0.13.2 | [0.13.2](https://crates.io/crates/ic-memory/0.13.2) | Current. |
| ic-testkit | ^0.9.0 | 0.9.0 | [0.9.0](https://crates.io/crates/ic-testkit/0.9.0) | Current. |
| ic-timers | ^0.7.0 | 0.7.0 | [0.7.0](https://crates.io/crates/ic-timers/0.7.0) | Current. |
| icrc-ledger-types | ^0.2.0 | 0.2.0 | [0.2.0](https://crates.io/crates/icrc-ledger-types/0.2.0) | Current. |
| num-bigint | ^0.4 | 0.4.8 | [0.5.1](https://crates.io/crates/num-bigint/0.5.1) | Retain Candid type alignment. |
| proc-macro-crate | ^3.5.0 | 3.5.0 | [3.5.0](https://crates.io/crates/proc-macro-crate/3.5.0) | Current. |
| proc-macro2 | ^1.0 | 1.0.107 | [1.0.107](https://crates.io/crates/proc-macro2/1.0.107) | Current. |
| proptest | ^1.9.0 | 1.11.0 | [1.11.0](https://crates.io/crates/proptest/1.11.0) | Current. |
| quote | ^1.0 | 1.0.47 | [1.0.47](https://crates.io/crates/quote/1.0.47) | Current. |
| rand_chacha | ^0.10 | 0.10.0 | [0.10.0](https://crates.io/crates/rand_chacha/0.10.0) | Current. |
| remain | ^0.2 | 0.2.16 | [0.2.16](https://crates.io/crates/remain/0.2.16) | Current. |
| rusqlite | =0.40.2 | 0.40.2 | [0.40.2](https://crates.io/crates/rusqlite/0.40.2) | Current. |
| rustyline | ^18.0 | 18.0.1 | [18.0.1](https://crates.io/crates/rustyline/18.0.1) | Current. |
| serde | ^1.0 | 1.0.229 | [1.0.229](https://crates.io/crates/serde/1.0.229) | Current. |
| serde_bytes | ^0.11 | 0.11.19 | [0.11.19](https://crates.io/crates/serde_bytes/0.11.19) | Current. |
| serde_json | ^1.0 | 1.0.151 | [1.0.151](https://crates.io/crates/serde_json/1.0.151) | Current. |
| sha2 | ^0.10.9 | 0.10.9 | [0.11.0](https://crates.io/crates/sha2/0.11.0) | Retain IC-stack alignment. |
| syn | ^3.0 | 3.0.5 | [3.0.5](https://crates.io/crates/syn/3.0.5) | Current. |
| thiserror | ^2.0 | 2.0.18 | [2.0.20](https://crates.io/crates/thiserror/2.0.20) | PocketIC exact pin blocks 2.0.20. |
| time | ^0.3.55 | 0.3.55 | [0.3.55](https://crates.io/crates/time/0.3.55) | Current. |
| trybuild | ^1.0 | 1.0.121 | [1.0.121](https://crates.io/crates/trybuild/1.0.121) | Current. |
| ulid | ^3.0.0 | 3.0.0 | [3.0.0](https://crates.io/crates/ulid/3.0.0) | Current. |
| xxhash-rust | ^0.8.15 | 0.8.18 | [0.8.18](https://crates.io/crates/xxhash-rust/0.8.18) | Current. |

## Complete duplicate inventory

All versions are from IcyDB's unchanged lockfile. Most rows are host tooling or target-specific dependencies; they are not all present in the canister runtime.

| Crate | Locked versions | Owner / disposition |
| --- | --- | --- |
| arrayvec | 0.5.2, 0.7.8 | pretty (Candid printer/build graph) retains 0.5; blake3 uses 0.7. |
| base64 | 0.13.1, 0.22.1, 0.23.1 | PocketIC requires 0.13; Hyper proxy requires 0.22; Reqwest requires 0.23. IcyDB uses 0.22. |
| cpufeatures | 0.2.17, 0.3.1 | sha2 0.10 requires 0.2; blake3/chacha20 require 0.3. Target/build dependent. |
| darling | 0.23.0, 0.24.1 | ic-cdk-macros requires 0.23; IcyDB macros require 0.24. Host proc macros. |
| darling_core | 0.23.0, 0.24.1 | Follows the two darling versions. Host proc macros. |
| darling_macro | 0.23.0, 0.24.1 | Follows the two darling versions. Host proc macros. |
| getrandom | 0.2.17, 0.3.4, 0.4.3 | ring requires 0.2; proptest/rand_core requires 0.3; native IcyDB and other tooling use 0.4. |
| half | 1.8.3, 2.7.1 | PocketIC serde_cbor requires 1; ic-memory ciborium uses 2. One runtime version. |
| r-efi | 5.3.0, 6.0.0 | getrandom 0.3/0.4 require distinct versions; UEFI target support. |
| rand | 0.9.5, 0.10.2 | proptest requires 0.9; quinn-proto uses 0.10; no rand crate on selected runtime graph. |
| rand_chacha | 0.9.0, 0.10.0 | proptest requires 0.9; IcyDB runtime uses 0.10. |
| rand_core | 0.9.5, 0.10.1 | Follows rand/rand_chacha split. Runtime uses 0.10. |
| syn | 1.0.109, 2.0.119, 3.0.5 | minicbor-derive retains 1; IC and other macros retain 2; IcyDB uses 3. Host proc macros. |
| thiserror | 1.0.69, 2.0.18 | Candid/ic_principal retain 1; runtime uses 2 elsewhere. PocketIC pins the 2.x line to 2.0.18. |
| thiserror-impl | 1.0.69, 2.0.18 | Follows thiserror versions. Host proc macros. |
| toml | 0.9.12+spec-1.1.0, 1.1.6+spec-1.1.0 | ic-testkit retains 0.9; trybuild uses 1.1. Confirmed removable old version after upstream upgrade. |
| toml_datetime | 0.7.5+spec-1.1.0, 1.1.1+spec-1.1.0 | Old version owned only by toml 0.9. Confirmed removable. |
| windows-sys | 0.52.0, 0.61.2 | ring retains 0.52; other native support uses 0.61. Windows target support. |
| winnow | 0.7.15, 1.0.4 | Old version owned only by toml 0.9. Confirmed removable. |

Upgrading IcyDB's own syn/darling further cannot remove versions required by current upstream proc macros. Proptest owns the older rand family; ring owns old getrandom/Windows support; PocketIC's serde_cbor owns half 1.x. Keep these requirements with their semantic owners instead of introducing forks or forced overrides solely to reduce the count. No compatible deduplication appeared in the update dry run.

## Other transitive dependencies behind latest stable

This table covers remaining names whose latest release is absent, excluding direct dependencies and duplicated names covered above. Each requires an upstream requirement change rather than an available IcyDB lockfile refresh. A latest release is an audit observation, not a claim of source/API compatibility. Platform rows may be inactive on the selected build target.

| Crate | Locked | Latest stable | Immediate owners in resolved graph |
| --- | --- | --- | --- |
| base32 | 0.4.0 | [0.5.1](https://crates.io/crates/base32/0.5.1) | icrc-ledger-types 0.2.0 |
| bit-set | 0.8.0 | [0.11.1](https://crates.io/crates/bit-set/0.11.1) | proptest 1.11.0 |
| bit-vec | 0.8.0 | [0.10.1](https://crates.io/crates/bit-vec/0.10.1) | bit-set 0.8.0, proptest 1.11.0 |
| block-buffer | 0.10.4 | [0.12.1](https://crates.io/crates/block-buffer/0.12.1) | digest 0.10.7 |
| constant_time_eq | 0.4.2 | [0.6.0](https://crates.io/crates/constant_time_eq/0.6.0) | blake3 1.8.7 |
| crypto-common | 0.1.7 | [0.2.2](https://crates.io/crates/crypto-common/0.2.2) | digest 0.10.7 |
| digest | 0.10.7 | [0.11.3](https://crates.io/crates/digest/0.11.3) | sha2 0.10.9 |
| erased-serde | 0.3.31 | [0.4.10](https://crates.io/crates/erased-serde/0.4.10) | slog 2.8.2 |
| generic-array | 0.14.7 | [1.4.5](https://crates.io/crates/generic-array/1.4.5) | block-buffer 0.10.4, crypto-common 0.1.7 |
| ic-management-canister-types | 0.8.0 | [0.10.0](https://crates.io/crates/ic-management-canister-types/0.10.0) | pocket-ic 16.0.0 |
| minicbor | 0.19.1 | [2.3.0](https://crates.io/crates/minicbor/2.3.0) | icrc-cbor 0.1.0, icrc-ledger-types 0.2.0 |
| minicbor-derive | 0.13.0 | [0.19.5](https://crates.io/crates/minicbor-derive/0.19.5) | minicbor 0.19.1 |
| object | 0.39.1 | [0.40.0](https://crates.io/crates/object/0.40.0) | ar_archive_writer 0.5.3 |
| quick-error | 1.2.3 | [2.0.1](https://crates.io/crates/quick-error/2.0.1) | rusty-fork 0.3.1 |
| quinn-udp | 0.5.15 | [0.6.2](https://crates.io/crates/quinn-udp/0.6.2) | quinn 0.11.11 |
| rand_xorshift | 0.4.0 | [0.5.0](https://crates.io/crates/rand_xorshift/0.5.0) | proptest 1.11.0 |
| redox_syscall | 0.5.18 | [0.9.4](https://crates.io/crates/redox_syscall/0.9.4) | parking_lot_core 0.9.12 |
| schemars | 0.8.22 | [1.2.2](https://crates.io/crates/schemars/1.2.2) | pocket-ic 16.0.0 |
| schemars_derive | 0.8.22 | [1.2.2](https://crates.io/crates/schemars_derive/1.2.2) | schemars 0.8.22 |
| serde_derive_internals | 0.29.1 | [0.30.0](https://crates.io/crates/serde_derive_internals/0.30.0) | schemars_derive 0.8.22 |
| strum | 0.26.3 | [0.28.0](https://crates.io/crates/strum/0.28.0) | ic-error-types 0.2.0, icrc-ledger-types 0.2.0, pocket-ic 16.0.0 |
| strum_macros | 0.26.4 | [0.28.0](https://crates.io/crates/strum_macros/0.28.0) | ic-error-types 0.2.0, icrc-ledger-types 0.2.0, pocket-ic 16.0.0, strum 0.26.3 |
| synstructure | 0.13.2 | [0.14.0](https://crates.io/crates/synstructure/0.14.0) | yoke-derive 0.8.2, zerofrom-derive 0.1.7 |
| tower-http | 0.6.11 | [0.7.1](https://crates.io/crates/tower-http/0.7.1) | reqwest 0.13.5 |
| wasi | 0.11.1+wasi-snapshot-preview1 | [0.14.7+wasi-0.2.4](https://crates.io/crates/wasi/0.14.7+wasi-0.2.4) | getrandom 0.2.17, mio 1.2.3 |
| wasip2 | 1.0.4+wasi-0.2.12 | [2.0.0+wasi-0.2.12](https://crates.io/crates/wasip2/2.0.0+wasi-0.2.12) | getrandom 0.3.4 |
| wasm-streams | 0.5.0 | [0.6.0](https://crates.io/crates/wasm-streams/0.6.0) | reqwest 0.13.5 |
| windows-link | 0.2.1 | [0.100.0](https://crates.io/crates/windows-link/0.100.0) | jni 0.22.4, parking_lot_core 0.9.12, windows-sys 0.61.2 |
| windows-targets | 0.52.6 | [0.53.5](https://crates.io/crates/windows-targets/0.53.5) | windows-sys 0.52.0 |
| windows_aarch64_gnullvm | 0.52.6 | [0.53.1](https://crates.io/crates/windows_aarch64_gnullvm/0.53.1) | windows-targets 0.52.6 |
| windows_aarch64_msvc | 0.52.6 | [0.53.1](https://crates.io/crates/windows_aarch64_msvc/0.53.1) | windows-targets 0.52.6 |
| windows_i686_gnu | 0.52.6 | [0.53.1](https://crates.io/crates/windows_i686_gnu/0.53.1) | windows-targets 0.52.6 |
| windows_i686_gnullvm | 0.52.6 | [0.53.1](https://crates.io/crates/windows_i686_gnullvm/0.53.1) | windows-targets 0.52.6 |
| windows_i686_msvc | 0.52.6 | [0.53.1](https://crates.io/crates/windows_i686_msvc/0.53.1) | windows-targets 0.52.6 |
| windows_x86_64_gnu | 0.52.6 | [0.53.1](https://crates.io/crates/windows_x86_64_gnu/0.53.1) | windows-targets 0.52.6 |
| windows_x86_64_gnullvm | 0.52.6 | [0.53.1](https://crates.io/crates/windows_x86_64_gnullvm/0.53.1) | windows-targets 0.52.6 |
| windows_x86_64_msvc | 0.52.6 | [0.53.1](https://crates.io/crates/windows_x86_64_msvc/0.53.1) | windows-targets 0.52.6 |
| wit-bindgen | 0.57.1 | [0.62.0](https://crates.io/crates/wit-bindgen/0.62.0) | wasip2 1.0.4+wasi-0.2.12 |

## Verification and limits

- Passed: locked, offline Cargo metadata for the whole workspace; online `cargo update --dry-run` (zero changes); fresh registry comparison for all 347 external names; `scripts/ci/check-dependency-graph-invariants.sh`; target-filtered normal runtime dependency inspection; isolated TOML resolver proposal.
- Passed: non-mutating, offline update previews for the three authored upstream local worktrees. Registry names all overlap the fresh IcyDB registry scan. Local upstream identities inspected: ic-memory `0dcd24d`, ic-testkit `22fea41`, ic-timers `c1d1fa0`; all were clean at inspection. Published package manifests, rather than local lockfiles, determine IcyDB's incoming requirements.
- Exact baseline graph commands: `cargo metadata --locked --offline --format-version 1`; `cargo tree --locked --offline -p icydb --target wasm32-unknown-unknown -e normal,no-proc-macro --prefix none --format '{p}'`. The latter excludes host proc-macro and build graphs, unlike a workspace-wide Wasm-target tree.
- The TOML probe used copied workspace manifests/lockfile and placeholder targets in `/tmp`, with published ic-testkit source and a temporary path override. It performed dependency resolution only, never compilation; the override is not a proposed shipped configuration.
- Skipped: source compilation, behavioral tests, full repository suites and vulnerability-advisory tooling. This audit changed no application or dependency code. Follow-up implementation needs the focused tests named above; full-suite execution remains user-owned.
- Raw Wasm bytes, IC cycles and instruction deltas: **unmeasured**. TOML's three-package reduction affects host tooling; no canister size/cycle benefit is claimed. No timing benchmarks or network lifecycle actions were performed.
- Delivery complexity: one Markdown audit report added; production file/line delta 0/0, implementation shape unchanged. Manifests, lockfiles, release metadata and upstream working trees were not edited.

Registry source: [crates.io sparse-index format](https://doc.rust-lang.org/cargo/reference/registry-index.html), read live per crate (for example [base64](https://index.crates.io/ba/se/base64), [sha2](https://index.crates.io/sh/a2/sha2), [num-bigint](https://index.crates.io/nu/m-/num-bigint), [ic-testkit](https://index.crates.io/ic/-t/ic-testkit), [toml](https://index.crates.io/to/ml/toml), [PocketIC](https://index.crates.io/po/ck/pocket-ic)). Published source manifests and base64's release notes were inspected in Cargo's local registry source cache; latest primary documentation independently confirms [base64 0.23.1](https://docs.rs/crate/base64/0.23.1), [sha2 0.11.0](https://docs.rs/crate/sha2/0.11.0), and [num-bigint 0.5.1](https://docs.rs/crate/num-bigint/0.5.1).
