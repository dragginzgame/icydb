# Shared Tooling Adoption

This document describes current ownership and qualification boundaries. Dependency
identities come from [Cargo.toml](../../Cargo.toml), [Cargo.lock](../../Cargo.lock)
and the [reviewed snapshot](../../.shared-tooling.snapshot), not historical notes.
[AGENTS.md](../../AGENTS.md) is IcyDB's local engineering overlay.

## Ownership and provenance

IcyDB selects Shared Tooling 0.26.23 at published revision
`48ec9794892da6f8e2812653ec16c5e8ea23df47`. The canonical exporter owns the
130 selected payloads and their hashes/modes. Refresh them canonically; a
sibling checkout's uncommitted changes are not part of this snapshot.

The complete pre-cleanup record remains in Git at commit
`b1dd1a5ba9a425bff9fbde06d639f714a3ec420d`, at this document's path.
[Earlier published records](https://github.com/dragginzgame/icydb/blob/9ac30667d26728a539e8c640bf2c14b4e1406ba7/docs/governance/shared-tooling.md)
are also available online. Historical versions, failures, temporary evidence
paths and qualification claims apply
to their recorded inputs. Future adoption replaces the current summary here;
release notes and owning GitHub issues retain detailed delivery evidence.

## Current capability boundary

IcyDB owns accepted-schema authority, query planning/execution, storage/key
formats, recovery semantics, its database facade and application-model generation.
The CLI and test actors consume those contracts. Existing packages remain in the
single root workspace under `crates/`, `canisters/`, `schema/` and `testing/`.
[The approved layout](../../rules/rust-workspaces.md#approved-icydb-layout) and
[IcyDB #310 — Approved existing layout](https://github.com/dragginzgame/icydb/issues/310)
own any future placement decision.

| Capability | Canonical owner | IcyDB integration retained |
| --- | --- | --- |
| IC exports, standalone lifecycle and startup timer hosting | Canic's `canic-icydb` adapter | Private database handlers, explicit mounting and installed boundary tests |
| Native payload/envelope encoding, sizing and TypeScript runtime | IC Wire | Endpoint identities, concrete metadata, resource policy and scalar validation |
| Principal identity and external Candid/CDK interfaces | IC Platform | Explicit platform features; no workspace-owned direct SDK use |
| Allocation admission and exclusive memory leases | IC Memory | Database keys, host grant declarations and role bindings |
| Governed collections and stable codec interfaces | IC Storage | Database codecs, indexes, transactions and retained-control admission |
| Measurement arithmetic | IC Metrics | Phase/entity attribution, report selection and resets |
| Cargo/Wasm acquisition and retained caches | IC Build | Actor recipes, optimizer/interface policy and report budgets |
| File/process/text primitives | IC Host Tooling | Product artifact admission and CLI presentation |
| PocketIC setup, admission, gateway and lifecycle | IC Testkit | Fixture topology, scenarios and native Wire calls |
| Installers, release plumbing and structural scanner | Shared Tooling | Local Make inputs, release callbacks and source inventories |

Applications explicitly mount `canic_icydb::start!` and `canic_icydb::endpoints!`.
The codec-only audit's raw export isolates its graph; it is not an application
endpoint owner. Complete managed memory/lifecycle composition remains with
[IcyDB #373 — Move generated endpoints into the Canic adapter](https://github.com/dragginzgame/icydb/issues/373).
Native database operations and precommit reply admission remain IcyDB contracts.
Consumer lifecycle, rollback, browser and recovery tests remain necessary.

## Dependency selection

The root workspace and lockfile own one dependency graph. Internal published
IcyDB edges use exact matching versions because generated code and schema
contracts move together. The bundled SQLite oracle has its own exact constraint;
it is host-only. [Pin exceptions](../../ci/dependency-pinning-exceptions.json)
record both reasons; release tooling updates only release-owned values.

Registry dependencies and the immutable Canic adapter Git revision are explicit
in the root catalog. Ordinary builds and validation use locked inputs. Prepare
an authorized dependency change with `make fetch` before offline validation.
Do not introduce sibling path overrides or silently update inputs during a gate.

`make check-dependency-pins` enables Shared's Cargo-inheritance checks across
normal, build, development and target-specific declarations. `check-invariants`
enables the common [external-IC inventory](../../ci/external-ic-dependencies.txt);
[local policy](../../ci/cargo-dependency-boundaries.json) adds stricter bans.
The local graph guard retains coupled pins, sensitive version uniqueness and
release-specific exclusions. Declaration/graph checks do not prove final Wasm
contents: [#378 — Enforce deployment dependency exclusions](https://github.com/dragginzgame/icydb/issues/378)
retains the artifact-admission obligation.

Tool versions come from the [shared host/Rust catalog](../../ci/tool-versions.env),
[IC archive matrix](../../ci/ic-tools.tsv) and [IcyDB utility selections](../../ci/icydb-tools.env).
After preparing Rust and locked dependencies, `make install-tools` explicitly
prepares the selected tools and Testkit; `make tools-check` admits them offline.
The [setup guide](../../INSTALLING.md) owns prerequisites and workstation usage.
Validation does not install missing tools. Cargo homes and target directories
come from the root Makefile; concurrent work must isolate them.

Testkit alone selects, provisions and admits its server. Shared selects the
locked Testkit CLI; IcyDB calls its setup/check interfaces. Retain no independent
PocketIC pin, installer or version parser. Binaryen archive provisioning and
IcyDB's [raw executable admission](../../scripts/ci/wasm-optimizer-checksums.tsv)
are separate obligations; artifact transformation remains explicitly qualified.

## Consumer tooling qualification

Current direct minimums are IC Build/Host/Testkit 0.69.0, Platform 0.7.0,
Wire 0.7.18, Memory/Storage 0.51.8 and Metrics 0.16.3. Timers' minimum remains
0.31.7; the compatible lock selects Memory/Storage 0.51.10 and Timers 0.31.10.
Registry availability was verified before locked cache preparation. Canic stays at its explicit adapter
revision in Cargo.toml; endpoint and lifecycle ownership are unchanged.

Platform 0.7.0 removes Candid frontend compiler exports that this workspace does
not use. Its Principal, Account, executor, ic0 and CDK entry modules match the
published 0.6.21 sources. Both runtime adapters use the same registry
`ic-cdk-executor` 2.0.0 identity. Memory/Storage 0.51.9, Timers 0.31.9 and
Tooling 0.69 select Platform 0.7.0. Canic still selects Platform 0.6.21
transitively; the two package identities are explicit in the lockfile.
The selected Core Wasm normal graph has no Candid; the existing adapter's external
entropy boundary and host Testkit graphs retain their upstream Candid selection.

Tooling 0.69.0 is published from
`f268aef5d205e51811e934648afcd13f026635f1`. Its seven consumed package archives
contain unchanged Rust sources relative to 0.68.12; the coordinated release
aligns their Platform-owned Principal identity with IcyDB's existing 0.7
selection. Shared Tooling 0.26.23 and Platform 0.7.0 are the latest verified
releases for this adoption.

Shared Tooling 0.26.23 owns Rust formatting orchestration. IcyDB selects
`FORMAT_RUST_ARGS := --sort-derives`, replacing its paired shell commands with
the shared grouped-manifest, derive and Rustfmt sequence
([#473](https://github.com/dragginzgame/icydb/issues/473)). The selected compiler,
repo-local Cargo home/target and prepared offline admission remain unchanged.
Manifest sorting now precedes derive sorting. Ordinary write mode retains
successful earlier writes on a later failure; the index-export hook isolates
those writes and refreshes selected files only after every pass succeeds.
The shared driver owns per-pass progress, retained failure logs and exact-input
success receipts; hook exports always run the passes. Progress durations are
informational, not performance qualification.

Focused Linux checks pass snapshot integrity, actual consumer formatting and
hook preservation, shared/release adapter dispatch, selected-source release
metadata, workstation orchestration and shell/Perl validation. The formatter
fixture qualifies retained exit-43
failure logs, manifest-before-derive partial writes, real-tool input reuse,
source invalidation and forced execution. The hook checks reject partial staging
and preserve role groups, lockfiles and unrelated edits, including unusual paths.
These results do not qualify native macOS, installed IC/browser behavior or the
full release gate. Cargo dependency and package-version selections are unchanged.

The new gzip ownership rule is already met by the production report writer,
which delegates encoding to IC Tooling. One shell report fixture still constructs
its gzip member with the system encoder; its independent follow-through is
tracked in [#475](https://github.com/dragginzgame/icydb/issues/475).

The earlier Shared Tooling 0.26.22 adoption changed governance payloads, including
owned-type Candid restrictions and mechanical error-derive guidance. Its
executable helpers and tool catalogs were unchanged. Snapshot integrity,
dependency pins and the canonical Cargo dependency boundary pass. The owned
Rust sources use no CandidType derives
or retired Platform Candid compiler APIs. The Tooling 0.69 / Memory 0.51.9 /
Timers 0.31.9 graph passed strict
core/schema/CLI/build/integration all-target Clippy, 21 build-tool consumer tests,
18 CLI process-boundary tests and Rust 1.91 facade SQL/migration Wasm compilation.
Selected Core and audit-actor normal/build graph checks retain host-tool/parser
exclusions, one Core Platform identity and one actor executor identity.

The installed deterministic post-link/upgrade contract passed on that graph
under Testkit-managed PocketIC. Its compiler artifact was 4,736,891 raw bytes
and its final deployable was 4,168,823 bytes. The test checks retained rows
and query results across upgrade from the compiler artifact to its transformed
artifact on this same dependency graph. It does not qualify an upgrade from an
older dependency release or establish matched dependency-adoption savings.
Testkit 0.69's CLI and server admission were prepared through the normal setup
target; Testkit retained all local server startup and cleanup ownership.

Memory/Storage 0.51.10 and Timers 0.31.10 were selected concurrently during
the subsequent text-construction correction. Those locked inputs were fetched
without editing their selections. Focused Core SQL/migration Clippy and Rust
1.91 facade Wasm compilation pass on that graph; the earlier installed result
does not qualify these later runtime package selections.

Earlier scalar/Wire and focused Core budget/traversal/memory-diagnostic results
belong to their recorded dependency graphs. This tooling adoption does not
claim those complete test families were rerun.
Native macOS, browser and full release gates remain separate qualification
obligations. Deployment artifact dependency exclusion enforcement remains
tracked separately in #378.

Earlier browser qualification covered 90 exact frames across 12 methods plus
3,108 truncations and malformed/value/limit controls. That evidence belongs to
its recorded dependency selections; this adoption does not rerun or claim
installed Chromium qualification.

The new [RPC-budget rule](../../rules/rpc-budgets.md) applies to browser contracts.
IcyDB already projects schema-owned policy through Wire generation and checks
configuration mutations. This is not full runtime-boundary qualification:
below/exact/above cases for every request/reply resource and an exercised
intentional asymmetry remain unqualified by the current browser harness.
Native allocation budgets are not browser heap guarantees.

### Earlier runtime qualification (0.51.0 / 0.31.0)

The following evidence belongs to the older selections stated below; it does
not qualify the current patch releases.

IC Memory/Storage 0.51.0 were selected together from published archives at
`e3939cd1b33a4c6839ff3a793b42c61a977f8f16`. Their library Rust sources match
0.50.17; collection formats, allocation leases and retained data are unchanged.
The public facade now exposes Memory 0.51 declaration/authority/pool identities,
so consumers exchanging those types must align their direct Memory dependency.
IcyDB does not use Persistence's stable Metrics adapter; no storage reset is
required by this adoption.

Combined Memory/Storage 0.51 and Timers 0.31 qualification passes seventeen
native memory/admission/lifecycle tests and all three installed lifecycle cases.
Strict facade/integration and four actor Wasm Clippy selections pass. The actual
Rust 1.91 generated endpoint consumer builds for Wasm with SQL/migration/metrics;
its external packages match the selected workspace lock. Graph invariants pass.
Testkit owns local PocketIC startup/cleanup. Init uses 5,773,664 instructions;
empty upgrade uses 3,414,275 and populated/converged upgrades use 3,421,834,
below the unchanged 12,750,000 ceiling. Stable extents remain 23,134,208 bytes.
The first installed attempt supplied the launcher instead of its admitted server;
correcting the existing check handoff resolves that pre-startup failure. These
are Linux correctness/current-cost observations, not matched savings. Native
macOS, browser and broad release gates were not run; Wasm-size, cycle and
instruction deltas remain unmeasured.

IC Timers 0.31.0 is selected from the published archive at
`c7ff54cbcee9c2b802f461b7a61f478d18699df3`. Its library Rust sources match
0.30.13; its public arithmetic identity is Metrics 0.16. The Canic adapter is
pinned to committed `06fd517254cc31531f165c63216b5a02c24ceebd`, which admits
Timers 0.31 while preserving the existing facade binding and lifecycle API.
It retains the Platform-based adapter at `550a92efd4adc3e09151bf4e3da771892e5002cf`;
the newer unfinished bootstrap API is excluded. Platform owns raw system calls,
the executor and bounded external management entropy; direct adapter CDK and
management-helper dependencies are removed. The selected graph has one timer
runtime and one Metrics arithmetic identity. This resolves IcyDB's recurrence
of [Canic #581](https://github.com/dragginzgame/canic/issues/581); integration of
the [isolated adapter contribution](https://github.com/dragginzgame/canic/pull/629)
and wider Canic publication remain upstream.

IC Tooling 0.68.6 is selected from published archives at
`dede95dd1d6ea1cb79367ecd3957233fcf3a2c81`; its
[migration guide](https://github.com/dragginzgame/ic-tooling/blob/dede95dd1d6ea1cb79367ecd3957233fcf3a2c81/docs/changelog/0.68.md)
owns API changes. Native executable acquisition now requires Cargo's successful
`build-finished` event as well as the artifact and process success. Host process
capture retries interrupted reads and rejects impossible read counts. IcyDB
retains its existing Build/Testkit interfaces without a local acquisition path.

IC Wire/derive 0.7.15 is selected from published archives at
`47f011702edd0fce99d4272fd80b02991ee5f94f`. IcyDB already supplies the mandatory
registry diagnostic callback and explicit bulk acknowledgements, rejecting
unclean diagnostics. The generator additionally rejects duplicate numeric method
identities. Valid frame bytes, endpoint identities and product limits remain
unchanged; explicit persisted database codecs remain separate. Unpublished
sibling changes are not consumer dependencies.

Current Linux qualification passes 236 CLI/build tests (one expensive artifact
contract remains explicitly ignored), strict CLI/build Clippy, Rust 1.91 facade
Wasm compilation with SQL/migration/metrics, dependency pins, declaration and
graph boundaries, and documentation checks. Generated TypeScript parity passes
90 native frames across twelve endpoints, 3,108 truncations and malformed/value/
limit controls. This Node check does not qualify installed Chromium or transport.
Native macOS, exact Rust 1.96 host-floor, installed-IC/browser execution and broad
release gates are separate obligations. Wasm-size/cycle/instruction deltas are
unmeasured.

The recorded 2026-10-24 Linux 0.67.0 adoption checks passed 233 CLI/build tests, strict
selected host/build/integration linting, public Rust 1.91 SQL/migration Wasm,
dependency checks, offline tool admission and formatting/release fixtures.
Installed startup and Chromium qualification passed 15 calls across twelve
methods, 90 native/browser frames and 3,111 truncations plus malformed/limit
controls. That graph included Memory/Storage 0.50.15 and Timers 0.30.11.
These are retained adoption results, not a fresh full gate for later changes.

| Consumer obligation | Maintained proof owner |
| --- | --- |
| Setup ordering and Rust tool selection | `scripts/ci/test-workstation-setup.sh`, `scripts/ci/test-shared-tooling-adapters.sh` |
| Dependency declarations and grouped release metadata | `scripts/ci/test-cargo-metadata-adoption.sh` |
| Testkit CLI/server integration | `scripts/ci/test-testkit-tooling.sh`, `scripts/ci/test-pocketic-server-wrapper.sh` |
| Formatting-hook preservation | `scripts/ci/test-pre-commit.sh` |
| Release receipts and publication | Release/publication fixtures selected in `check-portable-automation` |
| Failure evidence and fixture completion | `scripts/ci/test-tooling-fixture-completion.sh` and validation-adapter fixtures |

Shared owns generic installer and dependency-check regression matrices. IcyDB
qualifies its actual selections and consumer behavior rather than duplicating
those producer matrices. Substitute fixtures do not qualify installed tools.

## Structural Rust scanner source

The root snapshot owns the Syn-based panic scanner and its semantic fixtures.
[The private source-check package](../../testing/source-checks/Cargo.toml)
compiles that source under the root workspace/lockfile, with no database or
simulator dependency. IcyDB owns required source roots and nonempty inventories;
Shared owns Rust parsing, cfg/macro handling and scanner regressions.

## Consumer-owned instruction measurement

IcyDB selects published IC Metrics 0.16.0 at
`c0f033a8f41c280fd0fc77b2ec934d2f2b8176ed`. Its Rust arithmetic sources match
0.15.8, but the public `MeasurementAggregate` conversion now uses the 0.16 type
identity. IcyDB owns report projection and Wire shape; it does not use the stable
adapter extracted to Persistence. Timers 0.31.0 also selects Metrics 0.16.0,
so the resolved graph contains one arithmetic crate identity.

Metrics 0.16 qualification passes eleven metrics-state tests and fifteen host
reporting tests, strict Core/build Clippy and Rust 1.91 Wasm compilation with
metrics enabled. These are Linux correctness checks, not IC cost measurements;
native macOS and installed-IC execution were not rerun.

[The runtime reader](../../crates/icydb-core/src/runtime.rs) reads IC performance
counter 1 through Platform. IC Metrics owns arithmetic; IcyDB owns execution
spans, phase/entity attribution, reset semantics and journal-debt reporting.
Native zero counters and passing native tests are not IC cost measurements.
Use matched source, dependency, compiler, feature and artifact identities for
raw Wasm bytes, IC instructions and cycles. Raw Wasm takes priority over gzip;
wall-clock/native timing is not a supported performance metric.

## Audit and verification ownership

[Shared methods](../../audits/README.md) own audit procedure;
[IcyDB's catalog](../audits/README.md) selects product scopes and evidence.
[Archived local methods](../audits/archive/shared-adoption/README.md) and old
reports retain their original identities; adoption does not rescore them or
create an automatic broad gate. GitHub issues own follow-up tracking.

[Shared verification helpers](../verification-helpers.md) own Markdown navigation,
release-command smoke mechanics, registry observation and portable digests.
IcyDB retains document selection, product facts, release receipts/publication
policy and artifact subjects. An absent registry version and an unavailable
registry are distinct observations; neither authorizes publication.

## Host qualification

The [CI workflow](../../.github/workflows/ci.yml) declares Ubuntu 24.04 x86-64
and native macOS 15 ARM64/x86-64 lanes. Bash 3.2 remains the portable shell target.
The workflow and package manifests own compiler selections; Linux compilation,
cross-compilation, archive hashes and substitute fixtures do not qualify macOS.

The latest recorded Tooling adoption above has Linux evidence. Native macOS,
exact Rust 1.96 host-floor execution and installed-IC qualification were unrun
for that adoption; broad workspace/release gates were also unrun. Earlier
macOS successes in the committed history apply only to their named source and
graph. Verify the relevant CI run against the actual candidate before claiming
current native-host qualification. Local PocketIC execution is distinct from
live-IC deployment. Wasm-size, cycle and instruction deltas remain unmeasured.

## Standard releases and extraction

[The common release contract](../releases.md) owns execution and recovery.
IcyDB callbacks own exact metadata, dependency selections and source/tag-bound
receipts. Release preparation fetches locked dependencies before offline gates.
The shared runner owns version/staging/commit/tag/push effects; ordinary coding
and cleanup do not authorize them. Publication, deployment and cleanup remain
separately authorized. The local broad-gate exception in
[the operating manual](agent-operating-manual.md#workflow) remains in force.
