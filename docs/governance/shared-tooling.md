# Shared Tooling Adoption

This document describes current ownership and qualification boundaries. Dependency
identities come from [Cargo.toml](../../Cargo.toml), [Cargo.lock](../../Cargo.lock)
and the [reviewed snapshot](../../.shared-tooling.snapshot), not historical notes.
[AGENTS.md](../../AGENTS.md) is IcyDB's local engineering overlay.

## Ownership and provenance

IcyDB selects Shared Tooling 0.26.14 at revision
`e14050bcc0c627f9c8a9064051699cac44fd7ab3`. The receipt and source version now
agree, resolving the previously recorded metadata discrepancy
([Shared #304 — Reconcile committed VERSION with the delivered snapshot](https://github.com/dragginzgame/shared-tooling/issues/304)).
The exporter owns the 125 selected payloads and their
hashes/modes. Refresh them canonically; a sibling checkout cannot change them.

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

Shared Tooling 0.26.14 passes Linux snapshot integrity, release-runner simulation,
IcyDB release/validation adapters, workstation orchestration and real formatting
preservation fixtures. Dependency boundaries and documentation checks also pass.
Release fixtures use disposable repositories or command substitutes; these checks
perform no release or delivery effects in the working repository.

IC Timers 0.30.12 is selected from the published archive at
`798f559a8e6c9043ae2b830a3a34e0c0252b7219`. Its library Rust sources are unchanged
from 0.30.11; the release updates upstream probe/development tooling. The
existing 0.30 manifest range and incoming lockfile selection are retained.

IC Tooling 0.68.0 is selected from published archives at
`8f281177faaeee0adc51edc1031d4a7b268b0ce0`; its
[migration guide](https://github.com/dragginzgame/ic-tooling/blob/8f281177faaeee0adc51edc1031d4a7b268b0ce0/docs/changelog/0.68.md)
owns API changes. Single and batch fixture builds share caller-selected Cargo
configuration limits: 1 MiB/file, 16 MiB total, 128 files and depth 32. Build owns
bounded discovery/refusal; IcyDB adds no alternate traversal or cache route.

Host and Build Rust implementation sources are unchanged from 0.67.0. Testkit's
changed optional `wire` feature is not enabled by IcyDB; native fixture protocols
use the separately selected workspace Wire dependency.

IC Wire/derive 0.7.2 is selected from published archives at
`a4da015251beb8fa3346fb3780c0b3dc85c0ef32`. The decoder admits bounded primitive
representations in one pass without reserializing decoded values. Collection
normalization is not canonical-byte proof; applications retain original frames
for exact retries, hashes or signatures and own semantic ordering/uniqueness.
The public error no longer has `Noncanonical`; malformed primitives return
`Malformed`. Encoder bytes, frame identities and IcyDB's configured limits are
unchanged. Explicit persisted database codecs are separate and unchanged.

Wire 0.7.2 qualification passes 38 focused native tests, strict selected
consumer Clippy, public Rust 1.91 Wasm compilation and generated TypeScript
parity for 90 frames across twelve endpoints, 3,111 truncations and malformed,
value and limit controls. This is local cross-language qualification, not
installed Chromium or IC execution; Wasm-size/cycle/instruction deltas are
unmeasured.

Current 0.68.0 Linux qualification passes 166 CLI and 21 build tests, strict
CLI/build/integration library and test Clippy, public Rust 1.91 SQL/migration
Wasm compilation, declaration/graph boundaries and Testkit CLI-selection
fixtures. The CLI-selection fixture uses a substitute executable; it does not
qualify the installed Testkit server. Native macOS, exact Rust 1.96 host-floor,
installed-IC/browser execution and full release gates were not rerun.

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
