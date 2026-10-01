# 0.262 unused-surface closeout audit

Date: 2026-10-01. Method: `UNUSED-SURFACE-1`, a repository-wide static
inventory and consumer-tracing investigation requested before closing 0.262.
Verdict: **PASS WITH FINDINGS** for the inspected inventory. Three cleanup
findings remain open; this is not a whole-system correctness or release verdict.

## Scope and source identity

The audit covers workspace manifests and targets, Rust module wiring and local
consumers, compile fixtures, regression inputs, canisters, build/deployment
scripts, feature declarations, maintained documentation and non-Rust inputs.
Suspected leftovers were checked against generated-code use, conditional builds,
test harnesses and Git history before being classified.

Source: `aa9cd9839ce2824d37054a161e8e2a1d7b4468cc` (`v0.262.1`), including
the existing uncommitted C1–C3 implementation and lifecycle tests. Cargo remains
0.262.1; active release notes are 0.262.2. Snapshot hashes are recorded in
[findings.json](findings.json). Report output is excluded from that snapshot.

Two separately authorized documentation corrections were made before the audit:
[status](../../../../../../../docs/design/0.262-entity-creation/0.262-status.md)
now identifies the published .1 and active .2 notes, and the
[detailed changelog](../../../../../../../docs/changelog/0.262.md) describes
three Candid return arguments in the lifecycle probes.

An absent local caller alone does not establish that an exported library API is
unused by downstream applications. Historical reports and archived designs are
retained evidence. Formal domain-safety audits, runtime performance attribution,
downstream application builds and exhaustive behavioral validation are outside
this inventory's verdict. No findings were implemented during the audit.

## Inventory and reachability results

| Surface | Evidence and result |
| --- | --- |
| Workspace | Offline locked metadata accounts for 49 packages and 120 targets; no extra package manifest outside membership was identified. |
| Rust wiring | 1,664 Rust files: 1,589 connected through Cargo roots and module/include wiring; the remaining 75 are accounted for by compile-test globs and their included helpers. No unresolved module declaration or unpaired compile-fail snapshot was found. |
| Dependencies | `cargo-machete` 0.9.2 returned findings. Manual expansion/consumer tracing confirms the core `time` declaration is unused; generated Candid/CDK uses explain its other reports. An independent manifest scan finds one never-inherited workspace dependency alias. |
| Fixture ownership | One former E2E/admin family has no behavioral consumers (F2); other compile-only fixture declarations remain intentional test inputs. |
| Regression persistence | Both committed proptest seed files refer to deleted owning sources (F3); current proptest owners do not load those paths. |
| Canisters | All 25 canister packages have maintained owners: 24 inventory policies, plus the logical-memory actor explicitly built by its integration harness. |
| Scripts | 44 of 45 shell/Perl scripts have build, CI, installation, test or documented routes. The remaining manual `scripts/dev/cloc.sh` utility is not established as abandoned. |
| Features and formats | No unconsumed feature forwarding or retired decoder route was confirmed. Persisted-format policy and dependency/deployment inventory checks pass. |
| Maintained docs | The standard gate checks 225 references across 36 documents. Extended navigation checks cover 146 maintained Markdown documents with no missing local target. Historical narrative is excluded from current-navigation enforcement. |
| Other inputs | Logo, current Candid contract, SQL expected-output fixture and measurement workload JSON all have consumers. No additional orphan input was confirmed. |

The module traversal establishes source wiring, not that every function executes.
Lexical low-reference candidates were checked where ownership was uncertain;
trait implementations, macro token bodies and maintained public APIs were not
classified as dead solely from textual reference counts. Conditional SQL and
host-only model helpers also have maintained configurations.

## Findings

### 262-US-001 — Unused dependency declarations

Risk: **LOW**. Owner: package/workspace manifests. Disposition: **OPEN**;
remove the obsolete edges in a bounded manifest cleanup.

[icydb-core/Cargo.toml](../../../../../../../crates/icydb-core/Cargo.toml)
declares a direct `time` dependency, but core timestamp code reexports
`icydb_schema::Timestamp`; its remaining time imports are `std::time`.
External `time` operations are owned by
[icydb-schema](../../../../../../../crates/icydb-schema/src/time_atoms.rs)
and its date implementation. Keep that dependency and the workspace pin.

[Cargo.toml](../../../../../../../Cargo.toml) also declares the workspace
dependency alias `icydb-testing-test-fixtures`, which no member inherits.
Its crate remains a workspace member with independent tests; removing the alias
does not authorize deleting the crate.

These declarations obscure dependency ownership. They are not evidence of a
runtime defect or a promised Wasm reduction: `time` remains transitively needed.
Follow-up proof should check the affected core feature builds and unchanged
workspace metadata without changing package versions.

### 262-US-002 — Former E2E/admin fixtures lack behavioral consumers

Risk: **MEDIUM**, for misleading test ownership and redundant fixture upkeep.
Owner: `schema/test/fixtures`. Disposition: **OPEN**; retire the family after
preserving any unique compile-only contract in its maintained test owner.

The four files under
[e2e](../../../../../../../schema/test/fixtures/src/e2e/mod.rs) and
[macro_admin.rs](../../../../../../../schema/test/fixtures/src/macro_admin.rs)
contain 632 lines and 35 public declaration structs. There are no workspace
dependents or maintained E2E/admin behavioral callers. The admin fixture's
description still claims admin-interface testing without executing such tests.

These modules are compiled and their derives contribute schema registrations;
they are not entirely unreachable inputs. Their remaining compile coverage
must be compared with the maintained macro fixtures before deletion. The
candidate cleanup is these five files and their two exports in
[lib.rs](../../../../../../../schema/test/fixtures/src/lib.rs), not the whole
fixture package or a guarantee that every declaration can be dropped unchanged.

Retain the package's three behavioral tests for collection dereferencing,
iteration and newtype operators, its meaningful compile fixtures, and the
shared schema module used by those fixtures. Focused fixture-package and
affected macro checks should validate any eventual removal.

### 262-US-003 — Saved pagination regressions no longer replay

Risk: **MEDIUM**, for a concrete verification gap rather than a demonstrated
pagination defect. Owner: core executor regression coverage. Disposition:
**OPEN**; reconcile the minimized counterexamples with current tests, then
remove disconnected persistence files.

The committed
[pagination.txt](../../../../../../../crates/icydb-core/proptest-regressions/db/executor/tests/pagination.txt)
belongs to `src/db/executor/tests/pagination.rs`, deleted in `ae270682e`
(0.213.33). The nested
[range_edges_trace_matrix.txt](../../../../../../../crates/icydb-core/proptest-regressions/db/executor/tests/pagination/range_edges_trace_matrix.txt)
belongs to a source deleted in `212d5d881` (0.65.5).

Proptest's default `SourceParallel` persistence resolves a seed file from the
owning source path. Current tests neither retain those sources nor explicitly
load these seed paths; current proptest suites are in index envelope/key tests.
The stored minimized cases are 13 sequential codes with start/span seeds
143/3 and limit 1, and five sequential codes with seeds 0/0 and limit 1.

Current bounded pagination and range checks live in
[scalar_page_limits.rs](../../../../../../../crates/icydb-core/src/db/session/tests/cardinality_tiebreak/scalar_page_limits.rs).
Before deleting the seeds, determine whether these cases are represented there;
add a deterministic current-surface regression if necessary. Do not restore the
retired executor harness merely to replay its obsolete persistence paths.

## Retained surfaces and limits

- The fixture package is not unused as a whole: it still contains three
  behavioral tests and meaningful generated-schema compile inputs.
- Macro-expanded Candid dependencies in model-facade/logical-memory fixtures,
  and Candid/CDK dependencies in all four nested-relation actors, remain used.
  The bounded SQL actor also intentionally shares the regular SQL source root.
- Exported query/filter conveniences without repository callers remain public
  API; downstream use cannot be disproved by this checkout.
- Historical row layouts, current invalid-version rejection tests and retirement
  states protect maintained contracts. They are not obsolete decoder fallbacks.
- The older-numbered Candid contract directory is consumed through
  `include_str!`; its name is not evidence that its contents are unused.
- Archived designs and immutable audit reports remain historical evidence.

No unused production Rust module or compatibility decoder was confirmed.
This does not prove absence of all redundant code: generated expansion and
external consumers limit what static inventory can establish.

## Verification and closeout disposition

Passed during this audit: locked offline Cargo metadata, deployment inventory,
dependency-version graph, persisted-format policy, maintained documentation
references, extended module/fixture/navigation inventories and whitespace checks.
The dependency scanner exited 1 with findings, which were manually adjudicated
above; it did not report a clean dependency inventory.

No new compilation, behavioral suite, Clippy or Wasm build was needed for these
documentation edits and static findings. Full repository/workspace validation
remains user-owned. No local IC/PocketIC network was started or stopped. Raw
Wasm, IC cycle and instruction deltas are **unmeasured** for this audit; no
runtime code changed and no performance benefit is claimed.

This handoff changes two existing documentation files and adds this report and
its structured findings. It adds no runtime state, configuration or execution
route; implementation complexity is unchanged. The 632-line fixture footprint
is a removal candidate, not a measured performance delta.

Keep the unused-surface part of 0.262 closeout open for disposition of the three
findings. Cleanup should remain in the authorized 0.262 line, with focused proof
for preserved compiler contracts and pagination counterexamples. The audit
does not authorize deletions or amend the completed implementation tracker.
