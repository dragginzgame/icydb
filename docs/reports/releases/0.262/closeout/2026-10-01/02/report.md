# 0.262 unused-surface audit — second pass

Date: 2026-10-01. Method: `UNUSED-SURFACE-1`, repository-wide static inventory
with deeper test-consumer, source-selector and declaration tracing.
Verdict: **PASS WITH FINDINGS** for this inventory. Three new findings remain
open. No unused production Rust module was confirmed; this is not a release or
whole-system correctness verdict.

## Scope and source identity

The user requested another audit after U1 resolved the
[first run's findings](../01/report.md). This run includes those approved
cleanup changes and the existing C1–C3 worktree changes over
`aa9cd9839ce2824d37054a161e8e2a1d7b4468cc` (`v0.262.1`). Cargo remains
0.262.1; active release notes are 0.262.2. Snapshot hashes and structured
findings are in [findings.json](findings.json).

Coverage includes all workspace packages and targets, retained Rust wiring,
test helpers and suppressions, conditional/generated surfaces, ignored manual
tests, source-scanning guards, dependency consumers, regression inputs and
measurement-contract declarations. Historical reports, archived designs and
generated build outputs are excluded from deletion classification. External
application use of public APIs cannot be disproved by local reference counts.

No cleanup was implemented during this run. No design, changelog, manifest or
runtime source was changed. Prior audit output remains immutable.

## Inventory results

| Surface | Result |
| --- | --- |
| Workspace | Locked offline metadata accounts for 49 packages and 120 targets. |
| Rust wiring | 1,659 Rust files under crates/testing/schema/canisters: 1,584 connected to Cargo/module wiring and 75 maintained compile inputs. Cargo roots are unchanged; the first run's module map was reconciled with the five U1 deletions. No unpaired compiler snapshot was found. |
| Local consumers | Low-reference hints were inspected for test-only, generated, conditional and external API ownership. Source-selector tracing found the inert clauses in 262-US-005. |
| Duplicate tests | One exact-body candidate pair was found: primitive-catalog tests in model and model-macros. They validate separate generated consumers of the shared schema registry and remain justified. |
| Dependencies | The previous unused core/time edge and fixture alias are gone. The scanner's remaining reports are macro-expanded Candid/CDK uses, already traced to their maintained generated owners. |
| Literal paths | Missing file-literal candidates were synthetic diagnostic provenance fixtures. An empty historical directory exposed why existence alone does not validate source selectors. |
| Invariants/docs | Deployment inventory, dependency graph, persisted-format policy and maintained documentation references pass; the documentation gate checks 225 references across 36 documents. |
| Prior findings | The obsolete fixture modules and seed files remain removed; compact index compile coverage and current pagination counterexamples remain present. |

Source wiring is not proof that every function executes. This run adds consumer
and semantic inspection to that inventory; it does not claim an exhaustive
compiler reachability proof for every generated or downstream configuration.

## Findings

### 262-US-004 — A storage-boundary test scans retired trait names

Risk: **LOW**, obsolete test maintenance. Owner: core cross-subsystem tests.
Disposition: **OPEN**; remove the retired-name test and its private helpers,
preserving the current child test modules and shared source-guard utilities.

[db/tests/mod.rs](../../../../../../../crates/icydb-core/src/db/tests/mod.rs)
constructs `ValueSurfaceEncode` and `ValueSurfaceDecode` by concatenating name
fragments, then checks that storage sources do not contain those strings.
The traits were renamed in `a16bd6326` (0.127.4); neither the old names nor their
then-successors are present as current trait declarations. The test passed
in this run, but checks names unavailable to current code.

Its two private helpers and one test occupy most of the 78-line file. Keep
`ic_update_model` and `persisted_format_corpus` module wiring. Keep
[source_guard.rs](../../../../../../../crates/icydb-core/src/db/test_support/source_guard.rs),
which has other maintained consumers. Any further storage-boundary proof must
refer to current interfaces or behavior rather than preserving an old-name ban.
Do not add aliases or revive the retired traits.

### 262-US-005 — Test/CI scanners retain inert selectors and exclusions

Risk: **LOW**, stale verification scaffolding. Owners: IC update-model source
guard and production executor panic gate. Disposition: **OPEN**; remove the
inert clauses and retain the current synchronous-update/panic policies.

[ic_update_model.rs](../../../../../../../crates/icydb-core/src/db/tests/ic_update_model.rs)
still selects `src/db/executor/delete/`. The last source there was removed in
`ae270682e` (0.213.33); the leftover local directory contains zero Rust files.
The other selectors select 16 commit files, three mutation files, one migration
execution file, six SQL write files and the session write owner. Current deletes
converge through those maintained write/mutation owners. This is a redundant
selector, not evidence that current delete execution escapes the other guards.

[check-executor-no-production-panics.sh](../../../../../../../scripts/ci/check-executor-no-production-panics.sh)
also exempts `cfg(feature = "executor-benchmarks")` blocks. That feature is
undeclared and this exemption is its only maintained source occurrence.
No current executor code needs that skip route. Remove the feature-specific
branch while retaining test-item exclusion and typed-error enforcement.

Both maintained policies passed in this run. Removing obsolete clauses should
not add scan modes, retain old feature spellings or remove actual update-model
checks. A later cleanup should verify maintained selectors consume source files
so another empty historical directory cannot silently remain in the list.

### 262-US-006 — The 0.223 contract mixes live limits with declaration-only history

Risk: **MEDIUM**, misleading coverage/measurement ownership and unnecessary
maintenance. Owner: integration mutation-job contract. Disposition: **OPEN**;
trim historical samples and self-only planning tables while retaining live
scale-test inputs, current runtime-limit proofs and behavioral scenarios.

[durable_mutation_job_contract.rs](../../../../../../../testing/integration/src/durable_mutation_job_contract.rs)
is 630 lines. Fifty-four public constants/types/tables have no consumer outside
that file. This is a screening count, not permission to delete all 54: some of
those declarations support current runtime-limit checks within the file.

Confirmed obsolete families include the frozen 0.222.4 source identity, historical
instruction/Wasm samples and growth-review values, the 0.223 authority inventory
with `action_patch`, and declaration-only operation/failpoint matrices. The
matrices are read only by tests that check IDs, counts, strings and outcome-enum
coverage; they never select executed operations or injected faults. Frozen
`CURRENT_DURABLE_*` sample comparisons evaluate constants, not measurements of
the current implementation. Their historical evidence belongs in existing
release reports rather than compiled current-contract vocabulary.

Preserve
[durable_mutation_job_scale.rs](../../../../../../../testing/integration/tests/durable_mutation_job_scale.rs),
which consumes fixture cardinality, Forward/Verify work bounds, the three active
instruction ceilings and minimum-advance helpers. It checks actual returned
receipts and instruction observations. Preserve current exported-bound checks,
format/marker constraints and maintained recovery/failpoint behavior tests.
Do not remove the whole contract module or relax active resource bounds merely
because older samples and planning tables are unused.

All four contract tests passed here. That confirms the declarations remain
compiled; it does not turn their table inspection into behavioral qualification
or their frozen samples into current performance evidence.

## Retained candidates and limits

- Compile-only generic helpers, generated endpoints and macro token bodies have
  maintained consumers even when lexical call counts are small.
- Ignored IC measurements remain manually owned probes; an ignored marker or
  old release number alone does not establish obsolescence.
- The Linux native-memory attribution probe explicitly owns host-failure
  diagnostics. It was not run or used as an IC performance metric; its lack of
  an automatic CI caller alone is insufficient to declare it dead.
- Current malformed-format tests, field omission/authority checks and public
  architecture compile guards retain maintained contracts. They were not
  classified as obsolete solely because they assert rejection or absence.
- Empty untracked local directories are not committed dead code and need no
  repository cleanup; retained string selectors into them are finding 005.

## Verification and handoff

Twenty focused native tests passed: sixteen core cross-subsystem/corpus tests
and four mutation-contract tests. Deployment, dependency-version and persisted
format invariants, the executor panic gate, metadata, documentation and diff
checks passed. The dependency scanner returned exit 1 for the remaining
generated-code false positives; it did not report a clean global inventory.

Full repository/workspace suites, downstream builds and mainnet checks were not
run. No Clippy rerun was needed because no source changed; U1's focused Clippy
evidence remains applicable to the same source snapshot. No IC/PocketIC network
was started or stopped. Wasm bytes, IC cycles and instruction deltas for this
audit are **unmeasured**; fixed values inspected in finding 006 are historical
declarations, not new measurements.

This run adds only two report files. Runtime complexity and state-space are
unchanged. Further cleanup requires the user's disposition of these findings;
the audit itself neither implements them nor changes the 0.262 tracker.
