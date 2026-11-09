# IcyDB 0.303 — native tooling contracts

The maintainer authorized this minor after delivered 0.302 passed its complete
release gate. No package versions or deployment are changed by implementation.

## Landing slice

1. Adopt published Testkit 0.93 and Host Tools 0.86 end-to-end: dependency
   declarations and lock, affected native/Wasm consumers, focused semantic
   controls, provenance and group/detail notes. Preserve Testkit ownership of
   infrastructure and the separate Canic qualification boundary (#373).
2. Complete #110's eligible index-range versus residual consumer qualification
   against the current graph and the exact released source, retaining new
   regression guards in the normal checkout and recording the issue closeout.
3. Adopt Shared's delivered pure-hashing policy and migrate #631's BLAKE3
   callers without changing their bytes. Qualify root consumers and retain
   explicit independent Canic admission blockers.

## Authorized simplification slices — 8 November 2026

The maintainer extended 0.303 to cover the ten audited simplification
opportunities, authorizing hard cuts where necessary. Keep the three existing
slices and their evidence; the explicit extension adds these independent
outcomes rather than combining them into one release-sized implementation.
Package versions and delivery remain unchanged. Each slice must preserve
features, accepted-schema authority and independent corruption/recovery checks.

4. Consolidate SQL coverage declarations and projections. Preserve provider
   registration, reference-oracle eligibility and meaningful interaction checks.
5. Reuse accepted-schema test fixture setup and table-drive equivalent cases;
   retain installed, recovery, budget and rejection boundary coverage.
6. Reuse existing release-validation mechanics and stage ownership, preserving
   fail-closed qualification, receipts and archive custody (#603). Real broad
   qualification remains separately selected.
7. Converge eligible covering projections on the existing scalar pager, retaining
   hidden slots, liveness, progress and authenticated continuation (#345).
8. Consolidate schema-store catalog selection/maintenance mechanics while keeping
   canonical, materialized and durable authority explicit.
9. Share accepted SQL mutation preparation and RETURNING orchestration without
   merging operation-specific update/delete rules or atomicity contracts.
10. Reduce repetitive typed error-to-diagnostic projection using existing owners;
    retain stable codes, causal chains and host-only CLI prose.
11. Consolidate equivalent grouped page finalization; preserve specialized
    ingestion and independently justified physical routes.
12. Separate mutable heap identity high-water state from immutable accepted
    checkpoints through one journal/recovery owner (#102). Define the retained
    format cut and same-contract recovery before implementation.
13. Share identical entity/index-term name predicates between authoring and
    runtime construction; retain span diagnostics and persisted decoding checks.

These are accepted outcomes, not permission to introduce generic frameworks or
to delete distinct semantic coverage. Resolve each candidate against current
callers; record retained specialization where convergence has no demonstrated
simplification. Progress and qualification for these slices are recorded below.

## Four-outcome continuation — 8 November 2026

The maintainer explicitly selected all four refreshed audit priorities. Continue
slices 5 (ordinary schema-test fixture reuse), 4 (one generated-witness metadata
owner), 12 (separate identity records) and 7 (covering materialization through the
existing pager), in that implementation order. Keep the existing slice numbers
and prior work. The two runtime changes require direct recovery/cursor controls;
the authorized hard cut must reject prior images without modifying retained
bytes. Broad release/native-host gates and publication remain separately owned.

### Identity checkpoint cut

Keep one immutable candidate checkpoint and one bounded identity-only record per
live-schema store in separate views of the existing control tree. The identity
record carries the exact accepted root, store binding and the existing active/
retired inventory. Normal commit preparation and retained range verification
take the root from the accepted SchemaStore; they never reopen the candidate
bundle. This removes work proportional to immutable schema size, while retaining
the existing bounded inventory algorithm rather than adding per-owner allocation,
discovery, retirement and compaction machinery.

Schema publication preflights and encodes both records before either write. Both
writes occur synchronously in the existing marker-owned preflighted apply phase;
no yield occurs between them. Existing fatal apply-contradiction handling and IC
message rollback remain authoritative. A retained marker therefore resumes before
the publication or recognizes the complete after-state. A torn, mismatched or
missing pair is corruption, not a recovery alternative or initialization default.
Same-contract tests cover marker interruption/reopen, replay, retired owners,
root mismatches and unchanged candidate bytes across identity advances.

Change the database boot, control-header and immutable-checkpoint magic identities,
retaining V1 as the sole contract. Earlier images refuse without mutation; no
reader, converter, reset or data deletion is introduced. The control-tree record
ceiling accounts for two records per store. IC costs and Wasm deltas must remain
unmeasured until their actual permitted measurement is selected.

## Status

### Slice 14 — bounded typed bulk writes (#323)

The maintainer selected #323 and deferred Canic. Add one explicit multi-commit
terminal over the existing atomic typed batch; keep the atomic API unchanged.
Caller limits bound rows, canonical staged/result bytes per batch, total attempted
rows (including refusals) and successful commits. The canonical writer continues
to own encoding, constraints, identity allocation, journal admission and commit.
Only its byte refusal before the commit handoff permits halving an attempted
batch. Never infer retry authority from a general public diagnostic or retry a
constraint, work-budget, backlog or publication error. A single oversized input
stops. Preserve confirmed prefix results, original positions and batch boundaries;
callers own authorization, application revisions and cross-message resumption.

The existing atomic terminal alone forces Toko to recreate sizing and splitting.
The mutation-job prefix packer is update-specific and does not provide typed
insert/result admission. Reuse the atomic owner instead of adding a persistent
job, a second encoding estimator or another mutation executor. The sole new
execution surface is explicitly multi-commit; no persisted format or query axis
changes. Qualify exact/+1 limits, no-effect refusal, partial progress, assigned
keys/order, no retries on unrelated failures, and bounded attempted work.

The adoption slice is complete and locally ready. No IcyDB caller uses the
removed CBOR tuple or Platform-owned Tools decode-limit type. Strict native
CLI/build-helper/facade all-target Clippy passes with testing enabled; Wasm
facade compilation passes with SQL, migration, metrics and testing enabled.
All 22 build-helper tests, two startup semantics controls and invalid-budget
constructor admission pass. All 69 CLI process controls pass on the diagnostic
rerun. The initial run had two migration exit failures without captured Output;
preserve that evidence under #599 and include phase/full Output in future
assertion failures. The isolated advance control also passes. This is not a
root-cause diagnosis or a retry mechanism.

Additional #599 qualification: eight bounded concurrent migration cohorts pass
(96 executions), stopping on any failure. No failure reproduced. Extend full
Output diagnostics across exit, stderr, UTF-8, phase and rejection-content
assertions in the existing outcome helper; strict process-target Clippy passes.
Keep the original assertions and fixture retention, without retries or sleeps.
Cancellation controls also retain the selected signal, readiness/completion
facts and full child Output when readiness or outcome assertions fail. Existing
handshake/watchdog, one-call and argument-file cleanup assertions are retained.

Additional adoption acceptance: all 184 CLI binary unit tests and all 15
`write_wasm_size_report` tests pass against Host Tools 0.86 / Testkit 0.93.
No new failure or source correction was needed in these targets. These report
tests qualify behavior, not a new Wasm-size or IC-cost measurement.

The root graph has one identity per ic-* package; declaration and boundary
checks pass. Canic's isolated lock is unchanged. Pending notes select database
0.303.0 and CLI 0.296.5; product package versions remain 0.302.0 and 0.296.4.
No commit, push, publication or network lifecycle action occurred. Broad gates,
macOS and installed-IC qualification were not selected. IC cycles/instructions
and uncompressed total/code-section Wasm-size deltas are unmeasured.

## Delivered-issue closeout audit

#630 is closed as completed on 8 November 2026: source delivery, full local
release gates and matching hosted CI 42395222473 are complete/success for
database 0.302.0 / CLI 0.296.4. All eight jobs pass, including both macOS hosts.
#110 is closed as completed after the additional released-source qualification
below.
#599 retains historical root cause; #315 retains installed startup acceptance;
#322 retains consumer adoption and measured size evidence. #631's local closeout
below explicitly records the maintainer's Canic deferral and pending delivery.

Latest matching CI observation (8 November): all eight jobs completed
successfully, including workspace, static, Core, Tier A, browser, both macOS
hosts and the final check. This qualifies the delivered source, not the pending
0.303 dependency/diagnostic edits.

## Numeric index acceptance — 8 November 2026

The stored-SQL Nat128 regression qualifies physical index ranges versus
full-scan CASE expressions across `2^127`, `u128::MAX`, duplicate boundary keys,
all six comparisons and BETWEEN/NOT BETWEEN, with explicit expected IDs and
repeated execution. Explain proves route selection. A boolean identity CASE
was extracted into an index predicate; use a value-producing CASE control
instead. Reuse the existing typed numeric index fixture for Float64 and Nat128.

All 19 SQL scalar-comparison tests pass on the preserved incoming lock selecting
Storage 0.68.8 / Platform 0.25.1. Strict Core SQL/migration library/test Clippy
passes after correcting the fixture helper's similar binding names. The same
19 tests pass against a disposable regular-file snapshot of released commit
`17b6110c0507260302a2acaf2cc5e4d67d1b2ab1`, with only the test file overlaid;
its original lock hash remains
`ca03982c676092eb0b9533ad4ab93f965e3d57ccebd0de389eaaac035cdb0eb4`.
The delivered runtime and dependency graph are unchanged in that qualification.

This completes #110's remaining acceptance with the already delivered 0.302
implementation and successful eight-job CI. The new guards remain local for
0.303; no archive or hosted execution of those new guards is claimed. Additional
Rust delta: tests +83, production +0. No performance measurement, broad gate,
installed-IC execution, package-version edit, commit or push occurred here.

## Direct BLAKE3 ownership — 8 November 2026

Adopt Shared 0.31.6 at `84b52d14a9082eca0c43db807f649ddf8a74f02c` through
its canonical exporter after impact review. All 161 payloads verify. The
delivered Shared #364 policy/inventory permits these existing purposes and
guards known unapproved hashing packages. The 33 focused hashing boundary
cases pass from a disposable companion-complete copy under this repo's target.

Core, SQL generator and SQLite reference inherit BLAKE3 directly; remove their
Platform facade edges and root catalog entry. Restrict direct BLAKE3 ownership
to those three root packages and prohibit reintroducing root facade edges.
The before/after metadata selects exactly the same registry BLAKE3 1.8.7
identity and `std` feature. Core's normal and Wasm graphs exclude the facade
and retain Runtime/Types. All 20 Rust file changes only remove the facade import;
the hashing calls, input encodings/domains, budgets, digest lengths and XXH3
remain byte-for-byte unchanged. The Cargo-adoption fixture now copies the
boundary checker's companion closure, including the new hash inventory.

Before migration, all 17 fingerprint, 45 generator and 11 reference tests pass.
After migration those same tests and three cache-identity controls pass (76
Rust tests). Strict all-target Core/helper Clippy with SQL/migration, public
Wasm SQL/migration/metrics/testing compilation, root boundary/pin checks,
Cargo-adoption fixtures, ShellCheck and formatting pass on Rust 1.99.1.

Propagate direct BLAKE3 into the independent Canic root and integration fixture
imports, retaining genuine Platform PocketIC/principal capabilities. Both
before and after offline resolution initially failed selecting `thiserror`:
PocketIC 16.1.0 requires exactly 2.0.18 while the cache admitted 2.0.21. Follow-up
cache preparation fetched the already-locked thiserror/derive 2.0.18 packages;
that error clears without changing the lock. It was a missing offline input,
not proof of incompatible version requirements.

The prepared resolver now exposes the underlying singleton conflict: Platform
0.8.3 selects ic-cdk-executor 2.0.0, while Storage 0.64 selects Platform Runtime
0.17.1's guard with the same executor `links` value. Cargo refuses two owners.
Latest published Canic adapter remains 0.115.0; its published/current supplier
Wire line remains 0.16 against root IcyDB's 0.17. The independent lock remains
byte-identical; no pin, patch or alternate executor is introduced. Evidence is
under target/tmp/issue373-cache-admission. Qualification remains owned by #373;
#631 is closed for the completed root migration after the maintainer explicitly
accepted leaving Canic behind for now. Independent lock/build qualification is
deferred to #373; it is not declared complete. The source remains local and
uncommitted/unpushed for pending 0.303/CLI 0.296.5, so this closure is not a claim
of delivery or hosted qualification of the new changes. No broad/release/macOS/
installed gate, version change, commit or push ran; IC cost and Wasm-size deltas
remain unmeasured.

## Simplification progress — 8 November 2026

Slices 4, 5, 6, 8, 10 and 13 are implemented. Slice 6 preserves full fallback;
#603's fresh native full/selected qualification is not activated by local cleanup.
All package versions and incoming dependency/adoption work remain unchanged.

- Slice 4: coverage checks are root-owned in SQLite reference. All 106 features,
  100 providers and both generated schedules remain. Exemptions have one owner;
  required evidence/eligibility is inherited from interactions. Five stale test
  paths are corrected and the current artifact fingerprint is updated.
- Slice 5: ordinary field and sparse-store fixtures reuse their existing owner.
  SQL harness semantics are generator-owned; unit checks execute once in the
  root workspace, and installed integration consumes the helper. The dedicated
  Canic pure-harness lane and construction-only taxonomy tests are removed.
- Slice 6: four consumer file readers delegate bounded regular-file admission to
  Shared. Source reads have the common 64 MiB ceiling; receipt reads retain
  1 MiB and absolute durable paths. No new runner or observation module.
- Slice 8: durable publication and canonical folding call one exact catalog
  deletion routine; materialized overlays retain their distinct tombstone work.
- Slice 10: executor boundary-detail projection feeds the existing diagnostic
  registry instead of maintaining a second category match.
- Slice 13: Schema owns the identical entity/index-term text predicate. Keep
  source-span mapping, typed runtime errors and persisted decoding checks.

Slices 9 and 11 have been rechecked against their current owners. UPDATE/DELETE
already share shape classification, accepted-descriptor admission, candidate
collection and RETURNING validation. Their remaining branches carry different
assignment/default and atomicity semantics. Count and generic grouped folds
already share pagination/projection contracts; the count path intentionally
avoids generic candidate envelopes, while generic selection owns aggregate
ranking and compiled projection. Retain these boundaries for now: another
operation mode or universal candidate wrapper has no demonstrated simplification.
This is a no-build disposition, not a claim of measured cost equivalence.

Slices 7 (#345) and 12 (#102) remain pending. Covering pages require index-entry
values to survive transparent stream wrappers into the existing retained-slot
materializer; a guard-only change would lose cursor/progress correctness.
Identity checkpoint separation changes retained storage and must define marker
replay across partial publication before code changes. Hard cuts are authorized;
no data deletion, legacy fallback or unqualified format change is introduced.

Focused qualification passes: nine coverage checks, six relocated semantic
harness tests, 404 session fixture-family tests, 35 identity tests, five macro
compile/pass fixtures and 47 error tests. Strict selected Core/Schema/macros and
helper Clippy passes. Schema-store retention passes its 57 tests. Workflow lint,
129 actual workflow-policy fixtures, native Testkit caller controls and the
53-case consumer receipt/recovery fixture pass with substitute stage bodies.
The receipt fixture also passes against the final changed Make mapping, and the
actual consumer Make adapter projections pass with substitute Cargo/stage bodies.
No real native full/selected comparison or omission activation is claimed.

Task-attributed raw Rust lines, including comments and blanks and counting moves
on both sides: tests +7167 / -7463 / net -296; non-test +122 / -159 / net -37.
The before-image excludes the incoming adoption/diagnostic/Nat128 work. This is
source-line accounting, not functionality removed or a measured IC cost saving.
Final whitespace/source review passes; all changes remain local and uncommitted.
Broad workspace/release, macOS, installed-Canic and IC-cost/Wasm measurements
remain outside this selected development gate.

## Four-outcome implementation progress

The fixture and generated-witness continuations pass: 51 additional ordinary
field sites share the existing constructor, and all 42 generated metadata tuples
have one declaration owner. Direct qualification passes 431 Core fixture-family
cases, 51 generator cases and nine coverage checks, with strict affected lint.
Both scheduled-witness and coverage golden identities remain unchanged.

The identity split is implemented and passes strict Core SQL/migration lint,
189 selected checkpoint/control-tree/boot/commit-window/Identity/recovery cases,
and persisted-format plus mutation-atomicity checks. Three control-tree views
interleave safely. Current-image publication/reopen, exact/conflicting replay,
coalesced owners, retired-owner values, malformed/truncated frames, exact/cumulative
preparation budgets and missing/mismatched pairs retain typed refusal. Candidate
bytes remain unchanged through identity preparation and recovery advances. Prior
boot/control identities refuse without mutating retained bytes. No IC execution
or Wasm-size measurement is claimed. Covering-pager source convergence is implemented;
its final affected-family qualification is recorded below.

The incoming lock selections now include Runtime 0.20.1, Storage 0.68.10,
Wire 0.17.13 and Host Text 0.74.3. Preserve these unrelated patch selections;
fetch only their already-selected registry inputs to restore offline validation.
No package selection, version preparation, push or network lifecycle action is
owned by these simplification edits.

### Covering pager admission

The final retained-slot layout is resolved once against accepted schema authority.
A non-DISTINCT, direct-field, single-prefix/range plan with no residual filter and
no retained-value overrides may use recoverable index/PK/prefix-constant sources.
The existing index leaf retains only requested component payloads for this shape;
transparent wrappers preserve association with each yielded key. Composite and
lookahead streams refuse payload admission before consuming input. All scanning,
physical access bounds, lookahead, skip/keep accounting and cursor emission remain
in the existing scalar pager. Row presence remains authoritative, unsupported
payloads fall back to the canonical reader and corruption does not become fallback.
Expression, hybrid and composite covering readers remain justified specializations.

Initial direct qualification passed 212 existing projection/page/stream and
covering-plan cases. The direct regression controls pass hidden order/PK slots,
duplicate groups, both directions, live-page suffix replay, cold/warm SQL windows,
partial/sparse indexes, NULL completeness, row-presence refusal and no row-payload
reads for admitted pages. The nullable and partial fixtures have independent
fresh database identities; republishing a different initial candidate inside one
fixture correctly refused as corruption during test development. No runtime
recovery or compatibility exception was introduced to bypass that refusal.

### Four-outcome closeout

All four selected outcomes are implemented and locally qualified on 0.303.
Final strict Core SQL/migration library/test lint passes. All 534 affected session,
projection, stream, covering-plan and scalar-page tests pass, including durable
exhaustive pages with unchanged revision proofs and tampered-token refusal before
row reads. The initial exhaustive test used an inadmissible heap source; the final
control reuses the existing durable journaled fixture, preserving the durable-source
requirement. Targeted Wasm SQL/migration library compilation and the changed
executor/commit no-production-panics source check pass. Changed-document local
links and final whitespace review pass. Earlier fixture, witness, identity and
persisted-format/atomicity results above remain part of this batch's qualification.

Task-attributed Rust physical lines against the captured pre-edit dirty contents,
including comments/blanks and treating the declared fixture-only SQL packages as
tests: tests +1039 / -1211 / net -172; non-test +640 / -212 / net +428; combined
net +256. Incoming adoption and earlier 0.303 work are excluded. The storage and
pager contract separation adds runtime code; this batch reduces repeated fixture
and declaration code while preserving the supported features and specializations.

The selected four-outcome implementation has a ready closeout verdict for
user-owned broad validation. Full workspace/release and macOS qualification,
installed Canic/IC execution and actual IC cycle/instruction plus uncompressed
total/code-section Wasm-size deltas were not selected. #102/#345 delivery and
measurement follow-up remains with those existing GitHub issues; no issue is
closed merely on these local checks. No network lifecycle action, package-version
change, staging, commit, push or deployment is part of this batch.

## Requested tooling refresh — 8 November 2026

Adopt Shared 0.31.10 at `c5285791991c731d266fb5c85dc3ee876d29f6b9`
through the canonical revision-bound exporter, including its download-cache
action (162 payloads). Adopt published Build 0.79.0 and Testkit 0.93.1, and align
Host Text's declared floor with its already-locked 0.74.3 patch. Host Tools 0.86
and Host Artifacts/Fs/Process 0.75 were already current. Registry archive checksums
and clean source identities are recorded in the existing
[adoption provenance](../../governance/toko-tooling.md#current-0303-cohort-2026-11-08).
The only changed root lock versions are Build and Testkit. Preserve incoming
Runtime/Storage/Wire selections and the isolated Canic lock; no package version or
release boundary changes.

Build's specification owns the same target-maintenance policy in standalone and
batch execution; remove the retired batch configuration and consumer boolean
switch. Exact cache, configuration-refusal, post-link publication and dependency
admission controls pass: 22 build-library tests and 15 size-report tests. Strict
selected Build and native testing-facade lint pass. The public Testkit invalid-budget
control passes without a server. The installed-server/Canic acceptance remains
separate under #373; no network was started, stopped or restarted.

Actual Shared consumer checks pass formatting, crate/module/depth LOC selection
and unknown-module refusal, compiler admission (Rust/Cargo 1.99.1), dependency
boundaries, workflow invariants, workflow lint, local documentation links and
whitespace review. All 13 Rust setup lanes restore authenticated tool downloads
before preparation and save after setup. The main static CI job alone installs
Gitleaks and scans full history, with complete redaction; no release, publication
or matrix job repeats that scanner. Hosted cache/secret-scan execution and macOS
qualification await CI. No broad workspace or release gate was requested or run;
IC cycles/instructions and total/code-section Wasm-size deltas remain unmeasured.

## Issue acceptance continuation — 8 November 2026

Triaged the 12 current open issues, checked relevant bodies/discussions and open
work, and observed successful latest main CI (`42395222473`) plus native Tier C evidence
(`42435990300`) for delivered head `17b6110c0507260302a2acaf2cc5e4d67d1b2ab1`.
Those results do not cover the pending local 0.303 changes. The only open PR is
the separately owned memory-bootstrap handoff (#376); no overlapping pager PR
was found. Canic, installed-IC, consumer adoption and matched measurement owners
remain open rather than being inferred complete from native tests.

Complete #345's source-proof rejection regression gap using the existing durable
covering fixture: public/trusted and ASC/DESC missing-proof rejection, unchanged
resume, distinct data/access revision failures, and refusal to bind an old token
to a new proof. Zero-row allowances plus visited-key/row and data-read counters
prove rejection precedes traversal. All seven covering-page cases and strict Core
SQL/migration library/test lint pass. No production code changed. Pending group
and detail notes preserve #345 links. Updated [#345's evidence](https://github.com/dragginzgame/icydb/issues/345#issuecomment-6720501688)
and [#102's evidence](https://github.com/dragginzgame/icydb/issues/102#issuecomment-6720502258)
to separate completed local implementation from pending delivery/acceptance.

Task-only Rust physical delta against the captured pre-edit dirty test contents:
tests +125 / -1 / net +124; non-test +0 / -0 / net 0, including comments/blanks.
No full workspace/release, macOS or installed-IC checks ran. IC cycles/instructions
and total/code-section Wasm-size deltas remain unmeasured. No staging, commit,
push, package-version change, network lifecycle operation or issue closure.

## Shared 0.31.11 issue-closure adoption — 8 November 2026

Adopt reviewed GitHub commit `6c529e6940a97226cca826726491e03173b08cd2`
through the canonical exporter, retaining all 162 payloads and the 0.31.10
executable tooling unchanged. The four changed policy/maintenance documents make
local implementation, focused validation and the owning changelog link sufficient
for ordinary closure. Refresh the local baseline pin, provenance and group/detail
notes. Source-bound snapshot verification, 181 local documentation references and
whitespace checks pass; Cargo manifests/lock and product source are unchanged.
No new code tests, broad gates, delivery or network lifecycle work ran for closure.

Closed the two completed local implementation issues with honest delivery state:
[#102 — identity checkpoint split](https://github.com/dragginzgame/icydb/issues/102#issuecomment-6720618462)
and [#345 — covering scalar pages](https://github.com/dragginzgame/icydb/issues/345#issuecomment-6720617983).
Their ordinary source fixes and focused qualification are complete; the 0.303
changes remain uncommitted, unpushed and unpublished. Earlier comments' generic
delivery holds are superseded. No measured IC/Wasm saving is claimed.

Also closed the already-published IcyDB producer APIs:
[#315 — typed startup driver](https://github.com/dragginzgame/icydb/issues/315#issuecomment-6720618917)
and [#322 — whole-row mutation adapters](https://github.com/dragginzgame/icydb/issues/322#issuecomment-6720619381).
Existing producer checks and owning database changelog links remain present.
Toko's actual startup/adaptor migration and parser/helper deletion remain in its
existing #1796/#1803 consumer issues, with error convergence in #1794. IcyDB #373
retains composed endpoint/lifecycle qualification. Representative consumer rename,
install/upgrade and footprint acceptance are not inferred complete from producer
closure, and no Toko source was changed.

Eight IcyDB issues remain open for their own unfinished outcomes, including actual
full/selected qualification (#603), the historical CLI diagnosis (#599), composed
Canic integration (#373), guarded SQL (#335), bounded typed bulk writes (#323),
installed production-boundary qualification (#154), numeric IC measurements (#112)
and application-size attribution (#19). These remain tracked on GitHub; the former
generic release/consumer holds are not a second local queue or prerequisite for
closing another finished producer fix. The saved review transfer inventory remains
historical and unchanged. All prior delivery and measurement caveats above retain
their original source context; this section records the current closure decision.


## Slice 14 local implementation — bounded typed bulk writes

Implement #323 in the normal checkout on the pending database 0.303 line.
The public bulk terminal composes the existing atomic writer, with canonical
staged/result byte admission and a stage-qualified no-effect refusal. Its bounded
loop retains the reduced size, charges refused attempts, preserves ordered
assigned-key results and original positions, and returns confirmed prefix/commit
observations on failure. Existing atomic batches retain their behavior. Update
the durability guide and both owning database ledgers with the #323 outcome.

All 11 new cases and existing typed/result/generated-adapter regressions pass:
44 distinct focused native tests. Strict Core/facade/generated-fixture library
and test lint and SQL/migration Wasm facade compilation pass on Rust 1.99.1.
The first test run exposed two new fixture assumptions (reinitializing an active
fixture, and an eight-page helper for a 64-batch journal); correct those fixtures
and retain the failed and passing logs under `target/evidence/issue323/`.

The library implementation is locally ready. #323 retains its explicit installed
journal acceptance; native journal/backlog/interruption controls do not substitute
for that evidence. Canic remains deferred under #373, with no independent graph
or fixture edits. Actual IC cycles/instructions and total/code-section Wasm deltas
remain unmeasured; no saving or allocation benefit is claimed. Broad workspace/release and hosted/macOS gates were not run; installed-IC
acceptance remains deferred with Canic.
No staging, commit, push, version change, deployment, network lifecycle operation
or issue closure. Consumer authorization/revisions and Toko adoption remain with
their existing owners; the GitHub issue owns remaining acceptance.
