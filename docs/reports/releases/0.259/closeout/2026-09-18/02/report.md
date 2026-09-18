# 0.259 independent closeout audit

Verdict: **FAIL**. The maintained rehearsal passes, but restarting its successor
in `Prepared` leaves the startup watchdog repeatedly running without advancing
the migration. Correct that boundary before closing the line. No implementation,
release metadata or prior report was changed by this audit.

## Scope and method

- Method: `MIGRATION-STARTUP-CLOSEOUT-1 / DOMAIN-1`, explicitly bounded release
  investigation under `docs/audits/README.md`.
- Baseline: [qualification run 01](../01/report.md). Non-comparable as an overall
  verdict: this run independently follows the changed startup handoff through
  the distinct `Prepared` success/defer branch. The two maintained tests and
  exact actor bytes remain comparable anchors; no whole-system verdict follows.
- Snapshot: HEAD `fb69db0329535554b0131f5cc77a7286217e2ae9` plus the dirty 0.259
  implementation. All five implementation/test file SHA-256 identities, fixture
  schema, fixture actor and Cargo.lock match run 01. The five-file binary diff
  SHA-256 remains `75403d21ea9dfa1320d815013555907208caa807dc2bded45de82af6de5ea4c3`.
- Trigger: explicit user-requested closeout after M1–M3; changes to ordinary
  migration admission, typed startup failure classification and watchdog stopping.
- Owners: core schema application/preflight, facade generated-schema admission,
  startup receipt observation/driver, generated watchdog, retained-artifact
  integration helper and the migration guide.
- Obligations: preserve accepted authority and typed rejection, explicit command
  ownership, quiescent waiting after restart, retained current-format artifacts,
  successful control/interruption and pre-rewrite rejection/abort evidence,
  proportionate implementation and accurate documentation/cost claims.
- Exclusions: production applications/data, all-phase crash fuzzing, broad codec
  or dependency audits, large-data scaling, multi-page findings and full suites.
  These are not required to reproduce this specific handoff defect. Concurrent
  moves of 0.251–0.256 designs into `archive/` are not migration implementation;
  only their direct impact on links in the reviewed documents is noted below.

## Findings

### F1 — MEDIUM: Prepared restart busy-retries the startup watchdog

Owner: generated startup handoff and canonical generated-schema deferral.

The new stop branch in
`crates/icydb-model/src/build/actor/db/store.rs:341` handles only
`SCHEMA_MIGRATION_IN_PROGRESS`. An exact durable `Prepared` record instead returns
`Ok(true)` from
`crates/icydb-core/src/db/schema/application.rs:934`; the facade translates this
deferral to `Ok(())` in `crates/icydb/src/db/session/catalog.rs:69`.
No terminal generated-schema application receipt exists, so startup observation
remains `Recovering`. The handoff returns `Ok(false)` at store.rs:363, which the
watchdog converts to `ContinueImmediately`. Repetition cannot produce the missing
receipt: only explicit migration commands own progress.

A fresh isolated PocketIC probe reproduced this against the exact run-01 actors:

1. Install source, deliver startup and seed through `icydb_fixtures_load`.
2. Upgrade to the successor, observe `Idle`, issue one identity-bound `Advance`
   and assert `Prepared`.
3. Upgrade the identical successor again, deliver startup and assert exact status
   preservation.
4. Drain queued work with a two-second simulated advance and eight ticks. Observe
   subnet updates and canister cycles, then repeat that bounded idle window.
5. Require no new updates and unchanged migration status, without issuing another
   controller command.

Status remained identical, but update transactions rose **197 → 221** and the
canister lost **283,437,277 cycles** in the measured window. The quiescence
assertion failed: 0 passed, 1 failed, 0 ignored. This is a whole-window charge,
not migration-body instructions, a per-second estimate or a wall-clock metric.

Present consequence: unnecessary repeated update execution and cycle spending
while an operator legitimately pauses after preparation and restarts the actor.
No row corruption or premature publication was observed. This distinct branch
predates the change; the candidate's watchdog correction does not cover it.
It contradicts the candidate's unconditional no-busy-retry claim. The maintained
interruption test enters `Validating`, so it exercises the error/stop branch,
not this success/defer branch.

Disposition: fix before closeout. Converge the existing validated deferral and
watchdog handoff; do not loosen readiness, skip authority checks or add another
background runner. Add one focused Prepared-restart regression alongside the
existing idle/Validating checks. Update active completion claims after the fix
and requalification; preserve prior report 01 as historical evidence.

### F2 — LOW: archive moves break two active idea links

The checked documents currently contain two missing local targets:

- `docs/design/ideas/streaming-materialization.md` links to
  `../0.255-owned-value-handoff/0.255-status.md`.
- `docs/design/ideas/entity-lifecycle-and-relation-ddl.md` links to
  `../0.253-nested-relations/0.253-design.md`.

Owner: the concurrent design-archive housekeeping. The documents exist under
`docs/design/archive/`, but these incoming references still use their old paths.
Disposition: update the incoming links when finishing those moves; no aliases or
compatibility paths. This does not affect migration execution. No claim is made
that these are the only broken links across the entire archive relocation.

## Verification

Rust 1.98.1, PocketIC 16.0.0 and the package features/profile/lockfile recorded in
run 01. Cargo environment: repository `.cache/cargo/icydb`, `target/icydb`,
`TMPDIR=.cache`; PocketIC binary `.cache/pocket-ic-server-16.0.0/pocket-ic`.
PocketIC ran outside the sandbox on disposable instances, with local servers on
ports 42069 (probe) and 38389 (maintained target). No application network changed.

| Outcome | Selection/check | Evidence |
| --- | --- | --- |
| PASS | `cargo test --locked -p icydb-testing-integration --test schema_migration_closeout -- --list` | Both required tests listed before execution. |
| PASS | Same target with `-- --nocapture` | 2 passed, 0 failed, 0 ignored: control/interrupted success and rejection/abort. Assertions inspected, not inferred from names. |
| FAIL | Temporary probe `prepared_restart_is_quiescent --exact --nocapture` | Listed first using the same exact selector and `--list`; 1 selected, 0 passed, 1 failed, 0 ignored. Reproduction and observations above. |
| FAIL | Initial temporary-probe compilation | Selecting rlibs by recency mixed Cargo feature graphs. No test executed. Resolved using the current integration package's Cargo artifact output, not a product change. |
| PASS | Corrected temporary-probe compilation and listing | `rustc --edition=2024 --test` against current integration dependencies; no maintained source/test or manifest edit. |
| PASS | `cargo fmt --all --check`; `git diff --check` | Non-mutating formatting/whitespace verification. |
| FAIL | Local Markdown target check in eight affected documents | 276 links checked, two missing targets recorded as F2. Anchors/external URLs not checked. |

The temporary harness is `/tmp/icydb-259-audit-j5mTBz/prepared_probe.rs`; the
report records its full scenario and material output rather than maintaining
a second migration runner. Its helpers reuse the existing retained build and
fixture lifecycle owners. Full repository tests remain user-owned. Clippy and
owner-local unit checks were not rerun: no implementation changed in this audit;
the earlier executions remain attributed to their original runs.

## Cost and complexity

Fresh actor builds resolve to exactly run 01's retained bytes and BLAKE3 hashes:
source **8,767,079 bytes**, successor **8,803,709 bytes**, zero raw-Wasm delta.
The maintained tests reproduce all run-01 command cycle observations exactly.
Instruction counts and optimisation deltas remain unmeasured. F1's idle-window
cycles are separate evidence and must not be added to those command totals.

The implementation remains small in production scope: five Rust files,
approximately +258 net lines, primarily qualification coverage; three production
files account for +41 net lines including tests/comments. No new public mode,
format, authority or scheduler was introduced. F1 is an unconverged existing
handoff, not a reason to build another framework. This audit adds only this
report to the repository; runtime complexity and artifact size are unchanged.

Next: obtain implementation approval for the narrow handoff correction and its
regression proof, finish the archive link housekeeping, then requalify closeout.
