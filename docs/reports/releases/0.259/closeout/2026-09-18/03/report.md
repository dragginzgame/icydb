# 0.259 closeout correction qualification

Verdict: **PASS** for the approved closeout corrections and maintained migration
rehearsal. Both [run-02 findings](../02/report.md) are resolved. Full repository
release validation and publication remain user-owned; no application deployment
or production-data qualification is implied.

## Scope and snapshot

- Method: `MIGRATION-STARTUP-CLOSEOUT-1 / DOMAIN-1`; focused requalification of
  Prepared restart waiting, unchanged migration authority, eventual publication
  and the two audited documentation links.
- Baseline: [run 02](../02/report.md), comparable startup behavior and artifact
  configuration. [Run 01](../01/report.md) retains the original three-scenario
  evidence. No prior report was edited.
- Source: HEAD `fb69db0329535554b0131f5cc77a7286217e2ae9` plus the dirty 0.259
  candidate. No Cargo versions, dependency versions or lockfile changes.
- Eight-file implementation/test binary diff SHA-256:
  `4b32e66a5377a5c6cb49efb2831a1f93f468ffe2e5f50e1918eee2a53ede37a1`.
  Computed with `git diff --binary --`, in order: core schema/application.rs,
  schema/mod.rs, session/catalog.rs, startup/mod.rs; model actor/db/store.rs;
  facade session/catalog.rs; integration src/lib.rs and
  tests/schema_migration_closeout.rs, under their existing crate/test roots.
- Cargo.lock SHA-256 remains
  `2f54290603809514e794d6340aa0a950fcb2fa788d5b524b688553e18a69cb72`.
- Same Rust 1.98.1, PocketIC 16.0.0, ic-testkit 0.10.0, ic-memory 0.14.3,
  dev/debug actor profile and explicit source/successor features as run 01.
  Builds retained their artifacts through each byte read.

Exclusions remain production data/apps, all-phase crash fuzzing, large datasets,
multi-page findings, incompatible pre-1.0 formats and full workspace tests.
The unrelated archive relocation is not qualified beyond the two repaired links.

## Corrections and proof

**F1:** generated-schema admission now returns the existing typed migration-pending
error for every exact nonterminal migration, including Prepared. Existing
deployment/plan/database/head validation precedes that result. The boolean
Prepared deferral and facade early-success branch are removed, with an in-place
helper rename and no compatibility wrapper. The existing watchdog consumes the
pending signal and stops; generated startup remains gated until publication.
Core row-operation admission is unchanged. Terminal Applied/Aborted records still
proceed through ordinary generated application.

The maintained Prepared-restart regression failed before the fix: **24** extra
updates and **283,437,277 cycles** in the same bounded idle window used by the
audit. After the fix it observes **0 updates and 0 cycle loss**, unchanged status
and typed read gating. Explicit commands subsequently publish successfully and
preserve the expected row IDs, values and indexed lookup. Idle and Validating
restart checks also remain quiescent. Owner-local checks preserve typed
PlanChanged rejection for a mismatched deployed proposal and terminal handling.

**F2:** both active idea links now point to the archived 0.253/0.255 design owners.
No alias path was created. Active guide, tracker, design and release notes reflect
the additional restart proof and current qualification.

## Verification readout

Environment: `CARGO_HOME=/home/adam/projects/icydb/.cache/cargo/icydb`,
`CARGO_TARGET_DIR=/home/adam/projects/icydb/target/icydb`, `TMPDIR` at the
repository `.cache`, and PocketIC binary
`.cache/pocket-ic-server-16.0.0/pocket-ic`. Test selections were listed first with
the same filters/features and `-- --list`; no ignored required cases.

| Outcome | Command/selection | Result |
| --- | --- | --- |
| FAIL | `cargo test --locked -p icydb-testing-integration --test schema_migration_closeout prepared_restart_waits_without_background_work_and_resumes -- --exact --nocapture` before fix | Expected red regression: 0 passed, 1 failed, 0 ignored; reproduced the audit loop with maintained coverage. |
| PASS | `cargo test --locked -p icydb-core --lib --features migration physical_migration_` | 3 passed, 0 failed, 0 ignored: validation/staging/abort, typed findings, rewrite/recovery/publication. |
| PASS | `cargo test --locked -p icydb-testing-integration --test schema_migration_closeout -- --nocapture` after fix | 3 passed, 0 failed, 0 ignored: paired control/Validating restart, Prepared restart, rejection/abort. |
| PASS | `cargo clippy --locked -p icydb -p icydb-testing-integration --lib --test schema_migration_closeout --features icydb/migration -- -D warnings` | Facade and integration lint qualification. |
| PASS | `cargo clippy --locked -p icydb-core --lib --tests --features migration -- -D warnings` | Core implementation/test lint qualification. |
| PASS | `cargo check --locked -p icydb --no-default-features` | Non-migration feature boundary remains compilable. |
| PASS | `cargo fmt --all`; `git diff --check`; local Markdown target verification | Formatting/whitespace and affected local links; anchors/external URLs excluded. |

The red regression used local PocketIC port 44587; the final run used 38715.
Disposable instances only; no application networks changed. No full repository
test suite or release workflow was run. No failed check remains after correction.

## Measured cost and complexity

| Actor | Raw Wasm bytes | Delta from run 02 | BLAKE3 |
| --- | ---: | ---: | --- |
| Source | 8,767,079 | 0 | `b772962e5f1733270a57c3e4dcce19dc52e18bb438fdf464339d15f3cd332e99` |
| Successor | 8,803,598 | -111 | `a4e97c299bffe77c5ebec698210f3d09ade75520b6230c602592378f52be34cf` |

The idle check advances simulated time and ticks after draining queued work;
its cycle-balance delta is a whole-window charge, not native timing or isolated
migration-body instructions. The matched regression improves that charge by
283,437,277 cycles. Do not extrapolate a per-second or production cost from it.
All existing control/Validating/rejection command-cycle observations reproduce
run 01 exactly. Instructions and gzip deltas remain unmeasured.

This correction changes five Rust files by approximately **+10 net lines**,
including regression coverage, plus seven directly related documentation files
and this report: **13 files**. Production flow is simpler: a unit-valued admission
check replaces boolean deferral and its consumer branch. No new configuration,
phase, persisted format, public endpoint, scheduler or authority is introduced.
The completion helper is reused for the additional restart phase rather than
adding another runner. Next step: user-owned full release validation.
