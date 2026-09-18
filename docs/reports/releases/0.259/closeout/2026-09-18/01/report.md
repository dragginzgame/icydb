# 0.259 migration rehearsal closeout

Verdict: **PASS** for the maintained current-format rehearsal and its bounded
application handoff. M1–M3 are complete. No actionable finding remains within
this scope. Full repository validation, publication and actual application
qualification remain user-owned; this is not production deployment approval.

## Scope and identity

- Method: `MIGRATION-REHEARSAL-CLOSEOUT-1 / DOMAIN-1`, bounded release investigation
  under `docs/audits/README.md`, not a whole-system migration/recovery audit.
- Baseline report: N/A. First scoped closeout; non-comparable as an audit series.
  The raw-Wasm comparison explicitly reuses M2's matched pre-fix actors, not a
  preceding release or another actor shape. Cycle observations are a new baseline.
- Snapshot: `fb69db0329535554b0131f5cc77a7286217e2ae9` plus the dirty 0.259 worktree.
  HEAD has no tag; the workspace still declares 0.258.0. No publication or version
  bump is inferred. Tests ran on stable source; subsequent edits were documents.
- Trigger: closeout of automatic adjacent-schema rehearsal, the authorised
  startup/migration correction, and application adaptation guidance.
- Obligations: retained exact artifacts, current-format boundary, authoritative
  command bindings, final values/IDs/indexed reads, gated interruption/resume,
  rejection before rewrite/publication, permitted abort, non-spinning waiting,
  truthful measurement and downstream-qualification boundaries.
- Exclusions: real application data/CI, production deployment, old storage
  formats, all-phase interruption fuzzing, large datasets, multi-page findings,
  arbitrary relations/constraints, whole-dependency audits and full test suites.
  Those are not claimed as passing from the three-row fixture.

The five-file implementation/test diff against HEAD has SHA-256
`75403d21ea9dfa1320d815013555907208caa807dc2bded45de82af6de5ea4c3`
(`git diff --binary --` the paths below). Exact file SHA-256 identities:

| Input | SHA-256 |
| --- | --- |
| `crates/icydb-core/src/db/schema/application.rs` | `19834869233c50187ad29024f98809e052b63e39217769105ffeed5f54e46023` |
| `crates/icydb-core/src/db/startup/mod.rs` | `f0571b69b519f079f6ec72c6693cbb734f1343dadea3bba873fecc2c00536be6` |
| `crates/icydb-model/src/build/actor/db/store.rs` | `57d5a1e306b227616407a3b595d084b65bf2c1d91c585fa8a68305494a1abf9d` |
| `testing/integration/src/lib.rs` | `35140c212efdabe6772d0cbed686276ec3d88f25d142dc576e818f2c201976cb` |
| `testing/integration/tests/schema_migration_closeout.rs` | `7c4b44ac208224700670ed6e9235d7c339da48d47db604ac545a9763a072ba24` |
| `schema/test/sql/src/sql.rs` (unchanged fixture) | `7821fe3b48034f04fcae7d9435309587fa38edfe02b7da034235fae2f2e0e681` |
| `canisters/test/sql/src/lib.rs` (unchanged actor) | `959070ef568e8c9b233bc69625d9c6986133a469a46e9e500bc453fbb3a7475b` |
| `Cargo.lock` (unchanged) | `2f54290603809514e794d6340aa0a950fcb2fa788d5b524b688553e18a69cb72` |

## Build and fixture contract

Rust `1.98.1 (48a229cea 2026-09-01)`, PocketIC 16.0.0, ic-testkit 0.10.0 and
ic-memory 0.14.3. Both `canister_test_sql` actors use `wasm32-unknown-unknown`,
locked/no-default-feature builds, default **dev/debug** profile (opt-level 1,
16 codegen units, no LTO), and the existing retained post-link flow. No extra
`RUSTFLAGS`/`CARGO_ENCODED_RUSTFLAGS` were supplied. These are not release-profile
deployment cost estimates.

| Actor | Explicit package features | Raw post-linked Wasm bytes | BLAKE3 |
| --- | --- | ---: | --- |
| Source | `test-admin-api,local-sql-query,schema-migration-api` | 8,767,079 | `620d41d47b01da18956ed004166e25ef67b29743d4c44e31db71e0a763b4fdf9` |
| Successor | `test-admin-api,local-sql-query,schema-migration-v2` | 8,803,709 | `06f8c1c50c27cff5a8ad88aa7eaa5b208c91cbb130ef7a4f80514fad4f9d71d4` |

Each retained build owner stays alive through its byte read. Both actors are
built before starting the local canisters, and interruption reuses the same
successor bytes. Source revision 1-to-2 is a logical entity revision; both
artifacts use the same maintained version-1 internal representations.

The migration rewrites `SqlTestUser.age` from Int32 to Nat16 and renames `rank`
to `score`. Synthetic rows are alice/31/28, bob/24/25 and charlie/43/43; generated
IDs are captured and preserved within each installation. Other fixture rows
loaded by the existing seed endpoint are not claimed as a new migration test.
The rejection case changes alice's source age to 65,536 through normal admitted
writes. No production payload was inspected or retained.

## Qualification and boundary review

The successful control and interrupted run each validate/rewrite three users,
rebuild one index, publish once, preserve their IDs and compare typed logical
values, transitions and accepted heads. An indexed name lookup returns the
expected user. The interrupted run upgrades in `Validating`, retains the exact
status, and finishes with the same logical result as the control. It does not
prove interruptions after every physical page. Public field descriptions do
not expose accepted FieldId; no separate end-to-end FieldId assertion is claimed.

The rejected run yields exactly one typed Transform finding, validates three
rows, rewrites none, rebuilds no index and preserves the accepted head. Abort
then redeploys the exact source actor and compares selected source values/IDs.
One bounded finding page suffices; this does not qualify multi-page findings.
All scenarios stay within the explicit 32-command fixture advancement limit.

Admission remains owned by accepted schema/migration preflight. A validated
pending plan uses the existing pending error; genuine validation errors are not
converted into admission. Generated startup stops its watchdog for this pending
signal without writing a terminal failure. Ordinary requests still receive
typed recovery-pending. After queued timer delivery drains, isolated subnet
update counts remain unchanged across the checked idle rounds, including after
the interrupted upgrade. No error-string matching or private memory import is
used. Core `Prepared` row admission is distinct from generated startup readiness;
the application guide now states that distinction.

Fresh verification used `CARGO_HOME=/home/adam/projects/icydb/.cache/cargo/icydb`,
`CARGO_TARGET_DIR=/home/adam/projects/icydb/target/icydb`,
`TMPDIR=/home/adam/projects/icydb/.cache` and
`POCKET_IC_BIN=/home/adam/projects/icydb/.cache/pocket-ic-server-16.0.0/pocket-ic`.

| Outcome | Command/selection | Evidence |
| --- | --- | --- |
| PASS | `cargo test --locked -p icydb-testing-integration --test schema_migration_closeout -- --list` | Exactly the maintained control/interruption and rejection/abort tests selected; no ignored cases. |
| PASS | Same target with `-- --nocapture` | 2 passed, 0 failed, 0 ignored; three scenarios. The final qualification followed listing; an earlier measurement smoke run also passed but is not added to this count. |
| PASS | `cargo clippy --locked -p icydb-testing-integration --lib --test schema_migration_closeout -- -D warnings` | Host measurement change lint-clean. |
| PASS | `cargo fmt --all`; `git diff --check`; local Markdown file-link check | Source formatting and document consistency. No external URLs or anchors checked. |

The final PocketIC server started locally on port 37873; the earlier measurement
smoke used port 44509. Disposable instances only; application networks and
repositories were untouched. M2's 21 focused core/model/integration passes,
logical-memory checks and full clippy pass remain attributed to M2, not fresh
M3 executions. Full workspace tests/release validation were not run.

## Cost evidence

Host `cycle_balance` reads bracket each explicit `icydb_schema_migrate` update.
The observed decrease is the **whole-call canister charge**, including ingress
and any work scheduled during that call. It is not migration-body instructions.
No cycle transfers/top-ups occur inside the brackets. Install, upgrades, seeding,
separate queries, and startup delivery outside these calls are excluded.

| Returned phase | Control cycles | Interrupted cycles | Rejection/abort cycles |
| --- | ---: | ---: | ---: |
| Prepared | 151,530,240 | 151,530,240 | 151,530,240 |
| Validating | 70,504,461 | 70,504,461 | 70,504,461 |
| ReadyToRewrite | 165,037,132 | 180,630,035 | — |
| RewritingRows | 71,000,249 | 70,841,332 | — |
| RebuildingIndexes | 166,751,241 | 166,549,764 | — |
| FinalValidation | 164,766,722 | 164,841,710 | — |
| Publishing | 182,764,834 | 183,067,502 | — |
| Applied | 337,702,887 | 337,766,270 | — |
| Rejected | — | — | 165,217,779 |
| Aborted | — | — | 154,600,383 |
| Explicit-command total | 1,310,057,766 | 1,325,731,314 | 541,852,863 |

The interrupted/control difference is an observation of different execution
histories, not an optimisation delta or a per-row scaling estimate. There is
no new cost ceiling. Instruction counts, before/after cycle deltas and gzip
sizes are unmeasured; native/wall-clock timing is not a metric.

M3 leaves both actors byte-identical to M2: zero raw Wasm delta. M2's matched
pre-fix values were 8,767,053 and 8,803,666 bytes, so the authorised runtime
correction added 26 and 43 raw bytes. Do not compare source versus successor
size as a performance improvement.

## Handoff and complexity

The maintained guide now gives application adaptation steps without adding a
generic runner or public API. Applications own realistic seeds, domain endpoint
authorization, lifecycle/framework integration, complete CI and deployment.
Passing this fixture neither unlocks incompatible old data nor qualifies Canic
or Toko Miner. No additional runtime work is required by this scoped closeout.

M3 changes one existing Rust test by approximately +13 net lines, plus the guide,
design/status, active detailed release notes and this report. Runtime structure
is unchanged; measurements add two host reads per command and no actor code,
configuration, persisted state, report DTO or budget framework. The line's sole
runtime correction reuses existing admission and watchdog owners. The next
step is user-owned full release validation, not another implementation slice.
