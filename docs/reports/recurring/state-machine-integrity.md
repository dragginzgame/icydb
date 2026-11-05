> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# State Machine And Transition Integrity Audit

## Metadata And Frozen Scope

- Request: select the next least recently run active audit, improve its method,
  and execute it.

- Definition: `docs/audits/recurring/executor/executor-state-machine-integrity.md`.

- Method: **STATE-2 + DOMAIN-1**, fixed for this run.

- Snapshot: `b6111f5f7185136b56860ee35287b206d9debafa`, workspace `0.261.11`,
  with pre-existing Cargo dependency edits and the boundary-audit storage test.
  The earlier boundary definition, reports and changelog changes are retained.

- Compared baseline: **N/A**, no comparable STATE-2 run exists.

- Historical reference: [2026-05-13 run 01](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/05/13/state-machine-integrity/01/report.md),
  Method V4. No prior behavioral results are counted as new execution.

- Comparability: **non-comparable (method change)**. Affected deltas are
  `N/A (method change)`; marker-before-apply and admission-before-execution
  remain qualitative anchors.

Selection follows the latest canonical report for each active definition.
The boundary audit now has a 2026-09-27 run. State-machine integrity's latest
run is 2026-05-13; the next oldest active scopes (error taxonomy, recovery
consistency and resource model) were last run 2026-05-14. Archived methods and
summary reports are ineligible.

### Source Identity

SHA-256 inputs:

| Input | Hash |
| --- | --- |
| Revised audit definition | `d02d80be050018300ead408c11c1b743d52e796728c55062991d6a8c44b1000a` |
| `Cargo.toml` | `b34b059d3b555740d4d2ccdfb2a5ad39f102381294ed87313beee8d5cc78242c` |
| `Cargo.lock` | `695051feaba3c8de26eed05f5db590dddd2568f1f7493924e24b87ca08767716` |
| Pre-existing `db/index/scan/tests.rs` change | `635996f907893ddb6d3ab4b2dd34930e2d1bed95707b2d5639940d1b0dd6f938` |

All other Rust source is unchanged from HEAD. Earlier boundary reports remain
independent evidence; their PASS verdict is not substituted for transition
proof here. No production code, tests, Cargo inputs or release metadata were
changed in this run.

## Audit Review And Method Changes

The previous 593-line definition had three material problems:

- It explicitly delegated deep replay equivalence to recovery consistency,
  then required that audit's full equality/idempotence inventory here.

- Its readiness language could conflate committed online journal debt with
  incomplete startup recovery. Current `continue_recovery_with_failure_authority`
  and `ensure_recovery_admitted` deliberately distinguish them.

- It repeated state/transition/attack inventories, used a competing `PARTIAL`
  status, and referenced an obsolete daily report filename. Historical
  exclusivity claims and raw-cursor assumptions also needed current owner
  inspection rather than mechanical reuse.

STATE-2 reduces the definition to 175 lines, retains all five sampled
transition families, names protected state and admission boundaries, separates
native fault injection from IC rollback/delivery proof, and requires explicit
negative evidence and feature-correct test discovery. It allows structural
proof for private construction instead of demanding artificial illegal inputs.
It does not weaken marker durability, schema authority or readiness requirements.

## Findings And Verdict

No production transition violation was demonstrated in the declared scope.
The audit-definition defects above were corrected before this run. The initial
test listing omitted three required feature-gated schema cases; this was
caught before behavioral execution and corrected without dropping coverage.
The verification readout preserves that failed selection.

Drift triggers: new marker publication callers must preserve preflight and
wake-up ownership; new await/re-entry points require rechecking synchronous
predecessor assumptions; startup readiness changes must preserve the distinction
between terminal failure, startup recovery, reconciliation and online debt;
new route retry or cursor shapes require accepted-contract binding evidence.
No debt ledger, implementation tracker or release metadata was changed.

**Verdict: PASS for the declared sampled transition baseline.** Required
behavioral samples passed, and source inspection supports the structural
handoffs. No active finding or implementation follow-up remains. Source-only
rows and explicitly excluded platform/deep-replay obligations are not promoted
to executed proof or a whole-system verdict.

## Verification Readout

Toolchain: `rustc 1.98.1 (48a229cea 2026-09-01)`; native `icydb-core` unit target;
repository Cargo home/target paths from Makefile; `RUST_TEST_THREADS=8`.
No ignored-test override, test-count substitution or full-suite execution.

The exact final temporary wrapper (`/tmp/icydb-state-audit-selection.sh`) was:

| Command / check | Outcome | Selected / passed / failed / ignored | Interpretation |
| --- | --- | --- | --- | --- |
| Initial wrapper with `--features sql`, invoked with `--list` | FAIL | 17 of 20 required / 0 / 0 / 0 | Process exited successfully, but three required rename admission/publication cases were absent; no behavioral execution occurred. |
| `bash /tmp/icydb-state-audit-selection.sh --list` with final `sql,migration` features | PASS | 20 / N/A / N/A / 0 | All required cases listed after inspecting the `cfg(feature = "migration")` gate in `schema/application.rs:3154`. |
| `bash /tmp/icydb-state-audit-selection.sh` | PASS | 20 / 20 / 0 / 0 | All selected cases executed and passed; the two-test rename prefix accounts for the extra case beyond 19 selectors. |
| `git diff --check`, report-link and recorded-input verification | PASS | N/A | Audit definition and new report only; pre-existing inputs remain unchanged. |

The first selection's missing feature is resolved, not an outstanding runtime
failure. Its `sql`-only build reported the same 65 unused/dead-code warnings
seen in the preceding boundary audit. The final `sql,migration` build and test
run reported no warnings. No clippy run or failure occurred.

Evidence coverage: schema/accepted-authority admission and publication 6 cases;
write preflight/commit/wake-up/recovery 8 cases (including missing lifecycle
wiring); startup observation/reconciliation 3 cases; cursor/raw-anchor rejection
3 cases. Assertions were inspected for those obligations.
Failure-cut loops are not counted as extra tests. Plan assembly and marker-clear
failure handling remain explicitly source-inspected rather than claimed as
injected behavioral tests.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/09/27/state-machine-integrity/01/report.md).
