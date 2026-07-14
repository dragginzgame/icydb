# Velocity Preservation Audit - 2026-05-01

## Report Preamble

- scope: current `main` release head plus the immediately preceding content slice
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-22/velocity-preservation.md`
- code snapshot identifier: `c3329642b` (`clean` working tree)
- method tag/version: governance slice-shape gate plus route-planner import guard
- comparability status: partially comparable; this run uses the CI slice-shape gate directly rather than the full weekly 3-slice sample method

## Method Notes

| Method Component | Current | Previous | Comparable |
| ---- | ---- | ---- | ---- |
| feature-slice selection source/rules | direct commit-range audit: release head and prior content slice | changelog-guided release slices with manual file filtering | Partial |
| subsystem taxonomy | `scripts/ci/check-slice-shape.sh` primary-domain taxonomy | governance taxonomy plus manual subsystem mapping | Partial |
| boundary crossing rule set | route-planner import grep for `sql` / `session` leakage | invariant scripts plus route/load hub review | Partial |
| fan-in definition | not measured | manual hub pressure proxy | No |
| hub-family taxonomy | route-planner controlled hub only | planner semantics, access-route contracts, executor dispatch, terminal/load shaping, cursor/continuation, storage/recovery | Partial |
| independent-axis rule | not measured | slice-local axis count | No |
| facade/adapters inclusion | CI primary-domain classifier | included only when shipped slices touched facade/adapters | Partial |

## Slice-Shape Gate Results

### Current release head

Command:

```bash
bash scripts/ci/check-slice-shape.sh
```

Result:

```text
Slice shape
  Base: HEAD~1
  Head: HEAD
  Files changed: 2
  Primary domains touched: 0
[OK] Slice shape gate passed.
```

Interpretation: the current `Release 0.144.9` commit is release metadata only
and does not create feature-slice velocity risk.

### Prior content slice

Command:

```bash
SLICE_BASE_REF=d3f5dc480 SLICE_HEAD_REF=de813fe83 bash scripts/ci/check-slice-shape.sh
```

Result:

```text
Slice shape
  Base: d3f5dc480
  Head: de813fe83
  Files changed: 84
  Primary domains touched: 4
    - executor-planner
    - integration-tests
    - lowering-session
    - other-core
[WARN] Slice file count exceeded the soft target (15).
[WARN] Slice file count exceeded the hard limit (25).
[WARN] Slice touched more than 2 primary domains.
[ERROR] Slice shape exceeded guarded limits without a PR override.
```

Interpretation: the content slice requires the slice override contract:

```text
Slice-Override: yes
Slice-Justification: <why the cross-layer change is unavoidable>
```

## Change Surface

Range: `d3f5dc480..de813fe83`

| Metric | Value |
| ---- | ----: |
| Files changed | `84` |
| Lines added | `955` |
| Lines deleted | `725` |
| Total churn | `1,680` |
| Primary domains touched | `4` |

Domain distribution:

| Domain | Files |
| ---- | ----: |
| executor-planner | `76` |
| docs | `3` |
| other-core | `2` |
| lowering-session | `2` |
| integration-tests | `1` |

Largest changed files by added lines:

| File | Added | Deleted |
| ---- | ----: | ----: |
| `crates/icydb-core/src/db/query/plan/expr/function_semantics.rs` | `112` | `57` |
| `crates/icydb-core/src/db/query/plan/validate/errors.rs` | `67` | `53` |
| `crates/icydb-core/src/db/query/plan/model.rs` | `61` | `61` |
| `crates/icydb-core/src/db/session/tests/sql_blob.rs` | `48` | `0` |
| `docs/changelog/0.144.md` | `46` | `2` |
| `crates/icydb-core/src/db/executor/planning/route/planner/stages.rs` | `36` | `47` |

## Guarded Root Growth

| Guarded root | Added lines |
| ---- | ----: |
| `crates/icydb-core/src/db/sql/parser/mod.rs` | `0` |
| `crates/icydb-core/src/db/session/sql/mod.rs` | `0` |

Result: no root-module re-centralization violation.

## Route Planner Controlled Hub

Checked current route planner root:

- `crates/icydb-core/src/db/executor/planning/route/planner/mod.rs`

No direct `sql::*`, `session::*`, `db::sql`, or `db::session` imports were
found in the route-planner subtree for the audited content slice.

Result: route-planner import boundary passes.

## Verdict

**Velocity risk: high for the content slice, low for the release commit.**

The content slice is structurally coherent in one sense: most files land in
`executor-planner`. However, at `84` files it is far beyond the hard file-count
limit, and it crosses four primary domains. Under current governance, this is
not a routine slice and must carry an explicit override.

## Required Follow-Up

- Add a PR/body override for the wide content slice if it is already intended
  to ship as one unit.
- For the next similar feature, split into smaller slices:
  - query/planner structural changes
  - SQL/session edge tests and docs
  - other-core visibility or value/storage cleanup
- Keep the current route-planner boundary unchanged; no SQL/session leakage was
  detected.
