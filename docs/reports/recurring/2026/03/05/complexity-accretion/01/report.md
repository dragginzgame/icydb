# Complexity Accretion Audit - 2026-03-05 (Method V2 Rerun)

Scope: conceptual growth, branch pressure, flow multiplication, and drift sensitivity in `icydb-core` (`crates/icydb-core/src/db`, non-test runtime files).

Method changes in this rerun:
- Added decision-owner vs execution-consumer vs plumbing classification for cross-cutting concepts.
- Added trend-based branch pressure tables.
- Added axis-product flow multiplicity model.
- Added branch multiplier for high-impact enums.
- Added hub-pressure and refactor-noise filters.

## Step 0 - Baseline Capture

| Metric | Previous | Current | Delta |
| ---- | ----: | ----: | ----: |
| Runtime files in scope | 276 | 276 | 0 |
| Runtime LOC (`db/`, non-test) | 52,157 | 52,529 | +372 |
| Runtime files >=600 LOC | 12 | 11 | -1 |
| `continuation|anchor` mentions | 849 | 891 | +42 |
| `continuation|anchor` files | 76 | 79 | +3 |
| Continuation decision owners (mechanical classification) | N/A | 10 | N/A |
| Continuation execution consumers (mechanical classification) | N/A | 48 | N/A |
| Continuation plumbing modules (mechanical classification) | N/A | 21 | N/A |

## Step 1 - Variant Surface + Branch Multiplier

Switch-site counts are mechanical (`match`/`matches!` patterns on enum names).

| Enum | Variants | Switch Sites | Branch Multiplier | Risk |
| ---- | ----: | ----: | ----: | ---- |
| `AccessPath` | 6 | 31 | 186 | High |
| `ContinuationMode` | 3 | 2 | 6 | Medium |
| `RouteShapeKind` | 5 | 1 | 5 | Medium |
| `ExecutionOrdering` | 3 | 1 | 3 | Low-Medium |

## Step 2 - Branching Pressure (Trend)

| Area | Branch Signal | Previous | Delta | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `query/plan` hotspot | `expr/type_inference.rs` = 31 (`if|match`) | 31 | stable | Medium-High |
| `executor/route` | `if=57`, `match=13` | `if=56`, `match=15` | `if +1`, `match -2` | Medium |
| `executor/load` | `if=60`, `match=41` | `if=63`, `match=37` | `if -3`, `match +4` | Medium-High |
| `query/plan/validate` | `if=43`, `match=10` | same | stable | Medium-High |

## Step 3 - Execution Path Multiplicity (Effective Flows)

Theoretical space is shown for context, but risk is scored from effective valid flows only.

| Operation | Axes | Cardinalities | Theoretical Space | Effective Flows (Valid) | Risk |
| ---- | ---- | ---- | ----: | ----: | ---- |
| Load execution | shape, access path, cursor, ordering | `2 x 6 x 2 x 2` | 48 | 10 | Medium-High |
| Cursor continuation | cursor shape, ordering, resume mode | `2 x 2 x 3` | 12 | 6 | Medium |
| Delete execution | access path, ordering, recovery mode | `4 x 2 x 2` | 16 | 5 | Medium |
| Recovery replay | mutation type, index uniqueness, reverse relation | `3 x 2 x 2` | 12 | 6 | Medium |

Effective-flow estimation basis in this rerun:
- route/load dispatch branches and contract gates were used to prune illegal axis combinations.
- combinations disallowed by shape-ordering or cursor-policy contracts were excluded.

## Step 4 - Cross-Cutting Spread (Decision Owner / Consumer / Plumbing)

Owner classification is mechanical and path-scoped in this rerun:
- decision owners: explicit protocol-authority modules for each concept.
- execution consumers: non-owner modules with control-flow branching on concept state.
- plumbing: non-owner modules that transport concept state without branching.

| Concept | Decision Owners | Execution Consumers | Plumbing | Total | Semantic Layers | Transport Layers | Risk |
| ---- | ----: | ----: | ----: | ----: | ---- | ---- | ---- |
| Continuation / anchor | 10 | 48 | 21 | 79 | query, cursor, executor, index (4) | access, response, predicate, codec, contracts, diagnostics, db-root (7) | High |
| Envelope boundary semantics | 2 | 14 | 3 | 19 | index, cursor (2) | executor, query, access, response, db-root (5) | Medium |
| Plan-shape enforcement | 3 | 10 | 1 | 14 | query, executor (2) | db-root (1) | Medium |

## Step 5 - Cognitive Load (Hub + Call Depth)

Hub pressure rule: `LOC > 600` and `domain_count >= 3`.

| Area | Metric | Current | Previous | Delta | Risk |
| ---- | ---- | ----: | ----: | ----: | ---- |
| Hub-pressure files | count | 8 (of 11 >=600 LOC files) | N/A | N/A | Medium-High |
| Largest hub | `access/execution_contract.rs` LOC | 732 | 732 | 0 | Medium |
| Load hub | `executor/load/mod.rs` LOC | 622 | 864 | -242 | Improved |
| Core load call depth | approximate stack depth | 8 | N/A | N/A | Medium-High |

## Step 6 - Drift Sensitivity (Axis Count)

| Area | Decision Axes | Axis Count | Branch Multiplier Signal | Risk |
| ---- | ---- | ----: | ---- | ---- |
| Load routing/execution | shape, access path, cursor, ordering | 4 | AccessPath multiplier remains high | High |
| Continuation protocol | cursor shape, ordering, resume mode | 3 | continuation spread still rising | Medium-High |
| Recovery + index mutation | mutation type, uniqueness, relation mode | 3 | moderate | Medium |

## Step 7 - Weighted Complexity Risk Index

Weights:
- variant explosion x2
- branching trend x2
- flow multiplicity x2
- cross-layer spread x3
- hub pressure/call depth x2

| Area | Score (1-10) | Weight | Weighted Score |
| ---- | ----: | ----: | ----: |
| Variant explosion | 6.0 | 2 | 12.0 |
| Branching trend | 5.0 | 2 | 10.0 |
| Flow multiplicity | 5.0 | 2 | 10.0 |
| Cross-layer spread | 6.0 | 3 | 18.0 |
| Hub pressure/call depth | 5.0 | 2 | 10.0 |

Weighted total: `60 / 11 = 5.45`

## Step 8 - Refactor Noise Filter

| Signal | Raw Trend | Noise Filter Result | Adjusted Interpretation |
| ---- | ---- | ---- | ---- |
| Continuation mentions (`849 -> 891`) | Up | Mixed: semantic spread stable (`semantic layers=4`) but consumer spread remains high (`consumers=48`) | Protocol authority is concentrated; remaining drag is execution fan-out, not ownership fan-out |
| Stream/access module split | File surface changed | Structural improvement (`>=600 LOC` hubs dropped `12 -> 11`) | Do not classify as entropy growth |
| Route branching | mixed small deltas | Stable band | No material regression |

## Complexity Risk Index

**5/10**

Key conclusion:
- The audit now avoids theoretical-flow and transport-layer inflation; complexity pressure is moderate, with continuation execution fan-out (not owner spread) as the primary residual driver.
