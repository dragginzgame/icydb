# Structure / Module / Visibility Discipline Audit - 2026-03-04

Scope: `icydb-core` db subsystem structural boundaries and visibility discipline.

## Public Surface and Visibility Snapshot

| Metric (runtime, non-test, `db/`) | Value |
| ---- | ---- |
| Runtime Rust files | 237 |
| `pub` token occurrences | 2404 |
| Restricted visibility declarations (`pub(crate)`, `pub(super)`, `pub(in ...)`) | 1881 |
| Public `struct/enum/trait/fn` declarations | 306 |
| Public fields | 108 |

## Layer Directionality

| Check | Result |
| ---- | ---- |
| Upward imports (tracked layer edges) | 0 |
| Cross-layer policy re-derivations | 0 |
| Comparator definitions outside index | 0 |
| Include-str architecture scans | none detected |

## Structural Pressure

| Indicator | Current Signal | Risk |
| ---- | ---- | ---- |
| Large runtime modules | 14 files >= 600 LOC | Medium-High |
| Continuation concern spread | 757 mentions across 73 runtime files | High |
| Access-path fan-out | 74 mentions across 10 runtime files | Medium |

## Hub Import Pressure (Current Snapshot)

| Hub Module | Top Imports | Unique Sibling Subsystems | Cross-Layer Count | Delta vs Previous |
| ---- | ---- | ---- | ---- | ---- |
| `query/plan/planner.rs` | `query`, `access`, `predicate` | 3 | 2 | reduced (`db::index`-adjacent dependency removed from direct `db` import set) |
| `executor/route/planner/feasibility.rs` | `executor`, `query`, `direction` | 3 | 2 | increased (`direction` explicit in route feasibility imports) |
| `executor/load/entrypoints.rs` | `executor`, `cursor`, `index`, `response` | 4 | 3 | reduced (`query` import removed from load entrypoints) |

## Overall Structural Risk Index

**5/10**
