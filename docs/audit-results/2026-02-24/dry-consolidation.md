# DRY / Redundancy / Consolidation Audit - 2026-02-24

Scope: duplication risk and divergence pressure. No unsafe layer collapse recommendations.

## Step 1 - Structural Duplication Scan

| Pattern | Files | Duplication Type | Safety Critical? | Drift Risk | Risk Level |
| ---- | ---- | ---- | ---- | ---- | ---- |
| Projection terminal wrappers repeated across fluent/session/facade | `query/fluent/load.rs`, `db/session.rs`, `icydb/src/db/session/load.rs` | intentional API-layer duplication | No | Low | Low |
| Projection terminal execution helpers in aggregate executor | `executor/load/aggregate.rs` | implementation concentration, low duplication | Yes | Medium | Medium |
| Cursor compatibility checks appear in multiple validation layers | cursor spine + executable + tests | defensive duplication | Yes | Medium | Medium |
| Commit rollback pathways duplicated across normal/replay | commit window + recovery | defensive duplication | Yes | Medium | Medium |

## Step 2 - Pattern-Level Redundancy

| Pattern | Occurrences | Layers Involved | Consolidation Difficulty | Suggested Owner Layer | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |
| Load projection method family wrappers | 4 wrapper surfaces (core fluent, core session, facade, executor) | query/session/facade/executor | Low | query fluent + session boundary | Low |
| Unknown-field pre-scan rejection checks | executor helper + tests | executor/tests | Low | executor field-slot resolver | Low |
| Cursor mismatch classification mapping | cursor spine + error mapping | query/error | Medium | cursor spine | Medium |
| Commit rollback hooks | mutation commit window + replay | executor/commit | Medium | commit rollback helper | Medium |

## Step 3 - Over-Splitting / Under-Splitting Pressure

| Area | Pressure Type | Why | Risk |
| ---- | ---- | ---- | ---- |
| `aggregate.rs` (1698 LOC) | under-splitting pressure | many terminal behaviors in one file | High |
| `route/mod.rs` (1163 LOC) | under-splitting pressure | route orchestration remains dense | High |
| pagination tests (modular directory) | over-splitting risk low | split improved maintainability | Low |

## Step 4 - Invariant Repetition Risk

| Invariant | Repeated Where | Drift Risk | Risk |
| ---- | ---- | ---- | ---- |
| projection parity with `execute()` | multiple executor/session tests | Low | Low |
| distinct first-occurrence ordering | executor + session tests | Low | Low |
| scan-budget parity | dedicated tests per terminal | Low | Low |
| cursor compatibility guards | decode + spine + executable layers | Medium | Medium |

## Step 5 - Error Construction Redundancy

| Pattern | Status | Risk |
| ---- | ---- | ---- |
| projection unknown-field errors map to `Unsupported` via shared helper path | consolidated enough | Low |
| plan error -> invariant mapping in `InternalError` constructors | centralized | Low |
| relation/index error constructors remain domain-owned | compliant with error-construction rule | Low |

## Step 6 - Cursor and Index Duplication Focus

| Area | Defensive Duplication? | Keep or Consolidate | Risk |
| ---- | ---- | ---- | ---- |
| cursor compatibility checks | Yes | Keep (safety) | Medium |
| index-range anchor validation layers | Yes | Keep (boundary defense) | Medium |
| projection terminal wrappers | Minimal required | Keep | Low |

## Step 7 - Consolidation Candidates Table

| Candidate | Why Candidate | Safety Impact if Consolidated | Priority |
| ---- | ---- | ---- | ---- |
| internal projection core type for multi-slot specs | future-proofing for 0.29+ | positive (reduce wrapper drift) | Medium |
| aggregate executor helper extraction | reduce large-file concentration | positive if behavior-preserving | Medium |

## Step 8 - Dangerous Consolidations (Do NOT Merge)

| Unsafe Consolidation | Why Unsafe |
| ---- | ---- |
| merge cursor decode + plan compatibility into one unchecked path | weakens layered boundary defenses |
| collapse commit window and replay logic into one opaque flow | obscures marker authority and rollback symmetry |
| remove facade wrappers in favor of direct deep imports | violates module boundary contract |

## Step 9 - Quantitative Summary

- AccessPath fan-out count: 17 files
- AccessPath token references: 163
- Rust test count: 1029
- Projection terminal methods added in 0.28.x: 4 follow-up methods + `values_by` baseline

## Output Structure

1. Structural duplication hotspots: projection wrappers, cursor compatibility checks, commit rollback pathways.
2. Defensive duplication to keep: cursor and replay boundary checks.
3. Consolidation candidates: projection-core extraction; aggregate helper extraction.
4. Dangerous consolidations: listed above.
5. Quantitative summary: included.
6. Drift callout: aggregate module concentration increased.
7. DRY Risk Index (1-10, lower is better): **5/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
