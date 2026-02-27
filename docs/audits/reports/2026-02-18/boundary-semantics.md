# Boundary & Envelope Semantics Audit - 2026-02-18

Scope: correctness and invariant preservation only.

## 1. Bound Transformation Table

| Location | Original Bound | Transformed Bound | Correct? | Risk |
| -------- | -------------- | ----------------- | -------- | ---- |
| `crates/icydb-core/src/db/query/plan/planner.rs:498` | `>` / `>=` / `<` / `<=` predicates | `Gt->Excluded`, `Gte->Included`, `Lt->Excluded`, `Lte->Included` | Yes | Low |
| `crates/icydb-core/src/db/query/plan/planner.rs:526` | Equal bound value with mixed inclusivity | Tightens `Included -> Excluded` only when stricter | Yes | Low |
| `crates/icydb-core/src/db/index/range.rs:62` | Logical `Bound<Value>` | Same bound kind in encoded component (`Included/Excluded/Unbounded`) | Yes | Low |
| `crates/icydb-core/src/db/index/key/build.rs:289` | Component bounds + prefix | Raw `IndexKey` bounds preserving inclusivity/exclusivity | Yes | Low |
| `crates/icydb-core/src/db/index/store/lookup.rs:130` | Planned range lower/upper | Cursor continuation rewrites only lower to `Bound::Excluded(anchor)` | Yes | Low |
| `crates/icydb-core/src/db/query/plan/executable.rs:205` | Raw envelope bounds + anchor | Anchor must satisfy lower/upper envelope via `raw_key_within_bounds` | Yes | Low |
| `crates/icydb-core/src/db/query/plan/logical.rs:511` | Continuation boundary in ordered space | Strict continuation (`entity > boundary`) | Yes | Low |

## 2. Envelope Containment Table

| Scenario | Can Escape Envelope? | Why / Why Not | Risk |
| -------- | -------------------- | ------------- | ---- |
| Lower = Included(x), anchor = x | No | Anchor is valid in envelope; continuation rewrites lower to `Excluded(x)` and remains inside original upper (`lookup.rs:133`) | Low |
| Lower = Excluded(x), anchor = x | No | Rejected by envelope check (`executable.rs:250`, `executable.rs:223`) because `Excluded` requires `key > x` | Low |
| Upper = Included(x), anchor = x | No | Valid, then continuation becomes `(Excluded(x), Included(x))`, immediately empty by `range_is_empty` (`lookup.rs:216`) | Low |
| Upper = Excluded(x), anchor = x | No | Rejected by envelope check (`executable.rs:255`, `executable.rs:223`) because `Excluded` requires `key < x` | Low |
| Anchor exactly equal to upper bound | No | Included upper: empty continuation; Excluded upper: rejected | Low |
| Anchor exactly equal to lower bound | No | Included lower: valid strict resume; Excluded lower: rejected | Low |
| Anchor just below lower | No | Rejected by `raw_key_within_bounds` lower check (`executable.rs:249-250`) | Low |
| Anchor just above upper | No | Rejected by `raw_key_within_bounds` upper check (`executable.rs:254-255`) | Low |
| Empty range (`lower == upper`) | No | Planner rejects strict-empty (`plan/tests.rs:428`), store also short-circuits empties (`lookup.rs:216`) | Low |
| Single-element range | No | Allowed only for inclusive-equal bounds; one row then strict continuation empties | Low |
| Full unbounded range | No | Raw envelope uses canonical low/high sentinels (`build.rs:304`), continuation still only tightens lower | Low |

## 3. Duplication/Omission Table

| Case | Duplication Risk | Omission Risk | Explanation | Risk |
| ---- | ---------------- | ------------- | ----------- | ---- |
| Normal IndexRange continuation | Low | Low | Lower rewritten to strict `Excluded(anchor)` and guard asserts advancement (`lookup.rs:145`) | Low |
| Cursor boundary phase after ordering | Low | Low | Post-access keeps only rows strictly greater than boundary (`logical.rs:512`) | Low |
| Multi-page IndexRange pagination | Low | Low | Tests assert strict monotonic anchors and no duplicates (`pagination.rs:2634`, `pagination.rs:2750`) | Low |
| Boundary at terminal upper-edge row | Low | Low | Tests verify empty continuation page (`pagination.rs:2835`, `pagination.rs:3199`) | Low |
| Tampered token with inconsistent `boundary` vs `index_range_anchor` | Low | Medium | `plan_cursor` validates each independently (`executable.rs:125`, `executable.rs:131`) and execution uses both independently (`load/mod.rs:114-115`, `load/mod.rs:175`, `load/mod.rs:184`) | Medium |

## 4. Raw vs Logical Ordering Alignment

| Area | Raw Ordering Used? | Logical Conversion? | Drift Risk |
| ---- | ------------------ | ------------------- | ---------- |
| Index component encoding | Yes | Logical values encoded to canonical byte order (`ordered.rs:405`, `ordered.rs:421`) | Low |
| Logical range -> raw range | Yes | `raw_bounds_for_index_component_range` is canonical bridge (`range.rs:30`) | Low |
| Store traversal | Yes | BTree range uses raw bounds directly (`lookup.rs:140`) | Low |
| Cursor anchor materialization | Yes | Anchor emitted from actual index key raw bytes (`load/mod.rs:328`, `load/mod.rs:339`) | Low |
| Post-access continuation boundary | No (entity-order layer) | Uses same canonical order comparator and strict `>` semantics (`logical.rs:523`, `logical.rs:527`) | Low |

## 5. Drift Sensitivity

- Implicit ordering dependency: boundary correctness relies on canonical value ordering and canonical index-component encoding staying aligned (`logical.rs:484`, `ordered.rs:405`).
- Explicit envelope checks are present for IndexRange anchors (`executable.rs:223`), but boundary/anchor mutual consistency is not explicitly checked.
- Boundary test coverage is strong for legit continuation flows, duplicate-edge bounds, and terminal pages (`pagination.rs:2763`, `pagination.rs:3109`).
- Missing targeted adversarial test: no explicit test for a cursor token where `boundary` and `index_range_anchor` are individually valid but mutually inconsistent.

## Overall Boundary Risk Index

Risk Index (1–10, lower is better): **4/10**.

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

Rationale: envelope math, inclusive/exclusive transitions, and strict continuation are implemented consistently and heavily tested, but cursor payload consistency between boundary and raw anchor is a drift-sensitive gap.
