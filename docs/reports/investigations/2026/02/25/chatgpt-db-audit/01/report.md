 1. Execution Flow Findings

  Entrypoints mapped

  - Generic query entry: session.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/session.rs:181)
    (execute_query) dispatches to load vs delete executors.
  - Paged load entry: session.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/session.rs:602)
    (execute_load_query_paged_with_trace).
  - Aggregate wrappers: many session wrappers call query.plan() then specialized load aggregate APIs (example wrappers
    at session.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/session.rs:195), session.rs (/home/adam/
    projects/icydb/crates/icydb-core/src/db/session.rs:344), session.rs (/home/adam/projects/icydb/crates/icydb-core/
    src/db/session.rs:275)).
  - Delete entry: executor/delete/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/delete/
    mod.rs:61).

  Canonical load pipeline (observed)

  session.execute_query/load_paged
  -> query.plan() / executable plan lowering
  -> route planner (execution mode, direction, hints, fast-path order)
  -> resolve execution key stream (fast-path attempt in route order, else canonical access stream)
  -> physical key stream resolution (AccessPlan -> AccessPath -> store/index lookup)
  -> OrderedKeyStream production + optional DISTINCT wrapper
  -> row materialization (scan budget gate, deserialize)
  -> post-access phases (filter -> order -> cursor -> page -> delete-limit)
  -> continuation token emission
  -> response + trace finalization

  Key anchors:

  - Plan/route/resolve/materialize path in executor/load/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
    executor/load/mod.rs:238), executor/load/execute.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
    load/execute.rs:159), executor/access_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
    access_stream.rs:474), executor/physical_path.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
    physical_path.rs:23), executor/load/page.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/
    page.rs:27), executor/query_bridge/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
    query_bridge/mod.rs:104).

  Is there one canonical flow?

  - For paged load: mostly yes.
  - For the whole engine: no. There are parallel/shadow execution flows.

  Shadow/parallel flows

  - count_distinct_by re-implements most of load pipeline independently: load/aggregate/distinct.rs (/home/adam/
    projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/distinct.rs:37), residual retry at load/aggregate/
    distinct.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/distinct.rs:96).
  - Aggregate system bifurcates into route-driven streaming vs materialized fallback path: load/aggregate/mod.rs (/
    home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:295), fast-path dispatcher load/
    aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:596).
  - Delete path is materialized-access first (not ordered-key-stream canonical path): delete/mod.rs (/home/adam/
    projects/icydb/crates/icydb-core/src/db/executor/delete/mod.rs:86).
  - Terminal/projection APIs force self.execute(plan)? materialization and then apply terminal logic: load/terminal/
    mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/terminal/mod.rs:127), load/aggregate/
    helpers.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/helpers.rs:90), load/
    aggregate/projection.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
    projection.rs:88).

  Files violating canonical flow (structural divergence)

  - load/aggregate/distinct.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
    distinct.rs:37)
  - load/terminal/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/terminal/mod.rs:122)
  - load/aggregate/helpers.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
    helpers.rs:83)
  - load/aggregate/projection.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
    projection.rs:80)
  - executor/delete/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/delete/mod.rs:85)

  ———

  2. Layering Violations

  Explicit forbidden-edge check results

  - index -> query: no direct imports found.
  - data -> executor: no direct imports found.
  - commit -> query: no direct imports found.
  - predicate -> executor: no direct imports found.

  High severity (layer breach)

  - Query layer re-exporting index-layer type: query/plan/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
    query/plan/mod.rs:12) (use crate::db::index::KeyEnvelope).
  - Query cursor contract depending on index raw key internals: query/contracts/cursor.rs (/home/adam/projects/icydb/
    crates/icydb-core/src/db/query/contracts/cursor.rs:4) (RawIndexKey in query contract state).

  Medium severity (leaky abstraction)

  - Query predicate compile directly returns index predicate program type: query/predicate/eval/mod.rs (/home/adam/
    projects/icydb/crates/icydb-core/src/db/query/predicate/eval/mod.rs:3), query/predicate/eval/mod.rs (/home/adam/
    projects/icydb/crates/icydb-core/src/db/query/predicate/eval/mod.rs:110).
  - Index store lookup executes predicate logic during storage traversal: index/store/lookup.rs (/home/adam/projects/
    icydb/crates/icydb-core/src/db/index/store/lookup.rs:6), index/store/lookup.rs (/home/adam/projects/icydb/crates/
    icydb-core/src/db/index/store/lookup.rs:157).

  Low severity

  - Query test/explain path reaches raw index key internals: query/explain/continuation_signature.rs (/home/adam/
    projects/icydb/crates/icydb-core/src/db/query/explain/continuation_signature.rs:248).

  Circular logic

  - No direct import-cycle evidence found in db/ layer boundaries from static import scan.

  ———

  3. Routing Analysis

  Routing logic map

  - Route model/contracts: executor/route/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/
    mod.rs:323) (LOAD_FAST_PATH_ORDER), executor/route/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
    executor/route/mod.rs:331) (AGGREGATE_FAST_PATH_ORDER).
  - Central route planner: executor/route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
    route/planner.rs:155).
  - Load consumer dispatch: load/execute.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/
    execute.rs:159).
  - Aggregate consumer dispatch: load/aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
    load/aggregate/mod.rs:596).
  - Post-route runtime reroute for load fallback: load/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
    executor/load/mod.rs:331).
  - Budget-safety metadata feeding route decisions: query_bridge/mod.rs (/home/adam/projects/icydb/crates/icydb-core/
    src/db/executor/query_bridge/mod.rs:415).

  Is AccessPath declarative?

  - Mostly yes by type definition: query/plan/types.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/
    types.rs:175).
  - But route planner re-interprets ordering/window/pushdown semantics from plan shape at runtime in one large
    function: route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:155).

  Multiple routing decision systems

  - Primary system: route planner.
  - Secondary systems: runtime fallback/reroute gates in load and aggregate:
      - residual retry gate: load/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/mod.rs:396)
      - secondary extrema fallback gate: load/aggregate/helpers.rs (/home/adam/projects/icydb/crates/icydb-core/src/
        db/executor/load/aggregate/helpers.rs:370)

  Duplicated capability checks

  - Planner computes capabilities once: route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
    route/planner.rs:58).
  - Executors re-check spec arity/eligibility branch-locally:
      - load: load/execute.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/execute.rs:166)
      - aggregate: load/aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
        mod.rs:498)

  Aggregate vs load routing inconsistencies

  - Load uses conservative predicate subset compile: load/execute.rs (/home/adam/projects/icydb/crates/icydb-core/src/
    db/executor/load/execute.rs:233).
  - Aggregate streaming uses strict all-or-none compile and can force materialized on uncertainty: route/planner.rs (/
    home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:642), load/aggregate/mod.rs (/home/
    adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:395).
  - count_distinct_by routes through load route, not aggregate route: load/aggregate/distinct.rs (/home/adam/projects/
    icydb/crates/icydb-core/src/db/executor/load/aggregate/distinct.rs:45).

  ———

  4. Fast Path Inventory

  | Fast path | Gate | Fallback/Parity | Risk | Recommendation |
  |---|---|---|---|---|
  | PrimaryKey (load) | Route order + pk_order_fast_path_eligible: load/execute.rs (/home/adam/projects/icydb/crates/
  icydb-core/src/db/executor/load/execute.rs:175), load/pk_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/
  db/executor/load/pk_stream.rs:21) | Falls to canonical stream resolve on miss; parity tests for fast/non-fast
  continuation exist: pagination/cursor_pk.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/tests/
  pagination/cursor_pk.rs:482) | Low | Keep |
  | SecondaryPrefix (load) | Route + spec arity + path check: load/execute.rs (/home/adam/projects/icydb/crates/icydb-
  core/src/db/executor/load/execute.rs:186), load/secondary_index.rs (/home/adam/projects/icydb/crates/icydb-core/src/
  db/executor/load/secondary_index.rs:23) | Falls to canonical path; distinct/fallback parity covered: pagination/
  distinct.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/tests/pagination/distinct.rs:552) | Medium
  | Keep, unify gate code |
  | IndexRange limit pushdown (load) | index_range_limit_spec from route: load/execute.rs (/home/adam/projects/icydb/
  crates/icydb-core/src/db/executor/load/execute.rs:200), load/index_range_limit.rs (/home/adam/projects/icydb/crates/
  icydb-core/src/db/executor/load/index_range_limit.rs:22) | Residual retry reruns full fallback path: load/mod.rs (/
  home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/mod.rs:331); parity matrix exists: pagination/
  index_range.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/tests/pagination/index_range.rs:1287) |
  High | Keep, but unify retry implementation |
  | PrimaryKey (aggregate) | Aggregate fast-path order + branch eligibility: load/aggregate/mod.rs (/home/adam/
  projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:599), load/aggregate/mod.rs (/home/adam/
  projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:712) | Falls to streaming fold/materialized
  route | Medium | Keep |
  | SecondaryPrefix (aggregate) | Verified gate + probe/fallback logic: load/aggregate/mod.rs (/home/adam/projects/
  icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:506), load/aggregate/mod.rs (/home/adam/projects/
  icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:762) | MissingOk bounded-probe fallback: load/
  aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:812) | Medium |
  Keep, centralize fallback policy |
  | PrimaryScan (aggregate) | Requires route-provided physical_fetch_hint: load/aggregate/mod.rs (/home/adam/projects/
  icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:519), load/aggregate/mod.rs (/home/adam/projects/
  icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:859) | Falls to canonical streaming/materialized |
  Medium | Keep |
  | Composite (aggregate) | Route capability: load/aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/
  db/executor/load/aggregate/mod.rs:539), route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
  executor/route/planner.rs:868) | Uses canonical composite stream production | Medium | Keep |
  | Index-only predicate prefilter | Compile + execute in index traversal: load/execute.rs (/home/adam/projects/icydb/
  crates/icydb-core/src/db/executor/load/execute.rs:224), index/store/lookup.rs (/home/adam/projects/icydb/crates/
  icydb-core/src/db/index/store/lookup.rs:157) | Conservative mode + post-access predicate keeps correctness | Medium-
  High | Keep, but move evaluation boundary out of store |
  | DISTINCT key dedup wrapper | plan.distinct: load/execute.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
  executor/load/execute.rs:247), ordered_key_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
  ordered_key_stream.rs:163) | No separate fallback; relies on ordered contiguous duplicates | Medium | Keep, enforce
  stronger invariant checks |
  | Zero-window short-circuit (aggregate) | physical_fetch_hint == Some(0): load/aggregate/mod.rs (/home/adam/
  projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:614) | Returns default terminal output | Low
  | Keep |

  ———

  5. Streaming Model Findings

  - composite_stream.rs is no longer present; composite stream logic now lives in executor/access_stream.rs (/home/
    adam/projects/icydb/crates/icydb-core/src/db/executor/access_stream.rs:509) and executor/ordered_key_stream.rs (/
    home/adam/projects/icydb/crates/icydb-core/src/db/executor/ordered_key_stream.rs:418).
  - Ordering model is mostly canonical for key streams via KeyOrderComparator and monotonicity enforcement:
    ordered_key_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/ordered_key_stream.rs:233),
    ordered_key_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/ordered_key_stream.rs:335).
  - DISTINCT is not comparator-driven in implementation; it uses equality on last emitted key, comparator stored but
    unused: ordered_key_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
    ordered_key_stream.rs:166), ordered_key_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
    ordered_key_stream.rs:206).
  - Multiple ordering models coexist:
      - key-stream order normalization: direction.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
        direction.rs:5)
      - post-access row order: query_bridge/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
        query_bridge/mod.rs:208)
      - materialized terminal ranking: load/terminal/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
        executor/load/terminal/mod.rs:274)
  - Streaming is partially “pseudo-streaming”: key streams are often pre-collected into vectors:
      - physical path returns VecOrderedKeyStream: physical_path.rs (/home/adam/projects/icydb/crates/icydb-core/src/
        db/executor/physical_path.rs:75)
      - context collects all keys before row reads: context.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
        executor/context.rs:112)
  - Scan hints are enforced in load page materialization with explicit guards: load/page.rs (/home/adam/projects/
    icydb/crates/icydb-core/src/db/executor/load/page.rs:40).
  - Budget enforcement is not uniform across all terminal families; many terminals bypass stream budgeting by forcing
    materialized execute(plan).

  ———

  6. Predicate Audit

  Compilation rules (centralized in index compile path)

  - Conservative subset only special-cases AND, dropping unsupported children: query/predicate/eval/index_compile.rs
    (/home/adam/projects/icydb/crates/icydb-core/src/db/query/predicate/eval/index_compile.rs:20), query/predicate/
    eval/index_compile.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/predicate/eval/
    index_compile.rs:30).
  - Strict mode compiles all nodes or returns None: index_compile.rs (/home/adam/projects/icydb/crates/icydb-core/src/
    db/query/predicate/eval/index_compile.rs:60).
  - OR is fail-closed in strict compile (collect::<Option<Vec<_>>>()?): index_compile.rs (/home/adam/projects/icydb/
    crates/icydb-core/src/db/query/predicate/eval/index_compile.rs:73).
  - Compare compile requires strict coercion and indexed slot: index_compile.rs (/home/adam/projects/icydb/crates/
    icydb-core/src/db/query/predicate/eval/index_compile.rs:98), index_compile.rs (/home/adam/projects/icydb/crates/
    icydb-core/src/db/query/predicate/eval/index_compile.rs:102).
  - Unsupported for index compile: null/missing/empty/text contains families: index_compile.rs (/home/adam/projects/
    icydb/crates/icydb-core/src/db/query/predicate/eval/index_compile.rs:83), index_compile.rs (/home/adam/projects/
    icydb/crates/icydb-core/src/db/query/predicate/eval/index_compile.rs:88).

  Runtime evaluation paths

  - Entity-level predicate eval post-access: query_bridge/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
    executor/query_bridge/mod.rs:198).
  - Index-level predicate eval during index-store traversal: index/store/lookup.rs (/home/adam/projects/icydb/crates/
    icydb-core/src/db/index/store/lookup.rs:157), evaluator in index/predicate.rs (/home/adam/projects/icydb/crates/
    icydb-core/src/db/index/predicate.rs:118).

  Redundant evaluation points

  - Conservative index prefilter + full post-access predicate reevaluation on entities (intentional correctness-safe
    redundancy): load/execute.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/execute.rs:233),
    query_bridge/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/query_bridge/mod.rs:198).

  Correctness risks

  - No immediate unsoundness found in AND-subset or OR fail-closed logic.
  - Structural risk: strict compile is computed in route planner and again in aggregate descriptor (duplication/drift
    risk): route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:642), load/
    aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:275).

  ———

  7. Aggregate Audit

  Feature matrix

  - Route-driven aggregate terminals (count/exists/min/max/first/last): load/aggregate/mod.rs (/home/adam/projects/
    icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:107), execution core load/aggregate/mod.rs (/home/
    adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:295).
  - Field-target extrema (min_by/max_by) integrated into route+streaming when eligible: load/aggregate/mod.rs (/home/
    adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:159), load/aggregate/mod.rs (/home/
    adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:421).
  - nth_by/median_by/min_max_by: materialized helper path: load/aggregate/mod.rs (/home/adam/projects/icydb/crates/
    icydb-core/src/db/executor/load/aggregate/mod.rs:193), load/aggregate/helpers.rs (/home/adam/projects/icydb/
    crates/icydb-core/src/db/executor/load/aggregate/helpers.rs:83).
  - count_distinct_by: separate hybrid flow with duplicated load logic: load/aggregate/distinct.rs (/home/adam/
    projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/distinct.rs:37).
  - Value projection family (values_by, etc.): materialized wrappers: load/aggregate/projection.rs (/home/adam/
    projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/projection.rs:80).

  Redundant paths

  - Multiple modules call self.execute(plan)? then reprocess materialized rows:
      - terminal/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/terminal/mod.rs:145)
      - aggregate/helpers.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
        helpers.rs:90)
      - aggregate/projection.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
        projection.rs:88)
  - count_distinct_by duplicates residual retry and scan accounting from load path: aggregate/distinct.rs (/home/adam/
    projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/distinct.rs:96), corresponding canonical load code
    load/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/mod.rs:331).

  Forced materialization that could be reduced

  - Ranked terminals intentionally materialized (no heap-streaming): terminal/mod.rs (/home/adam/projects/icydb/
    crates/icydb-core/src/db/executor/load/terminal/mod.rs:145), route test asserting this contract route/tests/
    budget.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/tests/budget.rs:135).

  ———

  8. Commit & Storage Audit

  Integrity strengths

  - Marker-first authority and replay ownership are explicit: commit/mod.rs (/home/adam/projects/icydb/crates/icydb-
    core/src/db/commit/mod.rs:4).
  - Commit window orchestration is centralized with preflight + marker + apply guard:
      - open: mutation/commit_window.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/mutation/
        commit_window.rs:155)
      - apply: mutation/commit_window.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/mutation/
        commit_window.rs:174)
  - Marker decode/shape validation with bounded decoding:
      - decode/load: commit/store.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/commit/store.rs:52)
      - shape validation: commit/validate.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/commit/
        validate.rs:19)

  Corruption message/classification inconsistencies

  - Mixed formatting: standard helpers exist commit/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/commit/
    mod.rs:47), but some paths emit raw strings directly (example commit/store.rs (/home/adam/projects/icydb/crates/
    icydb-core/src/db/commit/store.rs:57), commit/prepare.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
    commit/prepare.rs:72)).
  - Same “oversize marker” condition is classified differently:
      - write-time serialize path: Unsupported style (store_unsupported) commit/store.rs (/home/adam/projects/icydb/
        crates/icydb-core/src/db/commit/store.rs:43)
      - persisted decode path: Corruption (store_corruption) commit/store.rs (/home/adam/projects/icydb/crates/icydb-
        core/src/db/commit/store.rs:57)

  Potential partial-commit risks

  - No high-severity marker-protocol break observed.
  - Low risk: generation guard protects index stores between preflight/apply, but no symmetric data-store generation
    guard: mutation/commit_window.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/mutation/
    commit_window.rs:232).

  Encoding duplication / performance waste

  - Data-key decoding logic exists in both validation and decode helpers: commit/validate.rs (/home/adam/projects/
    icydb/crates/icydb-core/src/db/commit/validate.rs:60), commit/decode.rs (/home/adam/projects/icydb/crates/icydb-
    core/src/db/commit/decode.rs:58).
  - Recovery rebuild materializes all rows and copies key/row bytes into marker-like ops: commit/recovery.rs (/home/
    adam/projects/icydb/crates/icydb-core/src/db/commit/recovery.rs:172), commit/recovery.rs (/home/adam/projects/
    icydb/crates/icydb-core/src/db/commit/recovery.rs:189).

  ———

  9. Complexity Report

  Quantitative metrics (db/)

  - Files: 175
  - Total LOC: 64,407
  - Submodule LOC:
      - executor: 35,705
      - query: 14,150
      - index: 5,990
      - commit: 2,833
      - data: 1,392
      - relation: 808
  - Function-size heuristic (all db .rs):
      - count: 2,445
      - average length: 24.08 lines
      - >150 lines: 14
  - Function-size heuristic (excluding tests):
      - count: 1,779
      - average length: 18.31 lines
      - >150 lines: 5

  Files >800 LOC

  - Non-test hotspots:
      - executor/load/aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
        mod.rs:1) (1001)
      - executor/route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:1)
        (949)
      - executor/ordered_key_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
        ordered_key_stream.rs:1) (1109)
  - Large test hotspots:
      - executor/tests/aggregate.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/tests/aggregate.rs:1)
        (7274)
      - executor/tests/semantics.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/tests/semantics.rs:1)
        (2328)
      - executor/tests/pagination/index_range.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/tests/
        pagination/index_range.rs:1) (2056)

  Cyclomatic hotspot proxies (control-flow density)

  - High absolute + dense:
      - executor/route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:155)
      - executor/load/aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
        mod.rs:295)
      - executor/ordered_key_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
        ordered_key_stream.rs:335)
      - executor/query_bridge/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/query_bridge/
        mod.rs:104)
      - executor/access_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/access_stream.rs:474)

  Pattern duplication hotspots

  - Residual retry duplication: load/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/
    mod.rs:331), load/aggregate/distinct.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/
    aggregate/distinct.rs:96)
  - Materialized wrapper duplication: load/terminal/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
    executor/load/terminal/mod.rs:136), load/aggregate/helpers.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
    executor/load/aggregate/helpers.rs:81), load/aggregate/projection.rs (/home/adam/projects/icydb/crates/icydb-core/
    src/db/executor/load/aggregate/projection.rs:80)

  ———

  10. Performance Risks

  Critical

  - Eager key materialization before row reads (Vec<DataKey> then Vec<DataRow>), which limits true streaming and
    increases peak memory:
      - executor/physical_path.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/physical_path.rs:43)
      - executor/context.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/context.rs:112)
  - O(n²) insertion-based ranking/sorting in materialized terminal paths:
      - load/terminal/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/terminal/mod.rs:274)
      - load/aggregate/helpers.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
        helpers.rs:283)
  - O(n²) distinct value extraction by linear membership scan:
      - load/aggregate/distinct.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
        distinct.rs:146)
      - load/aggregate/projection.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
        projection.rs:181)

  Moderate

  - Duplicate full retry scans for residual index-range shapes in two paths:
      - load/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/mod.rs:331)
      - load/aggregate/distinct.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
        distinct.rs:96)
  - Repeated strict compile work across route + aggregate descriptor:
      - route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:642)
      - load/aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:275)
  - Startup recovery rebuild allocates full per-store row vectors and copies bytes:
      - commit/recovery.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/commit/recovery.rs:172)

  Minor

  - Comparator object stored but unused in DistinctOrderedKeyStream:
      - ordered_key_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/ordered_key_stream.rs:166)
  - Repeated to_vec/cloning of spec arrays in hot entrypoints:
      - load/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/mod.rs:260)
      - load/aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:337)
      - load/aggregate/distinct.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
        distinct.rs:60)

  ———

  11. Test Gaps

  Strong coverage areas

  - Route contract and precedence matrices: route/tests/load.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
    executor/route/tests/load.rs:4), route/tests/aggregate.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
    executor/route/tests/aggregate.rs:284), route/tests/mutation.rs (/home/adam/projects/icydb/crates/icydb-core/src/
    db/executor/route/tests/mutation.rs:4).
  - Fast/fallback parity in pagination and index-range pushdown: pagination/index_range.rs (/home/adam/projects/icydb/
    crates/icydb-core/src/db/executor/tests/pagination/index_range.rs:1287), pagination/distinct.rs (/home/adam/
    projects/icydb/crates/icydb-core/src/db/executor/tests/pagination/distinct.rs:552), pagination/composite_budget.rs
    (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/tests/pagination/composite_budget.rs:375).
  - Aggregate parity and strict-prefilter behavior: tests/aggregate.rs (/home/adam/projects/icydb/crates/icydb-core/
    src/db/executor/tests/aggregate.rs:6656), tests/aggregate.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
    executor/tests/aggregate.rs:7001).
  - Commit corruption/recovery paths: commit/store.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/commit/
    store.rs:199), commit/tests.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/commit/tests.rs:1232).

  Missing/weak

  - No direct executor tests explicitly asserting compile-mode boundary (ConservativeSubset vs StrictAllOrNone)
    despite behavior-critical branching in:
      - load/execute.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/execute.rs:233)
      - route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:642)
  - DISTINCT end-to-end relies heavily on parity tests, but comparator/invariant behavior is mostly unit-level in
    stream tests; there is no dedicated end-to-end negative test for misordered/non-monotonic upstream key streams in
    load pipeline.
  - Redundant overlap risk: very large suites in executor/tests/aggregate.rs (/home/adam/projects/icydb/crates/icydb-
    core/src/db/executor/tests/aggregate.rs:1) and pagination files can mask conceptual duplication and increase
    maintenance load.

  ———

  12. Architecture Health Score

  Drift evidence vs 0.24.7

  - db/ delta is substantial: 156 files changed, +23,495 / -9,635.
  - Routing was centralized structurally:
      - Added route subsystem files (11 files under executor/route).
      - Old route-heavy modules removed/refactored (notably legacy load/route.rs and composite_stream.rs per git
        diff).
  - This is a net move toward explicit routing contracts, but execution still has shadow flows (count_distinct_by,
    terminal materialized wrappers, delete materialized path).

  Scores (1–10)

  - Overall architecture health: 7/10
  - Execution clarity: 6/10
  - Optimization stability: 7/10
  - Maintainability risk (10 = highest risk): 6/10

  ———

  13. Refactor Roadmap

  ### Tier 1 (Pre-0.30 hardening)

  1. Residual-retry unification
     Files: load/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/mod.rs:331), load/aggregate/
     distinct.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/distinct.rs:96)
     Risk: Medium
     Why: Two near-duplicate retry implementations can drift in scan accounting/cursor parity.
     Expected gain: Single correctness boundary for pushdown fallback.
  2. Remove query→index raw-key coupling in cursor contracts
     Files: query/contracts/cursor.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/contracts/
     cursor.rs:4), query/plan/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/mod.rs:12),
     executor/load/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/mod.rs:245)
     Risk: High (cursor compatibility)
     Why: Current layering breach pushes index internals into query contract surface.
     Expected gain: Strict layering and lower refactor friction in index internals.
  3. Single-source strict predicate compile decision
     Files: route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/planner.rs:642), load/
     aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:275)
     Risk: Medium
     Why: Duplicate strict-compile checks create drift risk.
     Expected gain: Route/aggregate mode parity and lower overhead.
  4. DISTINCT invariant tightening
     Files: ordered_key_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
     ordered_key_stream.rs:163), load/execute.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/
     execute.rs:241)
     Risk: Low
     Why: Comparator is unused in distinct wrapper; invariant is implicit.
     Expected gain: Stronger failure mode for upstream order violations.

  ### Tier 2 (Structural cleanup)

  1. Consolidate materialized terminal wrappers behind shared adapters
     Files: load/terminal/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/terminal/
     mod.rs:122), load/aggregate/helpers.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/
     aggregate/helpers.rs:83), load/aggregate/projection.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/
     executor/load/aggregate/projection.rs:80)
     Risk: Medium
     Why: Wrapper duplication is high and already drifting in behavior/metrics handling.
     Expected gain: Smaller API surface and easier parity enforcement.
  2. Split route planner into focused modules
     Files: executor/route/planner.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/route/
     planner.rs:155)
     Risk: Medium
     Why: One 949-line planner is the primary complexity hotspot.
     Expected gain: Lower cognitive load, clearer ownership of mode/hints/capabilities.
  3. Split aggregate orchestrator by concern
     Files: load/aggregate/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/
     mod.rs:295)
     Risk: Medium
     Why: 1001-line mixed orchestration/dispatch/impl file causes cross-cutting changes.
     Expected gain: Easier review and safer optimization work.

  ### Tier 3 (Architectural rework)

  1. True pull-based key streaming (remove eager key vectors)
     Files: executor/physical_path.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
     physical_path.rs:43), executor/context.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
     context.rs:112), index/store/lookup.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/index/store/
     lookup.rs:19), executor/ordered_key_stream.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/
     ordered_key_stream.rs:28)
     Risk: High
     Why: Current model is streaming-shaped but materialization-heavy.
     Expected gain: Significant memory/latency improvements on large scans.
  2. Unify load + aggregate + count_distinct execution through one route executor kernel
     Files: load/mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/mod.rs:238), load/aggregate/
     mod.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/mod.rs:295), load/aggregate/
     distinct.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/aggregate/distinct.rs:37),
     executor/fold.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/fold.rs:409)
     Risk: High
     Why: Parallel flows are the main execution-clarity and drift problem.
     Expected gain: Single canonical pipeline and lower bug surface.
  3. Move index predicate execution boundary out of store traversal
     Files: index/store/lookup.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/index/store/lookup.rs:157),
     index/predicate.rs (/home/adam/projects/icydb/crates/icydb-core/src/db/index/predicate.rs:118), load/execute.rs
     (/home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/execute.rs:224)
     Risk: High
     Why: Storage currently participates in query predicate semantics.
     Expected gain: Cleaner layer contracts and easier independent optimization.
