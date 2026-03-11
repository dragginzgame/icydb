Codex Response :


• 1. Classification Table

  Base analyzed: /home/adam/projects/icydb/crates/icydb-core/src/db/executor/load

  Directories

  | Path (relative) | Category |
  |---|---|
  | / | SHARED/UTILITY |
  | contracts/ | SHARED/UTILITY |
  | contracts/grouped/ | SHARED/UTILITY |
  | contracts/grouped/route_stage/ | SHARED/UTILITY |
  | entrypoints/ | PIPELINE |
  | entrypoints/pipeline/ | PIPELINE |
  | entrypoints/pipeline/orchestrate/ | PIPELINE |
  | entrypoints/scalar/ | PIPELINE |
  | execute/ | PIPELINE |
  | execute/contracts/ | SHARED/UTILITY |
  | execute/fast_path/ | PIPELINE |
  | fast_stream/ | SCAN |
  | fast_stream_route/ | SCAN |
  | grouped_distinct/ | AGGREGATE |
  | grouped_fold/ | AGGREGATE |
  | grouped_fold/candidate_rows/ | AGGREGATE |
  | grouped_output/ | AGGREGATE |
  | grouped_route/ | PIPELINE |
  | grouped_runtime/ | PIPELINE |
  | grouped_runtime/continuation/ | PIPELINE |
  | projection/ | SHARED/UTILITY |
  | projection/eval/ | SHARED/UTILITY |
  | projection/eval/operators/ | SHARED/UTILITY |
  | projection/eval/operators/binary/ | SHARED/UTILITY |
  | terminal/ | TERMINAL |
  | terminal/ranking/ | TERMINAL |
  | terminal/ranking/materialized/ | TERMINAL |

  Files

  | Path(s) (relative) | Category |
  |---|---|
  | mod.rs, context.rs | SHARED/UTILITY |
  | contracts/mod.rs | SHARED/UTILITY |
  | contracts/grouped/{mod.rs,stages.rs} | SHARED/UTILITY |
  | contracts/grouped/route_stage/{mod.rs,payload.rs,projection.rs} | SHARED/UTILITY |
  | entrypoints/{mod.rs,grouped.rs} | PIPELINE |
  | entrypoints/pipeline/mod.rs | PIPELINE |
  | entrypoints/pipeline/orchestrate/{mod.rs,state.rs,payload.rs,guards.rs} | PIPELINE |
  | entrypoints/scalar/{mod.rs,surface.rs,hints.rs} | PIPELINE |
  | execute/mod.rs | PIPELINE |
  | execute/fast_path/{mod.rs,strategy.rs} | PIPELINE |
  | execute/contracts/{mod.rs,inputs.rs,outcomes.rs,stream.rs} | SHARED/UTILITY |
  | fast_stream/{mod.rs,tests.rs} | SCAN |
  | fast_stream_route/{mod.rs,handlers.rs} | SCAN |
  | pk_stream.rs, secondary_index.rs, index_range_limit.rs | SCAN |
  | grouped_route/{mod.rs,resolve.rs,metrics.rs} | PIPELINE |
  | grouped_runtime/{mod.rs,runtime.rs} | PIPELINE |
  | grouped_runtime/continuation/{mod.rs,capabilities.rs,context.rs,window.rs} | PIPELINE |
  | grouped_distinct/{mod.rs,aggregate.rs,paging.rs} | AGGREGATE |
  | grouped_fold/{mod.rs,engine_init.rs,global_distinct.rs,ingest.rs,page_finalize.rs} | AGGREGATE |
  | grouped_fold/candidate_rows/{mod.rs,sink.rs} | AGGREGATE |
  | grouped_having.rs | AGGREGATE |
  | grouped_output/{mod.rs,finalize.rs,projection.rs} | AGGREGATE |
  | terminal/{mod.rs,bytes.rs,tests.rs} | TERMINAL |
  | terminal/ranking/{mod.rs,by_slot.rs,take.rs} | TERMINAL |
  | terminal/ranking/materialized/{mod.rs,projections.rs} | TERMINAL |
  | page.rs | TERMINAL |
  | projection/{mod.rs,grouped.rs,materialize.rs,tests.rs} | SHARED/UTILITY |
  | projection/eval/mod.rs | SHARED/UTILITY |
  | projection/eval/operators/{mod.rs,unary.rs} | SHARED/UTILITY |
  | projection/eval/operators/binary/{mod.rs,comparison.rs} | SHARED/UTILITY |

  ———

  2. Proposed Directory Tree

  executor/
    scan/
      mod.rs
      pk_stream.rs
      secondary_index.rs
      index_range_limit.rs
      fast_stream/
        mod.rs
        tests.rs
      fast_stream_route/
        mod.rs
        handlers.rs

    pipeline/
      mod.rs
      entrypoints/
        mod.rs
        grouped.rs
        scalar/
          mod.rs
          hints.rs
          surface.rs
        pipeline/
          mod.rs
          orchestrate/
            mod.rs
            state.rs
            payload.rs
            guards.rs
      execute/
        mod.rs
        fast_path/
          mod.rs
          strategy.rs
      grouped_route/
        mod.rs
        resolve.rs
        metrics.rs
      grouped_runtime/
        mod.rs
        runtime.rs
        continuation/
          mod.rs
          capabilities.rs
          context.rs
          window.rs

    aggregate/
      ...existing aggregate modules...
      load/
        mod.rs
        grouped_distinct/
          mod.rs
          aggregate.rs
          paging.rs
        grouped_fold/
          mod.rs
          engine_init.rs
          global_distinct.rs
          ingest.rs
          page_finalize.rs
          candidate_rows/
            mod.rs
            sink.rs
        grouped_having.rs
        grouped_output/
          mod.rs
          finalize.rs
          projection.rs

    terminal/
      mod.rs
      page.rs
      ranking/
        mod.rs
        by_slot.rs
        take.rs
        materialized/
          mod.rs
          projections.rs
      bytes.rs
      tests.rs

    shared/
      mod.rs
      load_context.rs
      load_contracts/
        mod.rs
        grouped/
          mod.rs
          stages.rs
          route_stage/
            mod.rs
            payload.rs
            projection.rs
      execution_contracts/
        mod.rs
        inputs.rs
        outcomes.rs
        stream.rs
      projection/
        mod.rs
        grouped.rs
        materialize.rs
        tests.rs
        eval/
          mod.rs
          operators/
            mod.rs
            unary.rs
            binary/
              mod.rs
              comparison.rs

    load/
      mod.rs   # temporary compatibility facade (re-export only), then remove

  ———

  3. File Move List

  | Move | Why this layer | Import changes | Public/private |
  |---|---|---|---|
  | load/pk_stream.rs, secondary_index.rs, index_range_limit.rs -> executor/scan/ | Physical key/index/range scan
  execution helpers | executor::load::* scan imports -> executor::scan::*; keep shared contracts from
  executor::shared::* | mod scan private to executor; functions stay pub(in crate::db::executor) |
  | load/fast_stream/** -> executor/scan/fast_stream/** | Raw fast stream execution boundary | same as above | private
  module |
  | load/fast_stream_route/** -> executor/scan/fast_stream_route/** | Scan-route dispatch for PK/index/range fast
  paths | update route request/type paths to scan::fast_stream_route::* | private module |
  | load/entrypoints/** -> executor/pipeline/entrypoints/** | Entrypoint orchestration and load-mode surface | path
  updates from load::entrypoints to pipeline::entrypoints; external callers can keep old path via facade | keep
  existing pub(in crate::db) entry methods |
  | load/execute/mod.rs, load/execute/fast_path/** -> executor/pipeline/execute/** | Pipeline execution orchestration
  and fast-path selection policy | update imports to pipeline::execute::*, scan::*, and shared::execution_contracts::*
  | private module; selected re-exports for tests |
  | load/grouped_route/** -> executor/pipeline/grouped_route/** | Route/handoff staging for grouped execution |
  imports from load::* to pipeline::* + shared::* | private module |
  | load/grouped_runtime/** -> executor/pipeline/grouped_runtime/** | Runtime grouped continuation/paging context used
  by orchestration | update grouped runtime paths | private module |
  | load/grouped_distinct/**, load/grouped_fold/**, load/grouped_having.rs -> executor/aggregate/load/** | GROUP BY /
  DISTINCT runtime engines | update executor::load::* references to executor::aggregate::load::* and shared::* |
  private to aggregate runtime |
  | load/grouped_output/** -> executor/aggregate/load/grouped_output/** | Aggregate-coupled output shaping/
  finalization | imports to aggregate::load::* + shared projection helpers | private |
  | load/terminal/**, load/page.rs -> executor/terminal/** | Final reducers and paging materialization | update paths
  to terminal::*; keep contracts from shared | terminal API methods keep current visibility |
  | load/contracts/** -> executor/shared/load_contracts/** | Cross-layer contracts used by pipeline, scan, aggregate,
  terminal | all modules import from shared::load_contracts | mostly pub(in crate::db::executor) |
  | load/execute/contracts/** -> executor/shared/execution_contracts/** | Shared execution input/output/stream
  contracts | all modules import from shared | pub(in crate::db::executor) |
  | load/projection/** -> executor/shared/projection/** | Shared projection evaluation used by scalar and grouped
  flows | grouped/scalar callsites import from shared projection module | private module with selected pub(in
  crate::db::executor) items |
  | load/context.rs -> executor/shared/load_context.rs | LoadExecutor construction and utility helpers reused across
  layers | update constructor/helper imports | shared internal |
  | load/mod.rs -> compatibility facade (re-export) | Minimize churn and keep mechanical move | temporary re-export of
  moved types/modules | temporary; remove in follow-up patch |

  ———

  4. Required Module Boundary Adjustments

  1. Move cross-layer types into shared contracts first.
     LoadExecutor, CursorPage, GroupedCursorPage, PageCursor, FastPathKeyResult, ExecutionInputs*,
     ResolvedExecutionKeyStream, ExecutionOutcomeMetrics should live in executor/shared/*, not pipeline/scan/
     aggregate.
  2. Normalize visibility scopes.
     Current pub(in crate::db::executor::load::...) visibilities will break on move. Use pub(in crate::db::executor)
     for cross-layer shared items and tighter pub(super) for leaf internals.
  3. Keep planner/executor boundary unchanged.
     All moved modules must keep consuming the same already-planned DTOs (ExecutablePlan, AccessPlannedQuery, grouped
     handoff projections). No planner logic migration.
  4. Keep scan isolation.
     Pipeline should call scan through request contracts (FastStreamRouteRequest style), not direct PK/index/range
     internals.
  5. Update structural tests that hardcode executor/load paths.
     Examples: /home/adam/projects/icydb/crates/icydb-core/src/db/executor/tests/load_structure.rs.

  ———

  5. Potential Risks / Follow-Up Refactors

  1. Visibility-path breakage is the biggest mechanical risk.
     Rust pub(in ...) paths tied to load will fail compile immediately after move unless normalized first.
  2. Likely cycle: pipeline <-> aggregate.
     Cause: grouped stage traits/payloads and finalization metrics crossing both layers.
     Fix: keep these contracts in executor/shared.
  3. Likely cycle: pipeline <-> scan.
     Cause: fast-path result/input contracts.
     Fix: keep scan IO contracts in executor/shared/execution_contracts.
  4. Potential cycle: aggregate <-> terminal if grouped projection helpers are placed in terminal.
     Fix: keep projection evaluator in executor/shared/projection and keep aggregate output adapters aggregate-owned.
  5. Files to split further (post-move, still mechanical):

  - /home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/terminal/bytes.rs: mixes scan dispatch and
    terminal reduction.
  - /home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/grouped_fold/mod.rs: mixes stream build, fold
    orchestration, and error mapping.
  - /home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/grouped_route/resolve.rs: mixes route resolution,
    continuation setup, and trace assembly.
  - /home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/context.rs: constructor + recovered context +
    aggregate slot helpers.

  If you want, next step I can produce a strictly mechanical migration sequence (patch order) that minimizes breakage
  and keeps cargo check green at each step.


------------------------------------------



• 1. Classification Table

  Base analyzed: /home/adam/projects/icydb/crates/icydb-core/src/db/executor/load

  Directories

  | Path (relative) | Category |
  |---|---|
  | / | SHARED/UTILITY |
  | contracts/ | SHARED/UTILITY |
  | contracts/grouped/ | SHARED/UTILITY |
  | contracts/grouped/route_stage/ | SHARED/UTILITY |
  | entrypoints/ | PIPELINE |
  | entrypoints/pipeline/ | PIPELINE |
  | entrypoints/pipeline/orchestrate/ | PIPELINE |
  | entrypoints/scalar/ | PIPELINE |
  | execute/ | PIPELINE |
  | execute/contracts/ | SHARED/UTILITY |
  | execute/fast_path/ | PIPELINE |
  | fast_stream/ | SCAN |
  | fast_stream_route/ | SCAN |
  | grouped_distinct/ | AGGREGATE |
  | grouped_fold/ | AGGREGATE |
  | grouped_fold/candidate_rows/ | AGGREGATE |
  | grouped_output/ | AGGREGATE |
  | grouped_route/ | PIPELINE |
  | grouped_runtime/ | PIPELINE |
  | grouped_runtime/continuation/ | PIPELINE |
  | projection/ | SHARED/UTILITY |
  | projection/eval/ | SHARED/UTILITY |
  | projection/eval/operators/ | SHARED/UTILITY |
  | projection/eval/operators/binary/ | SHARED/UTILITY |
  | terminal/ | TERMINAL |
  | terminal/ranking/ | TERMINAL |
  | terminal/ranking/materialized/ | TERMINAL |

  Files

  | Path(s) (relative) | Category |
  |---|---|
  | mod.rs, context.rs | SHARED/UTILITY |
  | contracts/mod.rs | SHARED/UTILITY |
  | contracts/grouped/{mod.rs,stages.rs} | SHARED/UTILITY |
  | contracts/grouped/route_stage/{mod.rs,payload.rs,projection.rs} | SHARED/UTILITY |
  | entrypoints/{mod.rs,grouped.rs} | PIPELINE |
  | entrypoints/pipeline/mod.rs | PIPELINE |
  | entrypoints/pipeline/orchestrate/{mod.rs,state.rs,payload.rs,guards.rs} | PIPELINE |
  | entrypoints/scalar/{mod.rs,surface.rs,hints.rs} | PIPELINE |
  | execute/mod.rs | PIPELINE |
  | execute/fast_path/{mod.rs,strategy.rs} | PIPELINE |
  | execute/contracts/{mod.rs,inputs.rs,outcomes.rs,stream.rs} | SHARED/UTILITY |
  | fast_stream/{mod.rs,tests.rs} | SCAN |
  | fast_stream_route/{mod.rs,handlers.rs} | SCAN |
  | pk_stream.rs, secondary_index.rs, index_range_limit.rs | SCAN |
  | grouped_route/{mod.rs,resolve.rs,metrics.rs} | PIPELINE |
  | grouped_runtime/{mod.rs,runtime.rs} | PIPELINE |
  | grouped_runtime/continuation/{mod.rs,capabilities.rs,context.rs,window.rs} | PIPELINE |
  | grouped_distinct/{mod.rs,aggregate.rs,paging.rs} | AGGREGATE |
  | grouped_fold/{mod.rs,engine_init.rs,global_distinct.rs,ingest.rs,page_finalize.rs} | AGGREGATE |
  | grouped_fold/candidate_rows/{mod.rs,sink.rs} | AGGREGATE |
  | grouped_having.rs | AGGREGATE |
  | grouped_output/{mod.rs,finalize.rs,projection.rs} | AGGREGATE |
  | terminal/{mod.rs,bytes.rs,tests.rs} | TERMINAL |
  | terminal/ranking/{mod.rs,by_slot.rs,take.rs} | TERMINAL |
  | terminal/ranking/materialized/{mod.rs,projections.rs} | TERMINAL |
  | page.rs | TERMINAL |
  | projection/{mod.rs,grouped.rs,materialize.rs,tests.rs} | SHARED/UTILITY |
  | projection/eval/mod.rs | SHARED/UTILITY |
  | projection/eval/operators/{mod.rs,unary.rs} | SHARED/UTILITY |
  | projection/eval/operators/binary/{mod.rs,comparison.rs} | SHARED/UTILITY |

  ———

  2. Proposed Directory Tree

  executor/
    scan/
      mod.rs
      pk_stream.rs
      secondary_index.rs
      index_range_limit.rs
      fast_stream/
        mod.rs
        tests.rs
      fast_stream_route/
        mod.rs
        handlers.rs

    pipeline/
      mod.rs
      entrypoints/
        mod.rs
        grouped.rs
        scalar/
          mod.rs
          hints.rs
          surface.rs
        pipeline/
          mod.rs
          orchestrate/
            mod.rs
            state.rs
            payload.rs
            guards.rs
      execute/
        mod.rs
        fast_path/
          mod.rs
          strategy.rs
      grouped_route/
        mod.rs
        resolve.rs
        metrics.rs
      grouped_runtime/
        mod.rs
        runtime.rs
        continuation/
          mod.rs
          capabilities.rs
          context.rs
          window.rs

    aggregate/
      ...existing aggregate modules...
      load/
        mod.rs
        grouped_distinct/
          mod.rs
          aggregate.rs
          paging.rs
        grouped_fold/
          mod.rs
          engine_init.rs
          global_distinct.rs
          ingest.rs
          page_finalize.rs
          candidate_rows/
            mod.rs
            sink.rs
        grouped_having.rs
        grouped_output/
          mod.rs
          finalize.rs
          projection.rs

    terminal/
      mod.rs
      page.rs
      ranking/
        mod.rs
        by_slot.rs
        take.rs
        materialized/
          mod.rs
          projections.rs
      bytes.rs
      tests.rs

    shared/
      mod.rs
      load_context.rs
      load_contracts/
        mod.rs
        grouped/
          mod.rs
          stages.rs
          route_stage/
            mod.rs
            payload.rs
            projection.rs
      execution_contracts/
        mod.rs
        inputs.rs
        outcomes.rs
        stream.rs
      projection/
        mod.rs
        grouped.rs
        materialize.rs
        tests.rs
        eval/
          mod.rs
          operators/
            mod.rs
            unary.rs
            binary/
              mod.rs
              comparison.rs

    load/
      mod.rs   # temporary compatibility facade (re-export only), then remove

  ———

  3. File Move List

  | Move | Why this layer | Import changes | Public/private |
  |---|---|---|---|
  | load/pk_stream.rs, secondary_index.rs, index_range_limit.rs -> executor/scan/ | Physical key/index/range scan
  execution helpers | executor::load::* scan imports -> executor::scan::*; keep shared contracts from
  executor::shared::* | mod scan private to executor; functions stay pub(in crate::db::executor) |
  | load/fast_stream/** -> executor/scan/fast_stream/** | Raw fast stream execution boundary | same as above | private
  module |
  | load/fast_stream_route/** -> executor/scan/fast_stream_route/** | Scan-route dispatch for PK/index/range fast
  paths | update route request/type paths to scan::fast_stream_route::* | private module |
  | load/entrypoints/** -> executor/pipeline/entrypoints/** | Entrypoint orchestration and load-mode surface | path
  updates from load::entrypoints to pipeline::entrypoints; external callers can keep old path via facade | keep
  existing pub(in crate::db) entry methods |
  | load/execute/mod.rs, load/execute/fast_path/** -> executor/pipeline/execute/** | Pipeline execution orchestration
  and fast-path selection policy | update imports to pipeline::execute::*, scan::*, and shared::execution_contracts::*
  | private module; selected re-exports for tests |
  | load/grouped_route/** -> executor/pipeline/grouped_route/** | Route/handoff staging for grouped execution |
  imports from load::* to pipeline::* + shared::* | private module |
  | load/grouped_runtime/** -> executor/pipeline/grouped_runtime/** | Runtime grouped continuation/paging context used
  by orchestration | update grouped runtime paths | private module |
  | load/grouped_distinct/**, load/grouped_fold/**, load/grouped_having.rs -> executor/aggregate/load/** | GROUP BY /
  DISTINCT runtime engines | update executor::load::* references to executor::aggregate::load::* and shared::* |
  private to aggregate runtime |
  | load/grouped_output/** -> executor/aggregate/load/grouped_output/** | Aggregate-coupled output shaping/
  finalization | imports to aggregate::load::* + shared projection helpers | private |
  | load/terminal/**, load/page.rs -> executor/terminal/** | Final reducers and paging materialization | update paths
  to terminal::*; keep contracts from shared | terminal API methods keep current visibility |
  | load/contracts/** -> executor/shared/load_contracts/** | Cross-layer contracts used by pipeline, scan, aggregate,
  terminal | all modules import from shared::load_contracts | mostly pub(in crate::db::executor) |
  | load/execute/contracts/** -> executor/shared/execution_contracts/** | Shared execution input/output/stream
  contracts | all modules import from shared | pub(in crate::db::executor) |
  | load/projection/** -> executor/shared/projection/** | Shared projection evaluation used by scalar and grouped
  flows | grouped/scalar callsites import from shared projection module | private module with selected pub(in
  crate::db::executor) items |
  | load/context.rs -> executor/shared/load_context.rs | LoadExecutor construction and utility helpers reused across
  layers | update constructor/helper imports | shared internal |
  | load/mod.rs -> compatibility facade (re-export) | Minimize churn and keep mechanical move | temporary re-export of
  moved types/modules | temporary; remove in follow-up patch |

  ———

  4. Required Module Boundary Adjustments

  1. Move cross-layer types into shared contracts first.
     LoadExecutor, CursorPage, GroupedCursorPage, PageCursor, FastPathKeyResult, ExecutionInputs*,
     ResolvedExecutionKeyStream, ExecutionOutcomeMetrics should live in executor/shared/*, not pipeline/scan/
     aggregate.
  2. Normalize visibility scopes.
     Current pub(in crate::db::executor::load::...) visibilities will break on move. Use pub(in crate::db::executor)
     for cross-layer shared items and tighter pub(super) for leaf internals.
  3. Keep planner/executor boundary unchanged.
     All moved modules must keep consuming the same already-planned DTOs (ExecutablePlan, AccessPlannedQuery, grouped
     handoff projections). No planner logic migration.
  4. Keep scan isolation.
     Pipeline should call scan through request contracts (FastStreamRouteRequest style), not direct PK/index/range
     internals.
  5. Update structural tests that hardcode executor/load paths.
     Examples: /home/adam/projects/icydb/crates/icydb-core/src/db/executor/tests/load_structure.rs.

  ———

  5. Potential Risks / Follow-Up Refactors

  1. Visibility-path breakage is the biggest mechanical risk.
     Rust pub(in ...) paths tied to load will fail compile immediately after move unless normalized first.
  2. Likely cycle: pipeline <-> aggregate.
     Cause: grouped stage traits/payloads and finalization metrics crossing both layers.
     Fix: keep these contracts in executor/shared.
  3. Likely cycle: pipeline <-> scan.
     Cause: fast-path result/input contracts.
     Fix: keep scan IO contracts in executor/shared/execution_contracts.
  4. Potential cycle: aggregate <-> terminal if grouped projection helpers are placed in terminal.
     Fix: keep projection evaluator in executor/shared/projection and keep aggregate output adapters aggregate-owned.
  5. Files to split further (post-move, still mechanical):

  - /home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/terminal/bytes.rs: mixes scan dispatch and
    terminal reduction.
  - /home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/grouped_fold/mod.rs: mixes stream build, fold
    orchestration, and error mapping.
  - /home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/grouped_route/resolve.rs: mixes route resolution,
    continuation setup, and trace assembly.
  - /home/adam/projects/icydb/crates/icydb-core/src/db/executor/load/context.rs: constructor + recovered context +
    aggregate slot helpers.

  If you want, next step I can produce a strictly mechanical migration sequence (patch order) that minimizes breakage
  and keeps cargo check green at each step.
