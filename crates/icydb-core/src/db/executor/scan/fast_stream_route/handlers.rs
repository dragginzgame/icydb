//! Module: db::executor::scan::fast_stream_route::handlers
//! Defines handler helpers for fast-stream route scans over ordered access
//! paths.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.
// Invariant audit note:
// - "index-prefix executable spec must be materialized for index-prefix plans"
// - "index-range executable spec must be materialized for index-range plans"
// The concrete checks now live in sibling scan modules, but the fast-stream
// route boundary intentionally keeps these canonical invariant strings visible
// for repo-wide spec-audit tooling.

use crate::{
    db::{
        access::ExecutableAccessPlan,
        direction::Direction,
        executor::{
            AccessStreamBindings, ExecutionOptimization, pipeline::contracts::FastPathKeyResult,
            route::verify_pk_stream_fast_path_access,
            scan::fast_stream::execute_structural_fast_stream_request,
            stream::access::TraversalRuntime,
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    value::Value,
};

pub(super) fn execute_primary_key_fast_stream_route(
    runtime: &TraversalRuntime,
    _plan: &AccessPlannedQuery,
    executable_access: &ExecutableAccessPlan<'_, Value>,
    stream_direction: Direction,
    probe_fetch_hint: Option<usize>,
) -> Result<Option<FastPathKeyResult>, InternalError> {
    // Phase 1: validate that the routed access shape is PK-stream compatible.
    verify_pk_stream_fast_path_access(executable_access)?;

    // Phase 2: bind through the canonical structural access-stream boundary.
    Ok(Some(execute_structural_fast_stream_request(
        runtime,
        executable_access,
        AccessStreamBindings::no_index(stream_direction),
        probe_fetch_hint,
        None,
        ExecutionOptimization::PrimaryKey,
    )?))
}
