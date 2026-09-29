//! Module: db::executor::scan::fast_stream_route::handlers
//! Defines handler helpers for fast-stream route scans over ordered access
//! paths.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        access::ExecutableAccessPlan,
        executor::{
            AccessStreamBindings, AccessStreamExecutionPolicy,
            pipeline::contracts::{AccessScanContinuationInput, FastPathKeyResult},
            route::verify_pk_stream_fast_path_access,
            scan::fast_stream::execute_structural_fast_stream_request,
            stream::access::TraversalRuntime,
        },
    },
    error::InternalError,
    value::Value,
};

pub(super) fn execute_primary_key_fast_stream_route(
    runtime: &TraversalRuntime,
    executable_access: &ExecutableAccessPlan<'_, Value>,
    continuation: AccessScanContinuationInput<'_>,
    probe_fetch_hint: Option<usize>,
) -> Result<Option<FastPathKeyResult>, InternalError> {
    // Phase 1: validate that the routed access shape is PK-stream compatible.
    verify_pk_stream_fast_path_access(executable_access)?;

    // Phase 2: bind through the canonical structural access-stream boundary.
    Ok(Some(execute_structural_fast_stream_request(
        runtime,
        executable_access,
        AccessStreamBindings::new(&[], &[], continuation),
        AccessStreamExecutionPolicy::canonical_key_order(probe_fetch_hint),
        None,
    )?))
}
