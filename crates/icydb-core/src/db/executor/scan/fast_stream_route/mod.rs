//! Module: executor::scan::fast_stream_route
//! Responsibility: shared fast-stream route dispatch across PK/secondary/range shapes.
//! Does not own: route-specific execution binding internals.
//! Boundary: canonical dispatch over contract-owned fast-stream requests.

mod handlers;

use crate::{
    db::executor::{
        pipeline::contracts::{FastPathKeyResult, FastStreamRouteRequest},
        scan::{
            fast_stream_route::handlers::execute_primary_key_fast_stream_route,
            index_range_limit::execute_index_range_fast_stream_route,
            secondary_index::execute_secondary_index_fast_stream_route,
        },
        stream::access::TraversalRuntime,
    },
    error::InternalError,
};

/// Execute one verified fast-stream route through the structural load dispatch boundary.
pub(in crate::db::executor) fn execute_fast_stream_route(
    runtime: &TraversalRuntime,
    request: FastStreamRouteRequest<'_, '_>,
) -> Result<Option<FastPathKeyResult>, InternalError> {
    match request {
        FastStreamRouteRequest::PrimaryKey {
            plan,
            executable_access,
            stream_direction,
            probe_fetch_hint,
        } => execute_primary_key_fast_stream_route(
            runtime,
            plan,
            executable_access,
            stream_direction,
            probe_fetch_hint,
        ),
        FastStreamRouteRequest::SecondaryIndex {
            plan,
            executable_access,
            bindings,
            probe_fetch_hint,
            index_predicate_execution,
        } => execute_secondary_index_fast_stream_route(
            runtime,
            plan,
            executable_access,
            bindings,
            probe_fetch_hint,
            index_predicate_execution,
        ),
        FastStreamRouteRequest::IndexRangeLimitPushdown {
            plan,
            executable_access,
            index_range_spec,
            continuation,
            effective_fetch,
            index_predicate_execution,
        } => execute_index_range_fast_stream_route(
            runtime,
            plan,
            executable_access,
            index_range_spec,
            continuation,
            effective_fetch,
            index_predicate_execution,
        ),
    }
}
