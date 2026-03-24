//! Module: executor::scan::fast_stream_route
//! Responsibility: shared fast-stream route dispatch across PK/secondary/range shapes.
//! Does not own: route-specific execution binding internals.
//! Boundary: route request contracts and canonical fast-stream route kind dispatch.

mod handlers;

use crate::{
    db::{
        direction::Direction,
        executor::{
            AccessScanContinuationInput, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            pipeline::contracts::FastPathKeyResult,
            scan::{
                fast_stream_route::handlers::execute_primary_key_fast_stream_route,
                index_range_limit::execute_index_range_fast_stream_route,
                secondary_index::execute_secondary_index_fast_stream_route,
            },
            stream::access::StructuralTraversalRuntime,
        },
        index::predicate::IndexPredicateExecution,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
};

///
/// FastStreamRouteKind
///
/// Canonical fast-stream route discriminator used by shared load adapters.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum FastStreamRouteKind {
    PrimaryKey,
    SecondaryIndex,
    IndexRangeLimitPushdown,
}

///
/// FastStreamRouteRequest
///
/// Route-specific stream binding payload consumed by shared fast-stream dispatch.
///

pub(in crate::db::executor) enum FastStreamRouteRequest<'a> {
    PrimaryKey {
        plan: &'a AccessPlannedQuery,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
    },
    SecondaryIndex {
        plan: &'a AccessPlannedQuery,
        index_prefix_spec: Option<&'a LoweredIndexPrefixSpec>,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    },
    IndexRangeLimitPushdown {
        plan: &'a AccessPlannedQuery,
        index_range_spec: Option<&'a LoweredIndexRangeSpec>,
        continuation: AccessScanContinuationInput<'a>,
        effective_fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    },
}

/// Execute one verified fast-stream route through the structural load dispatch boundary.
pub(in crate::db::executor) fn execute_fast_stream_route(
    runtime: &StructuralTraversalRuntime,
    route_kind: FastStreamRouteKind,
    request: FastStreamRouteRequest<'_>,
) -> Result<Option<FastPathKeyResult>, InternalError> {
    match (route_kind, request) {
        (
            FastStreamRouteKind::PrimaryKey,
            FastStreamRouteRequest::PrimaryKey {
                plan,
                stream_direction,
                probe_fetch_hint,
            },
        ) => {
            execute_primary_key_fast_stream_route(runtime, plan, stream_direction, probe_fetch_hint)
        }
        (
            FastStreamRouteKind::SecondaryIndex,
            FastStreamRouteRequest::SecondaryIndex {
                plan,
                index_prefix_spec,
                stream_direction,
                probe_fetch_hint,
                index_predicate_execution,
            },
        ) => execute_secondary_index_fast_stream_route(
            runtime,
            plan,
            index_prefix_spec,
            stream_direction,
            probe_fetch_hint,
            index_predicate_execution,
        ),
        (
            FastStreamRouteKind::IndexRangeLimitPushdown,
            FastStreamRouteRequest::IndexRangeLimitPushdown {
                plan,
                index_range_spec,
                continuation,
                effective_fetch,
                index_predicate_execution,
            },
        ) => execute_index_range_fast_stream_route(
            runtime,
            plan,
            index_range_spec,
            continuation,
            effective_fetch,
            index_predicate_execution,
        ),
        _ => Err(InternalError::fast_stream_route_kind_request_match_required()),
    }
}
