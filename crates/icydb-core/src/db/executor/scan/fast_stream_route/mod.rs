//! Module: executor::scan::fast_stream_route
//! Responsibility: shared fast-stream route dispatch across PK/secondary/range shapes.
//! Does not own: route-specific execution binding internals.
//! Boundary: route request contracts and canonical fast-stream route kind dispatch.

mod handlers;

use crate::{
    db::{
        Context,
        direction::Direction,
        executor::{
            AccessScanContinuationInput, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            pipeline::contracts::{FastPathKeyResult, LoadExecutor},
        },
        index::predicate::IndexPredicateExecution,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
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

pub(in crate::db::executor) enum FastStreamRouteRequest<'a, K> {
    PrimaryKey {
        plan: &'a AccessPlannedQuery<K>,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
    },
    SecondaryIndex {
        plan: &'a AccessPlannedQuery<K>,
        index_prefix_spec: Option<&'a LoweredIndexPrefixSpec>,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    },
    IndexRangeLimitPushdown {
        plan: &'a AccessPlannedQuery<K>,
        index_range_spec: Option<&'a LoweredIndexRangeSpec>,
        continuation: AccessScanContinuationInput<'a>,
        effective_fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    },
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Execute one verified fast-stream route through the shared load dispatch boundary.
    pub(in crate::db::executor) fn execute_fast_stream_route(
        ctx: &Context<'_, E>,
        route_kind: FastStreamRouteKind,
        request: FastStreamRouteRequest<'_, E::Key>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        match (route_kind, request) {
            (
                FastStreamRouteKind::PrimaryKey,
                FastStreamRouteRequest::PrimaryKey {
                    plan,
                    stream_direction,
                    probe_fetch_hint,
                },
            ) => Self::execute_primary_key_fast_stream_route(
                ctx,
                plan,
                stream_direction,
                probe_fetch_hint,
            ),
            (
                FastStreamRouteKind::SecondaryIndex,
                FastStreamRouteRequest::SecondaryIndex {
                    plan,
                    index_prefix_spec,
                    stream_direction,
                    probe_fetch_hint,
                    index_predicate_execution,
                },
            ) => Self::execute_secondary_index_fast_stream_route(
                ctx,
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
            ) => Self::execute_index_range_fast_stream_route(
                ctx,
                plan,
                index_range_spec,
                continuation,
                effective_fetch,
                index_predicate_execution,
            ),
            _ => Err(crate::db::error::query_executor_invariant(
                "fast-stream route kind/request mismatch",
            )),
        }
    }
}
