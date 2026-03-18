//! Module: executor::scan::index_range_limit
//! Responsibility: bounded index-range fast-path stream execution.
//! Does not own: index-range eligibility planning or cursor decode semantics.
//! Boundary: executes pre-lowered index-range specs when route gates allow pushdown.

use crate::{
    db::{
        Context,
        executor::{
            AccessScanContinuationInput, LoweredIndexRangeSpec,
            pipeline::contracts::{FastPathKeyResult, LoadExecutor},
            scan::{FastStreamRouteKind, FastStreamRouteRequest},
        },
        index::predicate::IndexPredicateExecution,
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Try one bounded index-range fast-path stream for semantically safe plan shapes.
    pub(in crate::db::executor) fn try_execute_index_range_limit_pushdown_stream(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        continuation: AccessScanContinuationInput<'_>,
        effective_fetch: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        Self::execute_fast_stream_route(
            ctx,
            FastStreamRouteKind::IndexRangeLimitPushdown,
            FastStreamRouteRequest::IndexRangeLimitPushdown {
                plan,
                index_range_spec,
                continuation,
                effective_fetch,
                index_predicate_execution,
            },
        )
    }
}
