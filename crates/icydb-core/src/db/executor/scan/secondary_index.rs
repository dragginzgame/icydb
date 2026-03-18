//! Module: executor::scan::secondary_index
//! Responsibility: secondary-index ordered fast-path stream execution helpers.
//! Does not own: planner eligibility derivation or cursor continuation semantics.
//! Boundary: consumes lowered index-prefix specs and emits ordered key streams.

use crate::{
    db::{
        Context,
        direction::Direction,
        executor::{
            LoweredIndexPrefixSpec,
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
    /// Try one secondary-index order fast path and return ordered keys when eligible.
    pub(in crate::db::executor) fn try_execute_secondary_index_order_stream(
        ctx: &Context<'_, E>,
        plan: &AccessPlannedQuery,
        index_prefix_spec: Option<&LoweredIndexPrefixSpec>,
        stream_direction: Direction,
        probe_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Option<FastPathKeyResult>, InternalError> {
        Self::execute_fast_stream_route(
            ctx,
            FastStreamRouteKind::SecondaryIndex,
            FastStreamRouteRequest::SecondaryIndex {
                plan,
                index_prefix_spec,
                stream_direction,
                probe_fetch_hint,
                index_predicate_execution,
            },
        )
    }
}
