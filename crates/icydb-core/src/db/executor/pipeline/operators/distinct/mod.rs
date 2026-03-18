//! Module: executor::pipeline::operators::distinct
//! Responsibility: DISTINCT stream decoration operators for execution runtime.
//! Does not own: DISTINCT eligibility planning or row materialization policy.
//! Boundary: reusable DISTINCT operators consumed by execution-kernel orchestration.

///
/// TESTS
///

#[cfg(test)]
mod tests;

use crate::db::{
    direction::Direction,
    executor::{
        KeyOrderComparator, OrderedKeyStreamBox,
        pipeline::contracts::{ResolvedExecutionKeyStream, key_stream_comparator_from_direction},
        stream::key::DistinctOrderedKeyStream,
    },
    query::plan::{AccessPlannedQuery, DistinctExecutionStrategy},
};
use std::{cell::Cell, rc::Rc};

fn wrap_distinct_ordered_key_stream(
    ordered_key_stream: OrderedKeyStreamBox,
    strategy: DistinctExecutionStrategy,
    key_comparator: KeyOrderComparator,
    dedup_counter: Option<Rc<Cell<u64>>>,
) -> (OrderedKeyStreamBox, Option<Rc<Cell<u64>>>) {
    match strategy {
        DistinctExecutionStrategy::None => return (ordered_key_stream, None),
        DistinctExecutionStrategy::PreOrdered | DistinctExecutionStrategy::HashMaterialize => {}
    }

    if let Some(counter) = dedup_counter {
        let wrapped = Box::new(DistinctOrderedKeyStream::new_with_dedup_counter(
            ordered_key_stream,
            key_comparator,
            counter.clone(),
        ));
        return (wrapped, Some(counter));
    }

    (
        Box::new(DistinctOrderedKeyStream::new(
            ordered_key_stream,
            key_comparator,
        )),
        None,
    )
}

/// Decorate one resolved execution key stream with DISTINCT behavior when requested.
pub(in crate::db::executor) fn decorate_resolved_execution_key_stream(
    resolved: ResolvedExecutionKeyStream,
    plan: &AccessPlannedQuery,
    direction: Direction,
) -> ResolvedExecutionKeyStream {
    let (
        key_stream,
        optimization,
        rows_scanned_override,
        index_predicate_applied,
        index_predicate_keys_rejected,
        _distinct_keys_deduped_counter,
    ) = resolved.into_parts();
    let key_comparator = key_stream_comparator_from_direction(direction);
    let strategy = plan.distinct_execution_strategy();
    let dedup_counter = strategy.is_enabled().then(|| Rc::new(Cell::new(0u64)));
    let (key_stream, dedup_counter) =
        wrap_distinct_ordered_key_stream(key_stream, strategy, key_comparator, dedup_counter);

    ResolvedExecutionKeyStream::new(
        key_stream,
        optimization,
        rows_scanned_override,
        index_predicate_applied,
        index_predicate_keys_rejected,
        dedup_counter,
    )
}

/// Decorate one ordered key stream with DISTINCT behavior using planner strategy.
pub(in crate::db::executor) fn decorate_key_stream_for_plan(
    ordered_key_stream: OrderedKeyStreamBox,
    plan: &AccessPlannedQuery,
    direction: Direction,
) -> OrderedKeyStreamBox {
    let key_comparator = key_stream_comparator_from_direction(direction);

    wrap_distinct_ordered_key_stream(
        ordered_key_stream,
        plan.distinct_execution_strategy(),
        key_comparator,
        None,
    )
    .0
}
