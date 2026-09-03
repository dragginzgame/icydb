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
    },
    query::plan::{AccessPlannedQuery, DistinctExecutionStrategy},
};

fn wrap_distinct_ordered_key_stream(
    ordered_key_stream: OrderedKeyStreamBox,
    strategy: DistinctExecutionStrategy,
    key_comparator: KeyOrderComparator,
) -> OrderedKeyStreamBox {
    match strategy {
        DistinctExecutionStrategy::None => return ordered_key_stream,
        DistinctExecutionStrategy::PreOrdered | DistinctExecutionStrategy::HashMaterialize => {}
    }

    OrderedKeyStreamBox::distinct(ordered_key_stream, key_comparator)
}

/// Decorate one resolved execution key stream with DISTINCT behavior when requested.
pub(in crate::db::executor) fn decorate_resolved_execution_key_stream(
    resolved: ResolvedExecutionKeyStream,
    plan: &AccessPlannedQuery,
    direction: Direction,
) -> ResolvedExecutionKeyStream {
    let key_comparator = key_stream_comparator_from_direction(direction);
    let strategy = plan.distinct_execution_strategy();

    resolved.decorate_key_stream(|key_stream| {
        wrap_distinct_ordered_key_stream(key_stream, strategy, key_comparator)
    })
}
