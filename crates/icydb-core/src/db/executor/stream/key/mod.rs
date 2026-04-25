//! Module: executor::stream::key
//! Responsibility: ordered key-stream contracts, combinators, and comparators.
//! Does not own: physical store traversal or planner-level route selection.
//! Boundary: key-stream abstractions consumed by access/load execution.

mod composite;
mod contracts;
mod distinct;
mod order;
#[cfg(test)]
mod tests;

pub(in crate::db::executor) use composite::{IntersectOrderedKeyStream, MergeOrderedKeyStream};
#[cfg(test)]
pub(in crate::db::executor) use contracts::BudgetedOrderedKeyStream;
#[cfg(test)]
pub(in crate::db::executor) use contracts::VecOrderedKeyStream;
pub(in crate::db::executor) use contracts::{
    KeyStreamLoopControl, OrderedKeyStream, OrderedKeyStreamBox, exact_output_key_count_hint,
    key_stream_budget_is_redundant, ordered_key_stream_from_materialized_keys,
};
pub(in crate::db::executor) use distinct::DistinctOrderedKeyStream;
pub(in crate::db::executor) use order::KeyOrderComparator;
