//! Module: executor::stream::key
//! Responsibility: ordered key-stream contracts, combinators, and comparators.
//! Does not own: physical store traversal or planner-level route selection.
//! Boundary: key-stream abstractions consumed by access/load execution.

mod composite;
mod contracts;
mod distinct;
mod order;

pub(crate) use composite::{IntersectOrderedKeyStream, MergeOrderedKeyStream};
pub(crate) use contracts::{
    BudgetedOrderedKeyStream, OrderedKeyStream, OrderedKeyStreamBox, VecOrderedKeyStream,
};
pub(in crate::db::executor) use distinct::DistinctOrderedKeyStream;
pub(crate) use order::KeyOrderComparator;
