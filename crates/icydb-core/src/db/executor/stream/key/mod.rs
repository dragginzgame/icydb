//! Module: executor::stream::key
//! Responsibility: ordered key-stream contracts, combinators, and comparators.
//! Does not own: physical store traversal or planner-level route selection.
//! Boundary: key-stream abstractions consumed by access/load execution.

mod composite;
mod contracts;
mod distinct;
mod order;

pub(in crate::db::executor) use composite::{IntersectOrderedKeyStream, MergeOrderedKeyStream};
pub(in crate::db::executor) use contracts::{
    BudgetedOrderedKeyStream, OrderedKeyStream, OrderedKeyStreamBox, VecOrderedKeyStream,
};
pub(in crate::db::executor) use distinct::DistinctOrderedKeyStream;
pub(in crate::db::executor) use order::KeyOrderComparator;
