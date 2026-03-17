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
    BudgetedOrderedKeyStream, KeyStreamLoopControl, OrderedKeyStream, OrderedKeyStreamBox,
    VecOrderedKeyStream, drive_key_stream_with_control_flow,
};
pub(in crate::db::executor) use distinct::DistinctOrderedKeyStream;
pub(in crate::db::executor) use order::KeyOrderComparator;
