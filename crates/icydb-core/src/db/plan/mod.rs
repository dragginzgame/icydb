//! Shared plan contracts used across query planning and executor runtime.

pub(crate) mod contract;

pub use contract::OrderDirection;
pub(crate) use contract::{AccessPlannedQuery, LogicalPlan};
pub(crate) use contract::{DeleteLimitSpec, OrderSpec, PageSpec};
