//! Module: executor::aggregate::contracts::state
//! Responsibility: scalar aggregate reducer state machines and grouped structural terminal reducers.
//! Does not own: grouped budget/accounting policy.
//! Boundary: state/fold mechanics used by aggregate execution kernels.

mod control;
mod distinct;
mod grouped;
mod reducer;

#[cfg(feature = "sql")]
pub(in crate::db::executor) use control::AggregateFoldMode;
pub(in crate::db::executor::aggregate::contracts::state) use control::ExtremumKind;
pub(in crate::db::executor) use control::FoldControl;
pub(in crate::db::executor) use distinct::GroupedDistinctExecutionMode;
pub(in crate::db::executor) use grouped::GroupedTerminalAggregateState;
pub(in crate::db::executor::aggregate::contracts::state) use reducer::GroupedAggregateReducerState;
