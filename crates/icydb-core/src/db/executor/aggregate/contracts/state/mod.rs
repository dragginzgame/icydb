//! Module: executor::aggregate::contracts::state
//! Responsibility: scalar aggregate reducer state machines and grouped structural terminal reducers.
//! Does not own: grouped budget/accounting policy.
//! Boundary: state/fold mechanics used by aggregate execution kernels.

mod control;
mod distinct;
mod factory;
mod grouped;
mod grouped_expr;
mod reducer;
mod scalar;

pub(in crate::db::executor::aggregate::contracts::state) use control::ExtremumKind;
pub(in crate::db::executor) use control::{AggregateFoldMode, FoldControl};
pub(in crate::db::executor) use distinct::GroupedDistinctExecutionMode;
pub(in crate::db::executor::aggregate::contracts::state) use distinct::{
    canonical_key_from_data_key, record_distinct_key,
};
pub(in crate::db::executor) use factory::AggregateStateFactory;
pub(in crate::db::executor) use grouped::GroupedTerminalAggregateState;
pub(in crate::db::executor::aggregate) use grouped_expr::GroupedCompiledExpr;
pub(in crate::db::executor::aggregate::contracts::state) use reducer::GroupedAggregateReducerState;
pub(in crate::db::executor) use reducer::ScalarAggregateReducerState;
pub(in crate::db::executor) use scalar::{ScalarAggregateState, ScalarTerminalAggregateState};
