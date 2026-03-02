//! Module: executor::aggregate::contracts
//! Responsibility: aggregate runtime contracts, specs, grouped state, and errors.
//! Does not own: aggregate execution branching/orchestration behavior.
//! Boundary: shared aggregate contract surface consumed by aggregate executors.
#![deny(unreachable_patterns)]

mod error;
mod grouped;
mod spec;
mod state;

pub(in crate::db::executor) use error::GroupError;
pub(in crate::db::executor) use grouped::{AggregateEngine, ExecutionConfig, ExecutionContext};
pub(in crate::db::executor) use spec::{AggregateKind, AggregateOutput};
pub(in crate::db::executor) use state::{
    AggregateFoldMode, AggregateState, AggregateStateFactory, FoldControl, TerminalAggregateState,
};

#[cfg(test)]
pub(in crate::db::executor) use grouped::GroupedAggregateOutput;
