//! Module: executor::aggregate::contracts::grouped
//! Responsibility: grouped aggregate budget/config/context/state/engine contracts.
//! Does not own: aggregate spec validation semantics.
//! Boundary: grouped runtime substrate layered over spec + state contracts.

mod context;
mod engine;

pub(in crate::db::executor) use context::{ExecutionConfig, ExecutionContext};
pub(in crate::db::executor) use engine::{
    GroupedAggregateEngine, ScalarAggregateEngine, execute_scalar_aggregate,
};

#[cfg(test)]
pub(in crate::db::executor) use engine::GroupedAggregateOutput;
