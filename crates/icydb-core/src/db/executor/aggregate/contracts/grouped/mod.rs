//! Module: executor::aggregate::contracts::grouped
//! Responsibility: grouped aggregate budget/config/context/state/engine contracts.
//! Does not own: aggregate spec validation semantics.
//! Boundary: grouped runtime substrate layered over spec + state contracts.

mod context;
pub(in crate::db::executor) use context::{ExecutionConfig, ExecutionContext};
