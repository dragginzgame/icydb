//! Module: db::executor::projection::eval::contracts
//! Responsibility: executor-facing compiled expression contracts.
//! Does not own: planner expression construction or expression lowering.
//! Boundary: centralizes query-plan expression DTOs consumed by projection evaluation.

pub(in crate::db) use crate::db::query::plan::expr::ProjectionEvalError;
pub(super) use crate::db::query::plan::{
    EffectiveRuntimeFilterProgram,
    expr::{CompiledExpr, CompiledExprValueReader, collapse_true_only_boolean_admission},
};
