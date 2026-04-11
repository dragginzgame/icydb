//! Module: db::executor::pipeline::contracts::execution
//! Re-exports the execution contracts shared by scalar pipeline preparation
//! and runtime.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod inputs;
mod outcomes;
mod stream;

pub(in crate::db) use inputs::StructuralCursorPage;
pub(in crate::db) use inputs::StructuralCursorPagePayload;
pub(in crate::db::executor) use inputs::{
    CursorEmissionMode, ExecutionInputs, ExecutionOutputOptions, ExecutionRuntimeAdapter,
    PreparedExecutionProjection, ProjectionMaterializationMode, RowCollectorMaterializationRequest,
    RuntimePageMaterializationRequest,
};
pub(in crate::db::executor) use outcomes::{
    ExecutionOutcomeMetrics, MaterializedExecutionAttempt, MaterializedExecutionPayload,
};
pub(in crate::db::executor) use stream::ResolvedExecutionKeyStream;
