//! Module: db::executor::pipeline::contracts::execution
//! Responsibility: module-local ownership and contracts for db::executor::pipeline::contracts::execution.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod erased;
mod inputs;
mod outcomes;
mod stream;

pub(in crate::db::executor::pipeline::contracts::execution) use erased::ErasedRuntimeBindings;
pub(in crate::db::executor) use inputs::{
    CoveringComponentScanState, CursorEmissionMode, DirectCoveringScanMaterializationRequest,
    ExecutionInputPreparation, ExecutionInputs, ExecutionRuntime, ExecutionRuntimeAdapter,
    PreparedExecutionProjection, ProjectionMaterializationMode, RowCollectorMaterializationRequest,
    RuntimePageMaterializationRequest, StructuralCursorPage,
};
pub(in crate::db::executor) use outcomes::{ExecutionOutcomeMetrics, MaterializedExecutionAttempt};
pub(in crate::db::executor) use stream::ResolvedExecutionKeyStream;
