//! Module: db::executor::pipeline::orchestrator::state
//! Responsibility: owns context artifacts and pipeline envelope state contracts.
//! Does not own: stage orchestration mechanics or terminal materialization logic.
//! Boundary: exports context + envelope seams for orchestrator stage execution.

mod context;
mod envelope;

pub(super) use context::{
    LoadAccessInputs, LoadAccessState, LoadExecutionContext, LoadExecutionPayload, LoadPayloadState,
};
pub(super) use envelope::LoadPipelineState;
