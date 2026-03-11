//! Module: db::executor::pipeline::stages
//! Responsibility: deterministic stage-descriptor contracts for load orchestration.
//! Does not own: stage execution semantics or payload materialization internals.
//! Boundary: defines canonical stage order + planner surface used by orchestrator.

mod planner;
mod stage;

pub(in crate::db::executor) use planner::plan_load_pipeline_stages;
pub(in crate::db::executor) use stage::LoadPipelineStage;
#[cfg(test)]
pub(in crate::db::executor) use stage::load_stage_labels;
