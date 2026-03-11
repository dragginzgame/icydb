//! Module: db::executor::pipeline::stages::planner
//! Responsibility: deterministic stage-plan selection for load execution.
//! Does not own: stage execution state transitions.
//! Boundary: returns canonical stage descriptors consumed by orchestrator loop.

use crate::db::executor::pipeline::stages::stage::{LOAD_PIPELINE_STAGES, LoadPipelineStage};

/// Build the deterministic load stage plan used by orchestrator execution loops.
#[must_use]
pub(in crate::db::executor) const fn plan_load_pipeline_stages() -> [LoadPipelineStage; 6] {
    LOAD_PIPELINE_STAGES
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::executor::pipeline::stages::{
        LoadPipelineStage, load_stage_labels, plan_load_pipeline_stages,
    };

    #[test]
    fn planner_stage_sequence_matches_linear_contract() {
        let planned = plan_load_pipeline_stages();

        assert_eq!(
            planned,
            [
                LoadPipelineStage::BuildExecutionContext,
                LoadPipelineStage::ExecuteAccessPath,
                LoadPipelineStage::ApplyGroupingProjection,
                LoadPipelineStage::ApplyPaging,
                LoadPipelineStage::ApplyTracing,
                LoadPipelineStage::MaterializeSurface,
            ],
            "stage planner must preserve deterministic linear orchestration order",
        );
    }

    #[test]
    fn planner_labels_match_stage_contract() {
        let labels = load_stage_labels();

        assert_eq!(
            labels,
            [
                "build_execution_context",
                "execute_access_path",
                "apply_grouping_projection",
                "apply_paging",
                "apply_tracing",
                "materialize_surface",
            ],
            "stage labels must stay aligned with deterministic stage sequence",
        );
    }
}
