#![cfg_attr(not(test), allow(dead_code))]

use crate::db::executor::pipeline::stages::{LoadPipelineStage, plan_load_pipeline_stages};

///
/// LoadExecutionDescriptor
///
/// Immutable load-orchestrator runtime descriptor for stage-loop control flow.
/// This is the B1 dynamic boundary used to decouple stage-plan authority from
/// typed `LoadExecutor<E>` entrypoints before deeper executor dyn migration.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) struct LoadExecutionDescriptor {
    stage_plan: [LoadPipelineStage; 6],
}

impl LoadExecutionDescriptor {
    /// Construct one load execution descriptor from one explicit stage plan.
    #[must_use]
    pub(in crate::db::executor) const fn new(stage_plan: [LoadPipelineStage; 6]) -> Self {
        Self { stage_plan }
    }

    /// Construct the canonical release-stage descriptor.
    #[must_use]
    pub(in crate::db::executor) const fn canonical() -> Self {
        Self::new(plan_load_pipeline_stages())
    }

    /// Borrow deterministic stage descriptors in execution order.
    #[must_use]
    pub(in crate::db::executor) const fn stage_plan(&self) -> &[LoadPipelineStage; 6] {
        &self.stage_plan
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::executor::pipeline::{
        orchestrator::contracts::LoadExecutionDescriptor,
        stages::{LoadPipelineStage, load_stage_labels, plan_load_pipeline_stages},
    };

    #[test]
    fn canonical_descriptor_stage_plan_matches_stage_planner() {
        let descriptor = LoadExecutionDescriptor::canonical();
        let planned = plan_load_pipeline_stages();

        assert_eq!(
            descriptor.stage_plan(),
            &planned,
            "canonical load execution descriptor must preserve stage planner ordering",
        );
    }

    #[test]
    fn canonical_descriptor_stage_labels_match_known_contract() {
        let descriptor = LoadExecutionDescriptor::canonical();
        let labels = load_stage_labels();
        let stage_labels = descriptor
            .stage_plan()
            .iter()
            .map(|stage| match stage {
                LoadPipelineStage::BuildExecutionContext => "build_execution_context",
                LoadPipelineStage::ExecuteAccessPath => "execute_access_path",
                LoadPipelineStage::ApplyGroupingProjection => "apply_grouping_projection",
                LoadPipelineStage::ApplyPaging => "apply_paging",
                LoadPipelineStage::ApplyTracing => "apply_tracing",
                LoadPipelineStage::MaterializeSurface => "materialize_surface",
            })
            .collect::<Vec<_>>();

        assert_eq!(
            stage_labels.as_slice(),
            labels.as_slice(),
            "descriptor stage label ordering must remain aligned with canonical stage labels",
        );
    }
}
