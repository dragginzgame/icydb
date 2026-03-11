//! Module: db::executor::pipeline::stages::stage
//! Responsibility: canonical load pipeline stage descriptors and labels.
//! Does not own: stage execution behavior or state transitions.
//! Boundary: provides deterministic stage identity used by orchestrator loops.

///
/// LoadPipelineStage
///
/// Stage descriptor for the linear load orchestration pipeline.
/// Preserves one canonical stage sequence for deterministic execution.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum LoadPipelineStage {
    BuildExecutionContext,
    ExecuteAccessPath,
    ApplyGroupingProjection,
    ApplyPaging,
    ApplyTracing,
    MaterializeSurface,
}

impl LoadPipelineStage {
    #[cfg(test)]
    /// Return one stable stage label used by structural tests and guards.
    #[must_use]
    pub(in crate::db::executor) const fn label(self) -> &'static str {
        match self {
            Self::BuildExecutionContext => "build_execution_context",
            Self::ExecuteAccessPath => "execute_access_path",
            Self::ApplyGroupingProjection => "apply_grouping_projection",
            Self::ApplyPaging => "apply_paging",
            Self::ApplyTracing => "apply_tracing",
            Self::MaterializeSurface => "materialize_surface",
        }
    }
}

pub(in crate::db::executor) const LOAD_PIPELINE_STAGES: [LoadPipelineStage; 6] = [
    LoadPipelineStage::BuildExecutionContext,
    LoadPipelineStage::ExecuteAccessPath,
    LoadPipelineStage::ApplyGroupingProjection,
    LoadPipelineStage::ApplyPaging,
    LoadPipelineStage::ApplyTracing,
    LoadPipelineStage::MaterializeSurface,
];

#[cfg(test)]
/// Return canonical stage labels in deterministic execution order.
#[must_use]
pub(in crate::db::executor) const fn load_stage_labels() -> [&'static str; 6] {
    [
        LOAD_PIPELINE_STAGES[0].label(),
        LOAD_PIPELINE_STAGES[1].label(),
        LOAD_PIPELINE_STAGES[2].label(),
        LOAD_PIPELINE_STAGES[3].label(),
        LOAD_PIPELINE_STAGES[4].label(),
        LOAD_PIPELINE_STAGES[5].label(),
    ]
}
