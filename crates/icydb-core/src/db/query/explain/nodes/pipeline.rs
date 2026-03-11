//! Module: query::explain::nodes::pipeline
//! Responsibility: pipeline-layer execution-node family ownership classification.
//! Does not own: renderer orchestration or cross-layer policy derivation.
//! Boundary: identifies node types that belong to the pipeline execution layer.

use crate::db::query::explain::ExplainExecutionNodeType;

pub(in crate::db::query::explain) const fn owns(node_type: ExplainExecutionNodeType) -> bool {
    matches!(
        node_type,
        ExplainExecutionNodeType::IndexPredicatePrefilter
            | ExplainExecutionNodeType::ResidualPredicateFilter
            | ExplainExecutionNodeType::OrderByAccessSatisfied
            | ExplainExecutionNodeType::OrderByMaterializedSort
            | ExplainExecutionNodeType::CursorResume
            | ExplainExecutionNodeType::IndexRangeLimitPushdown
            | ExplainExecutionNodeType::TopNSeek
            | ExplainExecutionNodeType::SecondaryOrderPushdown
    )
}
