//! Module: query::explain::nodes::terminal
//! Responsibility: terminal-layer execution-node family ownership classification.
//! Does not own: renderer orchestration or cross-layer policy derivation.
//! Boundary: identifies node types that belong to the terminal execution layer.

use crate::db::query::explain::ExplainExecutionNodeType;

pub(in crate::db::query::explain) const fn owns(node_type: ExplainExecutionNodeType) -> bool {
    matches!(
        node_type,
        ExplainExecutionNodeType::ProjectionMaterialized
            | ExplainExecutionNodeType::CoveringRead
            | ExplainExecutionNodeType::LimitOffset
    )
}
