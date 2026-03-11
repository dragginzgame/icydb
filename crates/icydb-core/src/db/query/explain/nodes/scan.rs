//! Module: query::explain::nodes::scan
//! Responsibility: scan-layer execution-node family ownership classification.
//! Does not own: renderer orchestration or cross-layer policy derivation.
//! Boundary: identifies node types that belong to the scan execution layer.

use crate::db::query::explain::ExplainExecutionNodeType;

pub(in crate::db::query::explain) const fn owns(node_type: ExplainExecutionNodeType) -> bool {
    matches!(
        node_type,
        ExplainExecutionNodeType::ByKeyLookup
            | ExplainExecutionNodeType::ByKeysLookup
            | ExplainExecutionNodeType::PrimaryKeyRangeScan
            | ExplainExecutionNodeType::IndexPrefixScan
            | ExplainExecutionNodeType::IndexRangeScan
            | ExplainExecutionNodeType::IndexMultiLookup
            | ExplainExecutionNodeType::FullScan
            | ExplainExecutionNodeType::Union
            | ExplainExecutionNodeType::Intersection
    )
}
