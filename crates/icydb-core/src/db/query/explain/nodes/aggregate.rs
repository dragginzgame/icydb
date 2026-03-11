//! Module: query::explain::nodes::aggregate
//! Responsibility: aggregate-layer execution-node family ownership classification.
//! Does not own: renderer orchestration or cross-layer policy derivation.
//! Boundary: identifies node types that belong to the aggregate execution layer.

use crate::db::query::explain::ExplainExecutionNodeType;

pub(in crate::db::query::explain) const fn owns(node_type: ExplainExecutionNodeType) -> bool {
    matches!(
        node_type,
        ExplainExecutionNodeType::DistinctPreOrdered
            | ExplainExecutionNodeType::DistinctMaterialized
            | ExplainExecutionNodeType::AggregateCount
            | ExplainExecutionNodeType::AggregateExists
            | ExplainExecutionNodeType::AggregateMin
            | ExplainExecutionNodeType::AggregateMax
            | ExplainExecutionNodeType::AggregateFirst
            | ExplainExecutionNodeType::AggregateLast
            | ExplainExecutionNodeType::AggregateSum
            | ExplainExecutionNodeType::AggregateSeekFirst
            | ExplainExecutionNodeType::AggregateSeekLast
            | ExplainExecutionNodeType::GroupedAggregateHashMaterialized
            | ExplainExecutionNodeType::GroupedAggregateOrderedMaterialized
    )
}
