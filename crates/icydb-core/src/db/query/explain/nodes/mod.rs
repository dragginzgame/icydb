//! Module: query::explain::nodes
//! Responsibility: execution-node family ownership and node-level observability helpers.
//! Does not own: text/JSON rendering orchestration or logical-plan projection.
//! Boundary: shared helper surface consumed by explain renderers and execution DTOs.

use crate::{
    db::query::explain::{
        ExplainExecutionMode, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
    },
    value::Value,
};

pub(in crate::db::query::explain) const fn layer_label(
    node_type: ExplainExecutionNodeType,
) -> &'static str {
    if scan_layer_owns(node_type) {
        return "scan";
    }
    if pipeline_layer_owns(node_type) {
        return "pipeline";
    }
    if aggregate_layer_owns(node_type) {
        return "aggregate";
    }
    if terminal_layer_owns(node_type) {
        return "terminal";
    }

    "unknown"
}

const fn scan_layer_owns(node_type: ExplainExecutionNodeType) -> bool {
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

const fn pipeline_layer_owns(node_type: ExplainExecutionNodeType) -> bool {
    matches!(
        node_type,
        ExplainExecutionNodeType::IndexPredicatePrefilter
            | ExplainExecutionNodeType::ResidualFilter
            | ExplainExecutionNodeType::OrderByAccessSatisfied
            | ExplainExecutionNodeType::OrderByMaterializedSort
            | ExplainExecutionNodeType::CursorResume
            | ExplainExecutionNodeType::IndexRangeLimitPushdown
            | ExplainExecutionNodeType::TopNSeek
            | ExplainExecutionNodeType::SecondaryOrderPushdown
    )
}

const fn aggregate_layer_owns(node_type: ExplainExecutionNodeType) -> bool {
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

const fn terminal_layer_owns(node_type: ExplainExecutionNodeType) -> bool {
    matches!(
        node_type,
        ExplainExecutionNodeType::ProjectionMaterialized
            | ExplainExecutionNodeType::CoveringRead
            | ExplainExecutionNodeType::LimitOffset
    )
}

pub(in crate::db::query::explain) const fn execution_mode_detail_label(
    mode: ExplainExecutionMode,
) -> &'static str {
    match mode {
        ExplainExecutionMode::Streaming => "streaming",
        ExplainExecutionMode::Materialized => "materialized",
    }
}

pub(in crate::db::query::explain) fn predicate_pushdown_mode(
    node: &ExplainExecutionNodeDescriptor,
) -> &'static str {
    match node.predicate_pushdown().as_ref() {
        None => "none",
        Some(pushdown) if *pushdown == "strict_all_or_none" => "full",
        Some(_) => {
            if node.residual_filter_predicate().is_some() {
                "partial"
            } else {
                "full"
            }
        }
    }
}

pub(in crate::db::query::explain) fn fast_path_selected(
    node: &ExplainExecutionNodeDescriptor,
) -> Option<bool> {
    let selected = node.node_properties().get("fast_path_selected")?;
    match selected {
        Value::Text(path) => Some(path.as_str() != "none"),
        _ => None,
    }
}

pub(in crate::db::query::explain) fn fast_path_reason(
    node: &ExplainExecutionNodeDescriptor,
) -> Option<&str> {
    let reason = node.node_properties().get("fast_path_selected_reason")?;
    match reason {
        Value::Text(reason) => Some(reason.as_str()),
        _ => None,
    }
}
