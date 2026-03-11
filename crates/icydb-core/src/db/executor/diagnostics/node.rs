//! Module: db::executor::diagnostics::node
//! Responsibility: stable execution-node identity contracts for diagnostics correlation.
//! Does not own: explain tree rendering or runtime metrics storage.
//! Boundary: executor-owned node identity projection for explain/metrics correlation.

#![cfg_attr(not(test), allow(dead_code))]

use crate::db::query::explain::ExplainExecutionNodeDescriptor;

///
/// ExecutionNodeLayer
///
/// Canonical executor-layer identity for execution diagnostics correlation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionNodeLayer {
    Scan,
    Pipeline,
    Aggregate,
    Terminal,
}

impl ExecutionNodeLayer {
    /// Return stable lowercase layer label used by explain diagnostics surfaces.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Pipeline => "pipeline",
            Self::Aggregate => "aggregate",
            Self::Terminal => "terminal",
        }
    }

    /// Resolve one diagnostics layer from an explain-layer string label.
    #[must_use]
    pub(crate) fn from_explain_label(label: &str) -> Self {
        match label {
            "scan" => Self::Scan,
            "aggregate" => Self::Aggregate,
            "terminal" => Self::Terminal,
            _ => Self::Pipeline,
        }
    }
}

///
/// ExecutionNodeIdentity
///
/// Stable executor diagnostics identity for one execution node.
/// Allows correlation between explain node topology and row-flow counters.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionNodeIdentity {
    node_id: u64,
    node_type: &'static str,
    layer: ExecutionNodeLayer,
}

impl ExecutionNodeIdentity {
    /// Construct one diagnostics node identity payload.
    #[must_use]
    pub(crate) const fn new(
        node_id: u64,
        node_type: &'static str,
        layer: ExecutionNodeLayer,
    ) -> Self {
        Self {
            node_id,
            node_type,
            layer,
        }
    }

    /// Build one diagnostics node identity from one explain node descriptor.
    #[must_use]
    pub(crate) fn from_explain_node(node_id: u64, node: &ExplainExecutionNodeDescriptor) -> Self {
        Self {
            node_id,
            node_type: node.node_type().as_str(),
            layer: ExecutionNodeLayer::from_explain_label(node.node_type().layer_label()),
        }
    }

    /// Return stable node id.
    #[must_use]
    pub(crate) const fn node_id(self) -> u64 {
        self.node_id
    }

    /// Return stable node type label.
    #[must_use]
    pub(crate) const fn node_type(self) -> &'static str {
        self.node_type
    }

    /// Return canonical diagnostics layer.
    #[must_use]
    pub(crate) const fn layer(self) -> ExecutionNodeLayer {
        self.layer
    }
}

/// Collect deterministic diagnostics identities in canonical explain-node order.
#[must_use]
pub(crate) fn collect_execution_node_identities(
    root: &ExplainExecutionNodeDescriptor,
) -> Vec<ExecutionNodeIdentity> {
    let mut next_node_id = 0_u64;
    let mut identities = Vec::new();
    collect_execution_node_identities_into(root, &mut next_node_id, &mut identities);

    identities
}

fn collect_execution_node_identities_into(
    node: &ExplainExecutionNodeDescriptor,
    next_node_id: &mut u64,
    identities: &mut Vec<ExecutionNodeIdentity>,
) {
    let node_id = *next_node_id;
    *next_node_id = next_node_id.saturating_add(1);
    identities.push(ExecutionNodeIdentity::from_explain_node(node_id, node));

    for child in node.children() {
        collect_execution_node_identities_into(child, next_node_id, identities);
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::db::query::explain::{
        ExplainExecutionMode, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
    };

    use super::{ExecutionNodeIdentity, ExecutionNodeLayer, collect_execution_node_identities};

    fn extract_node_ids_from_canonical_json(json: &str) -> Vec<u64> {
        let mut ids = Vec::new();
        let mut remaining = json;
        let node_id_prefix = "\"node_id\":";

        while let Some(prefix_index) = remaining.find(node_id_prefix) {
            let value_start = prefix_index.saturating_add(node_id_prefix.len());
            let digits_start = value_start;
            let digits_end = remaining[digits_start..]
                .find(|ch: char| !ch.is_ascii_digit())
                .map_or(remaining.len(), |relative| {
                    digits_start.saturating_add(relative)
                });
            let parsed = remaining[digits_start..digits_end]
                .parse::<u64>()
                .expect("node_id field must parse as u64");
            ids.push(parsed);
            remaining = &remaining[digits_end..];
        }

        ids
    }

    #[test]
    fn execution_node_identity_from_explain_node_preserves_node_and_layer_contract() {
        let descriptor = ExplainExecutionNodeDescriptor {
            node_type: ExplainExecutionNodeType::IndexPrefixScan,
            execution_mode: ExplainExecutionMode::Materialized,
            access_strategy: None,
            predicate_pushdown: None,
            residual_predicate: None,
            projection: None,
            ordering_source: None,
            limit: None,
            cursor: None,
            covering_scan: None,
            rows_expected: None,
            children: Vec::new(),
            node_properties: BTreeMap::new(),
        };

        let identity = ExecutionNodeIdentity::from_explain_node(7, &descriptor);
        assert_eq!(identity.node_id(), 7);
        assert_eq!(identity.node_type(), "IndexPrefixScan");
        assert_eq!(identity.layer(), ExecutionNodeLayer::Scan);
        assert_eq!(identity.layer().as_str(), "scan");
    }

    #[test]
    fn execution_node_identity_constructor_is_stable() {
        let identity = ExecutionNodeIdentity::new(3, "LimitOffset", ExecutionNodeLayer::Terminal);

        assert_eq!(identity.node_id(), 3);
        assert_eq!(identity.node_type(), "LimitOffset");
        assert_eq!(identity.layer(), ExecutionNodeLayer::Terminal);
        assert_eq!(identity.layer().as_str(), "terminal");
    }

    #[test]
    fn execution_node_identity_collection_preserves_node_id_order_and_layer_mapping() {
        let descriptor = ExplainExecutionNodeDescriptor {
            node_type: ExplainExecutionNodeType::TopNSeek,
            execution_mode: ExplainExecutionMode::Streaming,
            access_strategy: None,
            predicate_pushdown: None,
            residual_predicate: None,
            projection: None,
            ordering_source: None,
            limit: None,
            cursor: None,
            covering_scan: None,
            rows_expected: None,
            children: vec![
                ExplainExecutionNodeDescriptor {
                    node_type: ExplainExecutionNodeType::IndexPrefixScan,
                    execution_mode: ExplainExecutionMode::Materialized,
                    access_strategy: None,
                    predicate_pushdown: None,
                    residual_predicate: None,
                    projection: None,
                    ordering_source: None,
                    limit: None,
                    cursor: None,
                    covering_scan: None,
                    rows_expected: None,
                    children: Vec::new(),
                    node_properties: BTreeMap::new(),
                },
                ExplainExecutionNodeDescriptor {
                    node_type: ExplainExecutionNodeType::AggregateCount,
                    execution_mode: ExplainExecutionMode::Materialized,
                    access_strategy: None,
                    predicate_pushdown: None,
                    residual_predicate: None,
                    projection: None,
                    ordering_source: None,
                    limit: None,
                    cursor: None,
                    covering_scan: None,
                    rows_expected: None,
                    children: vec![ExplainExecutionNodeDescriptor {
                        node_type: ExplainExecutionNodeType::LimitOffset,
                        execution_mode: ExplainExecutionMode::Materialized,
                        access_strategy: None,
                        predicate_pushdown: None,
                        residual_predicate: None,
                        projection: None,
                        ordering_source: None,
                        limit: None,
                        cursor: None,
                        covering_scan: None,
                        rows_expected: None,
                        children: Vec::new(),
                        node_properties: BTreeMap::new(),
                    }],
                    node_properties: BTreeMap::new(),
                },
            ],
            node_properties: BTreeMap::new(),
        };
        let identities = collect_execution_node_identities(&descriptor);
        let actual = identities
            .iter()
            .map(|identity| {
                (
                    identity.node_id(),
                    identity.node_type().to_string(),
                    identity.layer().as_str().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let expected = vec![
            (0_u64, "TopNSeek".to_string(), "pipeline".to_string()),
            (1_u64, "IndexPrefixScan".to_string(), "scan".to_string()),
            (2_u64, "AggregateCount".to_string(), "aggregate".to_string()),
            (3_u64, "LimitOffset".to_string(), "terminal".to_string()),
        ];

        assert_eq!(
            actual, expected,
            "diagnostics identity collection must preserve canonical node ordering and layers",
        );
    }

    #[test]
    fn execution_node_identity_collection_node_ids_match_canonical_explain_json_ids() {
        let descriptor = ExplainExecutionNodeDescriptor {
            node_type: ExplainExecutionNodeType::TopNSeek,
            execution_mode: ExplainExecutionMode::Streaming,
            access_strategy: None,
            predicate_pushdown: None,
            residual_predicate: None,
            projection: None,
            ordering_source: None,
            limit: None,
            cursor: None,
            covering_scan: None,
            rows_expected: None,
            children: vec![
                ExplainExecutionNodeDescriptor {
                    node_type: ExplainExecutionNodeType::IndexPrefixScan,
                    execution_mode: ExplainExecutionMode::Materialized,
                    access_strategy: None,
                    predicate_pushdown: None,
                    residual_predicate: None,
                    projection: None,
                    ordering_source: None,
                    limit: None,
                    cursor: None,
                    covering_scan: None,
                    rows_expected: None,
                    children: Vec::new(),
                    node_properties: BTreeMap::new(),
                },
                ExplainExecutionNodeDescriptor {
                    node_type: ExplainExecutionNodeType::LimitOffset,
                    execution_mode: ExplainExecutionMode::Materialized,
                    access_strategy: None,
                    predicate_pushdown: None,
                    residual_predicate: None,
                    projection: None,
                    ordering_source: None,
                    limit: None,
                    cursor: None,
                    covering_scan: None,
                    rows_expected: None,
                    children: Vec::new(),
                    node_properties: BTreeMap::new(),
                },
            ],
            node_properties: BTreeMap::new(),
        };
        let identities = collect_execution_node_identities(&descriptor);
        let identity_ids = identities
            .iter()
            .map(|identity| identity.node_id())
            .collect::<Vec<_>>();
        let json_ids = extract_node_ids_from_canonical_json(&descriptor.render_json_canonical());

        assert_eq!(
            identity_ids, json_ids,
            "diagnostics node ids must match canonical explain JSON node ids",
        );
    }
}
