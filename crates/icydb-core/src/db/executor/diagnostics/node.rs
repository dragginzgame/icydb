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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::db::query::explain::{
        ExplainExecutionMode, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
    };

    use super::{ExecutionNodeIdentity, ExecutionNodeLayer};

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
}
