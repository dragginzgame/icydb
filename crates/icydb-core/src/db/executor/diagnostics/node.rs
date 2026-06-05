//! Module: db::executor::diagnostics::node
//! Responsibility: stable execution-node identity contracts for diagnostics correlation.
//! Does not own: explain tree rendering or runtime metrics storage.
//! Boundary: executor-owned node identity projection for explain/metrics correlation.

use crate::db::query::explain::ExplainExecutionNodeDescriptor;

///
/// ExecutionNodeLayer
///
/// Canonical executor-layer identity for execution diagnostics correlation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExecutionNodeLayer {
    Scan,
    Pipeline,
    Aggregate,
    Terminal,
}

impl ExecutionNodeLayer {
    /// Return stable lowercase layer label used by explain diagnostics surfaces.
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Pipeline => "pipeline",
            Self::Aggregate => "aggregate",
            Self::Terminal => "terminal",
        }
    }

    /// Resolve one diagnostics layer from an explain-layer string label.
    #[must_use]
    fn from_explain_label(label: &str) -> Self {
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
struct ExecutionNodeIdentity {
    node_id: u64,
    node_type: &'static str,
    layer: ExecutionNodeLayer,
}

impl ExecutionNodeIdentity {
    /// Construct one diagnostics node identity payload.
    #[must_use]
    const fn new(node_id: u64, node_type: &'static str, layer: ExecutionNodeLayer) -> Self {
        Self {
            node_id,
            node_type,
            layer,
        }
    }

    /// Build one diagnostics node identity from one explain node descriptor.
    #[must_use]
    fn from_explain_node(node_id: u64, node: &ExplainExecutionNodeDescriptor) -> Self {
        Self {
            node_id,
            node_type: node.node_type().as_str(),
            layer: ExecutionNodeLayer::from_explain_label(node.node_type().layer_label()),
        }
    }

    /// Return stable node id.
    #[must_use]
    const fn node_id(self) -> u64 {
        self.node_id
    }

    /// Return stable node type label.
    #[must_use]
    const fn node_type(self) -> &'static str {
        self.node_type
    }

    /// Return canonical diagnostics layer.
    #[must_use]
    const fn layer(self) -> ExecutionNodeLayer {
        self.layer
    }
}

/// Collect deterministic diagnostics identities in canonical explain-node order.
#[must_use]
fn collect_execution_node_identities(
    root: &ExplainExecutionNodeDescriptor,
) -> Vec<ExecutionNodeIdentity> {
    let mut identities = Vec::new();
    let mut next_node_id = 0_u64;

    root.for_each_preorder(&mut |node| {
        let node_id = next_node_id;
        next_node_id = next_node_id.saturating_add(1);
        identities.push(ExecutionNodeIdentity::from_explain_node(node_id, node));
    });

    identities
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
