use crate::db::query::explain::{
    ExplainExecutionMode, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
    ExplainPropertyMap,
};

fn node(
    node_type: ExplainExecutionNodeType,
    children: Vec<ExplainExecutionNodeDescriptor>,
) -> ExplainExecutionNodeDescriptor {
    ExplainExecutionNodeDescriptor {
        node_type,
        execution_mode: ExplainExecutionMode::Materialized,
        access_strategy: None,
        predicate_pushdown: None,
        filter_expr: None,
        residual_filter_expr: None,
        residual_filter_predicate: None,
        projection: None,
        ordering_source: None,
        limit: None,
        cursor: None,
        covering_scan: None,
        rows_expected: None,
        children,
        node_properties: ExplainPropertyMap::new(),
    }
}

#[test]
fn execution_node_contains_type_scans_preorder_tree() {
    let root = node(
        ExplainExecutionNodeType::Union,
        vec![
            node(ExplainExecutionNodeType::FullScan, Vec::new()),
            node(
                ExplainExecutionNodeType::Intersection,
                vec![node(ExplainExecutionNodeType::ResidualFilter, Vec::new())],
            ),
        ],
    );

    assert!(root.contains_type(ExplainExecutionNodeType::ResidualFilter));
    assert!(!root.contains_type(ExplainExecutionNodeType::TopNSeek));
}

#[test]
fn execution_node_preorder_visits_parent_before_children() {
    let root = node(
        ExplainExecutionNodeType::Union,
        vec![
            node(ExplainExecutionNodeType::FullScan, Vec::new()),
            node(
                ExplainExecutionNodeType::Intersection,
                vec![node(ExplainExecutionNodeType::ResidualFilter, Vec::new())],
            ),
        ],
    );
    let mut visited = Vec::new();

    root.for_each_preorder(&mut |node| visited.push(node.node_type()));

    assert_eq!(
        visited,
        vec![
            ExplainExecutionNodeType::Union,
            ExplainExecutionNodeType::FullScan,
            ExplainExecutionNodeType::Intersection,
            ExplainExecutionNodeType::ResidualFilter,
        ],
    );
}
