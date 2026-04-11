use super::*;

fn aggregate_terminal_plan_snapshot(plan: &ExplainAggregateTerminalPlan) -> String {
    let execution = plan.execution();
    let node = plan.execution_node_descriptor();
    let descriptor_json = node.render_json_canonical();

    format!(
        concat!(
            "terminal={:?}\n",
            "query_access={:?}\n",
            "query_order_by={:?}\n",
            "query_page={:?}\n",
            "query_grouping={:?}\n",
            "query_pushdown={:?}\n",
            "query_consistency={:?}\n",
            "execution_aggregation={:?}\n",
            "execution_mode={:?}\n",
            "execution_ordering_source={:?}\n",
            "execution_limit={:?}\n",
            "execution_cursor={}\n",
            "execution_covering_projection={}\n",
            "execution_node_properties={:?}\n",
            "execution_node_json={}",
        ),
        plan.terminal(),
        plan.query().access(),
        plan.query().order_by(),
        plan.query().page(),
        plan.query().grouping(),
        plan.query().order_pushdown(),
        plan.query().consistency(),
        execution.aggregation(),
        execution.execution_mode(),
        execution.ordering_source(),
        execution.limit(),
        execution.cursor(),
        execution.covering_projection(),
        execution.node_properties(),
        descriptor_json,
    )
}

#[test]
fn explain_aggregate_terminal_plan_snapshot_seek_route_is_stable() {
    // Phase 1: build a deterministic index-prefix query explain payload.
    let mut plan: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            ("tag".to_string(), OrderDirection::Asc),
            ("id".to_string(), OrderDirection::Asc),
        ],
    });
    let query_explain = plan.explain();

    // Phase 2: build one seek-route execution descriptor and snapshot the whole payload.
    let mut node_properties = ExplainPropertyMap::new();
    node_properties.insert("fetch", Value::from(1_u64));
    let terminal_plan = ExplainAggregateTerminalPlan::new(
        query_explain,
        AggregateKind::Min,
        ExplainExecutionDescriptor {
            access_strategy: ExplainAccessPath::IndexPrefix {
                name: "explain::pushdown_tag",
                fields: vec!["tag"],
                prefix_len: 1,
                values: vec![Value::Text("alpha".to_string())],
            },
            covering_projection: false,
            aggregation: AggregateKind::Min,
            execution_mode: ExplainExecutionMode::Materialized,
            ordering_source: ExplainExecutionOrderingSource::IndexSeekFirst { fetch: 1 },
            limit: None,
            cursor: false,
            node_properties,
        },
    );

    let actual = aggregate_terminal_plan_snapshot(&terminal_plan);
    let expected = "terminal=Min
query_access=IndexPrefix { name: \"explain::pushdown_tag\", fields: [\"tag\"], prefix_len: 1, values: [Text(\"alpha\")] }
query_order_by=Fields([ExplainOrder { field: \"tag\", direction: Asc }, ExplainOrder { field: \"id\", direction: Asc }])
query_page=None
query_grouping=None
query_pushdown=MissingModelContext
query_consistency=Ignore
execution_aggregation=Min
execution_mode=Materialized
execution_ordering_source=IndexSeekFirst { fetch: 1 }
execution_limit=None
execution_cursor=false
execution_covering_projection=false
execution_node_properties={\"fetch\": Uint(1)}
execution_node_json={\"node_id\":0,\"node_type\":\"AggregateSeekFirst\",\"layer\":\"aggregate\",\"execution_mode\":\"Materialized\",\"execution_mode_detail\":\"materialized\",\"access_strategy\":{\"type\":\"IndexPrefix\",\"name\":\"explain::pushdown_tag\",\"fields\":[\"tag\"],\"prefix_len\":1,\"values\":[\"Text(\\\"alpha\\\")\"]},\"predicate_pushdown_mode\":\"none\",\"predicate_pushdown\":null,\"fast_path_selected\":null,\"fast_path_reason\":null,\"residual_predicate\":null,\"projection\":null,\"ordering_source\":\"IndexSeekFirst\",\"limit\":null,\"cursor\":false,\"covering_scan\":false,\"rows_expected\":null,\"children\":[],\"node_properties\":{\"fetch\":\"Uint(1)\"}}";

    assert_eq!(
        actual, expected,
        "aggregate terminal seek-route explain snapshot drifted",
    );
}

#[test]
fn explain_aggregate_terminal_plan_snapshot_standard_route_is_stable() {
    // Phase 1: build a deterministic full-scan query explain payload.
    let mut plan: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    plan.scalar_plan_mut().page = Some(crate::db::query::plan::PageSpec {
        limit: Some(3),
        offset: 1,
    });
    let query_explain = plan.explain();

    // Phase 2: build one standard-route execution descriptor and snapshot the whole payload.
    let terminal_plan = ExplainAggregateTerminalPlan::new(
        query_explain,
        AggregateKind::Exists,
        ExplainExecutionDescriptor {
            access_strategy: ExplainAccessPath::FullScan,
            covering_projection: false,
            aggregation: AggregateKind::Exists,
            execution_mode: ExplainExecutionMode::Streaming,
            ordering_source: ExplainExecutionOrderingSource::AccessOrder,
            limit: Some(3),
            cursor: true,
            node_properties: ExplainPropertyMap::new(),
        },
    );

    let actual = aggregate_terminal_plan_snapshot(&terminal_plan);
    let expected = "terminal=Exists
query_access=FullScan
query_order_by=Fields([ExplainOrder { field: \"id\", direction: Asc }])
query_page=Page { limit: Some(3), offset: 1 }
query_grouping=None
query_pushdown=MissingModelContext
query_consistency=Ignore
execution_aggregation=Exists
execution_mode=Streaming
execution_ordering_source=AccessOrder
execution_limit=Some(3)
execution_cursor=true
execution_covering_projection=false
execution_node_properties={}
execution_node_json={\"node_id\":0,\"node_type\":\"AggregateExists\",\"layer\":\"aggregate\",\"execution_mode\":\"Streaming\",\"execution_mode_detail\":\"streaming\",\"access_strategy\":{\"type\":\"FullScan\"},\"predicate_pushdown_mode\":\"none\",\"predicate_pushdown\":null,\"fast_path_selected\":null,\"fast_path_reason\":null,\"residual_predicate\":null,\"projection\":null,\"ordering_source\":\"AccessOrder\",\"limit\":3,\"cursor\":true,\"covering_scan\":false,\"rows_expected\":null,\"children\":[],\"node_properties\":{}}";

    assert_eq!(
        actual, expected,
        "aggregate terminal standard-route explain snapshot drifted",
    );
}
