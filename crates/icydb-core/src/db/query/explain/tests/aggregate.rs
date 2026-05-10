use super::*;

fn assert_aggregate_terminal_public_contract(
    plan: &ExplainAggregateTerminalPlan,
    expected_node_type: ExplainExecutionNodeType,
    expected_layer: &str,
    expected_execution_mode_detail: &str,
) {
    let execution = plan.execution();
    let node = plan.execution_node_descriptor();
    let text = node.render_text_tree();
    let json = node.render_json_canonical();

    assert_eq!(
        node.node_type(),
        expected_node_type,
        "aggregate terminal explain must keep the high-level execution node family stable",
    );
    assert!(
        text.contains(&format!("layer={expected_layer}")),
        "aggregate terminal text explain must keep the node layer stable",
    );
    assert!(
        json.contains(&format!("\"layer\":\"{expected_layer}\"")),
        "aggregate terminal JSON explain must keep the node layer stable",
    );
    assert!(
        text.contains(&format!(
            "execution_mode_detail={expected_execution_mode_detail}"
        )),
        "aggregate terminal text explain must keep execution mode detail stable",
    );
    assert!(
        json.contains(&format!(
            "\"execution_mode_detail\":\"{expected_execution_mode_detail}\""
        )),
        "aggregate terminal JSON explain must keep execution mode detail stable",
    );
    assert!(
        json.contains("\"predicate_pushdown_mode\":\"none\""),
        "aggregate terminal JSON explain must keep the pushdown classification stable",
    );
    assert_eq!(
        execution.aggregation(),
        plan.terminal(),
        "aggregate terminal execution payload must stay aligned with the terminal kind",
    );
}

#[test]
fn explain_aggregate_terminal_seek_route_public_contract_is_stable() {
    // Phase 1: build a deterministic index-prefix query explain payload.
    let mut plan: AccessPlannedQuery = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::from_index(PUSHDOWN_INDEX),
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("tag", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
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
                name: "explain::pushdown_tag".to_string(),
                fields: vec!["tag".to_string()],
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

    assert_eq!(terminal_plan.terminal(), AggregateKind::Min);
    assert!(matches!(
        terminal_plan.query().access(),
        ExplainAccessPath::IndexPrefix { name, fields, prefix_len, values }
            if name == "explain::pushdown_tag"
                && fields == &vec!["tag".to_string()]
                && *prefix_len == 1
                && values == &vec![Value::Text("alpha".to_string())]
    ));
    assert!(matches!(
        terminal_plan.query().order_by(),
        ExplainOrderBy::Fields(fields)
            if fields.len() == 2
                && fields[0].field == "tag"
                && fields[0].direction == OrderDirection::Asc
                && fields[1].field == "id"
                && fields[1].direction == OrderDirection::Asc
    ));
    assert_eq!(terminal_plan.query().page(), &ExplainPagination::None);
    assert_eq!(terminal_plan.query().grouping(), &ExplainGrouping::None);
    assert_eq!(
        terminal_plan.execution().ordering_source(),
        ExplainExecutionOrderingSource::IndexSeekFirst { fetch: 1 },
    );
    assert_eq!(terminal_plan.execution().limit(), None);
    assert!(!terminal_plan.execution().cursor());
    assert!(!terminal_plan.execution().covering_projection());

    assert_aggregate_terminal_public_contract(
        &terminal_plan,
        ExplainExecutionNodeType::AggregateSeekFirst,
        "aggregate",
        "materialized",
    );
}

#[test]
fn explain_aggregate_terminal_standard_route_public_contract_is_stable() {
    // Phase 1: build a deterministic full-scan query explain payload.
    let mut plan: AccessPlannedQuery =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
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

    assert_eq!(terminal_plan.terminal(), AggregateKind::Exists);
    assert_eq!(terminal_plan.query().access(), &ExplainAccessPath::FullScan);
    assert!(matches!(
        terminal_plan.query().order_by(),
        ExplainOrderBy::Fields(fields)
            if fields.len() == 1
                && fields[0].field == "id"
                && fields[0].direction == OrderDirection::Asc
    ));
    assert_eq!(
        terminal_plan.query().page(),
        &ExplainPagination::Page {
            limit: Some(3),
            offset: 1,
        },
    );
    assert_eq!(terminal_plan.query().grouping(), &ExplainGrouping::None);
    assert_eq!(
        terminal_plan.execution().ordering_source(),
        ExplainExecutionOrderingSource::AccessOrder,
    );
    assert_eq!(terminal_plan.execution().limit(), Some(3));
    assert!(terminal_plan.execution().cursor());
    assert!(!terminal_plan.execution().covering_projection());

    assert_aggregate_terminal_public_contract(
        &terminal_plan,
        ExplainExecutionNodeType::AggregateExists,
        "aggregate",
        "streaming",
    );
}

#[test]
fn explain_aggregate_terminal_filtered_route_surfaces_filter_shape() {
    // Phase 1: build one deterministic full-scan aggregate explain payload and
    // annotate it with the same filter label the SQL aggregate explain surface
    // now threads into execution node properties.
    let query_explain =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore).explain();
    let mut node_properties = ExplainPropertyMap::new();
    node_properties.insert("filter_expr", Value::from("rank >= 10"));
    let terminal_plan = ExplainAggregateTerminalPlan::new(
        query_explain,
        AggregateKind::Count,
        ExplainExecutionDescriptor {
            access_strategy: ExplainAccessPath::FullScan,
            covering_projection: false,
            aggregation: AggregateKind::Count,
            execution_mode: ExplainExecutionMode::Streaming,
            ordering_source: ExplainExecutionOrderingSource::AccessOrder,
            limit: None,
            cursor: false,
            node_properties,
        },
    );

    // Phase 2: require the low-level execution explain surfaces to keep that
    // filter marker visible in both text and canonical JSON output.
    let node = terminal_plan.execution_node_descriptor();
    let text = node.render_text_tree();
    let json = node.render_json_canonical();

    assert!(
        text.contains("filter_expr=Text(\"rank >= 10\")"),
        "filtered aggregate terminal text explain should keep filter shape visible: {text}",
    );
    assert!(
        json.contains("\"filter_expr\":\"Text(\\\"rank >= 10\\\")\""),
        "filtered aggregate terminal JSON explain should keep filter shape visible: {json}",
    );
}
