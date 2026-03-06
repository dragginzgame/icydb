use super::*;

#[test]
fn session_load_aggregate_terminals_match_execute() {
    seed_simple_entities(&[8501, 8502, 8503, 8504, 8505]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<SimpleEntity>()
            .order_by("id")
            .offset(1)
            .limit(3)
    };

    let expected = load_window()
        .execute()
        .expect("baseline session execute should succeed");
    let expected_count = expected.count();
    let expected_exists = !expected.is_empty();
    let expected_min = expected.ids().min();
    let expected_max = expected.ids().max();
    let expected_min_by_id = expected.ids().min();
    let expected_max_by_id = expected.ids().max();
    let mut expected_ordered_ids: Vec<_> = expected.ids().collect();
    expected_ordered_ids.sort_unstable();
    let expected_nth_by_id = expected_ordered_ids.get(1).copied();
    let expected_first = expected.id();
    let expected_last = expected.ids().last();

    let actual_count = load_window().count().expect("session count should succeed");
    let actual_exists = load_window()
        .exists()
        .expect("session exists should succeed");
    let actual_min = load_window().min().expect("session min should succeed");
    let actual_max = load_window().max().expect("session max should succeed");
    let actual_min_by_id = load_window()
        .min_by("id")
        .expect("session min_by(id) should succeed");
    let actual_max_by_id = load_window()
        .max_by("id")
        .expect("session max_by(id) should succeed");
    let actual_nth_by_id = load_window()
        .nth_by("id", 1)
        .expect("session nth_by(id, 1) should succeed");
    let actual_first = load_window().first().expect("session first should succeed");
    let actual_last = load_window().last().expect("session last should succeed");

    assert_eq!(actual_count, expected_count, "session count parity failed");
    assert_eq!(
        actual_exists, expected_exists,
        "session exists parity failed"
    );
    assert_eq!(actual_min, expected_min, "session min parity failed");
    assert_eq!(actual_max, expected_max, "session max parity failed");
    assert_eq!(
        actual_min_by_id, expected_min_by_id,
        "session min_by(id) parity failed"
    );
    assert_eq!(
        actual_max_by_id, expected_max_by_id,
        "session max_by(id) parity failed"
    );
    assert_eq!(
        actual_nth_by_id, expected_nth_by_id,
        "session nth_by(id, 1) parity failed"
    );
    assert_eq!(actual_first, expected_first, "session first parity failed");
    assert_eq!(actual_last, expected_last, "session last parity failed");
}

#[test]
fn session_load_min_by_unknown_field_fails_before_scan_budget_consumption() {
    seed_pushdown_entities(&[
        (8_901, 7, 10),
        (8_902, 7, 20),
        (8_903, 7, 30),
        (8_904, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(0)
            .limit(3)
    };

    let (result, scanned_rows) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load_window().min_by("missing_field")
        });
    let Err(err) = result else {
        panic!("session min_by(missing_field) should be rejected");
    };

    assert!(
        matches!(err, QueryError::Execute(_)),
        "session unknown-field min_by should remain an execute-domain error: {err:?}"
    );
    assert_eq!(
        scanned_rows, 0,
        "session unknown-field min_by should fail before scan-budget consumption",
    );
    assert!(
        err.to_string().contains("unknown aggregate target field"),
        "session unknown-field min_by should preserve explicit field taxonomy: {err:?}",
    );
}

#[test]
fn session_load_numeric_field_aggregates_match_execute() {
    seed_pushdown_entities(&[
        (8_121, 7, 10),
        (8_122, 7, 20),
        (8_123, 7, 35),
        (8_124, 8, 99),
    ]);
    let session = DbSession::new(DB);

    let expected_response = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .execute()
        .expect("baseline execute for numeric field aggregates should succeed");
    let mut expected_sum = Decimal::ZERO;
    let mut expected_count = 0u64;
    for row in expected_response {
        let rank = Decimal::from_num(u64::from(row.entity().rank)).expect("rank decimal");
        expected_sum += rank;
        expected_count = expected_count.saturating_add(1);
    }
    let expected_sum_decimal = expected_sum;
    let expected_sum = Some(expected_sum_decimal);
    let expected_avg = if expected_count == 0 {
        None
    } else {
        Some(expected_sum_decimal / Decimal::from_num(expected_count).expect("count decimal"))
    };

    let actual_sum = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .sum_by("rank")
        .expect("session sum_by(rank) should succeed");
    let actual_avg = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .avg_by("rank")
        .expect("session avg_by(rank) should succeed");

    assert_eq!(
        actual_sum, expected_sum,
        "session sum_by(rank) parity failed"
    );
    assert_eq!(
        actual_avg, expected_avg,
        "session avg_by(rank) parity failed"
    );
}

#[test]
fn session_load_new_field_aggregates_match_execute() {
    seed_pushdown_entities(&[
        (8_311, 7, 10),
        (8_312, 7, 10),
        (8_313, 7, 20),
        (8_314, 7, 30),
        (8_315, 7, 40),
        (8_316, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for new field aggregates should succeed");
    let expected_median = expected_median_by_rank_id(&expected);
    let expected_count_distinct = expected_count_distinct_by_rank(&expected);
    let expected_min_max = expected_min_max_by_rank_ids(&expected);

    let actual_median = load_window()
        .median_by("rank")
        .expect("session median_by(rank) should succeed");
    let actual_count_distinct = load_window()
        .count_distinct_by("rank")
        .expect("session count_distinct_by(rank) should succeed");
    let actual_min_max = load_window()
        .min_max_by("rank")
        .expect("session min_max_by(rank) should succeed");

    assert_eq!(
        actual_median, expected_median,
        "session median_by(rank) parity failed"
    );
    assert_eq!(
        actual_count_distinct, expected_count_distinct,
        "session count_distinct_by(rank) parity failed"
    );
    assert_eq!(
        actual_min_max, expected_min_max,
        "session min_max_by(rank) parity failed"
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn session_load_terminal_explain_projects_seek_labels_for_min_and_max() {
    seed_pushdown_entities(&[
        (9_401, 7, 10),
        (9_402, 7, 20),
        (9_403, 7, 30),
        (9_404, 8, 99),
    ]);
    let session = DbSession::new(DB);

    let min_terminal_plan = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .order_by("id")
        .explain_min()
        .expect("session explain_min should succeed");
    assert_eq!(min_terminal_plan.terminal, AggregateKind::Min);
    assert!(matches!(
        min_terminal_plan.route,
        crate::db::ExplainAggregateTerminalRoute::IndexSeekFirst { fetch: 1 }
    ));
    assert_eq!(min_terminal_plan.execution.aggregation, AggregateKind::Min);
    assert!(matches!(
        min_terminal_plan.execution.ordering_source,
        crate::db::ExplainExecutionOrderingSource::IndexSeekFirst { fetch: 1 }
    ));
    assert_eq!(
        min_terminal_plan.execution.access_strategy,
        min_terminal_plan.query.access
    );
    assert_eq!(
        min_terminal_plan.execution.execution_mode,
        crate::db::ExplainExecutionMode::Materialized
    );
    assert_eq!(min_terminal_plan.execution.limit, None);
    assert!(!min_terminal_plan.execution.cursor);
    assert_eq!(
        min_terminal_plan.execution.node_properties.get("fetch"),
        Some(&Value::from(1u64)),
        "seek explain descriptor should expose seek fetch metadata",
    );
    let min_node = min_terminal_plan.execution_node_descriptor();
    assert_eq!(
        min_node.node_type,
        crate::db::ExplainExecutionNodeType::AggregateSeekFirst
    );
    assert_eq!(
        min_node.execution_mode,
        min_terminal_plan.execution.execution_mode
    );
    assert_eq!(
        min_node.access_strategy,
        Some(min_terminal_plan.execution.access_strategy)
    );
    assert_eq!(
        min_node.node_properties.get("fetch"),
        Some(&Value::from(1u64))
    );
    let min_tree = min_node.render_text_tree();
    assert!(
        min_tree.contains("AggregateSeekFirst execution_mode=Materialized"),
        "text tree should render seek node label and execution mode",
    );
    assert!(
        min_tree.contains("node_properties=fetch=Uint(1)"),
        "text tree should render seek fetch metadata in deterministic key order",
    );
    let min_json = min_node.render_json_canonical();
    assert!(
        min_json.contains("\"node_type\":\"AggregateSeekFirst\"")
            && min_json.contains("\"execution_mode\":\"Materialized\"")
            && min_json.contains("\"node_properties\":{\"fetch\":\"Uint(1)\"}"),
        "json rendering should expose canonical aggregate seek descriptor fields",
    );

    let max_terminal_plan = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by_desc("rank")
        .order_by_desc("id")
        .explain_max()
        .expect("session explain_max should succeed");
    assert_eq!(max_terminal_plan.terminal, AggregateKind::Max);
    assert!(matches!(
        max_terminal_plan.route,
        crate::db::ExplainAggregateTerminalRoute::IndexSeekLast { fetch: 1 }
    ));
    assert_eq!(max_terminal_plan.execution.aggregation, AggregateKind::Max);
    assert!(matches!(
        max_terminal_plan.execution.ordering_source,
        crate::db::ExplainExecutionOrderingSource::IndexSeekLast { fetch: 1 }
    ));
    assert_eq!(
        max_terminal_plan.execution.access_strategy,
        max_terminal_plan.query.access
    );
    assert_eq!(
        max_terminal_plan.execution.execution_mode,
        crate::db::ExplainExecutionMode::Materialized
    );
    assert_eq!(max_terminal_plan.execution.limit, None);
    assert!(!max_terminal_plan.execution.cursor);
    assert_eq!(
        max_terminal_plan.execution.node_properties.get("fetch"),
        Some(&Value::from(1u64)),
        "seek explain descriptor should expose seek fetch metadata",
    );
    let max_node = max_terminal_plan.execution_node_descriptor();
    assert_eq!(
        max_node.node_type,
        crate::db::ExplainExecutionNodeType::AggregateSeekLast
    );
    assert_eq!(
        max_node.execution_mode,
        max_terminal_plan.execution.execution_mode
    );
    assert_eq!(
        max_node.access_strategy,
        Some(max_terminal_plan.execution.access_strategy)
    );
    assert_eq!(
        max_node.node_properties.get("fetch"),
        Some(&Value::from(1u64))
    );
    let max_tree = max_node.render_text_tree();
    assert!(
        max_tree.contains("AggregateSeekLast execution_mode=Materialized"),
        "text tree should render seek node label and execution mode",
    );
    let max_json = max_node.render_json_canonical();
    assert!(
        max_json.contains("\"node_type\":\"AggregateSeekLast\"")
            && max_json.contains("\"node_properties\":{\"fetch\":\"Uint(1)\"}"),
        "json rendering should expose canonical aggregate seek descriptor fields",
    );
}

#[test]
fn session_show_indexes_reports_primary_and_secondary_indexes() {
    let session = DbSession::new(DB);

    assert_eq!(
        session.show_indexes::<SimpleEntity>(),
        vec!["PRIMARY KEY (id)".to_string()],
        "entities without secondary indexes should only report primary key metadata",
    );
    assert_eq!(
        session.show_indexes::<PushdownParityEntity>(),
        vec![
            "PRIMARY KEY (id)".to_string(),
            "INDEX group_rank (group, rank)".to_string(),
        ],
        "entities with one non-unique secondary index should report both primary and index rows",
    );
    assert_eq!(
        session.show_indexes::<UniqueIndexRangeEntity>(),
        vec![
            "PRIMARY KEY (id)".to_string(),
            "UNIQUE INDEX code_unique (code)".to_string(),
        ],
        "unique secondary indexes should be explicitly labeled as unique",
    );
}

#[test]
fn session_describe_entity_reports_fields_indexes_and_relations() {
    let session = DbSession::new(DB);

    let indexed = session.describe_entity::<PushdownParityEntity>();
    assert_eq!(indexed.entity_name, "PushdownParityEntity");
    assert_eq!(indexed.primary_key, "id");
    assert_eq!(indexed.fields.len(), 4);
    assert!(indexed.fields.iter().any(|field| {
        field.name == "rank" && field.kind == "uint" && field.queryable && !field.primary_key
    }));
    assert_eq!(
        indexed.indexes,
        vec![crate::db::EntityIndexDescription {
            name: "group_rank".to_string(),
            unique: false,
            fields: vec!["group".to_string(), "rank".to_string()],
        }],
    );
    assert!(
        indexed.relations.is_empty(),
        "non-relation entities should not emit relation describe rows",
    );

    let relation_session = DbSession::new(REL_DB);
    let weak_list = relation_session.describe_entity::<WeakListRelationSourceEntity>();
    assert!(
        weak_list.relations.iter().any(|relation| {
            relation.field == "targets"
                && relation.target_entity_name == "RelationTargetEntity"
                && relation.strength == crate::db::EntityRelationStrength::Weak
                && relation.cardinality == crate::db::EntityRelationCardinality::List
        }),
        "list relation metadata should carry target identity, weak strength, and list cardinality",
    );

    let strong_single = relation_session.describe_entity::<RelationSourceEntity>();
    assert!(
        strong_single.relations.iter().any(|relation| {
            relation.field == "target"
                && relation.target_entity_name == "RelationTargetEntity"
                && relation.strength == crate::db::EntityRelationStrength::Strong
                && relation.cardinality == crate::db::EntityRelationCardinality::Single
        }),
        "scalar strong relation metadata should be projected for describe consumers",
    );
}

#[test]
fn session_trace_query_reports_plan_hash_and_route_summary() {
    seed_pushdown_entities(&[
        (9_501, 7, 10),
        (9_502, 7, 20),
        (9_503, 7, 30),
        (9_504, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .order_by("id")
        .limit(2);

    let trace = session
        .trace_query(&query)
        .expect("session trace_query should succeed");
    let expected_hash = query
        .plan_hash_hex()
        .expect("query plan hash should derive from explain model");

    assert_eq!(
        trace.plan_hash, expected_hash,
        "trace payload must project the same hash as direct plan-hash derivation",
    );
    assert_eq!(
        trace.explain.access,
        query
            .explain()
            .expect("query explain for trace parity should succeed")
            .access,
        "trace explain access path should preserve planner-selected access shape",
    );
    assert!(
        trace.access_strategy.starts_with("Index")
            || trace.access_strategy.starts_with("PrimaryKeyRange")
            || trace.access_strategy == "FullScan"
            || trace.access_strategy.starts_with("Union(")
            || trace.access_strategy.starts_with("Intersection("),
        "trace access strategy summary should provide a human-readable selected access hint",
    );
    assert!(
        matches!(
            trace.execution_strategy,
            Some(crate::db::TraceExecutionStrategy::Ordered)
        ),
        "ordered load shapes should project ordered execution strategy in trace payload",
    );
    assert!(
        matches!(
            trace.explain.order_pushdown,
            crate::db::query::explain::ExplainOrderPushdown::EligibleSecondaryIndex { .. }
                | crate::db::query::explain::ExplainOrderPushdown::Rejected(_)
                | crate::db::query::explain::ExplainOrderPushdown::MissingModelContext
        ),
        "trace explain output must carry planner pushdown eligibility diagnostics",
    );
}

#[test]
fn session_load_terminal_explain_reports_standard_route_for_exists() {
    seed_pushdown_entities(&[
        (9_421, 7, 10),
        (9_422, 7, 20),
        (9_423, 7, 30),
        (9_424, 8, 99),
    ]);
    let session = DbSession::new(DB);

    let exists_terminal_plan = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .order_by("id")
        .explain_exists()
        .expect("session explain_exists should succeed");
    assert_eq!(exists_terminal_plan.terminal, AggregateKind::Exists);
    assert!(matches!(
        exists_terminal_plan.route,
        crate::db::ExplainAggregateTerminalRoute::Standard
    ));
    assert_eq!(
        exists_terminal_plan.execution.aggregation,
        AggregateKind::Exists
    );
    assert!(matches!(
        exists_terminal_plan.execution.ordering_source,
        crate::db::ExplainExecutionOrderingSource::AccessOrder
            | crate::db::ExplainExecutionOrderingSource::Materialized
    ));
    assert_eq!(
        exists_terminal_plan.execution.access_strategy,
        exists_terminal_plan.query.access
    );
    assert!(matches!(
        exists_terminal_plan.execution.execution_mode,
        crate::db::ExplainExecutionMode::Streaming | crate::db::ExplainExecutionMode::Materialized
    ));
    assert_eq!(exists_terminal_plan.execution.limit, None);
    assert!(!exists_terminal_plan.execution.cursor);
    assert!(
        exists_terminal_plan.execution.node_properties.is_empty(),
        "standard explain descriptor should emit no extra node properties by default",
    );
    let exists_node = exists_terminal_plan.execution_node_descriptor();
    assert_eq!(
        exists_node.node_type,
        crate::db::ExplainExecutionNodeType::AggregateExists
    );
    assert_eq!(
        exists_node.execution_mode,
        exists_terminal_plan.execution.execution_mode
    );
    assert_eq!(
        exists_node.access_strategy,
        Some(exists_terminal_plan.execution.access_strategy)
    );
    assert!(
        exists_node.node_properties.is_empty(),
        "standard terminal descriptor should keep node_properties empty",
    );
    let exists_tree = exists_node.render_text_tree();
    assert!(
        exists_tree.contains("AggregateExists execution_mode="),
        "text tree should render standard aggregate node label",
    );
    let exists_json = exists_node.render_json_canonical();
    let key_order = [
        "\"node_type\"",
        "\"execution_mode\"",
        "\"access_strategy\"",
        "\"predicate_pushdown\"",
        "\"residual_predicate\"",
        "\"projection\"",
        "\"ordering_source\"",
        "\"limit\"",
        "\"cursor\"",
        "\"covering_scan\"",
        "\"rows_expected\"",
        "\"children\"",
        "\"node_properties\"",
    ];
    let mut last = 0usize;
    for key in key_order {
        let pos = exists_json
            .find(key)
            .expect("json rendering should include canonical key");
        assert!(
            pos >= last,
            "json canonical key order must stay stable for explain snapshots",
        );
        last = pos;
    }
}

#[test]
fn session_load_explain_execution_projects_descriptor_tree_for_ordered_limited_index_access() {
    seed_pushdown_entities(&[
        (9_501, 7, 10),
        (9_502, 7, 20),
        (9_503, 7, 30),
        (9_504, 8, 99),
    ]);
    let session = DbSession::new(DB);

    let descriptor = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .order_by("id")
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect("session explain_execution should succeed");

    assert!(
        descriptor.access_strategy.is_some(),
        "execution descriptor root should carry one canonical access projection",
    );
    assert!(
        descriptor.children.iter().any(|child| {
            matches!(
                child.node_type,
                crate::db::ExplainExecutionNodeType::IndexPredicatePrefilter
                    | crate::db::ExplainExecutionNodeType::ResidualPredicateFilter
            )
        }),
        "predicate-bearing shapes should surface at least one predicate execution node",
    );

    if let Some(top_n_node) = descriptor
        .children
        .iter()
        .find(|child| child.node_type == crate::db::ExplainExecutionNodeType::TopNSeek)
    {
        assert_eq!(
            top_n_node.node_properties.get("fetch"),
            Some(&Value::from(3u64)),
            "top-n seek node should report bounded fetch count (offset + limit)",
        );
    }

    let limit_node = descriptor
        .children
        .iter()
        .find(|child| child.node_type == crate::db::ExplainExecutionNodeType::LimitOffset)
        .expect("paged shape should project limit/offset node");
    assert_eq!(limit_node.limit, Some(2));
    assert_eq!(
        limit_node.node_properties.get("offset"),
        Some(&Value::from(1u64)),
        "limit/offset node should keep logical offset metadata",
    );

    let text_tree = descriptor.render_text_tree();
    assert!(
        text_tree.contains(" execution_mode="),
        "base text rendering should include root access node label",
    );
    assert!(
        text_tree.contains(" access="),
        "base text rendering should include projected access summary",
    );
    assert!(
        text_tree.contains("LimitOffset execution_mode=") && text_tree.contains("limit=2"),
        "base text rendering should include limit node details",
    );
    if descriptor
        .children
        .iter()
        .any(|child| child.node_type == crate::db::ExplainExecutionNodeType::TopNSeek)
    {
        assert!(
            text_tree.contains("TopNSeek execution_mode="),
            "base text rendering should include top-n seek node label when present",
        );
    }
    let descriptor_json = descriptor.render_json_canonical();
    assert!(
        descriptor_json.contains("\"children\":["),
        "json rendering should include descriptor children array",
    );
    assert!(
        descriptor_json.contains("\"LimitOffset\""),
        "json rendering should include pipeline nodes from descriptor tree",
    );
}
