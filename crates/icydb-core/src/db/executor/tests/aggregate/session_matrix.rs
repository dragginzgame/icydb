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
fn session_load_bytes_matches_execute_window_persisted_payload_sum() {
    seed_pushdown_entities(&[
        (8_951, 7, 10),
        (8_952, 7, 20),
        (8_953, 7, 35),
        (8_954, 8, 99),
        (8_955, 7, 50),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
            .offset(1)
            .limit(2)
    };

    let expected_ids: Vec<_> = load_window()
        .execute()
        .expect("baseline execute for bytes parity should succeed")
        .ids()
        .collect();
    let expected_bytes = persisted_payload_bytes_for_ids::<PushdownParityEntity>(expected_ids);
    let actual_bytes = load_window()
        .bytes()
        .expect("session bytes terminal should succeed");

    assert_eq!(
        actual_bytes, expected_bytes,
        "session bytes parity should match persisted payload byte sum of the effective window"
    );
}

#[test]
fn session_load_bytes_empty_window_returns_zero() {
    seed_pushdown_entities(&[(8_961, 7, 10), (8_962, 7, 20), (8_963, 8, 99)]);
    let session = DbSession::new(DB);

    let actual_bytes = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 999))
        .order_by("rank")
        .bytes()
        .expect("session bytes terminal should succeed for empty windows");

    assert_eq!(
        actual_bytes, 0,
        "session bytes terminal should return zero for empty windows"
    );
}

#[test]
fn session_load_bytes_by_matches_execute_window_serialized_field_sum() {
    seed_pushdown_entities(&[
        (8_971, 7, 10),
        (8_972, 7, 20),
        (8_973, 7, 35),
        (8_974, 8, 99),
        (8_975, 7, 50),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
            .offset(1)
            .limit(2)
    };

    let expected_response = load_window()
        .execute()
        .expect("baseline execute for bytes_by parity should succeed");
    let expected_bytes = serialized_field_payload_bytes_for_rows(&expected_response, "rank");
    let actual_bytes = load_window()
        .bytes_by("rank")
        .expect("session bytes_by(rank) terminal should succeed");

    assert_eq!(
        actual_bytes, expected_bytes,
        "session bytes_by(rank) parity should match serialized field byte sum of the effective window"
    );
}

#[test]
fn session_load_bytes_by_structured_field_matches_execute_window() {
    seed_phase_entities(&[(8_981, 10), (8_982, 20), (8_983, 30), (8_984, 40)]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PhaseEntity>()
            .order_by("id")
            .offset(1)
            .limit(2)
    };

    let expected_response = load_window()
        .execute()
        .expect("baseline execute for structured bytes_by parity should succeed");
    let expected_bytes = serialized_field_payload_bytes_for_rows(&expected_response, "tags");
    let actual_bytes = load_window()
        .bytes_by("tags")
        .expect("session bytes_by(tags) terminal should succeed");

    assert_eq!(
        actual_bytes, expected_bytes,
        "session bytes_by(tags) parity should match serialized structured-field byte sum of the effective window"
    );
}

#[test]
fn session_load_bytes_by_empty_window_returns_zero() {
    seed_pushdown_entities(&[(8_991, 7, 10), (8_992, 7, 20), (8_993, 8, 99)]);
    let session = DbSession::new(DB);

    let actual_bytes = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 999))
        .order_by("rank")
        .bytes_by("rank")
        .expect("session bytes_by(rank) terminal should succeed for empty windows");

    assert_eq!(
        actual_bytes, 0,
        "session bytes_by(rank) terminal should return zero for empty windows"
    );
}

#[test]
fn session_load_bytes_by_unknown_field_fails_before_scan_budget_consumption() {
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
            load_window().bytes_by("missing_field")
        });
    let Err(err) = result else {
        panic!("session bytes_by(missing_field) should be rejected");
    };

    assert!(
        matches!(err, QueryError::Execute(_)),
        "session unknown-field bytes_by should remain an execute-domain error: {err:?}"
    );
    assert_eq!(
        scanned_rows, 0,
        "session unknown-field bytes_by should fail before scan-budget consumption",
    );
    assert!(
        err.to_string().contains("unknown aggregate target field"),
        "session unknown-field bytes_by should preserve explicit field taxonomy: {err:?}",
    );
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

fn session_aggregate_terminal_plan_snapshot(
    plan: &crate::db::ExplainAggregateTerminalPlan,
) -> String {
    let execution = plan.execution();
    let node = plan.execution_node_descriptor();
    let descriptor_json = node.render_json_canonical();

    format!(
        concat!(
            "terminal={:?}\n",
            "route={:?}\n",
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
        plan.route(),
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
fn session_load_terminal_explain_plan_snapshots_for_seek_and_standard_routes_are_stable() {
    // Phase 1: snapshot a deterministic seek-route terminal explain payload.
    seed_pushdown_entities(&[
        (9_811, 7, 10),
        (9_812, 7, 20),
        (9_813, 7, 30),
        (9_814, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let min_terminal_plan = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7))
        .order_by("rank")
        .order_by("id")
        .explain_min()
        .expect("session explain_min snapshot should succeed");

    let min_actual = session_aggregate_terminal_plan_snapshot(&min_terminal_plan);
    let min_expected = "terminal=Min
route=IndexSeekFirst { fetch: 1 }
query_access=FullScan
query_order_by=Fields([ExplainOrder { field: \"rank\", direction: Asc }, ExplainOrder { field: \"id\", direction: Asc }])
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
execution_node_json={\"node_type\":\"AggregateSeekFirst\",\"execution_mode\":\"Materialized\",\"access_strategy\":{\"type\":\"FullScan\"},\"predicate_pushdown\":null,\"residual_predicate\":null,\"projection\":null,\"ordering_source\":\"IndexSeekFirst\",\"limit\":null,\"cursor\":false,\"covering_scan\":false,\"rows_expected\":null,\"children\":[],\"node_properties\":{\"fetch\":\"Uint(1)\"}}";
    assert_eq!(
        min_actual, min_expected,
        "seek-route terminal explain snapshot drifted: actual={min_actual}",
    );

    // Phase 2: snapshot a deterministic standard-route terminal explain payload.
    seed_simple_entities(&[9_821, 9_822]);
    let simple_session = DbSession::new(DB);
    let exists_terminal_plan = simple_session
        .load::<SimpleEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Eq,
            Value::Ulid(Ulid::from_u128(9_821)),
            CoercionId::Strict,
        )))
        .explain_exists()
        .expect("session explain_exists snapshot should succeed");

    let exists_actual = session_aggregate_terminal_plan_snapshot(&exists_terminal_plan);
    let exists_expected = "terminal=Exists
route=Standard
query_access=ByKey { key: Ulid(Ulid(Ulid(9821))) }
query_order_by=None
query_page=None
query_grouping=None
query_pushdown=MissingModelContext
query_consistency=Ignore
execution_aggregation=Exists
execution_mode=Streaming
execution_ordering_source=AccessOrder
execution_limit=None
execution_cursor=false
execution_covering_projection=false
execution_node_properties={}
execution_node_json={\"node_type\":\"AggregateExists\",\"execution_mode\":\"Streaming\",\"access_strategy\":{\"type\":\"ByKey\",\"key\":\"Ulid(Ulid(Ulid(9821)))\"},\"predicate_pushdown\":null,\"residual_predicate\":null,\"projection\":null,\"ordering_source\":\"AccessOrder\",\"limit\":null,\"cursor\":false,\"covering_scan\":false,\"rows_expected\":null,\"children\":[],\"node_properties\":{}}";
    assert_eq!(
        exists_actual, exists_expected,
        "standard-route terminal explain snapshot drifted: actual={exists_actual}",
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
    assert_eq!(min_terminal_plan.terminal(), AggregateKind::Min);
    assert!(matches!(
        min_terminal_plan.route(),
        crate::db::ExplainAggregateTerminalRoute::IndexSeekFirst { fetch: 1 }
    ));
    let min_execution = min_terminal_plan.execution();
    assert_eq!(min_execution.aggregation(), AggregateKind::Min);
    assert!(matches!(
        min_execution.ordering_source(),
        crate::db::ExplainExecutionOrderingSource::IndexSeekFirst { fetch: 1 }
    ));
    assert_eq!(
        min_execution.access_strategy(),
        min_terminal_plan.query().access()
    );
    assert_eq!(
        min_execution.execution_mode(),
        crate::db::ExplainExecutionMode::Materialized
    );
    assert_eq!(min_execution.limit(), None);
    assert!(!min_execution.cursor());
    assert_eq!(
        min_execution.node_properties().get("fetch"),
        Some(&Value::from(1u64)),
        "seek explain descriptor should expose seek fetch metadata",
    );
    let min_node = min_terminal_plan.execution_node_descriptor();
    assert_eq!(
        min_node.node_type(),
        crate::db::ExplainExecutionNodeType::AggregateSeekFirst
    );
    assert_eq!(min_node.execution_mode(), min_execution.execution_mode());
    assert_eq!(
        min_node.access_strategy(),
        Some(min_execution.access_strategy())
    );
    assert_eq!(
        min_node.node_properties().get("fetch"),
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
    assert_eq!(max_terminal_plan.terminal(), AggregateKind::Max);
    assert!(matches!(
        max_terminal_plan.route(),
        crate::db::ExplainAggregateTerminalRoute::IndexSeekLast { fetch: 1 }
    ));
    let max_execution = max_terminal_plan.execution();
    assert_eq!(max_execution.aggregation(), AggregateKind::Max);
    assert!(matches!(
        max_execution.ordering_source(),
        crate::db::ExplainExecutionOrderingSource::IndexSeekLast { fetch: 1 }
    ));
    assert_eq!(
        max_execution.access_strategy(),
        max_terminal_plan.query().access()
    );
    assert_eq!(
        max_execution.execution_mode(),
        crate::db::ExplainExecutionMode::Materialized
    );
    assert_eq!(max_execution.limit(), None);
    assert!(!max_execution.cursor());
    assert_eq!(
        max_execution.node_properties().get("fetch"),
        Some(&Value::from(1u64)),
        "seek explain descriptor should expose seek fetch metadata",
    );
    let max_node = max_terminal_plan.execution_node_descriptor();
    assert_eq!(
        max_node.node_type(),
        crate::db::ExplainExecutionNodeType::AggregateSeekLast
    );
    assert_eq!(max_node.execution_mode(), max_execution.execution_mode());
    assert_eq!(
        max_node.access_strategy(),
        Some(max_execution.access_strategy())
    );
    assert_eq!(
        max_node.node_properties().get("fetch"),
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
    assert_eq!(indexed.entity_name(), "PushdownParityEntity");
    assert_eq!(indexed.primary_key(), "id");
    assert_eq!(indexed.fields().len(), 4);
    assert!(indexed.fields().iter().any(|field| {
        field.name() == "rank"
            && field.kind() == "uint"
            && field.queryable()
            && !field.primary_key()
    }));
    assert_eq!(
        indexed.indexes(),
        vec![crate::db::EntityIndexDescription {
            name: "group_rank".to_string(),
            unique: false,
            fields: vec!["group".to_string(), "rank".to_string()],
        }],
    );
    assert!(
        indexed.relations().is_empty(),
        "non-relation entities should not emit relation describe rows",
    );

    let relation_session = DbSession::new(REL_DB);
    let weak_list = relation_session.describe_entity::<WeakListRelationSourceEntity>();
    assert!(
        weak_list.relations().iter().any(|relation| {
            relation.field() == "targets"
                && relation.target_entity_name() == "RelationTargetEntity"
                && relation.strength() == crate::db::EntityRelationStrength::Weak
                && relation.cardinality() == crate::db::EntityRelationCardinality::List
        }),
        "list relation metadata should carry target identity, weak strength, and list cardinality",
    );

    let strong_single = relation_session.describe_entity::<RelationSourceEntity>();
    assert!(
        strong_single.relations().iter().any(|relation| {
            relation.field() == "target"
                && relation.target_entity_name() == "RelationTargetEntity"
                && relation.strength() == crate::db::EntityRelationStrength::Strong
                && relation.cardinality() == crate::db::EntityRelationCardinality::Single
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
    let trace_explain = trace.explain();
    let query_explain = query
        .explain()
        .expect("query explain for trace parity should succeed");

    assert_eq!(
        trace.plan_hash(),
        expected_hash,
        "trace payload must project the same hash as direct plan-hash derivation",
    );
    assert_eq!(
        trace_explain.access(),
        query_explain.access(),
        "trace explain access path should preserve planner-selected access shape",
    );
    assert!(
        trace.access_strategy().starts_with("Index")
            || trace.access_strategy().starts_with("PrimaryKeyRange")
            || trace.access_strategy() == "FullScan"
            || trace.access_strategy().starts_with("Union(")
            || trace.access_strategy().starts_with("Intersection("),
        "trace access strategy summary should provide a human-readable selected access hint",
    );
    assert!(
        matches!(
            trace.execution_strategy(),
            Some(crate::db::TraceExecutionStrategy::Ordered)
        ),
        "ordered load shapes should project ordered execution strategy in trace payload",
    );
    assert!(
        matches!(
            trace_explain.order_pushdown(),
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
    assert_eq!(exists_terminal_plan.terminal(), AggregateKind::Exists);
    assert!(matches!(
        exists_terminal_plan.route(),
        crate::db::ExplainAggregateTerminalRoute::Standard
    ));
    let exists_execution = exists_terminal_plan.execution();
    assert_eq!(exists_execution.aggregation(), AggregateKind::Exists);
    assert!(matches!(
        exists_execution.ordering_source(),
        crate::db::ExplainExecutionOrderingSource::AccessOrder
            | crate::db::ExplainExecutionOrderingSource::Materialized
    ));
    assert_eq!(
        exists_execution.access_strategy(),
        exists_terminal_plan.query().access()
    );
    assert!(matches!(
        exists_execution.execution_mode(),
        crate::db::ExplainExecutionMode::Streaming | crate::db::ExplainExecutionMode::Materialized
    ));
    assert_eq!(exists_execution.limit(), None);
    assert!(!exists_execution.cursor());
    assert!(
        !exists_execution.covering_projection(),
        "ordered exists explain shape should not mark index-only covering projection",
    );
    assert!(
        exists_execution.node_properties().is_empty(),
        "standard explain descriptor should emit no extra node properties by default",
    );
    let exists_node = exists_terminal_plan.execution_node_descriptor();
    assert_eq!(
        exists_node.node_type(),
        crate::db::ExplainExecutionNodeType::AggregateExists
    );
    assert_eq!(
        exists_node.execution_mode(),
        exists_execution.execution_mode()
    );
    assert_eq!(
        exists_node.access_strategy(),
        Some(exists_execution.access_strategy())
    );
    assert!(
        exists_node.node_properties().is_empty(),
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
        descriptor.access_strategy().is_some(),
        "execution descriptor root should carry one canonical access projection",
    );
    assert!(
        explain_execution_contains_node_type(
            &descriptor,
            crate::db::ExplainExecutionNodeType::IndexPredicatePrefilter,
        ) || explain_execution_contains_node_type(
            &descriptor,
            crate::db::ExplainExecutionNodeType::ResidualPredicateFilter,
        ),
        "predicate-bearing shapes should surface at least one predicate execution node",
    );

    if let Some(top_n_node) = explain_execution_find_first_node(
        &descriptor,
        crate::db::ExplainExecutionNodeType::TopNSeek,
    ) {
        assert_eq!(
            top_n_node.node_properties().get("fetch"),
            Some(&Value::from(3u64)),
            "top-n seek node should report bounded fetch count (offset + limit)",
        );
    }

    let limit_node = descriptor
        .children()
        .iter()
        .find(|child| child.node_type() == crate::db::ExplainExecutionNodeType::LimitOffset)
        .expect("paged shape should project limit/offset node");
    assert_eq!(limit_node.limit(), Some(2));
    assert_eq!(
        limit_node.node_properties().get("offset"),
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
    if explain_execution_contains_node_type(
        &descriptor,
        crate::db::ExplainExecutionNodeType::TopNSeek,
    ) {
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

#[test]
fn session_load_explain_execution_access_root_matrix_is_stable() {
    seed_simple_entities(&[9_701, 9_702]);
    let simple_session = DbSession::new(DB);
    let by_key = simple_session
        .load::<SimpleEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Eq,
            Value::Ulid(Ulid::from_u128(9_701)),
            CoercionId::Strict,
        )))
        .order_by("id")
        .explain_execution()
        .expect("by-key explain execution should succeed");
    assert_eq!(
        by_key.node_type(),
        crate::db::ExplainExecutionNodeType::ByKeyLookup,
        "single id predicate should keep by-key execution root",
    );

    seed_pushdown_entities(&[
        (9_711, 7, 10),
        (9_712, 7, 20),
        (9_713, 8, 30),
        (9_714, 8, 40),
    ]);
    let pushdown_session = DbSession::new(DB);
    let prefix = pushdown_session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate_strict("group", 7))
        .order_by("rank")
        .order_by("id")
        .explain_execution()
        .expect("index-prefix explain execution should succeed");
    assert_eq!(
        prefix.node_type(),
        crate::db::ExplainExecutionNodeType::IndexPrefixScan,
        "strict equality on leading index field should keep index-prefix root",
    );

    let multi = pushdown_session
        .load::<PushdownParityEntity>()
        .filter(u32_in_predicate_strict("group", &[7, 8]))
        .order_by("rank")
        .order_by("id")
        .explain_execution()
        .expect("index-multi explain execution should succeed");
    assert_eq!(
        multi.node_type(),
        crate::db::ExplainExecutionNodeType::IndexMultiLookup,
        "IN predicate on indexed field should keep index-multi root",
    );

    seed_unique_index_range_entities(&[
        (9_721, 101),
        (9_722, 102),
        (9_723, 103),
        (9_724, 104),
        (9_725, 105),
    ]);
    let range_session = DbSession::new(DB);
    let range = range_session
        .load::<UniqueIndexRangeEntity>()
        .filter(u32_range_predicate("code", 101, 105))
        .order_by("code")
        .order_by("id")
        .explain_execution()
        .expect("index-range explain execution should succeed");
    assert_eq!(
        range.node_type(),
        crate::db::ExplainExecutionNodeType::IndexRangeScan,
        "bounded range predicate should keep index-range root",
    );
}

#[test]
fn session_load_explain_execution_predicate_stage_and_limit_zero_matrix_is_stable() {
    seed_pushdown_entities(&[
        (9_731, 7, 10),
        (9_732, 7, 20),
        (9_733, 7, 30),
        (9_734, 8, 40),
    ]);
    let session = DbSession::new(DB);

    let strict_prefilter = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate_strict("group", 7))
        .order_by("rank")
        .order_by("id")
        .explain_execution()
        .expect("strict prefilter explain execution should succeed");
    assert!(
        explain_execution_contains_node_type(
            &strict_prefilter,
            crate::db::ExplainExecutionNodeType::IndexPredicatePrefilter,
        ),
        "strict index-compatible predicate should emit prefilter stage node",
    );
    assert!(
        !explain_execution_contains_node_type(
            &strict_prefilter,
            crate::db::ExplainExecutionNodeType::ResidualPredicateFilter,
        ),
        "strict index-compatible predicate should not emit residual stage node",
    );

    let residual_predicate = Predicate::And(vec![
        u32_eq_predicate_strict("group", 7),
        Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::Eq,
            Value::Text("g7-r20".to_string()),
            CoercionId::Strict,
        )),
    ]);
    let residual = session
        .load::<PushdownParityEntity>()
        .filter(residual_predicate)
        .order_by("rank")
        .order_by("id")
        .explain_execution()
        .expect("residual predicate explain execution should succeed");
    assert!(
        explain_execution_contains_node_type(
            &residual,
            crate::db::ExplainExecutionNodeType::ResidualPredicateFilter,
        ),
        "mixed index/non-index predicate should emit residual stage node",
    );

    let limit_zero = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate_strict("group", 7))
        .order_by("rank")
        .order_by("id")
        .limit(0)
        .explain_execution()
        .expect("limit-zero explain execution should succeed");
    if let Some(top_n) = explain_execution_find_first_node(
        &limit_zero,
        crate::db::ExplainExecutionNodeType::TopNSeek,
    ) {
        assert_eq!(
            top_n.node_properties().get("fetch"),
            Some(&Value::from(0u64)),
            "limit-zero top-n node should freeze fetch=0 contract",
        );
    } else {
        assert!(
            explain_execution_contains_node_type(
                &limit_zero,
                crate::db::ExplainExecutionNodeType::OrderByMaterializedSort,
            ),
            "limit-zero routes without top-n seek should still expose materialized order fallback",
        );
    }
    let limit_node = explain_execution_find_first_node(
        &limit_zero,
        crate::db::ExplainExecutionNodeType::LimitOffset,
    )
    .expect("limit-zero route should emit limit/offset node");
    assert_eq!(limit_node.limit(), Some(0));
}

#[test]
fn session_load_explain_execution_text_and_json_snapshot_for_strict_index_prefix_shape() {
    seed_pushdown_entities(&[
        (9_741, 7, 10),
        (9_742, 7, 20),
        (9_743, 7, 30),
        (9_744, 8, 40),
    ]);
    let session = DbSession::new(DB);
    let query = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate_strict("group", 7))
        .order_by("rank")
        .order_by("id")
        .offset(1)
        .limit(2);

    let text_tree = query
        .explain_execution_text()
        .expect("strict index-prefix execution text explain should succeed");
    let expected_text = r#"IndexPrefixScan execution_mode=Materialized access=IndexPrefix(group_rank)
  IndexPredicatePrefilter execution_mode=Materialized predicate_pushdown=strict_all_or_none
  SecondaryOrderPushdown execution_mode=Materialized node_properties=index=Text("group_rank"),prefix_len=Uint(1)
  OrderByMaterializedSort execution_mode=Materialized
  LimitOffset execution_mode=Materialized limit=2 cursor=false node_properties=offset=Uint(1)"#;
    assert_eq!(
        text_tree, expected_text,
        "execution text-tree snapshot drifted: actual={text_tree}",
    );

    let descriptor_json = query
        .explain_execution_json()
        .expect("strict index-prefix execution json explain should succeed");
    let expected_json = r#"{"node_type":"IndexPrefixScan","execution_mode":"Materialized","access_strategy":{"type":"IndexPrefix","name":"group_rank","fields":["group","rank"],"prefix_len":1,"values":["Uint(7)"]},"predicate_pushdown":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[{"node_type":"IndexPredicatePrefilter","execution_mode":"Materialized","access_strategy":null,"predicate_pushdown":"strict_all_or_none","residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{}},{"node_type":"SecondaryOrderPushdown","execution_mode":"Materialized","access_strategy":null,"predicate_pushdown":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"index":"Text(\"group_rank\")","prefix_len":"Uint(1)"}},{"node_type":"OrderByMaterializedSort","execution_mode":"Materialized","access_strategy":null,"predicate_pushdown":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{}},{"node_type":"LimitOffset","execution_mode":"Materialized","access_strategy":null,"predicate_pushdown":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":2,"cursor":false,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"offset":"Uint(1)"}}],"node_properties":{}}"#;
    assert_eq!(
        descriptor_json, expected_json,
        "execution json snapshot drifted: actual={descriptor_json}",
    );
}
