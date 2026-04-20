use super::*;
use crate::db::{FieldRef, FilterExpr};

fn assert_primary_key_covering_descriptor(
    descriptor: &crate::db::ExplainExecutionNodeDescriptor,
    expected_root: ExplainExecutionNodeType,
    expected_row_mode: &str,
    context: &str,
) {
    assert_eq!(
        descriptor.node_type(),
        expected_root,
        "{context} should explain through the expected root node",
    );
    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "{context} should expose the explicit covering route",
    );
    let projection_node =
        explain_execution_find_first_node(descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("PK-only covering explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text(expected_row_mode.to_string())),
        "{context} should surface the expected existing-row mode",
    );
}

#[test]
fn session_sql_global_aggregate_explain_execution_stays_off_secondary_authority_surface() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(&session, &[(9_451, 7, 10), (9_452, 7, 20), (9_453, 8, 99)]);

    let explain = statement_explain_sql::<SessionAggregateEntity>(
        &session,
        "EXPLAIN EXECUTION SELECT COUNT(*) FROM SessionAggregateEntity",
    )
    .expect("global aggregate EXPLAIN EXECUTION should succeed");

    assert!(
        !explain.contains("authority_decision")
            && !explain.contains("authority_reason")
            && !explain.contains("index_state"),
        "aggregate EXPLAIN EXECUTION should stay off the removed secondary-read label surface",
    );
}

#[test]
fn session_sql_filtered_global_aggregate_explain_execution_hides_non_ready_secondary_indexes_from_planner_visibility()
 {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("Sam", 30), ("Sasha", 24), ("Soren", 18), ("Mira", 40)],
    );
    let sql = "EXPLAIN EXECUTION SELECT COUNT(*) FROM IndexedSessionSqlEntity WHERE name = 'Sam'";

    let ready_explain = statement_explain_sql::<IndexedSessionSqlEntity>(&session, sql)
        .expect("filtered aggregate EXPLAIN EXECUTION should succeed while the index is ready");
    assert!(
        ready_explain.contains("AggregateCount execution_mode=")
            && ready_explain.contains("access_strategy=IndexPrefix"),
        "ready filtered aggregate EXPLAIN EXECUTION should keep the planner-visible name index: {ready_explain}",
    );
    assert!(
        !ready_explain.contains("access_strategy=FullScan")
            && !ready_explain.contains("authority_decision")
            && !ready_explain.contains("authority_reason")
            && !ready_explain.contains("index_state"),
        "ready filtered aggregate EXPLAIN EXECUTION should stay off both the full-scan fallback and the removed secondary-read label surface: {ready_explain}",
    );

    INDEXED_SESSION_SQL_DB
        .recovered_store(IndexedSessionSqlStore::PATH)
        .expect("indexed SQL store should recover")
        .mark_index_building();

    let building_explain = statement_explain_sql::<IndexedSessionSqlEntity>(&session, sql)
        .expect("filtered aggregate EXPLAIN EXECUTION should still succeed once the shared index becomes building");
    assert!(
        building_explain.contains("AggregateCount execution_mode=")
            && building_explain.contains("access_strategy=FullScan"),
        "building filtered aggregate EXPLAIN EXECUTION should fall back to FullScan once the name index becomes planner-invisible: {building_explain}",
    );
    assert!(
        !building_explain.contains("access_strategy=IndexPrefix")
            && !building_explain.contains("authority_decision")
            && !building_explain.contains("authority_reason")
            && !building_explain.contains("index_state"),
        "building filtered aggregate EXPLAIN EXECUTION should not keep the hidden index or any removed secondary-read labels: {building_explain}",
    );
}

// Matrix-style explain contract test that keeps strict-pushdown, residual, and
// limit-zero behavior together on one session-local indexed fixture.
#[test]
fn session_explain_execution_predicate_stage_and_limit_zero_matrix_is_stable() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("Sam", 30), ("Sasha", 24), ("Soren", 18), ("Mira", 40)],
    );

    let strict_prefilter = session
        .load::<IndexedSessionSqlEntity>()
        .filter(FieldRef::new("name").eq("Sam"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .explain_execution()
        .expect("strict indexed prefilter explain_execution should succeed");
    assert!(
        explain_execution_contains_node_type(
            &strict_prefilter,
            ExplainExecutionNodeType::IndexPredicatePrefilter,
        ),
        "strict index-compatible predicate should emit a prefilter stage node",
    );
    assert!(
        !explain_execution_contains_node_type(
            &strict_prefilter,
            ExplainExecutionNodeType::ResidualFilter,
        ),
        "strict index-compatible predicate should not emit a residual stage node",
    );
    let strict_prefilter_node = explain_execution_find_first_node(
        &strict_prefilter,
        ExplainExecutionNodeType::IndexPredicatePrefilter,
    )
    .expect("strict index-compatible predicate should project a prefilter node");
    assert!(
        strict_prefilter_node
            .node_properties()
            .contains_key("pushdown"),
        "strict prefilter node should expose pushed predicate summary metadata",
    );

    let residual = session
        .load::<IndexedSessionSqlEntity>()
        .filter(FilterExpr::and(vec![
            FieldRef::new("name").eq("Sasha"),
            FieldRef::new("age").eq(24_u64),
        ]))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .explain_execution()
        .expect("mixed indexed and non-indexed predicate explain_execution should succeed");
    assert!(
        explain_execution_contains_node_type(&residual, ExplainExecutionNodeType::ResidualFilter,),
        "mixed index/non-index predicate should emit a residual stage node",
    );
    let residual_node =
        explain_execution_find_first_node(&residual, ExplainExecutionNodeType::ResidualFilter)
            .expect("mixed index/non-index predicate should project a residual node");
    assert!(
        residual_node.predicate_pushdown().is_some(),
        "residual node should report pushed access predicate separately from the residual filter",
    );
    assert_eq!(
        residual_node.filter_expr(),
        Some("name = 'Sasha' AND age = 24"),
        "residual node should expose the semantic scalar filter expression separately from the derived predicate contract",
    );
    assert!(
        residual_node.residual_filter_predicate().is_some(),
        "residual node should still expose the derived predicate contract",
    );

    let limit_zero = session
        .load::<IndexedSessionSqlEntity>()
        .filter(FieldRef::new("name").eq("Sam"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(0)
        .explain_execution()
        .expect("limit-zero explain_execution should succeed");
    if let Some(top_n_node) =
        explain_execution_find_first_node(&limit_zero, ExplainExecutionNodeType::TopNSeek)
    {
        assert_eq!(
            top_n_node.node_properties().get("fetch"),
            Some(&Value::from(0u64)),
            "limit-zero top-n nodes should freeze the fetch=0 contract",
        );
    } else {
        assert!(
            explain_execution_contains_node_type(
                &limit_zero,
                ExplainExecutionNodeType::OrderByMaterializedSort,
            ),
            "limit-zero routes without top-n seek should still expose materialized order fallback",
        );
    }
    let limit_node =
        explain_execution_find_first_node(&limit_zero, ExplainExecutionNodeType::LimitOffset)
            .expect("limit-zero route should emit a limit/offset node");
    assert_eq!(limit_node.limit(), Some(0));
}

#[test]
fn session_explain_execution_access_root_matrix_is_stable() {
    reset_session_sql_store();
    let session = sql_session();
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(9_701),
            name: "alpha".to_string(),
            age: 21,
        })
        .expect("by-key session seed should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(9_702),
            name: "beta".to_string(),
            age: 22,
        })
        .expect("by-key session seed should succeed");

    let by_key = session
        .load::<SessionSqlEntity>()
        .filter(FieldRef::new("id").eq(Ulid::from_u128(9_701)))
        .order_term(crate::db::asc("id"))
        .explain_execution()
        .expect("by-key explain_execution should succeed");
    assert_eq!(
        by_key.node_type(),
        ExplainExecutionNodeType::ByKeyLookup,
        "single id predicate should keep by-key execution root",
    );

    reset_indexed_session_sql_store();
    let indexed_session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &indexed_session,
        &[("Sam", 30), ("Sasha", 24), ("Soren", 18), ("Mira", 40)],
    );

    let prefix = indexed_session
        .load::<IndexedSessionSqlEntity>()
        .filter(FieldRef::new("name").eq("Sam"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .explain_execution()
        .expect("index-prefix explain_execution should succeed");
    assert_eq!(
        prefix.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan,
        "strict equality on the indexed field should keep index-prefix root",
    );

    let multi = indexed_session
        .load::<IndexedSessionSqlEntity>()
        .filter(FieldRef::new("name").in_list(["Sam", "Sasha"]))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .explain_execution()
        .expect("index-multi explain_execution should succeed");
    assert_eq!(
        multi.node_type(),
        ExplainExecutionNodeType::IndexMultiLookup,
        "IN predicate on the indexed field should keep index-multi root",
    );
}

#[test]
fn session_explain_execution_covering_scan_requires_coverable_projection_route() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);

    let entity_descriptor = session
        .load::<IndexedSessionSqlEntity>()
        .filter(FieldRef::new("name").eq("Sam"))
        .explain_execution()
        .expect("unordered strict index-prefix entity explain_execution should succeed");

    assert_eq!(
        entity_descriptor.covering_scan(),
        Some(false),
        "all-field entity loads should stay on the materialized route even when access stays index-backed",
    );

    let projected_descriptor = lower_select_query_for_tests::<IndexedSessionSqlEntity>(
        &session,
        "SELECT id, name FROM IndexedSessionSqlEntity WHERE name = 'Sam' ORDER BY id ASC LIMIT 1",
    )
    .expect("coverable SQL projection query should lower")
    .explain_execution()
    .expect("coverable SQL projection explain_execution should succeed");

    assert_eq!(
        projected_descriptor.covering_scan(),
        Some(true),
        "coverable projected reads should report the explicit covering-read route",
    );
    let projection_node = explain_execution_find_first_node(
        &projected_descriptor,
        ExplainExecutionNodeType::CoveringRead,
    )
    .expect("coverable projection explain trees should emit an explicit covering-read node");
    assert_eq!(
        projection_node.projection(),
        Some("covering_read"),
        "projection node should label the covering-read terminal route explicitly",
    );
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "projection node should expose the planner-proven existing-row mode explicitly",
    );
}

#[test]
fn execute_sql_projection_primary_key_covering_full_scan_returns_ordered_ids() {
    reset_session_sql_store();
    let session = sql_session();

    // Phase 1: seed deterministic primary-key order.
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(9_801),
            name: "alpha".to_string(),
            age: 21,
        })
        .expect("PK-covering session seed should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(9_802),
            name: "beta".to_string(),
            age: 22,
        })
        .expect("PK-covering session seed should succeed");

    // Phase 2: execute the PK-only projection through the SQL statement execution lane.
    let rows = statement_projection_rows::<SessionSqlEntity>(
        &session,
        "SELECT id FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
    )
    .expect("PK-only covering projection query should execute");

    // Phase 3: preserve the canonical ordered window on the projection output.
    assert_eq!(rows, vec![vec![Value::Ulid(Ulid::from_u128(9_801))]]);
}

#[test]
fn session_explain_execution_primary_key_covering_full_scan_is_planner_proven() {
    reset_session_sql_store();
    let session = sql_session();

    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(9_811),
            name: "alpha".to_string(),
            age: 21,
        })
        .expect("PK-covering session seed should succeed");
    session
        .insert(SessionSqlEntity {
            id: Ulid::from_u128(9_812),
            name: "beta".to_string(),
            age: 22,
        })
        .expect("PK-covering session seed should succeed");

    let descriptor = lower_select_query_for_tests::<SessionSqlEntity>(
        &session,
        "SELECT id FROM SessionSqlEntity ORDER BY id ASC LIMIT 1",
    )
    .expect("PK-only covering query should lower")
    .explain_execution()
    .expect("PK-only covering explain_execution should succeed");

    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "PK-only primary-store projection should expose the explicit covering route",
    );
    let projection_node =
        explain_execution_find_first_node(&descriptor, ExplainExecutionNodeType::CoveringRead)
            .expect("PK-only covering explain tree should emit a covering-read node");
    assert_eq!(
        projection_node.node_properties().get("existing_row_mode"),
        Some(&Value::Text("planner_proven".to_string())),
        "PK-only primary-store covering should surface the planner-proven row mode",
    );
}

#[test]
fn session_explain_execution_primary_key_covering_matrix_uses_expected_row_modes() {
    let by_key = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .select_fields(["id"])
    .filter(FieldRef::new("id").eq(Ulid::from_u128(9_811)))
    .order_term(crate::db::asc("id"))
    .explain_execution()
    .expect("PK-only covering by-key explain_execution should succeed");
    let by_keys = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .select_fields(["id"])
    .filter(FieldRef::new("id").in_list([Ulid::from_u128(9_811), Ulid::from_u128(9_813)]))
    .order_term(crate::db::asc("id"))
    .explain_execution()
    .expect("PK-only covering by-keys explain_execution should succeed");
    let key_range = crate::db::query::intent::Query::<SessionSqlEntity>::new(
        crate::db::predicate::MissingRowPolicy::Ignore,
    )
    .select_fields(["id"])
    .filter(FilterExpr::and(vec![
        FieldRef::new("id").gte(Ulid::from_u128(9_811)),
        FieldRef::new("id").lt(Ulid::from_u128(9_813)),
    ]))
    .order_term(crate::db::asc("id"))
    .limit(1)
    .explain_execution()
    .expect("PK-only covering key-range explain_execution should succeed");

    for (descriptor, expected_root, expected_row_mode, context) in [
        (
            by_key,
            ExplainExecutionNodeType::ByKeyLookup,
            "row_check_required",
            "PK-only exact-key projection",
        ),
        (
            by_keys,
            ExplainExecutionNodeType::ByKeysLookup,
            "row_check_required",
            "PK-only exact-key-set projection",
        ),
        (
            key_range,
            ExplainExecutionNodeType::PrimaryKeyRangeScan,
            "planner_proven",
            "PK-only bounded primary-key projection",
        ),
    ] {
        assert_primary_key_covering_descriptor(
            &descriptor,
            expected_root,
            expected_row_mode,
            context,
        );
    }
}

#[test]
fn session_count_full_scan_ignores_other_entities_in_shared_store() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();

    // Seed two entity types into the same underlying store so the COUNT fast
    // path must stay scoped to the requested entity tag.
    seed_indexed_session_sql_entities(&session, &[("Sam", 30), ("Sasha", 24), ("Mira", 40)]);
    seed_session_explain_entities(&session, &[(9_501, 7, 10), (9_502, 7, 20)]);

    let expected = session
        .load::<SessionExplainEntity>()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("shared-store execute should succeed")
        .count();
    let actual = session
        .load::<SessionExplainEntity>()
        .count()
        .expect("shared-store count should succeed");

    assert_eq!(
        actual, expected,
        "full-scan count must ignore rows that belong to sibling entities sharing the same store",
    );
    assert_eq!(
        actual, 2,
        "shared-store count should report only the SessionExplainEntity rows",
    );
}

#[test]
fn session_explain_execution_projects_descriptor_tree_for_ordered_limited_index_access() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("Sam", 30), ("Sasha", 24), ("Soren", 18), ("Mira", 40)],
    );

    let descriptor = session
        .load::<IndexedSessionSqlEntity>()
        .filter(FieldRef::new("name").eq("Sam"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .offset(1)
        .limit(2)
        .explain_execution()
        .expect("ordered limited indexed explain_execution should succeed");

    assert!(
        descriptor.access_strategy().is_some(),
        "execution descriptor root should carry one canonical access projection",
    );
    assert!(matches!(
        descriptor.node_type(),
        ExplainExecutionNodeType::IndexPrefixScan | ExplainExecutionNodeType::IndexRangeScan
    ));
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "ordered scalar load execution roots should report explicit non-covering status",
    );

    let limit_node = descriptor
        .children()
        .iter()
        .find(|child| child.node_type() == ExplainExecutionNodeType::LimitOffset)
        .expect("paged shape should project one limit/offset node");
    assert_eq!(limit_node.limit(), Some(2));
    assert_eq!(
        limit_node.node_properties().get("offset"),
        Some(&Value::from(1u64)),
        "limit/offset node should keep logical offset metadata",
    );

    let order_node = descriptor
        .children()
        .iter()
        .find(|child| {
            child.node_type() == ExplainExecutionNodeType::OrderByAccessSatisfied
                || child.node_type() == ExplainExecutionNodeType::OrderByMaterializedSort
        })
        .expect("ordered shape should project one ORDER BY execution node");
    let _ = order_node;
}

#[test]
fn session_non_ready_secondary_indexes_are_hidden_from_planning_and_execution() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_indexed_session_sql_entities(
        &session,
        &[("Sam", 30), ("Sasha", 24), ("Soren", 18), ("Mira", 40)],
    );

    let planner_query = Query::<IndexedSessionSqlEntity>::new(MissingRowPolicy::Ignore)
        .filter(FieldRef::new("name").eq("Sam"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(1);
    let execution_query = session
        .load::<IndexedSessionSqlEntity>()
        .filter(FieldRef::new("name").eq("Sam"))
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::asc("id"))
        .limit(1);

    INDEXED_SESSION_SQL_DB
        .recovered_store(IndexedSessionSqlStore::PATH)
        .expect("indexed SQL store should recover")
        .mark_index_building();

    let visible_indexes = session
        .visible_indexes_for_store_model(
            IndexedSessionSqlStore::PATH,
            <IndexedSessionSqlEntity as crate::traits::EntitySchema>::MODEL,
        )
        .expect("non-ready store should still resolve planner-visible index slice");
    assert!(
        visible_indexes.as_slice().is_empty(),
        "planner boundary must hide non-ready secondary indexes before access selection",
    );

    let compiled = planner_query
        .plan_with_visible_indexes(&visible_indexes)
        .expect("planning with no visible secondary indexes should still succeed");
    assert!(
        matches!(compiled.explain().access(), ExplainAccessPath::FullScan),
        "planner output must fall back to FullScan once the secondary index is no longer ready",
    );

    let descriptor = execution_query
        .explain_execution()
        .expect("non-ready secondary index explain_execution should succeed");
    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::FullScan,
        "non-ready secondary indexes must disappear from planner visibility instead of downgrading in execution",
    );
    assert_ne!(
        descriptor.covering_scan(),
        Some(true),
        "non-ready secondary indexes must not leave behind a covering-read route",
    );

    let rows = execution_query
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("non-ready secondary index query should still execute");
    assert_eq!(
        rows.len(),
        1,
        "planner visibility fallback must preserve the bounded query window",
    );
    assert_eq!(
        rows[0].entity_ref().name,
        "Sam",
        "planner visibility fallback must preserve the filtered row identity",
    );
    assert_eq!(
        rows[0].entity_ref().age,
        30,
        "planner visibility fallback must preserve the projected entity payload",
    );
}

#[test]
fn session_terminal_explain_seek_labels_for_min_and_max_are_stable() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_session_explain_entities(
        &session,
        &[
            (9_401, 7, 10),
            (9_402, 7, 20),
            (9_403, 7, 30),
            (9_404, 8, 99),
        ],
    );

    let min_terminal_plan = session
        .load::<SessionExplainEntity>()
        .filter(FieldRef::new("group").eq(7_u64))
        .order_term(crate::db::asc("rank"))
        .order_term(crate::db::asc("id"))
        .explain_min()
        .expect("session explain_min should succeed");
    assert_eq!(min_terminal_plan.terminal(), AggregateKind::Min);
    assert!(matches!(
        min_terminal_plan.execution().ordering_source(),
        crate::db::ExplainExecutionOrderingSource::IndexSeekFirst { fetch: 1 }
    ));
    let min_execution = min_terminal_plan.execution();
    assert_eq!(min_execution.aggregation(), AggregateKind::Min);
    assert!(matches!(
        min_execution.ordering_source(),
        crate::db::ExplainExecutionOrderingSource::IndexSeekFirst { fetch: 1 }
    ));
    assert_eq!(
        min_execution.execution_mode(),
        crate::db::ExplainExecutionMode::Materialized
    );
    let min_node = min_terminal_plan.execution_node_descriptor();
    assert_eq!(
        min_node.node_type(),
        ExplainExecutionNodeType::AggregateSeekFirst
    );
    assert_eq!(min_node.execution_mode(), min_execution.execution_mode());
    assert!(
        matches!(
            min_node.ordering_source(),
            Some(crate::db::ExplainExecutionOrderingSource::IndexSeekFirst { fetch: 1 })
        ),
        "seek-first explain node should keep the canonical seek ordering source",
    );
    assert_eq!(
        min_node.node_properties().get("fetch"),
        Some(&Value::from(1u64)),
        "seek-first explain node should keep the canonical fetch contract",
    );

    let max_terminal_plan = session
        .load::<SessionExplainEntity>()
        .filter(FieldRef::new("group").eq(7_u64))
        .order_term(crate::db::desc("rank"))
        .order_term(crate::db::desc("id"))
        .explain_max()
        .expect("session explain_max should succeed");
    assert_eq!(max_terminal_plan.terminal(), AggregateKind::Max);
    assert!(matches!(
        max_terminal_plan.execution().ordering_source(),
        crate::db::ExplainExecutionOrderingSource::IndexSeekLast { fetch: 1 }
    ));
    let max_execution = max_terminal_plan.execution();
    assert_eq!(max_execution.aggregation(), AggregateKind::Max);
    assert!(matches!(
        max_execution.ordering_source(),
        crate::db::ExplainExecutionOrderingSource::IndexSeekLast { fetch: 1 }
    ));
    assert_eq!(
        max_execution.execution_mode(),
        crate::db::ExplainExecutionMode::Materialized
    );
    let max_node = max_terminal_plan.execution_node_descriptor();
    assert_eq!(
        max_node.node_type(),
        ExplainExecutionNodeType::AggregateSeekLast
    );
    assert_eq!(max_node.execution_mode(), max_execution.execution_mode());
    assert!(
        matches!(
            max_node.ordering_source(),
            Some(crate::db::ExplainExecutionOrderingSource::IndexSeekLast { fetch: 1 })
        ),
        "seek-last explain node should keep the canonical seek ordering source",
    );
    assert_eq!(
        max_node.node_properties().get("fetch"),
        Some(&Value::from(1u64)),
        "seek-last explain node should keep the canonical fetch contract",
    );
}

#[test]
fn session_explain_execution_text_and_json_surface_for_strict_index_prefix_shape() {
    reset_indexed_session_sql_store();
    let session = indexed_sql_session();
    seed_session_explain_entities(
        &session,
        &[
            (9_741, 7, 10),
            (9_742, 7, 20),
            (9_743, 7, 30),
            (9_744, 8, 40),
        ],
    );
    let query = session
        .load::<SessionExplainEntity>()
        .filter(FieldRef::new("group").eq(7_u64))
        .order_term(crate::db::asc("rank"))
        .order_term(crate::db::asc("id"))
        .offset(1)
        .limit(2);

    let text_tree = query
        .explain_execution_text()
        .expect("strict index-prefix execution text explain should succeed");
    assert!(
        text_tree.contains("IndexPrefixScan execution_mode="),
        "execution text should expose the canonical index-prefix root label",
    );
    assert!(
        text_tree.contains("LimitOffset execution_mode=") && text_tree.contains("limit=2"),
        "execution text should expose the paged terminal node",
    );
    assert!(
        text_tree.contains("IndexPredicatePrefilter execution_mode=")
            || text_tree.contains("ResidualFilter execution_mode="),
        "execution text should expose one predicate-stage node",
    );

    let descriptor_json = query
        .explain_execution_json()
        .expect("strict index-prefix execution json explain should succeed");
    assert!(
        descriptor_json.contains("\"node_type\":\"IndexPrefixScan\""),
        "execution json should expose the canonical index-prefix root node type",
    );
    assert!(
        descriptor_json.contains("\"node_type\":\"LimitOffset\""),
        "execution json should expose the paged terminal node type",
    );
    assert!(
        descriptor_json.contains("\"node_type\":\"IndexPredicatePrefilter\"")
            || descriptor_json.contains("\"node_type\":\"ResidualFilter\""),
        "execution json should expose one predicate-stage node type",
    );
}
