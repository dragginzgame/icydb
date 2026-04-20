use super::*;

fn assert_aggregate_terminal_plan_semantic_parity(
    left: &crate::db::ExplainAggregateTerminalPlan,
    right: &crate::db::ExplainAggregateTerminalPlan,
) {
    assert_eq!(left.terminal(), right.terminal());
    assert_eq!(left.query().access(), right.query().access());
    assert_eq!(left.query().order_by(), right.query().order_by());
    assert_eq!(left.query().page(), right.query().page());
    assert_eq!(left.query().grouping(), right.query().grouping());
    assert_eq!(
        left.query().order_pushdown(),
        right.query().order_pushdown()
    );
    assert_eq!(left.query().consistency(), right.query().consistency());
    assert_eq!(
        left.execution().aggregation(),
        right.execution().aggregation()
    );
    assert_eq!(
        left.execution().execution_mode(),
        right.execution().execution_mode()
    );
    assert_eq!(
        left.execution().ordering_source(),
        right.execution().ordering_source()
    );
    assert_eq!(left.execution().limit(), right.execution().limit());
    assert_eq!(left.execution().cursor(), right.execution().cursor());
    assert_eq!(
        left.execution().covering_projection(),
        right.execution().covering_projection()
    );
    assert_eq!(
        left.execution_node_descriptor().node_type(),
        right.execution_node_descriptor().node_type()
    );
    assert_eq!(
        left.execution_node_descriptor().execution_mode(),
        right.execution_node_descriptor().execution_mode()
    );
}

fn assert_execution_descriptor_semantic_parity(
    left: &ExplainExecutionNodeDescriptor,
    right: &ExplainExecutionNodeDescriptor,
) {
    assert_eq!(left.node_type(), right.node_type());
    assert_eq!(left.execution_mode(), right.execution_mode());
    assert_eq!(left.access_strategy(), right.access_strategy());
    assert_eq!(left.predicate_pushdown(), right.predicate_pushdown());
    assert_eq!(
        left.residual_filter_predicate(),
        right.residual_filter_predicate()
    );
    assert_eq!(left.projection(), right.projection());
    assert_eq!(left.ordering_source(), right.ordering_source());
    assert_eq!(left.limit(), right.limit());
    assert_eq!(left.cursor(), right.cursor());
    assert_eq!(left.covering_scan(), right.covering_scan());
    assert_eq!(left.rows_expected(), right.rows_expected());
}

#[test]
fn session_aggregate_ranked_rows_are_invariant_to_insertion_order() {
    let rows_a = [
        (8_3961, 7, 10),
        (8_3962, 7, 40),
        (8_3963, 7, 20),
        (8_3964, 7, 30),
        (8_3965, 7, 40),
    ];
    let rows_b = [
        (8_3965, 7, 40),
        (8_3963, 7, 20),
        (8_3961, 7, 10),
        (8_3964, 7, 30),
        (8_3962, 7, 40),
    ];
    let ranked_ids_for = |rows: &[(u128, u64, u64)]| {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_aggregate_entities(&session, rows);
        let top_ids = session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_filter(7))
            .order_term(crate::db::asc("id"))
            .top_k_by("rank", 3)
            .expect("session aggregate top_k_by(rank, 3) insertion-order test should succeed");
        let bottom_ids = session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_filter(7))
            .order_term(crate::db::asc("id"))
            .bottom_k_by("rank", 3)
            .expect("session aggregate bottom_k_by(rank, 3) insertion-order test should succeed");

        (
            session_aggregate_ids(&top_ids),
            session_aggregate_ids(&bottom_ids),
        )
    };

    assert_eq!(
        ranked_ids_for(&rows_a).0,
        ranked_ids_for(&rows_b).0,
        "session aggregate top_k_by(rank, 3) should be invariant to seed insertion order",
    );
    assert_eq!(
        ranked_ids_for(&rows_a).1,
        ranked_ids_for(&rows_b).1,
        "session aggregate bottom_k_by(rank, 3) should be invariant to seed insertion order",
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn session_aggregate_identity_terminals_match_execute() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_8501, 7, 10),
            (8_8502, 7, 20),
            (8_8503, 7, 30),
            (8_8504, 7, 40),
            (8_8505, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_filter(7))
            .order_term(crate::db::asc("id"))
            .offset(1)
            .limit(3)
    };
    let expected = load_window()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("session aggregate identity baseline execute should succeed");
    let expected_ids = session_aggregate_ids(&expected);

    assert_eq!(
        load_window()
            .count()
            .expect("session aggregate count should succeed"),
        expected.count(),
        "session aggregate count should match execute() cardinality",
    );
    assert_eq!(
        load_window()
            .exists()
            .expect("session aggregate exists should succeed"),
        !expected.is_empty(),
        "session aggregate exists should match execute() emptiness",
    );
    assert_eq!(
        load_window()
            .not_exists()
            .expect("session aggregate not_exists should succeed"),
        expected.is_empty(),
        "session aggregate not_exists should match execute() emptiness",
    );
    assert_eq!(
        load_window()
            .is_empty()
            .expect("session aggregate is_empty should succeed"),
        expected.is_empty(),
        "session aggregate is_empty should match execute() emptiness",
    );
    assert_eq!(
        load_window()
            .min()
            .expect("session aggregate min should succeed")
            .map(|id| id.key()),
        expected_ids.iter().copied().min(),
        "session aggregate min should match execute() minimum id",
    );
    assert_eq!(
        load_window()
            .max()
            .expect("session aggregate max should succeed")
            .map(|id| id.key()),
        expected_ids.iter().copied().max(),
        "session aggregate max should match execute() maximum id",
    );
    assert_eq!(
        load_window()
            .min_by("id")
            .expect("session aggregate min_by(id) should succeed")
            .map(|id| id.key()),
        expected_ids.iter().copied().min(),
        "session aggregate min_by(id) should match execute() minimum id",
    );
    assert_eq!(
        load_window()
            .max_by("id")
            .expect("session aggregate max_by(id) should succeed")
            .map(|id| id.key()),
        expected_ids.iter().copied().max(),
        "session aggregate max_by(id) should match execute() maximum id",
    );
    assert_eq!(
        load_window()
            .nth_by("id", 1)
            .expect("session aggregate nth_by(id, 1) should succeed")
            .map(|id| id.key()),
        expected_ids.get(1).copied(),
        "session aggregate nth_by(id, 1) should match ordered execute() ids",
    );
    assert_eq!(
        load_window()
            .first()
            .expect("session aggregate first should succeed")
            .map(|id| id.key()),
        expected.id().map(|id| id.key()),
        "session aggregate first should match execute() head id",
    );
    assert_eq!(
        load_window()
            .last()
            .expect("session aggregate last should succeed")
            .map(|id| id.key()),
        expected_ids.last().copied(),
        "session aggregate last should match execute() tail id",
    );
}

#[test]
fn session_aggregate_exists_not_exists_and_is_empty_share_early_stop_scan_budget() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_401, 7, 10),
            (8_402, 7, 20),
            (8_403, 7, 30),
            (8_404, 7, 40),
            (8_405, 7, 50),
            (8_406, 7, 60),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .order_term(crate::db::asc("id"))
            .offset(2)
    };

    let (actual_exists, exists_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || load_window().exists());
    let (actual_not_exists, not_exists_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            load_window().not_exists()
        });
    let (actual_is_empty, is_empty_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || load_window().is_empty());

    assert!(
        actual_exists.expect("session aggregate exists should succeed"),
        "window should report at least one matching row",
    );
    assert!(
        !actual_not_exists.expect("session aggregate not_exists should succeed"),
        "not_exists should be false when one matching row is present",
    );
    assert!(
        !actual_is_empty.expect("session aggregate is_empty should succeed"),
        "is_empty should be false when one matching row is present",
    );
    assert_eq!(
        exists_rows_scanned, 3,
        "exists should stop after offset + 1 rows on a non-empty ordered window",
    );
    assert_eq!(
        not_exists_rows_scanned, exists_rows_scanned,
        "not_exists should preserve exists scan-budget behavior",
    );
    assert_eq!(
        is_empty_rows_scanned, exists_rows_scanned,
        "is_empty should preserve exists scan-budget behavior",
    );
}

#[test]
fn session_aggregate_primary_key_is_null_optimizations_preserve_empty_access_and_or_parity() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_421, 7, 10),
            (8_422, 7, 20),
            (8_423, 7, 30),
            (8_424, 8, 99),
        ],
    );

    // Phase 1: require primary-key IS NULL to lower to an empty access path
    // without consuming scan budget across the identity terminals.
    let null_pk_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(crate::db::FieldRef::new("id").is_null())
    };
    let (actual_count, count_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || null_pk_window().count());
    let (actual_exists, exists_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || null_pk_window().exists());
    let (actual_not_exists, not_exists_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            null_pk_window().not_exists()
        });

    assert_eq!(
        actual_count.expect("count should succeed for primary_key IS NULL"),
        0,
        "primary_key IS NULL should match no rows",
    );
    assert!(
        !actual_exists.expect("exists should succeed for primary_key IS NULL"),
        "exists should be false for primary_key IS NULL windows",
    );
    assert!(
        actual_not_exists.expect("not_exists should succeed for primary_key IS NULL"),
        "not_exists should be true for primary_key IS NULL windows",
    );
    assert_eq!(count_rows_scanned, 0);
    assert_eq!(exists_rows_scanned, 0);
    assert_eq!(not_exists_rows_scanned, 0);

    // Phase 2: require one null-or-equality predicate to collapse onto the
    // equality branch semantics when the null primary-key branch is impossible.
    let target = Ulid::from_u128(8_423);
    let eq_id_filter = crate::db::FieldRef::new("id").eq(target);
    let or_filter = crate::db::FilterExpr::or(vec![
        crate::db::FieldRef::new("id").is_null(),
        eq_id_filter.clone(),
    ]);
    let strict_eq_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(eq_id_filter.clone())
            .order_term(crate::db::asc("id"))
    };
    let null_or_eq_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(or_filter.clone())
            .order_term(crate::db::asc("id"))
    };

    let expected = strict_eq_window()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("strict id equality execute should succeed");
    let actual = null_or_eq_window()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("null-or-id execute should succeed");
    assert_eq!(
        actual.ids().collect::<Vec<_>>(),
        expected.ids().collect::<Vec<_>>()
    );

    let expected_count = strict_eq_window().count().expect("count should succeed");
    let actual_count = null_or_eq_window().count().expect("count should succeed");
    assert_eq!(actual_count, expected_count);
    let (expected_exists, expected_exists_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            strict_eq_window().exists()
        });
    let (actual_exists, actual_exists_rows_scanned) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            null_or_eq_window().exists()
        });
    assert_eq!(
        actual_exists.expect("exists should succeed"),
        expected_exists.expect("exists should succeed")
    );
    assert_eq!(actual_exists_rows_scanned, expected_exists_rows_scanned);
}

#[test]
fn session_aggregate_min_by_unknown_field_fails_before_scan_budget_consumption() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_901, 7, 10),
            (8_902, 7, 20),
            (8_903, 7, 30),
            (8_904, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_filter(7))
            .order_term(crate::db::desc("id"))
            .offset(0)
            .limit(3)
    };

    let (result, scanned_rows) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            load_window().min_by("missing_field")
        });
    let Err(err) = result else {
        panic!("session min_by(missing_field) should be rejected");
    };

    assert!(matches!(err, QueryError::Execute(_)));
    assert_eq!(scanned_rows, 0);
    assert!(err.to_string().contains("unknown aggregate target field"));
}

#[test]
fn session_aggregate_field_aggregates_match_execute_projection() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_311, 7, 10),
            (8_312, 7, 10),
            (8_313, 7, 20),
            (8_314, 7, 30),
            (8_315, 7, 40),
            (8_316, 8, 99),
        ],
    );

    // Phase 1: use the ordered execute window as the parity baseline for the
    // newer rank-field aggregate identities.
    let new_field_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_filter(7))
            .order_term(crate::db::desc("id"))
            .offset(1)
            .limit(4)
    };
    let new_field_expected = new_field_window()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("session aggregate new-field baseline execute should succeed");

    assert_eq!(
        new_field_window()
            .median_by("rank")
            .expect("session aggregate median_by(rank) should succeed")
            .map(|id| id.key()),
        session_aggregate_expected_median_by_rank_id(&new_field_expected),
    );
    assert_eq!(
        new_field_window()
            .count_distinct_by("rank")
            .expect("session aggregate count_distinct_by(rank) should succeed"),
        session_aggregate_expected_count_distinct_by_rank(&new_field_expected),
    );
    assert_eq!(
        new_field_window()
            .min_max_by("rank")
            .expect("session aggregate min_max_by(rank) should succeed")
            .map(|(min_id, max_id)| (min_id.key(), max_id.key())),
        session_aggregate_expected_min_max_by_rank_ids(&new_field_expected),
    );

    // Phase 2: reuse the same fixture to lock numeric field aggregates against
    // the ordered execute projection contract.
    let numeric_expected = session
        .load::<SessionAggregateEntity>()
        .filter(session_aggregate_group_filter(7))
        .order_term(crate::db::asc("rank"))
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("session aggregate numeric baseline execute should succeed");

    let mut expected_sum = crate::types::Decimal::ZERO;
    let mut expected_count = 0u64;
    for row in numeric_expected {
        let rank =
            crate::types::Decimal::from_num(row.entity().rank).expect("rank decimal should build");
        expected_sum += rank;
        expected_count = expected_count.saturating_add(1);
    }
    let expected_sum_decimal = expected_sum;
    let expected_sum = Some(expected_sum_decimal);
    let expected_avg = if expected_count == 0 {
        None
    } else {
        Some(
            expected_sum_decimal
                / crate::types::Decimal::from_num(expected_count)
                    .expect("count decimal should build"),
        )
    };

    assert_eq!(
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_filter(7))
            .order_term(crate::db::asc("rank"))
            .sum_by("rank")
            .expect("session aggregate sum_by(rank) should succeed"),
        expected_sum,
    );
    assert_eq!(
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_filter(7))
            .order_term(crate::db::asc("rank"))
            .avg_by("rank")
            .expect("session aggregate avg_by(rank) should succeed"),
        expected_avg,
    );
}

#[test]
fn session_aggregate_prepared_strategy_explain_matrix_matches_public_projection() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_2221, 7, 10),
            (8_2222, 7, 20),
            (8_2223, 7, 20),
            (8_2224, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_filter(7))
            .order_term(crate::db::asc("rank"))
    };
    let rank_slot = FieldSlot::resolve(SessionAggregateEntity::MODEL, "rank")
        .expect("rank field slot should resolve");

    // Phase 1: require the public numeric aggregate explains to remain exact
    // snapshots of the prepared strategy projection.
    let prepared_sum = session
        .explain_query_prepared_aggregate_terminal_with_visible_indexes(
            load_window().query(),
            &PreparedFluentNumericFieldStrategy::sum_by_slot(rank_slot.clone()),
        )
        .expect("prepared numeric SUM explain should build");
    let prepared_avg_distinct = session
        .explain_query_prepared_aggregate_terminal_with_visible_indexes(
            load_window().query(),
            &PreparedFluentNumericFieldStrategy::avg_distinct_by_slot(rank_slot.clone()),
        )
        .expect("prepared numeric AVG DISTINCT explain should build");
    let public_sum = load_window()
        .explain_sum_by("rank")
        .expect("public fluent SUM explain should build");
    let public_avg_distinct = load_window()
        .explain_avg_distinct_by("rank")
        .expect("public fluent AVG DISTINCT explain should build");

    let prepared_sum_node = prepared_sum.execution_node_descriptor();
    assert_eq!(prepared_sum.terminal(), AggregateKind::Sum);
    assert_eq!(
        prepared_sum_node.node_type(),
        ExplainExecutionNodeType::AggregateSum
    );

    assert_aggregate_terminal_plan_semantic_parity(&public_sum, &prepared_sum);
    assert_aggregate_terminal_plan_semantic_parity(&public_avg_distinct, &prepared_avg_distinct);

    // Phase 2: require the public projection terminals to remain exact
    // renderings of the prepared projection strategies.
    let prepared_count_distinct = session
        .explain_query_prepared_projection_terminal_with_visible_indexes(
            load_window().query(),
            &PreparedFluentProjectionStrategy::count_distinct_by_slot(rank_slot.clone()),
        )
        .expect("prepared projection COUNT DISTINCT explain should build");
    let prepared_last_value = session
        .explain_query_prepared_projection_terminal_with_visible_indexes(
            load_window().query(),
            &PreparedFluentProjectionStrategy::last_value_by_slot(rank_slot),
        )
        .expect("prepared projection terminal-value explain should build");
    let public_count_distinct = load_window()
        .explain_count_distinct_by("rank")
        .expect("public fluent COUNT DISTINCT explain should build");
    let public_last_value = load_window()
        .explain_last_value_by("rank")
        .expect("public fluent last_value_by explain should build");

    assert_execution_descriptor_semantic_parity(&public_count_distinct, &prepared_count_distinct);
    assert_execution_descriptor_semantic_parity(&public_last_value, &prepared_last_value);
}

#[test]
fn session_aggregate_nth_by_rank_uses_deterministic_rank_and_id_ordering() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_4041, 7, 10),
            (8_4042, 7, 10),
            (8_4043, 7, 20),
            (8_4044, 7, 30),
            (8_4045, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_filter(7))
            .order_term(crate::db::desc("id"))
            .limit(4)
    };
    let expected = load_window()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("session aggregate nth_by baseline execute should succeed");

    assert_eq!(
        load_window()
            .nth_by("rank", 0)
            .expect("session aggregate nth_by(rank, 0) should succeed")
            .map(|id| id.key()),
        session_aggregate_expected_nth_by_rank_id(&expected, 0),
    );
    assert_eq!(
        load_window()
            .nth_by("rank", 1)
            .expect("session aggregate nth_by(rank, 1) should succeed")
            .map(|id| id.key()),
        session_aggregate_expected_nth_by_rank_id(&expected, 1),
    );
    assert_eq!(
        load_window()
            .nth_by("rank", 4)
            .expect("session aggregate nth_by(rank, ordinal) should succeed")
            .map(|id| id.key()),
        None,
    );
}
