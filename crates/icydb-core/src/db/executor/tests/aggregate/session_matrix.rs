//! Module: db::executor::tests::aggregate::session_matrix
//! Responsibility: module-local ownership and contracts for db::executor::tests::aggregate::session_matrix.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

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
    let expected_not_exists = expected.is_empty();
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
    let actual_not_exists = load_window()
        .not_exists()
        .expect("session not_exists should succeed");
    let actual_is_empty = load_window()
        .is_empty()
        .expect("session is_empty should succeed");
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
    assert_eq!(
        actual_not_exists, expected_not_exists,
        "session not_exists parity failed"
    );
    assert_eq!(
        actual_is_empty, expected_not_exists,
        "session is_empty parity failed"
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
fn session_load_exists_not_exists_and_is_empty_share_early_stop_scan_budget() {
    seed_simple_entities(&[8_401, 8_402, 8_403, 8_404, 8_405, 8_406]);
    let session = DbSession::new(DB);
    let load_window = || session.load::<SimpleEntity>().order_by("id").offset(2);

    // Phase 1: run the three existence aliases under metrics capture.
    let (actual_exists, exists_rows_scanned) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || load_window().exists());
    let (actual_not_exists, not_exists_rows_scanned) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || load_window().not_exists());
    let (actual_is_empty, is_empty_rows_scanned) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || load_window().is_empty());

    let actual_exists = actual_exists.expect("session exists should succeed");
    let actual_not_exists = actual_not_exists.expect("session not_exists should succeed");
    let actual_is_empty = actual_is_empty.expect("session is_empty should succeed");

    // Phase 2: lock semantic parity and early-stop scan-budget parity.
    assert!(
        actual_exists,
        "window should report at least one matching row"
    );
    assert!(
        !actual_not_exists,
        "not_exists should be false when one matching row is present"
    );
    assert!(
        !actual_is_empty,
        "is_empty should be false when one matching row is present"
    );
    assert_eq!(
        exists_rows_scanned, 3,
        "exists should stop after offset + 1 rows on a non-empty ordered window"
    );
    assert_eq!(
        not_exists_rows_scanned, exists_rows_scanned,
        "not_exists should preserve exists scan-budget behavior"
    );
    assert_eq!(
        is_empty_rows_scanned, exists_rows_scanned,
        "is_empty should preserve exists scan-budget behavior"
    );
}

#[test]
fn session_load_primary_key_is_null_lowers_to_empty_access_without_scan() {
    seed_simple_entities(&[8_411, 8_412, 8_413]);
    let session = DbSession::new(DB);
    let null_pk_window = || {
        session.load::<SimpleEntity>().filter(Predicate::IsNull {
            field: "id".to_string(),
        })
    };

    // Phase 1: capture terminal outputs under metrics for the impossible
    // `primary_key IS NULL` shape.
    let (actual_count, count_rows_scanned) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || null_pk_window().count());
    let (actual_exists, exists_rows_scanned) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || null_pk_window().exists());
    let (actual_not_exists, not_exists_rows_scanned) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || null_pk_window().not_exists());

    let actual_count = actual_count.expect("count should succeed for primary_key IS NULL");
    let actual_exists = actual_exists.expect("exists should succeed for primary_key IS NULL");
    let actual_not_exists =
        actual_not_exists.expect("not_exists should succeed for primary_key IS NULL");

    // Phase 2: assert semantic parity plus zero-scan lower-to-empty behavior.
    assert_eq!(actual_count, 0, "primary_key IS NULL should match no rows");
    assert!(
        !actual_exists,
        "exists should be false for primary_key IS NULL windows"
    );
    assert!(
        actual_not_exists,
        "not_exists should be true for primary_key IS NULL windows"
    );
    assert_eq!(
        count_rows_scanned, 0,
        "count should not scan rows when planner lowers primary_key IS NULL to empty access",
    );
    assert_eq!(
        exists_rows_scanned, 0,
        "exists should not scan rows when planner lowers primary_key IS NULL to empty access",
    );
    assert_eq!(
        not_exists_rows_scanned, 0,
        "not_exists should not scan rows when planner lowers primary_key IS NULL to empty access",
    );
}

#[test]
fn session_load_primary_key_is_null_or_id_eq_matches_id_eq_branch_parity() {
    seed_simple_entities(&[8_421, 8_422, 8_423, 8_424]);
    let session = DbSession::new(DB);
    let target = Value::Ulid(Ulid::from_u128(8_423));
    let eq_id_predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::Eq,
        target,
        CoercionId::Strict,
    ));
    let or_predicate = Predicate::Or(vec![
        Predicate::IsNull {
            field: "id".to_string(),
        },
        eq_id_predicate.clone(),
    ]);
    let strict_eq_window = || {
        session
            .load::<SimpleEntity>()
            .filter(eq_id_predicate.clone())
            .order_by("id")
    };
    let null_or_eq_window = || {
        session
            .load::<SimpleEntity>()
            .filter(or_predicate.clone())
            .order_by("id")
    };

    // Phase 1: lock result-set parity between strict id equality and
    // equivalent OR shape that includes impossible `primary_key IS NULL`.
    let expected = strict_eq_window()
        .execute()
        .expect("strict id equality execute should succeed");
    let actual = null_or_eq_window()
        .execute()
        .expect("null-or-id execute should succeed");
    assert_eq!(
        actual.ids().collect::<Vec<_>>(),
        expected.ids().collect::<Vec<_>>()
    );

    // Phase 2: lock scalar terminal parity and scan-budget parity.
    let expected_count = strict_eq_window().count().expect("count should succeed");
    let actual_count = null_or_eq_window().count().expect("count should succeed");
    assert_eq!(
        actual_count, expected_count,
        "null-or-id count should match strict id-equality count"
    );
    let (expected_exists, expected_exists_rows_scanned) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || strict_eq_window().exists());
    let (actual_exists, actual_exists_rows_scanned) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || null_or_eq_window().exists());
    assert_eq!(
        actual_exists.expect("exists should succeed"),
        expected_exists.expect("exists should succeed"),
        "null-or-id exists should match strict id-equality exists"
    );
    assert_eq!(
        actual_exists_rows_scanned, expected_exists_rows_scanned,
        "null-or-id exists should preserve strict id-equality scan budget"
    );
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
fn session_load_temporal_views_and_projection_values_preserve_semantic_types() {
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    seed_temporal_boundary_entities(&[
        (8_941, day_one, at_one, elapsed_one),
        (8_942, day_two, at_two, elapsed_two),
    ]);
    let session = DbSession::new(DB);

    // Phase 1: lock semantic view-field projection types and values.
    let response = session
        .load::<TemporalBoundaryEntity>()
        .order_by("id")
        .execute()
        .expect("temporal execute should succeed");
    let views: Vec<_> = response.views().collect();
    assert_eq!(views.len(), 2, "temporal fixture should return two rows");
    let first = &views[0];
    let second = &views[1];
    let _: Date = first.occurred_on;
    let _: Timestamp = first.occurred_at;
    let _: Duration = first.elapsed;
    assert_eq!(first.occurred_on, day_one);
    assert_eq!(second.occurred_on, day_two);
    assert_eq!(first.occurred_at, at_one);
    assert_eq!(second.occurred_at, at_two);
    assert_eq!(first.elapsed, elapsed_one);
    assert_eq!(second.elapsed, elapsed_two);

    // Phase 2: lock scalar projection value typing for temporal fields.
    let day_values = session
        .load::<TemporalBoundaryEntity>()
        .order_by("id")
        .values_by("occurred_on")
        .expect("occurred_on projection should succeed");
    let at_values = session
        .load::<TemporalBoundaryEntity>()
        .order_by("id")
        .values_by("occurred_at")
        .expect("occurred_at projection should succeed");
    let elapsed_values = session
        .load::<TemporalBoundaryEntity>()
        .order_by("id")
        .values_by("elapsed")
        .expect("elapsed projection should succeed");
    assert_eq!(day_values, vec![Value::Date(day_one), Value::Date(day_two)]);
    assert_eq!(
        at_values,
        vec![Value::Timestamp(at_one), Value::Timestamp(at_two)]
    );
    assert_eq!(
        elapsed_values,
        vec![Value::Duration(elapsed_one), Value::Duration(elapsed_two)]
    );
}

#[test]
fn session_load_temporal_grouped_keys_preserve_semantic_types() {
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    seed_temporal_boundary_entities(&[
        (8_943, day_one, at_one, elapsed_one),
        (8_944, day_one, at_two, elapsed_one),
        (8_945, day_two, at_two, elapsed_two),
    ]);
    let session = DbSession::new(DB);

    // Phase 1: group by Date and lock semantic key typing in grouped output.
    let by_day = session
        .load::<TemporalBoundaryEntity>()
        .group_by("occurred_on")
        .expect("group_by(occurred_on) should resolve")
        .aggregate(crate::db::count())
        .execute_grouped()
        .expect("grouped by occurred_on should execute");
    assert_eq!(
        by_day
            .rows()
            .iter()
            .map(crate::db::GroupedRow::group_key)
            .collect::<Vec<_>>(),
        vec![&[Value::Date(day_one)][..], &[Value::Date(day_two)][..]],
        "grouped Date keys should stay semantic Date values",
    );
    assert_eq!(
        by_day
            .rows()
            .iter()
            .map(crate::db::GroupedRow::aggregate_values)
            .collect::<Vec<_>>(),
        vec![&[Value::Uint(2)][..], &[Value::Uint(1)][..]],
        "grouped Date counts should match fixture cardinality",
    );

    // Phase 2: group by Timestamp and lock semantic key typing in grouped output.
    let by_timestamp = session
        .load::<TemporalBoundaryEntity>()
        .group_by("occurred_at")
        .expect("group_by(occurred_at) should resolve")
        .aggregate(crate::db::count())
        .execute_grouped()
        .expect("grouped by occurred_at should execute");
    assert_eq!(
        by_timestamp
            .rows()
            .iter()
            .map(crate::db::GroupedRow::group_key)
            .collect::<Vec<_>>(),
        vec![
            &[Value::Timestamp(at_one)][..],
            &[Value::Timestamp(at_two)][..]
        ],
        "grouped Timestamp keys should stay semantic Timestamp values",
    );
    assert_eq!(
        by_timestamp
            .rows()
            .iter()
            .map(crate::db::GroupedRow::aggregate_values)
            .collect::<Vec<_>>(),
        vec![&[Value::Uint(1)][..], &[Value::Uint(2)][..]],
        "grouped Timestamp counts should match fixture cardinality",
    );

    // Phase 3: group by Duration and lock semantic key typing in grouped output.
    let by_duration = session
        .load::<TemporalBoundaryEntity>()
        .group_by("elapsed")
        .expect("group_by(elapsed) should resolve")
        .aggregate(crate::db::count())
        .execute_grouped()
        .expect("grouped by elapsed should execute");
    assert_eq!(
        by_duration
            .rows()
            .iter()
            .map(crate::db::GroupedRow::group_key)
            .collect::<Vec<_>>(),
        vec![
            &[Value::Duration(elapsed_one)][..],
            &[Value::Duration(elapsed_two)][..]
        ],
        "grouped Duration keys should stay semantic Duration values",
    );
    assert_eq!(
        by_duration
            .rows()
            .iter()
            .map(crate::db::GroupedRow::aggregate_values)
            .collect::<Vec<_>>(),
        vec![&[Value::Uint(2)][..], &[Value::Uint(1)][..]],
        "grouped Duration counts should match fixture cardinality",
    );
}

#[test]
fn session_load_temporal_distinct_projection_values_preserve_semantic_types() {
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    seed_temporal_boundary_entities(&[
        (8_946, day_one, at_one, elapsed_one),
        (8_947, day_one, at_two, elapsed_one),
        (8_948, day_two, at_two, elapsed_two),
    ]);
    let session = DbSession::new(DB);

    // Phase 1: lock Date/Timestamp/Duration distinct projection typing and
    // first-observed value ordering under one deterministic window.
    let distinct_days = session
        .load::<TemporalBoundaryEntity>()
        .order_by("id")
        .distinct_values_by("occurred_on")
        .expect("distinct_values_by(occurred_on) should succeed");
    let distinct_timestamps = session
        .load::<TemporalBoundaryEntity>()
        .order_by("id")
        .distinct_values_by("occurred_at")
        .expect("distinct_values_by(occurred_at) should succeed");
    let distinct_durations = session
        .load::<TemporalBoundaryEntity>()
        .order_by("id")
        .distinct_values_by("elapsed")
        .expect("distinct_values_by(elapsed) should succeed");

    // Phase 2: assert semantic temporal value variants are preserved across
    // distinct projection boundaries.
    assert_eq!(
        distinct_days,
        vec![Value::Date(day_one), Value::Date(day_two)],
        "distinct Date projections should stay semantic Date values",
    );
    assert_eq!(
        distinct_timestamps,
        vec![Value::Timestamp(at_one), Value::Timestamp(at_two)],
        "distinct Timestamp projections should stay semantic Timestamp values",
    );
    assert_eq!(
        distinct_durations,
        vec![Value::Duration(elapsed_one), Value::Duration(elapsed_two)],
        "distinct Duration projections should stay semantic Duration values",
    );
}

#[test]
fn session_load_temporal_first_last_projection_values_preserve_semantic_types() {
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    seed_temporal_boundary_entities(&[
        (8_949, day_one, at_one, elapsed_one),
        (8_950, day_two, at_two, elapsed_two),
    ]);
    let session = DbSession::new(DB);
    let load_window = || session.load::<TemporalBoundaryEntity>().order_by("id");

    // Phase 1: lock first-value temporal projection typing for scalar terminals.
    let first_day = load_window()
        .first_value_by("occurred_on")
        .expect("first_value_by(occurred_on) should succeed");
    let first_timestamp = load_window()
        .first_value_by("occurred_at")
        .expect("first_value_by(occurred_at) should succeed");
    let first_duration = load_window()
        .first_value_by("elapsed")
        .expect("first_value_by(elapsed) should succeed");

    // Phase 2: lock last-value temporal projection typing for scalar terminals.
    let last_day = load_window()
        .last_value_by("occurred_on")
        .expect("last_value_by(occurred_on) should succeed");
    let last_timestamp = load_window()
        .last_value_by("occurred_at")
        .expect("last_value_by(occurred_at) should succeed");
    let last_duration = load_window()
        .last_value_by("elapsed")
        .expect("last_value_by(elapsed) should succeed");

    assert_eq!(first_day, Some(Value::Date(day_one)));
    assert_eq!(first_timestamp, Some(Value::Timestamp(at_one)));
    assert_eq!(first_duration, Some(Value::Duration(elapsed_one)));
    assert_eq!(last_day, Some(Value::Date(day_two)));
    assert_eq!(last_timestamp, Some(Value::Timestamp(at_two)));
    assert_eq!(last_duration, Some(Value::Duration(elapsed_two)));
}

#[test]
fn session_load_temporal_values_with_ids_preserve_semantic_types() {
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    let id_one = Id::<TemporalBoundaryEntity>::from_key(Ulid::from_u128(8_951));
    let id_two = Id::<TemporalBoundaryEntity>::from_key(Ulid::from_u128(8_952));
    seed_temporal_boundary_entities(&[
        (8_951, day_one, at_one, elapsed_one),
        (8_952, day_two, at_two, elapsed_two),
    ]);
    let session = DbSession::new(DB);
    let load_window = || session.load::<TemporalBoundaryEntity>().order_by("id");

    // Phase 1: lock temporal typing for id/value projection pairs.
    let day_pairs = load_window()
        .values_by_with_ids("occurred_on")
        .expect("values_by_with_ids(occurred_on) should succeed");
    let timestamp_pairs = load_window()
        .values_by_with_ids("occurred_at")
        .expect("values_by_with_ids(occurred_at) should succeed");
    let duration_pairs = load_window()
        .values_by_with_ids("elapsed")
        .expect("values_by_with_ids(elapsed) should succeed");

    // Phase 2: assert semantic temporal variants are preserved alongside ids.
    assert_eq!(
        day_pairs,
        vec![
            (id_one, Value::Date(day_one)),
            (id_two, Value::Date(day_two))
        ]
    );
    assert_eq!(
        timestamp_pairs,
        vec![
            (id_one, Value::Timestamp(at_one)),
            (id_two, Value::Timestamp(at_two))
        ]
    );
    assert_eq!(
        duration_pairs,
        vec![
            (id_one, Value::Duration(elapsed_one)),
            (id_two, Value::Duration(elapsed_two))
        ]
    );
}

#[test]
fn session_load_temporal_ranked_projection_values_preserve_semantic_types() {
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let day_three = Date::new_checked(2025, 10, 21).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let at_three = Timestamp::from_millis(1_761_040_800_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    let elapsed_three = Duration::from_millis(4_100);
    let id_one = Id::<TemporalBoundaryEntity>::from_key(Ulid::from_u128(8_953));
    let id_two = Id::<TemporalBoundaryEntity>::from_key(Ulid::from_u128(8_954));
    let id_three = Id::<TemporalBoundaryEntity>::from_key(Ulid::from_u128(8_955));
    seed_temporal_boundary_entities(&[
        (8_953, day_one, at_one, elapsed_one),
        (8_954, day_two, at_two, elapsed_two),
        (8_955, day_three, at_three, elapsed_three),
    ]);
    let session = DbSession::new(DB);
    let load_window = || session.load::<TemporalBoundaryEntity>();

    // Phase 1: lock temporal value typing for ranked value projections.
    let top_days = load_window()
        .top_k_by_values("occurred_on", 2)
        .expect("top_k_by_values(occurred_on) should succeed");
    let bottom_days = load_window()
        .bottom_k_by_values("occurred_on", 2)
        .expect("bottom_k_by_values(occurred_on) should succeed");
    let top_timestamps = load_window()
        .top_k_by_values("occurred_at", 2)
        .expect("top_k_by_values(occurred_at) should succeed");
    let bottom_timestamps = load_window()
        .bottom_k_by_values("occurred_at", 2)
        .expect("bottom_k_by_values(occurred_at) should succeed");
    let top_durations = load_window()
        .top_k_by_values("elapsed", 2)
        .expect("top_k_by_values(elapsed) should succeed");
    let bottom_durations = load_window()
        .bottom_k_by_values("elapsed", 2)
        .expect("bottom_k_by_values(elapsed) should succeed");

    assert_eq!(top_days, vec![Value::Date(day_three), Value::Date(day_two)]);
    assert_eq!(
        bottom_days,
        vec![Value::Date(day_one), Value::Date(day_two)]
    );
    assert_eq!(
        top_timestamps,
        vec![Value::Timestamp(at_three), Value::Timestamp(at_two)]
    );
    assert_eq!(
        bottom_timestamps,
        vec![Value::Timestamp(at_one), Value::Timestamp(at_two)]
    );
    assert_eq!(
        top_durations,
        vec![Value::Duration(elapsed_three), Value::Duration(elapsed_two)]
    );
    assert_eq!(
        bottom_durations,
        vec![Value::Duration(elapsed_one), Value::Duration(elapsed_two)]
    );

    // Phase 2: lock temporal value typing for ranked id/value projections.
    let top_day_pairs = load_window()
        .top_k_by_with_ids("occurred_on", 2)
        .expect("top_k_by_with_ids(occurred_on) should succeed");
    let bottom_day_pairs = load_window()
        .bottom_k_by_with_ids("occurred_on", 2)
        .expect("bottom_k_by_with_ids(occurred_on) should succeed");
    let top_timestamp_pairs = load_window()
        .top_k_by_with_ids("occurred_at", 2)
        .expect("top_k_by_with_ids(occurred_at) should succeed");
    let bottom_duration_pairs = load_window()
        .bottom_k_by_with_ids("elapsed", 2)
        .expect("bottom_k_by_with_ids(elapsed) should succeed");

    assert_eq!(
        top_day_pairs,
        vec![
            (id_three, Value::Date(day_three)),
            (id_two, Value::Date(day_two))
        ]
    );
    assert_eq!(
        bottom_day_pairs,
        vec![
            (id_one, Value::Date(day_one)),
            (id_two, Value::Date(day_two))
        ]
    );
    assert_eq!(
        top_timestamp_pairs,
        vec![
            (id_three, Value::Timestamp(at_three)),
            (id_two, Value::Timestamp(at_two))
        ]
    );
    assert_eq!(
        bottom_duration_pairs,
        vec![
            (id_one, Value::Duration(elapsed_one)),
            (id_two, Value::Duration(elapsed_two))
        ]
    );
}

#[test]
fn session_load_temporal_ranked_row_terminals_preserve_semantic_types() {
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let day_three = Date::new_checked(2025, 10, 21).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let at_three = Timestamp::from_millis(1_761_040_800_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    let elapsed_three = Duration::from_millis(4_100);
    seed_temporal_boundary_entities(&[
        (8_956, day_one, at_one, elapsed_one),
        (8_957, day_two, at_two, elapsed_two),
        (8_958, day_three, at_three, elapsed_three),
    ]);
    let session = DbSession::new(DB);
    let load_window = || session.load::<TemporalBoundaryEntity>();

    // Phase 1: lock top-k row terminal typing and ordering for temporal ranking.
    let top_response = load_window()
        .top_k_by("occurred_on", 2)
        .expect("top_k_by(occurred_on, 2) should succeed");
    let top_views: Vec<_> = top_response.views().collect();
    assert_eq!(top_views.len(), 2, "top_k_by should return two rows");
    let _: Date = top_views[0].occurred_on;
    let _: Timestamp = top_views[0].occurred_at;
    let _: Duration = top_views[0].elapsed;
    assert_eq!(top_views[0].occurred_on, day_three);
    assert_eq!(top_views[1].occurred_on, day_two);
    assert_eq!(top_views[0].occurred_at, at_three);
    assert_eq!(top_views[1].occurred_at, at_two);
    assert_eq!(top_views[0].elapsed, elapsed_three);
    assert_eq!(top_views[1].elapsed, elapsed_two);

    // Phase 2: lock bottom-k row terminal typing and ordering for temporal ranking.
    let bottom_response = load_window()
        .bottom_k_by("elapsed", 2)
        .expect("bottom_k_by(elapsed, 2) should succeed");
    let bottom_views: Vec<_> = bottom_response.views().collect();
    assert_eq!(bottom_views.len(), 2, "bottom_k_by should return two rows");
    let _: Date = bottom_views[0].occurred_on;
    let _: Timestamp = bottom_views[0].occurred_at;
    let _: Duration = bottom_views[0].elapsed;
    assert_eq!(bottom_views[0].elapsed, elapsed_one);
    assert_eq!(bottom_views[1].elapsed, elapsed_two);
    assert_eq!(bottom_views[0].occurred_on, day_one);
    assert_eq!(bottom_views[1].occurred_on, day_two);
    assert_eq!(bottom_views[0].occurred_at, at_one);
    assert_eq!(bottom_views[1].occurred_at, at_two);
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
execution_node_properties={\"fetch\": Uint(1), \"projected_field\": Text(\"none\"), \"projection_mode\": Text(\"entity_terminal\")}
execution_node_json={\"node_id\":0,\"node_type\":\"AggregateSeekFirst\",\"execution_mode\":\"Materialized\",\"execution_mode_detail\":\"materialized\",\"access_strategy\":{\"type\":\"FullScan\"},\"predicate_pushdown_mode\":\"none\",\"predicate_pushdown\":null,\"fast_path_selected\":null,\"fast_path_reason\":null,\"residual_predicate\":null,\"projection\":null,\"ordering_source\":\"IndexSeekFirst\",\"limit\":null,\"cursor\":false,\"covering_scan\":false,\"rows_expected\":null,\"children\":[],\"node_properties\":{\"fetch\":\"Uint(1)\",\"projected_field\":\"Text(\\\"none\\\")\",\"projection_mode\":\"Text(\\\"entity_terminal\\\")\"}}";
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
execution_node_properties={\"projected_field\": Text(\"none\"), \"projection_mode\": Text(\"scalar_aggregate\")}
execution_node_json={\"node_id\":0,\"node_type\":\"AggregateExists\",\"execution_mode\":\"Streaming\",\"execution_mode_detail\":\"streaming\",\"access_strategy\":{\"type\":\"ByKey\",\"key\":\"Ulid(Ulid(Ulid(9821)))\"},\"predicate_pushdown_mode\":\"none\",\"predicate_pushdown\":null,\"fast_path_selected\":null,\"fast_path_reason\":null,\"residual_predicate\":null,\"projection\":null,\"ordering_source\":\"AccessOrder\",\"limit\":null,\"cursor\":false,\"covering_scan\":false,\"rows_expected\":null,\"children\":[],\"node_properties\":{\"projected_field\":\"Text(\\\"none\\\")\",\"projection_mode\":\"Text(\\\"scalar_aggregate\\\")\"}}";
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
    assert_eq!(
        min_execution.node_properties().get("projected_field"),
        Some(&Value::from("none")),
        "seek explain descriptor should expose projected-field metadata",
    );
    assert_eq!(
        min_execution.node_properties().get("projection_mode"),
        Some(&Value::from("entity_terminal")),
        "seek explain descriptor should expose projection-mode metadata",
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
    assert_eq!(
        min_node.node_properties().get("projection_mode"),
        Some(&Value::from("entity_terminal"))
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
            && min_json.contains("\"fetch\":\"Uint(1)\"")
            && min_json.contains("\"projected_field\":\"Text(\\\"none\\\")\"")
            && min_json.contains("\"projection_mode\":\"Text(\\\"entity_terminal\\\")\""),
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
    assert_eq!(
        max_execution.node_properties().get("projected_field"),
        Some(&Value::from("none")),
        "seek explain descriptor should expose projected-field metadata",
    );
    assert_eq!(
        max_execution.node_properties().get("projection_mode"),
        Some(&Value::from("entity_terminal")),
        "seek explain descriptor should expose projection-mode metadata",
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
    assert_eq!(
        max_node.node_properties().get("projection_mode"),
        Some(&Value::from("entity_terminal"))
    );
    let max_tree = max_node.render_text_tree();
    assert!(
        max_tree.contains("AggregateSeekLast execution_mode=Materialized"),
        "text tree should render seek node label and execution mode",
    );
    let max_json = max_node.render_json_canonical();
    assert!(
        max_json.contains("\"node_type\":\"AggregateSeekLast\"")
            && max_json.contains("\"fetch\":\"Uint(1)\"")
            && max_json.contains("\"projected_field\":\"Text(\\\"none\\\")\"")
            && max_json.contains("\"projection_mode\":\"Text(\\\"entity_terminal\\\")\""),
        "json rendering should expose canonical aggregate seek descriptor fields",
    );
}

#[test]
fn session_select_one_returns_constant_without_execution_metrics() {
    let session = DbSession::new(DB);

    let (value, events) = capture_metrics_events(|| session.select_one());
    assert_eq!(value, Value::Int(1), "select_one should return constant 1");
    assert!(
        events.is_empty(),
        "select_one should bypass planner/executor metrics emission",
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
        .expect("query plan hash should derive from planner contracts");
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
    assert_eq!(
        exists_execution.node_properties().get("projected_field"),
        Some(&Value::from("none")),
        "standard explain descriptor should expose projected-field metadata",
    );
    assert_eq!(
        exists_execution.node_properties().get("projection_mode"),
        Some(&Value::from("scalar_aggregate")),
        "standard explain descriptor should expose scalar projection-mode metadata",
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
    assert_eq!(
        exists_node.node_properties().get("projection_mode"),
        Some(&Value::from("scalar_aggregate")),
        "standard terminal descriptor should expose scalar projection-mode metadata",
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
fn session_load_terminal_explain_not_exists_alias_matches_exists_plan() {
    seed_pushdown_entities(&[
        (9_431, 7, 10),
        (9_432, 7, 20),
        (9_433, 7, 30),
        (9_434, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let query = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
            .order_by("id")
    };

    let exists_plan = query()
        .explain_exists()
        .expect("session explain_exists should succeed");
    let not_exists_plan = query()
        .explain_not_exists()
        .expect("session explain_not_exists should succeed");

    assert_eq!(
        not_exists_plan.terminal(),
        AggregateKind::Exists,
        "not_exists explain alias should remain backed by exists terminal execution",
    );
    assert_eq!(
        session_aggregate_terminal_plan_snapshot(&not_exists_plan),
        session_aggregate_terminal_plan_snapshot(&exists_plan),
        "not_exists explain alias must remain plan-identical to exists explain",
    );
}

#[test]
fn session_load_terminal_explain_first_last_preserve_temporal_order_shape_parity() {
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let day_three = Date::new_checked(2025, 10, 21).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let at_three = Timestamp::from_millis(1_761_040_800_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    let elapsed_three = Duration::from_millis(4_100);
    seed_temporal_boundary_entities(&[
        (9_441, day_one, at_one, elapsed_one),
        (9_442, day_two, at_two, elapsed_two),
        (9_443, day_three, at_three, elapsed_three),
    ]);
    let session = DbSession::new(DB);
    let temporal_window = || {
        session
            .load::<TemporalBoundaryEntity>()
            .order_by("occurred_on")
            .order_by("id")
    };

    // Phase 1: build explain plans for both temporal boundary terminals.
    let first_plan = temporal_window()
        .explain_first()
        .expect("session explain_first should succeed");
    let last_plan = temporal_window()
        .explain_last()
        .expect("session explain_last should succeed");
    assert_eq!(first_plan.terminal(), AggregateKind::First);
    assert_eq!(last_plan.terminal(), AggregateKind::Last);
    assert_eq!(
        first_plan.route(),
        crate::db::ExplainAggregateTerminalRoute::Standard,
        "first explain should remain on the standard terminal route",
    );
    assert_eq!(
        last_plan.route(),
        crate::db::ExplainAggregateTerminalRoute::Standard,
        "last explain should remain on the standard terminal route",
    );

    // Phase 2: lock query and execution parity for shared temporal shape fields.
    assert_eq!(
        first_plan.query().access(),
        last_plan.query().access(),
        "first vs last explain should preserve access-shape parity for equivalent temporal windows",
    );
    assert_eq!(first_plan.query().order_by(), last_plan.query().order_by());
    assert_eq!(first_plan.query().page(), last_plan.query().page());
    assert_eq!(first_plan.query().grouping(), last_plan.query().grouping());
    assert_eq!(
        first_plan.query().order_pushdown(),
        last_plan.query().order_pushdown()
    );
    assert_eq!(
        first_plan.query().consistency(),
        last_plan.query().consistency()
    );
    assert_eq!(
        first_plan.execution().access_strategy(),
        last_plan.execution().access_strategy(),
    );
    assert_eq!(
        first_plan.execution().execution_mode(),
        last_plan.execution().execution_mode(),
        "first vs last temporal explains should agree on execution-mode classification",
    );
    assert_eq!(
        first_plan.execution().ordering_source(),
        last_plan.execution().ordering_source(),
        "first vs last temporal explains should agree on ordering-source classification",
    );
    assert_eq!(first_plan.execution().limit(), None);
    assert_eq!(last_plan.execution().limit(), None);
    assert!(!first_plan.execution().cursor());
    assert!(!last_plan.execution().cursor());

    // Phase 3: keep descriptor parity except for terminal-specific node labels.
    let first_node = first_plan.execution_node_descriptor();
    let last_node = last_plan.execution_node_descriptor();
    assert_eq!(
        first_node.node_type(),
        crate::db::ExplainExecutionNodeType::AggregateFirst
    );
    assert_eq!(
        last_node.node_type(),
        crate::db::ExplainExecutionNodeType::AggregateLast
    );
    assert_eq!(first_node.execution_mode(), last_node.execution_mode());
    assert_eq!(first_node.access_strategy(), last_node.access_strategy());
    assert_eq!(first_node.ordering_source(), last_node.ordering_source());
    assert_eq!(first_node.limit(), last_node.limit());
    assert_eq!(first_node.cursor(), last_node.cursor());
    assert_eq!(first_node.covering_scan(), last_node.covering_scan());
    assert_eq!(first_node.rows_expected(), last_node.rows_expected());
    assert_eq!(
        first_node.node_properties(),
        last_node.node_properties(),
        "first vs last descriptor metadata should remain stable for equivalent temporal windows",
    );
}

#[test]
#[expect(clippy::too_many_lines)]
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
    if matches!(
        descriptor.node_type(),
        crate::db::ExplainExecutionNodeType::IndexPrefixScan
            | crate::db::ExplainExecutionNodeType::IndexRangeScan
    ) {
        assert!(
            descriptor.node_properties().contains_key("prefix_len"),
            "index scan roots should expose matched prefix length metadata",
        );
    }
    assert_eq!(
        descriptor.covering_scan(),
        Some(false),
        "scalar load execution roots should report explicit non-covering status",
    );
    assert!(
        descriptor
            .node_properties()
            .contains_key("access_choice_chosen"),
        "execution root should expose chosen access-choice metadata",
    );
    assert!(
        descriptor
            .node_properties()
            .contains_key("access_choice_alternatives"),
        "execution root should expose alternative access-choice metadata",
    );
    assert!(
        descriptor
            .node_properties()
            .contains_key("access_choice_chosen_reason"),
        "execution root should expose chosen access-choice reason-code metadata",
    );
    assert!(
        descriptor
            .node_properties()
            .contains_key("access_choice_rejections"),
        "execution root should expose rejected access-choice reason-code metadata",
    );
    assert!(
        descriptor
            .node_properties()
            .contains_key("covering_scan_reason"),
        "execution root should expose covering-scan reason metadata",
    );
    assert!(
        descriptor.node_properties().contains_key("scan_direction"),
        "execution root should expose scan direction metadata",
    );
    assert!(
        descriptor
            .node_properties()
            .contains_key("continuation_mode"),
        "execution root should expose continuation mode metadata",
    );
    assert!(
        descriptor.node_properties().contains_key("resume_from"),
        "execution root should expose resume-source metadata",
    );
    assert!(
        descriptor
            .node_properties()
            .contains_key("fast_path_selected"),
        "execution root should expose selected fast-path metadata",
    );
    assert!(
        descriptor
            .node_properties()
            .contains_key("fast_path_selected_reason"),
        "execution root should expose selected fast-path reason metadata",
    );
    assert!(
        descriptor
            .node_properties()
            .contains_key("fast_path_rejections"),
        "execution root should expose rejected fast-path reason metadata",
    );
    assert!(
        descriptor
            .node_properties()
            .contains_key("projected_fields"),
        "execution root should expose projected-fields metadata",
    );
    assert!(
        descriptor
            .node_properties()
            .contains_key("projection_pushdown"),
        "execution root should expose projection-pushdown metadata",
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
        assert_eq!(
            descriptor.node_properties().get("fetch"),
            Some(&Value::from(3u64)),
            "scan root should mirror pushed fetch count when top-n seek is active",
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
    let order_node = descriptor
        .children()
        .iter()
        .find(|child| {
            child.node_type() == crate::db::ExplainExecutionNodeType::OrderByAccessSatisfied
                || child.node_type() == crate::db::ExplainExecutionNodeType::OrderByMaterializedSort
        })
        .expect("ordered shape should project one ORDER BY execution node");
    assert_eq!(
        order_node.node_properties().get("order_satisfied_by_index"),
        Some(&Value::from(matches!(
            order_node.node_type(),
            crate::db::ExplainExecutionNodeType::OrderByAccessSatisfied
        ))),
        "order node should expose explicit index-order satisfaction metadata",
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
    assert_eq!(
        multi.node_properties().get("prefix_values"),
        Some(&Value::List(vec![Value::from(7u64), Value::from(8u64)])),
        "index-multi roots should expose canonical IN prefix values",
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
fn session_load_explain_execution_covering_scan_reports_true_for_unordered_strict_index_shape() {
    seed_pushdown_entities(&[(9_726, 7, 10), (9_727, 7, 20), (9_728, 8, 30)]);
    let session = DbSession::new(DB);
    let descriptor = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate_strict("group", 7))
        .explain_execution()
        .expect("unordered strict index-prefix explain execution should succeed");

    assert_eq!(
        descriptor.covering_scan(),
        Some(true),
        "unordered strict index-prefix load shapes should report covering eligibility",
    );
    assert_eq!(
        descriptor.node_properties().get("covering_scan_reason"),
        Some(&Value::from("index_covering_existing_rows_eligible")),
        "covering-eligible loads should expose explicit covering reason code",
    );
    assert_eq!(
        descriptor.node_properties().get("projection_pushdown"),
        Some(&Value::from(true)),
        "covering-eligible loads should expose projection-pushdown eligibility",
    );
    assert_eq!(
        descriptor.node_properties().get("projected_fields"),
        Some(&Value::List(vec![
            Value::from("id"),
            Value::from("group"),
            Value::from("rank"),
            Value::from("label"),
        ])),
        "projection metadata should preserve canonical field order",
    );
}

#[test]
#[expect(clippy::too_many_lines)]
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
    let strict_prefilter_node = explain_execution_find_first_node(
        &strict_prefilter,
        crate::db::ExplainExecutionNodeType::IndexPredicatePrefilter,
    )
    .expect("strict index-compatible predicate should project prefilter node");
    assert_eq!(
        strict_prefilter_node.node_properties().get("pushdown"),
        Some(&Value::from("group=Uint(7)")),
        "strict prefilter node should expose pushed predicate summary",
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
    let residual_node = explain_execution_find_first_node(
        &residual,
        crate::db::ExplainExecutionNodeType::ResidualPredicateFilter,
    )
    .expect("mixed index/non-index predicate should project residual node");
    assert_eq!(
        residual_node.predicate_pushdown(),
        Some("group=Uint(7)"),
        "residual node should report pushed access predicate separately from residual filter",
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
    let expected_text = r#"IndexPrefixScan execution_mode=Materialized node_id=0 execution_mode_detail=materialized predicate_pushdown_mode=none fast_path_selected=true fast_path_reason=secondary_order_pushdown_eligible access=IndexPrefix(group_rank) covering_scan=false node_properties=access_choice_alternatives=List([]),access_choice_chosen=Text("index:group_rank"),access_choice_chosen_reason=Text("single_candidate"),access_choice_rejections=List([]),continuation_mode=Text("initial"),covering_scan_reason=Text("order_requires_materialization"),fast_path_rejections=List([Text("primary_key=pk_order_fast_path_ineligible"), Text("index_range=index_range_limit_pushdown_disabled")]),fast_path_selected=Text("secondary_prefix"),fast_path_selected_reason=Text("secondary_order_pushdown_eligible"),prefix_len=Uint(1),projected_fields=List([Text("id"), Text("group"), Text("rank"), Text("label")]),projection_pushdown=Bool(false),resume_from=Text("none"),scan_direction=Text("asc")
  IndexPredicatePrefilter execution_mode=Materialized node_id=1 execution_mode_detail=materialized predicate_pushdown_mode=full predicate_pushdown=strict_all_or_none node_properties=pushdown=Text("group=Uint(7)")
  SecondaryOrderPushdown execution_mode=Materialized node_id=2 execution_mode_detail=materialized predicate_pushdown_mode=none node_properties=index=Text("group_rank"),prefix_len=Uint(1)
  OrderByMaterializedSort execution_mode=Materialized node_id=3 execution_mode_detail=materialized predicate_pushdown_mode=none node_properties=order_satisfied_by_index=Bool(false)
  LimitOffset execution_mode=Materialized node_id=4 execution_mode_detail=materialized predicate_pushdown_mode=none limit=2 cursor=false node_properties=offset=Uint(1)"#;
    assert_eq!(
        text_tree, expected_text,
        "execution text-tree snapshot drifted: actual={text_tree}",
    );

    let descriptor_json = query
        .explain_execution_json()
        .expect("strict index-prefix execution json explain should succeed");
    let expected_json = r#"{"node_id":0,"node_type":"IndexPrefixScan","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":{"type":"IndexPrefix","name":"group_rank","fields":["group","rank"],"prefix_len":1,"values":["Uint(7)"]},"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":true,"fast_path_reason":"secondary_order_pushdown_eligible","residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[{"node_id":1,"node_type":"IndexPredicatePrefilter","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"full","predicate_pushdown":"strict_all_or_none","fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"pushdown":"Text(\"group=Uint(7)\")"}},{"node_id":2,"node_type":"SecondaryOrderPushdown","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"index":"Text(\"group_rank\")","prefix_len":"Uint(1)"}},{"node_id":3,"node_type":"OrderByMaterializedSort","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"order_satisfied_by_index":"Bool(false)"}},{"node_id":4,"node_type":"LimitOffset","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":2,"cursor":false,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"offset":"Uint(1)"}}],"node_properties":{"access_choice_alternatives":"List([])","access_choice_chosen":"Text(\"index:group_rank\")","access_choice_chosen_reason":"Text(\"single_candidate\")","access_choice_rejections":"List([])","continuation_mode":"Text(\"initial\")","covering_scan_reason":"Text(\"order_requires_materialization\")","fast_path_rejections":"List([Text(\"primary_key=pk_order_fast_path_ineligible\"), Text(\"index_range=index_range_limit_pushdown_disabled\")])","fast_path_selected":"Text(\"secondary_prefix\")","fast_path_selected_reason":"Text(\"secondary_order_pushdown_eligible\")","prefix_len":"Uint(1)","projected_fields":"List([Text(\"id\"), Text(\"group\"), Text(\"rank\"), Text(\"label\")])","projection_pushdown":"Bool(false)","resume_from":"Text(\"none\")","scan_direction":"Text(\"asc\")"}}"#;
    assert_eq!(
        descriptor_json, expected_json,
        "execution json snapshot drifted: actual={descriptor_json}",
    );
}
