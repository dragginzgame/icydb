use super::*;

// This temporal projection matrix keeps the semantic-type assertions and
// projection-shape checks together so the temporal contract stays obvious.
#[expect(
    clippy::too_many_lines,
    reason = "temporal projection matrix is intentionally exhaustive"
)]
#[test]
fn session_temporal_projection_matrix_preserves_semantic_types() {
    reset_session_sql_store();
    let session = sql_session();
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    let id_one = Id::<SessionTemporalEntity>::from_key(Ulid::from_u128(8_941));
    let id_two = Id::<SessionTemporalEntity>::from_key(Ulid::from_u128(8_942));
    seed_session_temporal_entities(
        &session,
        &[
            (8_941, day_one, at_one, elapsed_one),
            (8_942, day_two, at_two, elapsed_two),
        ],
    );
    let load_window = || {
        session
            .load::<SessionTemporalEntity>()
            .order_term(crate::db::asc("id"))
    };

    // Phase 1: lock semantic entity-field projection types and values.
    let response = load_window()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("temporal execute should succeed");
    let entities = response.entities();
    assert_eq!(entities.len(), 2, "temporal fixture should return two rows");
    let first = &entities[0];
    let second = &entities[1];
    let _: Date = first.occurred_on;
    let _: Timestamp = first.occurred_at;
    let _: Duration = first.elapsed;
    assert_eq!(first.occurred_on, day_one);
    assert_eq!(second.occurred_on, day_two);
    assert_eq!(first.occurred_at, at_one);
    assert_eq!(second.occurred_at, at_two);
    assert_eq!(first.elapsed, elapsed_one);
    assert_eq!(second.elapsed, elapsed_two);

    // Phase 2: lock scalar projection value typing and id/value pairing.
    let day_values = load_window()
        .values_by("occurred_on")
        .expect("occurred_on projection should succeed");
    let at_values = load_window()
        .values_by("occurred_at")
        .expect("occurred_at projection should succeed");
    let elapsed_values = load_window()
        .values_by("elapsed")
        .expect("elapsed projection should succeed");
    let day_pairs = load_window()
        .values_by_with_ids("occurred_on")
        .expect("values_by_with_ids(occurred_on) should succeed");
    let timestamp_pairs = load_window()
        .values_by_with_ids("occurred_at")
        .expect("values_by_with_ids(occurred_at) should succeed");
    let duration_pairs = load_window()
        .values_by_with_ids("elapsed")
        .expect("values_by_with_ids(elapsed) should succeed");

    assert_eq!(
        day_values,
        outputs(vec![Value::Date(day_one), Value::Date(day_two)])
    );
    assert_eq!(
        at_values,
        outputs(vec![Value::Timestamp(at_one), Value::Timestamp(at_two)])
    );
    assert_eq!(
        elapsed_values,
        outputs(vec![
            Value::Duration(elapsed_one),
            Value::Duration(elapsed_two),
        ])
    );
    assert_eq!(
        day_pairs,
        outputs_with_ids(vec![
            (id_one, Value::Date(day_one)),
            (id_two, Value::Date(day_two))
        ]),
    );
    assert_eq!(
        timestamp_pairs,
        outputs_with_ids(vec![
            (id_one, Value::Timestamp(at_one)),
            (id_two, Value::Timestamp(at_two))
        ]),
    );
    assert_eq!(
        duration_pairs,
        outputs_with_ids(vec![
            (id_one, Value::Duration(elapsed_one)),
            (id_two, Value::Duration(elapsed_two))
        ]),
    );

    // Phase 3: lock first/last scalar terminal typing on the same ordered
    // temporal window.
    let first_day = load_window()
        .first_value_by("occurred_on")
        .expect("first_value_by(occurred_on) should succeed");
    let first_timestamp = load_window()
        .first_value_by("occurred_at")
        .expect("first_value_by(occurred_at) should succeed");
    let first_duration = load_window()
        .first_value_by("elapsed")
        .expect("first_value_by(elapsed) should succeed");
    let last_day = load_window()
        .last_value_by("occurred_on")
        .expect("last_value_by(occurred_on) should succeed");
    let last_timestamp = load_window()
        .last_value_by("occurred_at")
        .expect("last_value_by(occurred_at) should succeed");
    let last_duration = load_window()
        .last_value_by("elapsed")
        .expect("last_value_by(elapsed) should succeed");

    assert_eq!(first_day, Some(output(Value::Date(day_one))));
    assert_eq!(first_timestamp, Some(output(Value::Timestamp(at_one))));
    assert_eq!(first_duration, Some(output(Value::Duration(elapsed_one))));
    assert_eq!(last_day, Some(output(Value::Date(day_two))));
    assert_eq!(last_timestamp, Some(output(Value::Timestamp(at_two))));
    assert_eq!(last_duration, Some(output(Value::Duration(elapsed_two))));
}

#[test]
fn session_temporal_grouped_keys_preserve_semantic_types() {
    reset_session_sql_store();
    let session = sql_session();
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    seed_session_temporal_entities(
        &session,
        &[
            (8_943, day_one, at_one, elapsed_one),
            (8_944, day_one, at_two, elapsed_one),
            (8_945, day_two, at_two, elapsed_two),
        ],
    );

    // Phase 1: group by Date and lock semantic key typing in grouped output.
    let by_day = session
        .load::<SessionTemporalEntity>()
        .group_by("occurred_on")
        .expect("group_by(occurred_on) should resolve")
        .aggregate(crate::db::count())
        .execute()
        .and_then(crate::db::LoadQueryResult::into_grouped)
        .expect("grouped by occurred_on should execute");
    assert_eq!(
        by_day
            .rows()
            .iter()
            .map(|row| runtime_outputs(row.group_key()))
            .collect::<Vec<_>>(),
        vec![vec![Value::Date(day_one)], vec![Value::Date(day_two)]],
        "grouped Date keys should stay semantic Date values",
    );
    assert_eq!(
        by_day
            .rows()
            .iter()
            .map(|row| runtime_outputs(row.aggregate_values()))
            .collect::<Vec<_>>(),
        vec![vec![Value::Uint(2)], vec![Value::Uint(1)]],
        "grouped Date counts should match fixture cardinality",
    );

    // Phase 2: group by Timestamp and lock semantic key typing in grouped output.
    let by_timestamp = session
        .load::<SessionTemporalEntity>()
        .group_by("occurred_at")
        .expect("group_by(occurred_at) should resolve")
        .aggregate(crate::db::count())
        .execute()
        .and_then(crate::db::LoadQueryResult::into_grouped)
        .expect("grouped by occurred_at should execute");
    assert_eq!(
        by_timestamp
            .rows()
            .iter()
            .map(|row| runtime_outputs(row.group_key()))
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Timestamp(at_one)],
            vec![Value::Timestamp(at_two)]
        ],
        "grouped Timestamp keys should stay semantic Timestamp values",
    );
    assert_eq!(
        by_timestamp
            .rows()
            .iter()
            .map(|row| runtime_outputs(row.aggregate_values()))
            .collect::<Vec<_>>(),
        vec![vec![Value::Uint(1)], vec![Value::Uint(2)]],
        "grouped Timestamp counts should match fixture cardinality",
    );

    // Phase 3: group by Duration and lock semantic key typing in grouped output.
    let by_duration = session
        .load::<SessionTemporalEntity>()
        .group_by("elapsed")
        .expect("group_by(elapsed) should resolve")
        .aggregate(crate::db::count())
        .execute()
        .and_then(crate::db::LoadQueryResult::into_grouped)
        .expect("grouped by elapsed should execute");
    assert_eq!(
        by_duration
            .rows()
            .iter()
            .map(|row| runtime_outputs(row.group_key()))
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Duration(elapsed_one)],
            vec![Value::Duration(elapsed_two)]
        ],
        "grouped Duration keys should stay semantic Duration values",
    );
    assert_eq!(
        by_duration
            .rows()
            .iter()
            .map(|row| runtime_outputs(row.aggregate_values()))
            .collect::<Vec<_>>(),
        vec![vec![Value::Uint(2)], vec![Value::Uint(1)]],
        "grouped Duration counts should match fixture cardinality",
    );
}

#[test]
fn session_temporal_distinct_projection_values_preserve_semantic_types() {
    reset_session_sql_store();
    let session = sql_session();
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    seed_session_temporal_entities(
        &session,
        &[
            (8_946, day_one, at_one, elapsed_one),
            (8_947, day_one, at_two, elapsed_one),
            (8_948, day_two, at_two, elapsed_two),
        ],
    );

    // Phase 1: lock Date/Timestamp/Duration distinct projection typing and
    // first-observed value ordering under one deterministic window.
    let distinct_days = session
        .load::<SessionTemporalEntity>()
        .order_term(crate::db::asc("id"))
        .distinct_values_by("occurred_on")
        .expect("distinct_values_by(occurred_on) should succeed");
    let distinct_timestamps = session
        .load::<SessionTemporalEntity>()
        .order_term(crate::db::asc("id"))
        .distinct_values_by("occurred_at")
        .expect("distinct_values_by(occurred_at) should succeed");
    let distinct_durations = session
        .load::<SessionTemporalEntity>()
        .order_term(crate::db::asc("id"))
        .distinct_values_by("elapsed")
        .expect("distinct_values_by(elapsed) should succeed");

    // Phase 2: assert semantic temporal value variants are preserved across
    // distinct projection boundaries.
    assert_eq!(
        distinct_days,
        outputs(vec![Value::Date(day_one), Value::Date(day_two)]),
        "distinct Date projections should stay semantic Date values",
    );
    assert_eq!(
        distinct_timestamps,
        outputs(vec![Value::Timestamp(at_one), Value::Timestamp(at_two)]),
        "distinct Timestamp projections should stay semantic Timestamp values",
    );
    assert_eq!(
        distinct_durations,
        outputs(vec![
            Value::Duration(elapsed_one),
            Value::Duration(elapsed_two),
        ]),
        "distinct Duration projections should stay semantic Duration values",
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn session_temporal_ranked_projection_values_preserve_semantic_types() {
    reset_session_sql_store();
    let session = sql_session();
    let day_one = Date::new_checked(2025, 10, 19).expect("date should build");
    let day_two = Date::new_checked(2025, 10, 20).expect("date should build");
    let day_three = Date::new_checked(2025, 10, 21).expect("date should build");
    let at_one = Timestamp::from_millis(1_760_868_000_000);
    let at_two = Timestamp::from_millis(1_760_954_400_000);
    let at_three = Timestamp::from_millis(1_761_040_800_000);
    let elapsed_one = Duration::from_millis(1_500);
    let elapsed_two = Duration::from_millis(2_750);
    let elapsed_three = Duration::from_millis(4_100);
    let id_one = Id::<SessionTemporalEntity>::from_key(Ulid::from_u128(8_953));
    let id_two = Id::<SessionTemporalEntity>::from_key(Ulid::from_u128(8_954));
    let id_three = Id::<SessionTemporalEntity>::from_key(Ulid::from_u128(8_955));
    seed_session_temporal_entities(
        &session,
        &[
            (8_953, day_one, at_one, elapsed_one),
            (8_954, day_two, at_two, elapsed_two),
            (8_955, day_three, at_three, elapsed_three),
        ],
    );
    let load_window = || session.load::<SessionTemporalEntity>();

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

    assert_eq!(
        top_days,
        outputs(vec![Value::Date(day_three), Value::Date(day_two)])
    );
    assert_eq!(
        bottom_days,
        outputs(vec![Value::Date(day_one), Value::Date(day_two)])
    );
    assert_eq!(
        top_timestamps,
        outputs(vec![Value::Timestamp(at_three), Value::Timestamp(at_two)])
    );
    assert_eq!(
        bottom_timestamps,
        outputs(vec![Value::Timestamp(at_one), Value::Timestamp(at_two)])
    );
    assert_eq!(
        top_durations,
        outputs(vec![
            Value::Duration(elapsed_three),
            Value::Duration(elapsed_two),
        ])
    );
    assert_eq!(
        bottom_durations,
        outputs(vec![
            Value::Duration(elapsed_one),
            Value::Duration(elapsed_two),
        ])
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
        outputs_with_ids(vec![
            (id_three, Value::Date(day_three)),
            (id_two, Value::Date(day_two))
        ])
    );
    assert_eq!(
        bottom_day_pairs,
        outputs_with_ids(vec![
            (id_one, Value::Date(day_one)),
            (id_two, Value::Date(day_two))
        ])
    );
    assert_eq!(
        top_timestamp_pairs,
        outputs_with_ids(vec![
            (id_three, Value::Timestamp(at_three)),
            (id_two, Value::Timestamp(at_two))
        ])
    );
    assert_eq!(
        bottom_duration_pairs,
        outputs_with_ids(vec![
            (id_one, Value::Duration(elapsed_one)),
            (id_two, Value::Duration(elapsed_two))
        ])
    );

    // Phase 3: lock top-k / bottom-k row terminal typing and ordering on the
    // same ranked temporal fixture.
    let top_response = load_window()
        .top_k_by("occurred_on", 2)
        .expect("top_k_by(occurred_on, 2) should succeed");
    let top_entities = top_response.entities();
    assert_eq!(top_entities.len(), 2, "top_k_by should return two rows");
    let _: Date = top_entities[0].occurred_on;
    let _: Timestamp = top_entities[0].occurred_at;
    let _: Duration = top_entities[0].elapsed;
    assert_eq!(top_entities[0].occurred_on, day_three);
    assert_eq!(top_entities[1].occurred_on, day_two);
    assert_eq!(top_entities[0].occurred_at, at_three);
    assert_eq!(top_entities[1].occurred_at, at_two);
    assert_eq!(top_entities[0].elapsed, elapsed_three);
    assert_eq!(top_entities[1].elapsed, elapsed_two);

    let bottom_response = load_window()
        .bottom_k_by("elapsed", 2)
        .expect("bottom_k_by(elapsed, 2) should succeed");
    let bottom_entities = bottom_response.entities();
    assert_eq!(
        bottom_entities.len(),
        2,
        "bottom_k_by should return two rows"
    );
    let _: Date = bottom_entities[0].occurred_on;
    let _: Timestamp = bottom_entities[0].occurred_at;
    let _: Duration = bottom_entities[0].elapsed;
    assert_eq!(bottom_entities[0].elapsed, elapsed_one);
    assert_eq!(bottom_entities[1].elapsed, elapsed_two);
    assert_eq!(bottom_entities[0].occurred_on, day_one);
    assert_eq!(bottom_entities[1].occurred_on, day_two);
    assert_eq!(bottom_entities[0].occurred_at, at_one);
    assert_eq!(bottom_entities[1].occurred_at, at_two);
}
