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
    let expected_min = expected.ids().into_iter().min();
    let expected_max = expected.ids().into_iter().max();
    let expected_min_by_id = expected.ids().into_iter().min();
    let expected_max_by_id = expected.ids().into_iter().max();
    let mut expected_ordered_ids = expected.ids();
    expected_ordered_ids.sort_unstable();
    let expected_nth_by_id = expected_ordered_ids.get(1).copied();
    let expected_first = expected.id();
    let expected_last = expected.ids().last().copied();

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
    for (_, entity) in expected_response {
        let rank = Decimal::from_num(u64::from(entity.rank)).expect("rank decimal");
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
