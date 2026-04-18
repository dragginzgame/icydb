use super::*;

#[test]
fn session_aggregate_projection_terminal_matrix_matches_execute_projection() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_321, 7, 10),
            (8_322, 7, 10),
            (8_323, 7, 20),
            (8_324, 7, 30),
            (8_325, 7, 40),
            (8_326, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_term(crate::db::desc("id"))
            .offset(1)
            .limit(4)
    };

    // Phase 1: establish the execute() window as the shared parity baseline.
    let expected = load_window()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("session aggregate execute-projection baseline should succeed");

    // Phase 2: compare the projection terminals against the execute baseline.
    let values = run_session_aggregate_projection_terminal(
        &session,
        SessionAggregateProjectionTerminal::ValuesBy,
    )
    .expect("session values_by(rank) should succeed");
    let values_with_ids = run_session_aggregate_projection_terminal(
        &session,
        SessionAggregateProjectionTerminal::ValuesByWithIds,
    )
    .expect("session values_by_with_ids(rank) should succeed");
    let distinct_values = run_session_aggregate_projection_terminal(
        &session,
        SessionAggregateProjectionTerminal::DistinctValuesBy,
    )
    .expect("session distinct_values_by(rank) should succeed");

    assert_eq!(
        values,
        SessionAggregateResult::Values(session_aggregate_values_by_rank(&expected)),
        "session values_by(rank) should match execute() projection order",
    );
    assert_eq!(
        values_with_ids,
        SessionAggregateResult::ValuesWithIds(session_aggregate_values_by_rank_with_ids(&expected)),
        "session values_by_with_ids(rank) should match execute() projection order",
    );
    assert_eq!(
        distinct_values,
        SessionAggregateResult::Values(vec![Value::Uint(30), Value::Uint(20), Value::Uint(10),]),
        "session distinct_values_by(rank) should preserve first-observed dedup order",
    );
    let distinct_values = match distinct_values {
        SessionAggregateResult::Values(values) => values,
        other => panic!("distinct_values_by(rank) should stay on the values terminal: {other:?}"),
    };
    let mut expected_distinct = Vec::new();
    for value in session_aggregate_values_by_rank(&expected) {
        if expected_distinct.iter().any(|existing| existing == &value) {
            continue;
        }
        expected_distinct.push(value);
    }
    assert!(
        session_aggregate_values_by_rank(&expected).len() >= distinct_values.len(),
        "values_by(field).len() must be >= distinct_values_by(field).len()",
    );
    assert_eq!(
        distinct_values, expected_distinct,
        "distinct_values_by(field) must equal values_by(field) deduped by first occurrence",
    );

    assert!(
        load_window()
            .first_value_by("rank")
            .expect("session aggregate first_value_by(rank) should succeed")
            == session_aggregate_first_value_by_rank(&expected),
        "session aggregate first_value_by(rank) should match execute() projection order",
    );
    assert_eq!(
        load_window()
            .last_value_by("rank")
            .expect("session aggregate last_value_by(rank) should succeed"),
        session_aggregate_last_value_by_rank(&expected),
        "session aggregate last_value_by(rank) should match execute() projection order",
    );
}

#[test]
fn session_aggregate_values_by_unknown_field_fails_before_scan_budget_consumption() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_3391, 7, 10),
            (8_3392, 7, 20),
            (8_3393, 7, 30),
            (8_3394, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_term(crate::db::desc("id"))
            .offset(0)
            .limit(3)
    };

    let (result, scanned_rows) =
        capture_rows_scanned_for_entity(SessionAggregateEntity::PATH, || {
            load_window().values_by("missing_field")
        });
    let Err(err) = result else {
        panic!("session values_by(missing_field) should be rejected");
    };

    assert!(
        matches!(err, QueryError::Execute(_)),
        "session unknown-field projection should remain an execute-domain error: {err:?}",
    );
    assert_eq!(
        scanned_rows, 0,
        "session unknown-field projection should fail before scan-budget consumption",
    );
    assert!(
        err.to_string().contains("unknown aggregate target field"),
        "session unknown-field projection should preserve explicit field taxonomy: {err:?}",
    );
}

#[test]
fn session_aggregate_take_matches_execute_prefix() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_3601, 7, 10),
            (8_3602, 7, 20),
            (8_3603, 7, 30),
            (8_3604, 7, 40),
            (8_3605, 7, 50),
            (8_3606, 8, 99),
        ],
    );
    let load_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_term(crate::db::desc("id"))
            .offset(1)
            .limit(4)
    };

    let expected = load_window()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("baseline execute for session aggregate take should succeed");
    let take_two = load_window()
        .take(2)
        .expect("session aggregate take(2) should succeed");
    let take_ten = load_window()
        .take(10)
        .expect("session aggregate take(10) should succeed");

    assert_eq!(
        session_aggregate_ids(&take_two),
        session_aggregate_ids(&expected)
            .into_iter()
            .take(2)
            .collect::<Vec<_>>(),
        "session aggregate take(2) should match the execute() prefix",
    );
    assert_eq!(
        session_aggregate_ids(&take_ten),
        session_aggregate_ids(&expected),
        "session aggregate take(k) above response size should preserve the full response",
    );
}

// This ranked-terminal parity test is intentionally table-shaped end to end.
// Splitting it further would hide the ranked baseline and terminal checks.
#[expect(
    clippy::too_many_lines,
    reason = "table-driven ranked terminal parity test"
)]
#[test]
fn session_aggregate_ranked_projection_terminals_match_ranked_rows() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_3701, 7, 20),
            (8_3702, 7, 40),
            (8_3703, 7, 40),
            (8_3704, 7, 10),
            (8_3705, 7, 30),
            (8_3706, 8, 99),
        ],
    );
    let ordering_window = || {
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_term(crate::db::desc("id"))
            .offset(0)
            .limit(5)
    };
    let ordering_expected = ordering_window()
        .execute()
        .and_then(crate::db::LoadQueryResult::into_rows)
        .expect("baseline execute for ranked session aggregate parity should succeed");
    let mut descending_rank = ordering_expected
        .iter()
        .map(|row| (row.entity_ref().rank, row.id().key()))
        .collect::<Vec<_>>();
    let mut ascending_rank = descending_rank.clone();
    descending_rank.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        right_rank
            .cmp(left_rank)
            .then_with(|| left_id.cmp(right_id))
    });
    ascending_rank.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        left_rank
            .cmp(right_rank)
            .then_with(|| left_id.cmp(right_id))
    });

    let actual_top = ordering_window()
        .top_k_by("rank", 3)
        .expect("session aggregate top_k_by(rank, 3) should succeed");
    let actual_bottom = ordering_window()
        .bottom_k_by("rank", 3)
        .expect("session aggregate bottom_k_by(rank, 3) should succeed");

    assert_eq!(
        session_aggregate_ids(&actual_top),
        descending_rank
            .into_iter()
            .take(3)
            .map(|(_, id)| id)
            .collect::<Vec<_>>(),
        "session aggregate top_k_by(rank, 3) should match deterministic rank-desc ordering",
    );
    assert_eq!(
        session_aggregate_ids(&actual_bottom),
        ascending_rank
            .into_iter()
            .take(3)
            .map(|(_, id)| id)
            .collect::<Vec<_>>(),
        "session aggregate bottom_k_by(rank, 3) should match deterministic rank-asc ordering",
    );

    let cases = [
        (
            &[
                (8_3771, 7, 20),
                (8_3772, 7, 40),
                (8_3773, 7, 40),
                (8_3774, 7, 10),
                (8_3775, 7, 30),
                (8_3776, 8, 99),
            ][..],
            SessionAggregateRankTerminal::Top,
            SessionAggregateRankOutput::Values,
        ),
        (
            &[
                (8_3781, 7, 20),
                (8_3782, 7, 40),
                (8_3783, 7, 40),
                (8_3784, 7, 10),
                (8_3785, 7, 30),
                (8_3786, 8, 99),
            ][..],
            SessionAggregateRankTerminal::Bottom,
            SessionAggregateRankOutput::Values,
        ),
        (
            &[
                (8_3807, 7, 20),
                (8_3808, 7, 40),
                (8_3809, 7, 40),
                (8_3810, 7, 10),
                (8_3811, 7, 30),
                (8_3812, 8, 99),
            ][..],
            SessionAggregateRankTerminal::Top,
            SessionAggregateRankOutput::ValuesWithIds,
        ),
        (
            &[
                (8_3813, 7, 20),
                (8_3814, 7, 40),
                (8_3815, 7, 40),
                (8_3816, 7, 10),
                (8_3817, 7, 30),
                (8_3818, 8, 99),
            ][..],
            SessionAggregateRankTerminal::Bottom,
            SessionAggregateRankOutput::ValuesWithIds,
        ),
    ];

    // Phase 1: use the ranked row response as the parity baseline for each case.
    for (rows, terminal, output) in cases {
        reset_session_sql_store();
        let session = sql_session();
        seed_session_aggregate_entities(&session, rows);
        let load_window = || {
            session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_term(crate::db::desc("id"))
                .offset(0)
                .limit(5)
        };
        let ranked_rows = match terminal {
            SessionAggregateRankTerminal::Top => load_window()
                .top_k_by("rank", 3)
                .expect("session aggregate top_k_by(rank, 3) should succeed"),
            SessionAggregateRankTerminal::Bottom => load_window()
                .bottom_k_by("rank", 3)
                .expect("session aggregate bottom_k_by(rank, 3) should succeed"),
        };

        // Phase 2: compare the projection terminal against ranked-row projection.
        let actual = run_session_aggregate_rank_terminal(&session, terminal, output)
            .expect("session ranked projection terminal should succeed");
        let expected = match output {
            SessionAggregateRankOutput::Values => {
                SessionAggregateResult::Values(session_aggregate_values_by_rank(&ranked_rows))
            }
            SessionAggregateRankOutput::ValuesWithIds => SessionAggregateResult::ValuesWithIds(
                session_aggregate_values_by_rank_with_ids(&ranked_rows),
            ),
        };
        assert_eq!(
            actual, expected,
            "session ranked projection terminal should match ranked-row projection",
        );
    }
}

#[test]
#[expect(clippy::too_many_lines)]
fn session_aggregate_ranked_terminals_are_invariant_to_base_order_direction() {
    reset_session_sql_store();
    let session = sql_session();
    seed_session_aggregate_entities(
        &session,
        &[
            (8_3711, 7, 10),
            (8_3712, 7, 40),
            (8_3713, 7, 20),
            (8_3714, 7, 30),
            (8_3715, 7, 40),
            (8_3716, 8, 99),
        ],
    );

    // Phase 1: capture the ascending base-order outputs.
    let asc_top_ids = SessionAggregateResult::Ids(session_aggregate_ids(
        &session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_term(crate::db::asc("id"))
            .top_k_by("rank", 3)
            .expect("ascending session aggregate top_k_by should succeed"),
    ));
    let asc_bottom_ids = SessionAggregateResult::Ids(session_aggregate_ids(
        &session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_term(crate::db::asc("id"))
            .bottom_k_by("rank", 3)
            .expect("ascending session aggregate bottom_k_by should succeed"),
    ));
    let asc_top_values = SessionAggregateResult::Values(
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_term(crate::db::asc("id"))
            .top_k_by_values("rank", 3)
            .expect("ascending session aggregate top_k_by_values should succeed"),
    );
    let asc_bottom_values = SessionAggregateResult::Values(
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_term(crate::db::asc("id"))
            .bottom_k_by_values("rank", 3)
            .expect("ascending session aggregate bottom_k_by_values should succeed"),
    );
    let asc_top_values_with_ids = SessionAggregateResult::ValuesWithIds(
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_term(crate::db::asc("id"))
            .top_k_by_with_ids("rank", 3)
            .expect("ascending session aggregate top_k_by_with_ids should succeed")
            .into_iter()
            .map(|(id, value)| (id.key(), value))
            .collect(),
    );
    let asc_bottom_values_with_ids = SessionAggregateResult::ValuesWithIds(
        session
            .load::<SessionAggregateEntity>()
            .filter(session_aggregate_group_predicate(7))
            .order_term(crate::db::asc("id"))
            .bottom_k_by_with_ids("rank", 3)
            .expect("ascending session aggregate bottom_k_by_with_ids should succeed")
            .into_iter()
            .map(|(id, value)| (id.key(), value))
            .collect(),
    );

    // Phase 2: assert parity against descending base-order outputs.
    assert_eq!(
        asc_top_ids,
        SessionAggregateResult::Ids(session_aggregate_ids(
            &session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_term(crate::db::desc("id"))
                .top_k_by("rank", 3)
                .expect("descending session aggregate top_k_by should succeed"),
        )),
        "session aggregate top_k_by(rank, 3) should be invariant to base order direction",
    );
    assert_eq!(
        asc_bottom_ids,
        SessionAggregateResult::Ids(session_aggregate_ids(
            &session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_term(crate::db::desc("id"))
                .bottom_k_by("rank", 3)
                .expect("descending session aggregate bottom_k_by should succeed"),
        )),
        "session aggregate bottom_k_by(rank, 3) should be invariant to base order direction",
    );
    assert_eq!(
        asc_top_values,
        SessionAggregateResult::Values(
            session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_term(crate::db::desc("id"))
                .top_k_by_values("rank", 3)
                .expect("descending session aggregate top_k_by_values should succeed"),
        ),
        "session aggregate top_k_by_values(rank, 3) should be invariant to base order direction",
    );
    assert_eq!(
        asc_bottom_values,
        SessionAggregateResult::Values(
            session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_term(crate::db::desc("id"))
                .bottom_k_by_values("rank", 3)
                .expect("descending session aggregate bottom_k_by_values should succeed"),
        ),
        "session aggregate bottom_k_by_values(rank, 3) should be invariant to base order direction",
    );
    assert_eq!(
        asc_top_values_with_ids,
        SessionAggregateResult::ValuesWithIds(
            session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_term(crate::db::desc("id"))
                .top_k_by_with_ids("rank", 3)
                .expect("descending session aggregate top_k_by_with_ids should succeed")
                .into_iter()
                .map(|(id, value)| (id.key(), value))
                .collect(),
        ),
        "session aggregate top_k_by_with_ids(rank, 3) should be invariant to base order direction",
    );
    assert_eq!(
        asc_bottom_values_with_ids,
        SessionAggregateResult::ValuesWithIds(
            session
                .load::<SessionAggregateEntity>()
                .filter(session_aggregate_group_predicate(7))
                .order_term(crate::db::desc("id"))
                .bottom_k_by_with_ids("rank", 3)
                .expect("descending session aggregate bottom_k_by_with_ids should succeed")
                .into_iter()
                .map(|(id, value)| (id.key(), value))
                .collect(),
        ),
        "session aggregate bottom_k_by_with_ids(rank, 3) should be invariant to base order direction",
    );
}
