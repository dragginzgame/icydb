use super::*;

/// SessionExecuteProjectionTerminal
///
/// Declares one projection terminal compared against execute() projection output.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionExecuteProjectionTerminal {
    ValuesBy,
    ValuesByWithIds,
    DistinctValuesBy,
}

///
/// SessionExecuteProjectionCase
///
/// One matrix row for session execute-projection parity over a shared window shape.
/// Each row binds fixture, projection terminal kind, and behavior-cell coordinates.
///

struct SessionExecuteProjectionCase {
    label: &'static str,
    cell: RankedDirectionBehaviorCell,
    rows: &'static [(u128, u32, u32)],
    terminal: SessionExecuteProjectionTerminal,
}

const SESSION_EXECUTE_PROJECTION_VALUES_BY_ROWS: [(u128, u32, u32); 6] = [
    (8_321, 7, 10),
    (8_322, 7, 10),
    (8_323, 7, 20),
    (8_324, 7, 30),
    (8_325, 7, 40),
    (8_326, 8, 99),
];
const SESSION_EXECUTE_PROJECTION_VALUES_BY_WITH_IDS_ROWS: [(u128, u32, u32); 6] = [
    (8_3311, 7, 10),
    (8_3312, 7, 10),
    (8_3313, 7, 20),
    (8_3314, 7, 30),
    (8_3315, 7, 40),
    (8_3316, 8, 99),
];
const SESSION_EXECUTE_PROJECTION_DISTINCT_VALUES_BY_ROWS: [(u128, u32, u32); 6] = [
    (8_341, 7, 10),
    (8_342, 7, 10),
    (8_343, 7, 20),
    (8_344, 7, 30),
    (8_345, 7, 20),
    (8_346, 8, 99),
];

fn run_session_execute_projection_actual(
    session: &DbSession<TestCanister>,
    terminal: SessionExecuteProjectionTerminal,
) -> Result<RankedDirectionResult, QueryError> {
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
    };

    match terminal {
        SessionExecuteProjectionTerminal::ValuesBy => Ok(RankedDirectionResult::Values(
            load_window().values_by("rank")?,
        )),
        SessionExecuteProjectionTerminal::ValuesByWithIds => Ok(
            RankedDirectionResult::ValuesWithIds(load_window().values_by_with_ids("rank")?),
        ),
        SessionExecuteProjectionTerminal::DistinctValuesBy => Ok(RankedDirectionResult::Values(
            load_window().distinct_values_by("rank")?,
        )),
    }
}

fn session_execute_projection_expected(
    response: &Response<PushdownParityEntity>,
    terminal: SessionExecuteProjectionTerminal,
) -> RankedDirectionResult {
    match terminal {
        SessionExecuteProjectionTerminal::ValuesBy => {
            RankedDirectionResult::Values(expected_values_by_rank(response))
        }
        SessionExecuteProjectionTerminal::ValuesByWithIds => {
            RankedDirectionResult::ValuesWithIds(expected_values_by_rank_with_ids(response))
        }
        SessionExecuteProjectionTerminal::DistinctValuesBy => {
            RankedDirectionResult::Values(expected_distinct_values_by_rank(response))
        }
    }
}

fn session_execute_projection_cases() -> [SessionExecuteProjectionCase; 3] {
    [
        SessionExecuteProjectionCase {
            label: "values_by",
            cell: RankedDirectionBehaviorCell {
                capability: "projection",
                path: "execute_projection_parity",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &SESSION_EXECUTE_PROJECTION_VALUES_BY_ROWS,
            terminal: SessionExecuteProjectionTerminal::ValuesBy,
        },
        SessionExecuteProjectionCase {
            label: "values_by_with_ids",
            cell: RankedDirectionBehaviorCell {
                capability: "projection",
                path: "execute_projection_parity",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &SESSION_EXECUTE_PROJECTION_VALUES_BY_WITH_IDS_ROWS,
            terminal: SessionExecuteProjectionTerminal::ValuesByWithIds,
        },
        SessionExecuteProjectionCase {
            label: "distinct_values_by",
            cell: RankedDirectionBehaviorCell {
                capability: "projection",
                path: "execute_projection_parity",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &SESSION_EXECUTE_PROJECTION_DISTINCT_VALUES_BY_ROWS,
            terminal: SessionExecuteProjectionTerminal::DistinctValuesBy,
        },
    ]
}

#[test]
fn session_load_projection_terminals_match_execute_projection_matrix() {
    for case in session_execute_projection_cases() {
        seed_pushdown_entities(case.rows);
        let session = DbSession::new(DB);
        let load_window = || {
            session
                .load::<PushdownParityEntity>()
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("id")
                .offset(1)
                .limit(4)
        };

        // Phase 1: execute baseline window once per matrix row.
        let expected_response = load_window()
            .execute()
            .expect("session execute-projection matrix baseline should succeed");

        // Phase 2: execute projection terminal and assert parity with execute projection.
        let actual = run_session_execute_projection_actual(&session, case.terminal)
            .expect("session execute-projection matrix terminal should succeed");
        let expected = session_execute_projection_expected(&expected_response, case.terminal);
        assert_eq!(
            actual, expected,
            "session execute-projection parity failed for case={} cell={:?}",
            case.label, case.cell
        );
    }
}

#[test]
fn session_load_projection_terminals_execute_projection_matrix_covers_all_forms() {
    let labels = session_execute_projection_cases().map(|case| case.label);
    assert_eq!(
        labels,
        ["values_by", "values_by_with_ids", "distinct_values_by"],
        "session execute-projection matrix must enumerate all projection terminal forms"
    );
}

#[test]
fn session_load_take_matches_execute_prefix() {
    seed_pushdown_entities(&[
        (8_3601, 7, 10),
        (8_3602, 7, 20),
        (8_3603, 7, 30),
        (8_3604, 7, 40),
        (8_3605, 7, 50),
        (8_3606, 8, 99),
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
        .expect("baseline execute for take should succeed");
    let actual_take_two = load_window()
        .take(2)
        .expect("session take(2) should succeed");
    let actual_take_ten = load_window()
        .take(10)
        .expect("session take(10) should succeed");
    let expected_take_two_ids: Vec<Id<PushdownParityEntity>> =
        expected.ids().into_iter().take(2).collect();

    assert_eq!(
        actual_take_two.ids(),
        expected_take_two_ids,
        "session take(2) should match first two execute() rows in effective response order"
    );
    assert_eq!(
        actual_take_ten.ids(),
        expected.ids(),
        "session take(k) with k above response size should preserve full effective response"
    );
}

#[test]
fn session_load_top_k_by_matches_execute_field_ordering() {
    seed_pushdown_entities(&[
        (8_3701, 7, 20),
        (8_3702, 7, 40),
        (8_3703, 7, 40),
        (8_3704, 7, 10),
        (8_3705, 7, 30),
        (8_3706, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(0)
            .limit(5)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for top_k_by should succeed");
    let actual_top_three = load_window()
        .top_k_by("rank", 3)
        .expect("session top_k_by(rank, 3) should succeed");
    let mut expected_rank_order = expected
        .0
        .iter()
        .map(|(id, entity)| (entity.rank, *id))
        .collect::<Vec<_>>();
    expected_rank_order.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        right_rank
            .cmp(left_rank)
            .then_with(|| left_id.key().cmp(&right_id.key()))
    });
    let expected_top_three_ids: Vec<Id<PushdownParityEntity>> = expected_rank_order
        .into_iter()
        .take(3)
        .map(|(_, id)| id)
        .collect();

    assert_eq!(
        actual_top_three.ids(),
        expected_top_three_ids,
        "session top_k_by(rank, 3) should match execute() reduced by deterministic (rank desc, id asc) ordering"
    );
}

#[test]
fn session_load_bottom_k_by_matches_execute_field_ordering() {
    seed_pushdown_entities(&[
        (8_3721, 7, 20),
        (8_3722, 7, 40),
        (8_3723, 7, 40),
        (8_3724, 7, 10),
        (8_3725, 7, 30),
        (8_3726, 8, 99),
    ]);
    let session = DbSession::new(DB);
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(0)
            .limit(5)
    };

    let expected = load_window()
        .execute()
        .expect("baseline execute for bottom_k_by should succeed");
    let actual_bottom_three = load_window()
        .bottom_k_by("rank", 3)
        .expect("session bottom_k_by(rank, 3) should succeed");
    let mut expected_rank_order = expected
        .0
        .iter()
        .map(|(id, entity)| (entity.rank, *id))
        .collect::<Vec<_>>();
    expected_rank_order.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        left_rank
            .cmp(right_rank)
            .then_with(|| left_id.key().cmp(&right_id.key()))
    });
    let expected_bottom_three_ids: Vec<Id<PushdownParityEntity>> = expected_rank_order
        .into_iter()
        .take(3)
        .map(|(_, id)| id)
        .collect();

    assert_eq!(
        actual_bottom_three.ids(),
        expected_bottom_three_ids,
        "session bottom_k_by(rank, 3) should match execute() reduced by deterministic (rank asc, id asc) ordering"
    );
}

///
/// SessionRankedProjectionTerminal
///
/// Selects top-vs-bottom rank ordering for session projection parity rows.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionRankedProjectionTerminal {
    Top,
    Bottom,
}

///
/// SessionRankedProjectionOutput
///
/// Selects one session projection form to compare against ranked-row projection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionRankedProjectionOutput {
    Values,
    ValuesWithIds,
}

///
/// SessionRankedProjectionCase
///
/// One matrix row for session ranked projection parity against ranked rows.
/// Each row binds fixture, rank terminal orientation, and projection form.
///

struct SessionRankedProjectionCase {
    label: &'static str,
    cell: RankedDirectionBehaviorCell,
    rows: &'static [(u128, u32, u32)],
    terminal: SessionRankedProjectionTerminal,
    output: SessionRankedProjectionOutput,
}

const SESSION_RANKED_PROJECTION_TOP_VALUES_ROWS: [(u128, u32, u32); 6] = [
    (8_3771, 7, 20),
    (8_3772, 7, 40),
    (8_3773, 7, 40),
    (8_3774, 7, 10),
    (8_3775, 7, 30),
    (8_3776, 8, 99),
];
const SESSION_RANKED_PROJECTION_BOTTOM_VALUES_ROWS: [(u128, u32, u32); 6] = [
    (8_3781, 7, 20),
    (8_3782, 7, 40),
    (8_3783, 7, 40),
    (8_3784, 7, 10),
    (8_3785, 7, 30),
    (8_3786, 8, 99),
];
const SESSION_RANKED_PROJECTION_TOP_VALUES_WITH_IDS_ROWS: [(u128, u32, u32); 6] = [
    (8_3807, 7, 20),
    (8_3808, 7, 40),
    (8_3809, 7, 40),
    (8_3810, 7, 10),
    (8_3811, 7, 30),
    (8_3812, 8, 99),
];
const SESSION_RANKED_PROJECTION_BOTTOM_VALUES_WITH_IDS_ROWS: [(u128, u32, u32); 6] = [
    (8_3813, 7, 20),
    (8_3814, 7, 40),
    (8_3815, 7, 40),
    (8_3816, 7, 10),
    (8_3817, 7, 30),
    (8_3818, 8, 99),
];

fn run_session_ranked_rows_for_projection(
    session: &DbSession<TestCanister>,
    terminal: SessionRankedProjectionTerminal,
) -> Result<Response<PushdownParityEntity>, QueryError> {
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(0)
            .limit(5)
    };

    match terminal {
        SessionRankedProjectionTerminal::Top => load_window().top_k_by("rank", 3),
        SessionRankedProjectionTerminal::Bottom => load_window().bottom_k_by("rank", 3),
    }
}

fn run_session_ranked_projection(
    session: &DbSession<TestCanister>,
    terminal: SessionRankedProjectionTerminal,
    output: SessionRankedProjectionOutput,
) -> Result<RankedDirectionResult, QueryError> {
    let load_window = || {
        session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(0)
            .limit(5)
    };

    match (terminal, output) {
        (SessionRankedProjectionTerminal::Top, SessionRankedProjectionOutput::Values) => Ok(
            RankedDirectionResult::Values(load_window().top_k_by_values("rank", 3)?),
        ),
        (SessionRankedProjectionTerminal::Bottom, SessionRankedProjectionOutput::Values) => Ok(
            RankedDirectionResult::Values(load_window().bottom_k_by_values("rank", 3)?),
        ),
        (SessionRankedProjectionTerminal::Top, SessionRankedProjectionOutput::ValuesWithIds) => Ok(
            RankedDirectionResult::ValuesWithIds(load_window().top_k_by_with_ids("rank", 3)?),
        ),
        (SessionRankedProjectionTerminal::Bottom, SessionRankedProjectionOutput::ValuesWithIds) => {
            Ok(RankedDirectionResult::ValuesWithIds(
                load_window().bottom_k_by_with_ids("rank", 3)?,
            ))
        }
    }
}

fn session_ranked_projection_cases() -> [SessionRankedProjectionCase; 4] {
    [
        SessionRankedProjectionCase {
            label: "top_k_by_values",
            cell: RankedDirectionBehaviorCell {
                capability: "projection",
                path: "ranked_row_projection_parity",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &SESSION_RANKED_PROJECTION_TOP_VALUES_ROWS,
            terminal: SessionRankedProjectionTerminal::Top,
            output: SessionRankedProjectionOutput::Values,
        },
        SessionRankedProjectionCase {
            label: "bottom_k_by_values",
            cell: RankedDirectionBehaviorCell {
                capability: "projection",
                path: "ranked_row_projection_parity",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &SESSION_RANKED_PROJECTION_BOTTOM_VALUES_ROWS,
            terminal: SessionRankedProjectionTerminal::Bottom,
            output: SessionRankedProjectionOutput::Values,
        },
        SessionRankedProjectionCase {
            label: "top_k_by_with_ids",
            cell: RankedDirectionBehaviorCell {
                capability: "projection",
                path: "ranked_row_projection_parity",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &SESSION_RANKED_PROJECTION_TOP_VALUES_WITH_IDS_ROWS,
            terminal: SessionRankedProjectionTerminal::Top,
            output: SessionRankedProjectionOutput::ValuesWithIds,
        },
        SessionRankedProjectionCase {
            label: "bottom_k_by_with_ids",
            cell: RankedDirectionBehaviorCell {
                capability: "projection",
                path: "ranked_row_projection_parity",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &SESSION_RANKED_PROJECTION_BOTTOM_VALUES_WITH_IDS_ROWS,
            terminal: SessionRankedProjectionTerminal::Bottom,
            output: SessionRankedProjectionOutput::ValuesWithIds,
        },
    ]
}

#[test]
fn session_load_ranked_projection_terminals_match_ranked_rows_matrix() {
    for case in session_ranked_projection_cases() {
        seed_pushdown_entities(case.rows);
        let session = DbSession::new(DB);

        // Phase 1: establish ranked rows as the parity baseline for each matrix row.
        let ranked_rows = run_session_ranked_rows_for_projection(&session, case.terminal)
            .expect("session ranked projection matrix baseline should succeed");

        // Phase 2: execute projection terminal and compare against baseline projection.
        let actual = run_session_ranked_projection(&session, case.terminal, case.output)
            .expect("session ranked projection matrix terminal should succeed");
        let expected = match case.output {
            SessionRankedProjectionOutput::Values => {
                RankedDirectionResult::Values(expected_values_by_rank(&ranked_rows))
            }
            SessionRankedProjectionOutput::ValuesWithIds => {
                RankedDirectionResult::ValuesWithIds(expected_values_by_rank_with_ids(&ranked_rows))
            }
        };
        assert_eq!(
            actual, expected,
            "session ranked projection parity failed for case={} cell={:?}",
            case.label, case.cell
        );
    }
}

#[test]
fn session_load_ranked_projection_terminals_matrix_covers_all_forms() {
    let labels = session_ranked_projection_cases().map(|case| case.label);
    assert_eq!(
        labels,
        [
            "top_k_by_values",
            "bottom_k_by_values",
            "top_k_by_with_ids",
            "bottom_k_by_with_ids",
        ],
        "session ranked projection matrix must enumerate all rank projection forms"
    );
}

#[test]
fn session_load_distinct_values_by_matches_values_by_first_observed_dedup() {
    seed_pushdown_entities(&[
        (8_341, 7, 10),
        (8_342, 7, 10),
        (8_343, 7, 20),
        (8_344, 7, 30),
        (8_345, 7, 20),
        (8_346, 8, 99),
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

    let values = load_window()
        .values_by("rank")
        .expect("session values_by(rank) should succeed");
    let distinct_values = load_window()
        .distinct_values_by("rank")
        .expect("session distinct_values_by(rank) should succeed");

    let mut expected_distinct_from_values = Vec::new();
    for value in &values {
        if expected_distinct_from_values
            .iter()
            .any(|existing| existing == value)
        {
            continue;
        }
        expected_distinct_from_values.push(value.clone());
    }

    assert!(
        values.len() >= distinct_values.len(),
        "session values_by(field).len() must be >= distinct_values_by(field).len()"
    );
    assert_eq!(
        distinct_values, expected_distinct_from_values,
        "session distinct_values_by(field) must equal values_by(field) deduped by first occurrence"
    );
}

#[test]
fn session_load_terminal_value_projection_matches_execute_projection() {
    seed_pushdown_entities(&[
        (8_3511, 7, 10),
        (8_3512, 7, 10),
        (8_3513, 7, 20),
        (8_3514, 7, 30),
        (8_3515, 7, 40),
        (8_3516, 8, 99),
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
        .expect("baseline execute for terminal value projection should succeed");
    let actual_first = load_window()
        .first_value_by("rank")
        .expect("session first_value_by(rank) should succeed");
    let actual_last = load_window()
        .last_value_by("rank")
        .expect("session last_value_by(rank) should succeed");

    assert_eq!(
        actual_first,
        expected_first_value_by_rank(&expected),
        "session first_value_by(rank) parity failed"
    );
    assert_eq!(
        actual_last,
        expected_last_value_by_rank(&expected),
        "session last_value_by(rank) parity failed"
    );
}

///
/// ProjectionScanBudgetTerminal
///
/// Declares one projection terminal that must preserve execute() scan budget.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectionScanBudgetTerminal {
    ValuesBy,
    DistinctValuesBy,
    ValuesByWithIds,
}

///
/// ProjectionScanBudgetCase
///
/// One matrix row for projection-terminal scan-budget parity against execute().
/// Each row binds a fixture, terminal kind, and behavior-cell coordinates.
///

struct ProjectionScanBudgetCase {
    label: &'static str,
    cell: RankedDirectionBehaviorCell,
    rows: &'static [(u128, u32, u32)],
    terminal: ProjectionScanBudgetTerminal,
}

const PROJECTION_SCAN_BUDGET_VALUES_BY_ROWS: [(u128, u32, u32); 6] = [
    (8_331, 7, 10),
    (8_332, 7, 10),
    (8_333, 7, 20),
    (8_334, 7, 30),
    (8_335, 7, 40),
    (8_336, 8, 99),
];
const PROJECTION_SCAN_BUDGET_DISTINCT_VALUES_BY_ROWS: [(u128, u32, u32); 6] = [
    (8_351, 7, 10),
    (8_352, 7, 10),
    (8_353, 7, 20),
    (8_354, 7, 30),
    (8_355, 7, 20),
    (8_356, 8, 99),
];
const PROJECTION_SCAN_BUDGET_VALUES_BY_WITH_IDS_ROWS: [(u128, u32, u32); 6] = [
    (8_361, 7, 10),
    (8_362, 7, 10),
    (8_363, 7, 20),
    (8_364, 7, 30),
    (8_365, 7, 20),
    (8_366, 8, 99),
];

fn run_projection_scan_budget_terminal(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
    terminal: ProjectionScanBudgetTerminal,
) -> Result<(), InternalError> {
    match terminal {
        ProjectionScanBudgetTerminal::ValuesBy => {
            load.values_by(plan, "rank")?;
        }
        ProjectionScanBudgetTerminal::DistinctValuesBy => {
            load.distinct_values_by(plan, "rank")?;
        }
        ProjectionScanBudgetTerminal::ValuesByWithIds => {
            load.values_by_with_ids(plan, "rank")?;
        }
    }

    Ok(())
}

fn projection_scan_budget_cases() -> [ProjectionScanBudgetCase; 3] {
    [
        ProjectionScanBudgetCase {
            label: "values_by",
            cell: RankedDirectionBehaviorCell {
                capability: "projection",
                path: "scan_budget_parity",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &PROJECTION_SCAN_BUDGET_VALUES_BY_ROWS,
            terminal: ProjectionScanBudgetTerminal::ValuesBy,
        },
        ProjectionScanBudgetCase {
            label: "distinct_values_by",
            cell: RankedDirectionBehaviorCell {
                capability: "projection",
                path: "scan_budget_parity",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &PROJECTION_SCAN_BUDGET_DISTINCT_VALUES_BY_ROWS,
            terminal: ProjectionScanBudgetTerminal::DistinctValuesBy,
        },
        ProjectionScanBudgetCase {
            label: "values_by_with_ids",
            cell: RankedDirectionBehaviorCell {
                capability: "projection",
                path: "scan_budget_parity",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &PROJECTION_SCAN_BUDGET_VALUES_BY_WITH_IDS_ROWS,
            terminal: ProjectionScanBudgetTerminal::ValuesByWithIds,
        },
    ]
}

#[test]
fn aggregate_field_target_projection_terminals_preserve_scan_budget_parity_with_execute_matrix() {
    for case in projection_scan_budget_cases() {
        seed_pushdown_entities(case.rows);
        let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
        let build_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("id")
                .offset(1)
                .limit(4)
                .plan()
                .expect("projection scan-budget matrix plan should build")
        };

        // Phase 1: establish execute() baseline scan budget for the shared matrix shape.
        let (_, scanned_execute) =
            capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
                load.execute(build_plan())
                    .expect("projection scan-budget execute baseline should succeed")
            });

        // Phase 2: execute the matrix terminal and assert scan-budget parity.
        let ((), scanned_terminal) =
            capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
                run_projection_scan_budget_terminal(&load, build_plan(), case.terminal)
                    .expect("projection scan-budget matrix terminal should succeed");
            });
        assert_eq!(
            scanned_terminal, scanned_execute,
            "projection terminal scan-budget parity failed for case={} cell={:?}",
            case.label, case.cell
        );
    }
}

#[test]
fn aggregate_field_target_projection_terminals_scan_budget_matrix_covers_all_forms() {
    let labels = projection_scan_budget_cases().map(|case| case.label);
    assert_eq!(
        labels,
        ["values_by", "distinct_values_by", "values_by_with_ids"],
        "projection scan-budget matrix must enumerate all projection terminal forms"
    );
}
