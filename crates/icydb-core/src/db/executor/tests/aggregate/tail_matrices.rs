use super::*;

/// RankOrderTerminal
///
/// Selects rank orientation shared by k-ranked matrix assertions.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RankOrderTerminal {
    Top,
    Bottom,
}

///
/// BoundedRankWindowCase
///
/// One matrix row for bounded-window rank behavior against execute() oracle.
/// Verifies bounded ranking, scan-budget parity, and bounded-vs-unbounded divergence.
///

struct BoundedRankWindowCase {
    label: &'static str,
    rows: &'static [(u128, u32, u32)],
    terminal: RankOrderTerminal,
}

const BOUNDED_RANK_WINDOW_TOP_ROWS: [(u128, u32, u32); 6] = [
    (8_3811, 7, 10),
    (8_3812, 7, 20),
    (8_3813, 7, 30),
    (8_3814, 7, 100),
    (8_3815, 7, 90),
    (8_3816, 7, 80),
];
const BOUNDED_RANK_WINDOW_BOTTOM_ROWS: [(u128, u32, u32); 6] = [
    (8_3821, 7, 100),
    (8_3822, 7, 90),
    (8_3823, 7, 80),
    (8_3824, 7, 10),
    (8_3825, 7, 20),
    (8_3826, 7, 30),
];

fn run_pushdown_rank_terminal(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
    terminal: RankOrderTerminal,
    k: u32,
) -> Result<Response<PushdownParityEntity>, InternalError> {
    match terminal {
        RankOrderTerminal::Top => load.top_k_by(plan, "rank", k),
        RankOrderTerminal::Bottom => load.bottom_k_by(plan, "rank", k),
    }
}

fn expected_pushdown_rank_ids(
    response: &Response<PushdownParityEntity>,
    terminal: RankOrderTerminal,
    k: usize,
) -> Vec<Id<PushdownParityEntity>> {
    let mut expected_rank_order = response
        .0
        .iter()
        .map(|(id, entity)| (entity.rank, *id))
        .collect::<Vec<_>>();
    expected_rank_order.sort_unstable_by(|(left_rank, left_id), (right_rank, right_id)| {
        match terminal {
            RankOrderTerminal::Top => right_rank
                .cmp(left_rank)
                .then_with(|| left_id.key().cmp(&right_id.key())),
            RankOrderTerminal::Bottom => left_rank
                .cmp(right_rank)
                .then_with(|| left_id.key().cmp(&right_id.key())),
        }
    });

    expected_rank_order
        .into_iter()
        .take(k)
        .map(|(_, id)| id)
        .collect()
}

fn bounded_rank_window_cases() -> [BoundedRankWindowCase; 2] {
    [
        BoundedRankWindowCase {
            label: "top_k_by",
            rows: &BOUNDED_RANK_WINDOW_TOP_ROWS,
            terminal: RankOrderTerminal::Top,
        },
        BoundedRankWindowCase {
            label: "bottom_k_by",
            rows: &BOUNDED_RANK_WINDOW_BOTTOM_ROWS,
            terminal: RankOrderTerminal::Bottom,
        },
    ]
}

#[test]
fn aggregate_field_target_rank_terminals_bounded_window_scan_budget_and_oracle_matrix() {
    for case in bounded_rank_window_cases() {
        seed_pushdown_entities(case.rows);
        let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
        let build_bounded_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("id")
                .limit(3)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("bounded rank-window matrix plan should build")
        };
        let build_unbounded_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("id")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("unbounded rank-window matrix plan should build")
        };

        // Phase 1: establish execute baseline and bounded terminal result with scan budgets.
        let (bounded_execute, scanned_execute) =
            capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
                load.execute(build_bounded_plan())
                    .expect("bounded rank-window matrix execute baseline should succeed")
            });
        let (bounded_ranked, scanned_ranked) =
            capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
                run_pushdown_rank_terminal(&load, build_bounded_plan(), case.terminal, 2)
                    .expect("bounded rank-window matrix terminal should succeed")
            });

        // Phase 2: compare bounded terminal output to execute oracle and assert scan-budget parity.
        let expected_bounded_ids = expected_pushdown_rank_ids(&bounded_execute, case.terminal, 2);
        assert_eq!(
            bounded_ranked.ids(),
            expected_bounded_ids,
            "bounded rank-window execute oracle mismatch for case={}",
            case.label
        );
        assert_eq!(
            scanned_ranked, scanned_execute,
            "bounded rank-window scan-budget parity failed for case={}",
            case.label
        );

        // Phase 3: assert bounded behavior diverges from unbounded query on same dataset.
        let unbounded_ranked =
            run_pushdown_rank_terminal(&load, build_unbounded_plan(), case.terminal, 2)
                .expect("unbounded rank-window matrix terminal should succeed");
        assert_ne!(
            bounded_ranked.ids(),
            unbounded_ranked.ids(),
            "bounded rank-window behavior should differ from unbounded behavior for case={}",
            case.label
        );
    }
}

///
/// ForcedShapeRankCase
///
/// One matrix row for forced FullScan/IndexRange execute-oracle rank assertions.
/// Each row verifies shape lock plus execute parity for top/bottom terminals.
///

struct ForcedShapeRankCase {
    label: &'static str,
    full_scan_rows: &'static [u128],
    index_range_rows: &'static [(u128, u32)],
    terminal: RankOrderTerminal,
}

const FORCED_SHAPE_FULL_SCAN_TOP_ROWS: [u128; 6] = [8_3901, 8_3902, 8_3903, 8_3904, 8_3905, 8_3906];
const FORCED_SHAPE_FULL_SCAN_BOTTOM_ROWS: [u128; 6] =
    [8_3921, 8_3922, 8_3923, 8_3924, 8_3925, 8_3926];
const FORCED_SHAPE_INDEX_RANGE_TOP_ROWS: [(u128, u32); 6] = [
    (8_3911, 100),
    (8_3912, 101),
    (8_3913, 102),
    (8_3914, 103),
    (8_3915, 104),
    (8_3916, 105),
];
const FORCED_SHAPE_INDEX_RANGE_BOTTOM_ROWS: [(u128, u32); 6] = [
    (8_3931, 100),
    (8_3932, 101),
    (8_3933, 102),
    (8_3934, 103),
    (8_3935, 104),
    (8_3936, 105),
];

fn run_simple_rank_terminal(
    load: &LoadExecutor<SimpleEntity>,
    plan: ExecutablePlan<SimpleEntity>,
    terminal: RankOrderTerminal,
    k: u32,
) -> Result<Response<SimpleEntity>, InternalError> {
    match terminal {
        RankOrderTerminal::Top => load.top_k_by(plan, "id", k),
        RankOrderTerminal::Bottom => load.bottom_k_by(plan, "id", k),
    }
}

fn run_unique_index_rank_terminal(
    load: &LoadExecutor<UniqueIndexRangeEntity>,
    plan: ExecutablePlan<UniqueIndexRangeEntity>,
    terminal: RankOrderTerminal,
    k: u32,
) -> Result<Response<UniqueIndexRangeEntity>, InternalError> {
    match terminal {
        RankOrderTerminal::Top => load.top_k_by(plan, "code", k),
        RankOrderTerminal::Bottom => load.bottom_k_by(plan, "code", k),
    }
}

fn expected_simple_rank_ids(
    response: &Response<SimpleEntity>,
    terminal: RankOrderTerminal,
    k: usize,
) -> Vec<Id<SimpleEntity>> {
    let mut expected = response.ids();
    match terminal {
        RankOrderTerminal::Top => {
            expected.sort_unstable_by_key(|id| std::cmp::Reverse(id.key()));
        }
        RankOrderTerminal::Bottom => expected.sort_unstable_by_key(Id::key),
    }
    expected.truncate(k);
    expected
}

fn expected_unique_index_rank_ids(
    response: &Response<UniqueIndexRangeEntity>,
    terminal: RankOrderTerminal,
    k: usize,
) -> Vec<Id<UniqueIndexRangeEntity>> {
    let mut ranked = response
        .0
        .iter()
        .map(|(id, entity)| (entity.code, *id))
        .collect::<Vec<_>>();
    ranked.sort_unstable_by(
        |(left_code, left_id), (right_code, right_id)| match terminal {
            RankOrderTerminal::Top => right_code
                .cmp(left_code)
                .then_with(|| left_id.key().cmp(&right_id.key())),
            RankOrderTerminal::Bottom => left_code
                .cmp(right_code)
                .then_with(|| left_id.key().cmp(&right_id.key())),
        },
    );

    ranked.into_iter().take(k).map(|(_, id)| id).collect()
}

fn forced_shape_rank_cases() -> [ForcedShapeRankCase; 2] {
    [
        ForcedShapeRankCase {
            label: "top_k_by",
            full_scan_rows: &FORCED_SHAPE_FULL_SCAN_TOP_ROWS,
            index_range_rows: &FORCED_SHAPE_INDEX_RANGE_TOP_ROWS,
            terminal: RankOrderTerminal::Top,
        },
        ForcedShapeRankCase {
            label: "bottom_k_by",
            full_scan_rows: &FORCED_SHAPE_FULL_SCAN_BOTTOM_ROWS,
            index_range_rows: &FORCED_SHAPE_INDEX_RANGE_BOTTOM_ROWS,
            terminal: RankOrderTerminal::Bottom,
        },
    ]
}

#[test]
fn aggregate_field_target_rank_terminals_forced_shape_execute_oracle_matrix() {
    for case in forced_shape_rank_cases() {
        // Phase 1: force FullScan shape and assert execute-oracle parity for id-ranking.
        seed_simple_entities(case.full_scan_rows);
        let simple_load = LoadExecutor::<SimpleEntity>::new(DB, false);
        let build_full_scan_plan = || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .offset(1)
                .limit(4)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("forced-shape FullScan matrix plan should build")
        };
        let full_scan_plan = build_full_scan_plan();
        assert!(
            matches!(full_scan_plan.explain().access, ExplainAccessPath::FullScan),
            "forced-shape FullScan matrix must force FullScan for case={}",
            case.label
        );
        let full_scan_execute = simple_load
            .execute(build_full_scan_plan())
            .expect("forced-shape FullScan execute baseline should succeed");
        let full_scan_ranked =
            run_simple_rank_terminal(&simple_load, build_full_scan_plan(), case.terminal, 2)
                .expect("forced-shape FullScan terminal should succeed");
        assert_eq!(
            full_scan_ranked.ids(),
            expected_simple_rank_ids(&full_scan_execute, case.terminal, 2),
            "forced-shape FullScan execute oracle mismatch for case={}",
            case.label
        );

        // Phase 2: force IndexRange shape and assert execute-oracle parity for code-ranking.
        seed_unique_index_range_entities(case.index_range_rows);
        let range_load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
        let code_range = u32_range_predicate("code", 101, 106);
        let build_index_range_plan = || {
            Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
                .filter(code_range.clone())
                .order_by_desc("code")
                .offset(1)
                .limit(3)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("forced-shape IndexRange matrix plan should build")
        };
        let index_range_plan = build_index_range_plan();
        assert!(
            matches!(
                index_range_plan.explain().access,
                ExplainAccessPath::IndexRange { .. }
            ),
            "forced-shape IndexRange matrix must force IndexRange for case={}",
            case.label
        );
        let index_range_execute = range_load
            .execute(build_index_range_plan())
            .expect("forced-shape IndexRange execute baseline should succeed");
        let index_range_ranked =
            run_unique_index_rank_terminal(&range_load, build_index_range_plan(), case.terminal, 2)
                .expect("forced-shape IndexRange terminal should succeed");
        assert_eq!(
            index_range_ranked.ids(),
            expected_unique_index_rank_ids(&index_range_execute, case.terminal, 2),
            "forced-shape IndexRange execute oracle mismatch for case={}",
            case.label
        );
    }
}

///
/// SimpleTerminalProbeKind
///
/// Declares one simple-entity aggregate terminal for short-circuit probes.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SimpleTerminalProbeKind {
    Exists,
    Min,
    Max,
    First,
    Last,
}

///
/// SimpleTerminalExpected
///
/// Canonical expected payload for simple-entity short-circuit probe rows.
/// IDs are represented as raw `u128` values for table readability.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SimpleTerminalExpected {
    Exists(bool),
    Id(Option<Ulid>),
}

///
/// SimpleTerminalProbeCase
///
/// One short-circuit matrix row for simple-entity terminal semantics.
/// Each row binds terminal kind, direction/window controls, and expected behavior.
///

#[derive(Clone, Copy)]
struct SimpleTerminalProbeCase {
    label: &'static str,
    ids: &'static [u128],
    terminal: SimpleTerminalProbeKind,
    direction: OrderDirection,
    offset: u32,
    limit: Option<u32>,
    expected: SimpleTerminalExpected,
    expected_scanned: Option<usize>,
}

const SIMPLE_PROBE_EXISTS_IDS: [u128; 6] = [9201, 9202, 9203, 9204, 9205, 9206];
const SIMPLE_PROBE_FIRST_ROW_EXTREMA_IDS: [u128; 6] = [9301, 9302, 9303, 9304, 9305, 9306];
const SIMPLE_PROBE_OFFSET_EXTREMA_IDS: [u128; 7] = [9401, 9402, 9403, 9404, 9405, 9406, 9407];
const SIMPLE_PROBE_OFFSET_FIRST_IDS: [u128; 7] = [9451, 9452, 9453, 9454, 9455, 9456, 9457];
const SIMPLE_PROBE_LIMITED_LAST_IDS: [u128; 7] = [9461, 9462, 9463, 9464, 9465, 9466, 9467];
const SIMPLE_PROBE_UNBOUNDED_LAST_IDS: [u128; 7] = [9471, 9472, 9473, 9474, 9475, 9476, 9477];
const SIMPLE_PROBE_DIRECTION_IDS: [u128; 5] = [9481, 9482, 9483, 9484, 9485];

fn run_simple_terminal_probe(
    load: &LoadExecutor<SimpleEntity>,
    case: SimpleTerminalProbeCase,
) -> Result<SimpleTerminalExpected, InternalError> {
    let mut query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore);
    query = match case.direction {
        OrderDirection::Asc => query.order_by("id"),
        OrderDirection::Desc => query.order_by_desc("id"),
    };
    if case.offset > 0 {
        query = query.offset(case.offset);
    }
    if let Some(limit) = case.limit {
        query = query.limit(limit);
    }
    let plan = query
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("simple short-circuit probe matrix plan should build");

    let output = match case.terminal {
        SimpleTerminalProbeKind::Exists => {
            SimpleTerminalExpected::Exists(load.aggregate_exists(plan)?)
        }
        SimpleTerminalProbeKind::Min => {
            SimpleTerminalExpected::Id(load.aggregate_min(plan)?.map(|id| id.key()))
        }
        SimpleTerminalProbeKind::Max => {
            SimpleTerminalExpected::Id(load.aggregate_max(plan)?.map(|id| id.key()))
        }
        SimpleTerminalProbeKind::First => {
            SimpleTerminalExpected::Id(load.aggregate_first(plan)?.map(|id| id.key()))
        }
        SimpleTerminalProbeKind::Last => {
            SimpleTerminalExpected::Id(load.aggregate_last(plan)?.map(|id| id.key()))
        }
    };

    Ok(output)
}

#[expect(clippy::too_many_arguments)]
fn simple_terminal_probe_case(
    label: &'static str,
    ids: &'static [u128],
    terminal: SimpleTerminalProbeKind,
    direction: OrderDirection,
    offset: u32,
    limit: Option<u32>,
    expected: SimpleTerminalExpected,
    expected_scanned: Option<usize>,
) -> SimpleTerminalProbeCase {
    SimpleTerminalProbeCase {
        label,
        ids,
        terminal,
        direction,
        offset,
        limit,
        expected,
        expected_scanned,
    }
}

#[expect(clippy::too_many_lines)]
fn simple_terminal_probe_cases() -> [SimpleTerminalProbeCase; 16] {
    [
        simple_terminal_probe_case(
            "exists_asc_early_stop",
            &SIMPLE_PROBE_EXISTS_IDS,
            SimpleTerminalProbeKind::Exists,
            OrderDirection::Asc,
            0,
            None,
            SimpleTerminalExpected::Exists(true),
            Some(1),
        ),
        simple_terminal_probe_case(
            "exists_desc_early_stop",
            &SIMPLE_PROBE_EXISTS_IDS,
            SimpleTerminalProbeKind::Exists,
            OrderDirection::Desc,
            0,
            None,
            SimpleTerminalExpected::Exists(true),
            Some(1),
        ),
        simple_terminal_probe_case(
            "min_asc_first_row_short_circuit",
            &SIMPLE_PROBE_FIRST_ROW_EXTREMA_IDS,
            SimpleTerminalProbeKind::Min,
            OrderDirection::Asc,
            0,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9301))),
            Some(1),
        ),
        simple_terminal_probe_case(
            "max_desc_first_row_short_circuit",
            &SIMPLE_PROBE_FIRST_ROW_EXTREMA_IDS,
            SimpleTerminalProbeKind::Max,
            OrderDirection::Desc,
            0,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9306))),
            Some(1),
        ),
        simple_terminal_probe_case(
            "min_asc_offset_plus_one",
            &SIMPLE_PROBE_OFFSET_EXTREMA_IDS,
            SimpleTerminalProbeKind::Min,
            OrderDirection::Asc,
            3,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9404))),
            Some(4),
        ),
        simple_terminal_probe_case(
            "max_desc_offset_plus_one",
            &SIMPLE_PROBE_OFFSET_EXTREMA_IDS,
            SimpleTerminalProbeKind::Max,
            OrderDirection::Desc,
            3,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9404))),
            Some(4),
        ),
        simple_terminal_probe_case(
            "first_asc_offset_plus_one",
            &SIMPLE_PROBE_OFFSET_FIRST_IDS,
            SimpleTerminalProbeKind::First,
            OrderDirection::Asc,
            3,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9454))),
            Some(4),
        ),
        simple_terminal_probe_case(
            "first_desc_offset_plus_one",
            &SIMPLE_PROBE_OFFSET_FIRST_IDS,
            SimpleTerminalProbeKind::First,
            OrderDirection::Desc,
            3,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9454))),
            Some(4),
        ),
        simple_terminal_probe_case(
            "last_asc_limited_window_offset_plus_limit",
            &SIMPLE_PROBE_LIMITED_LAST_IDS,
            SimpleTerminalProbeKind::Last,
            OrderDirection::Asc,
            2,
            Some(3),
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9465))),
            Some(5),
        ),
        simple_terminal_probe_case(
            "last_desc_limited_window_offset_plus_limit",
            &SIMPLE_PROBE_LIMITED_LAST_IDS,
            SimpleTerminalProbeKind::Last,
            OrderDirection::Desc,
            2,
            Some(3),
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9463))),
            Some(5),
        ),
        simple_terminal_probe_case(
            "last_asc_unbounded_window_scans_full_stream",
            &SIMPLE_PROBE_UNBOUNDED_LAST_IDS,
            SimpleTerminalProbeKind::Last,
            OrderDirection::Asc,
            2,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9477))),
            Some(7),
        ),
        simple_terminal_probe_case(
            "last_desc_unbounded_window_scans_full_stream",
            &SIMPLE_PROBE_UNBOUNDED_LAST_IDS,
            SimpleTerminalProbeKind::Last,
            OrderDirection::Desc,
            2,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9471))),
            Some(7),
        ),
        simple_terminal_probe_case(
            "first_asc_respects_direction",
            &SIMPLE_PROBE_DIRECTION_IDS,
            SimpleTerminalProbeKind::First,
            OrderDirection::Asc,
            0,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9481))),
            None,
        ),
        simple_terminal_probe_case(
            "first_desc_respects_direction",
            &SIMPLE_PROBE_DIRECTION_IDS,
            SimpleTerminalProbeKind::First,
            OrderDirection::Desc,
            0,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9485))),
            None,
        ),
        simple_terminal_probe_case(
            "last_asc_respects_direction",
            &SIMPLE_PROBE_DIRECTION_IDS,
            SimpleTerminalProbeKind::Last,
            OrderDirection::Asc,
            0,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9485))),
            None,
        ),
        simple_terminal_probe_case(
            "last_desc_respects_direction",
            &SIMPLE_PROBE_DIRECTION_IDS,
            SimpleTerminalProbeKind::Last,
            OrderDirection::Desc,
            0,
            None,
            SimpleTerminalExpected::Id(Some(Ulid::from_u128(9481))),
            None,
        ),
    ]
}

#[test]
fn aggregate_simple_terminal_short_circuit_and_direction_matrix() {
    for case in simple_terminal_probe_cases() {
        seed_simple_entities(case.ids);
        let load = LoadExecutor::<SimpleEntity>::new(DB, false);

        let (actual, scanned) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
            run_simple_terminal_probe(&load, case)
                .expect("simple short-circuit probe matrix execution should succeed")
        });
        assert_eq!(
            actual, case.expected,
            "simple short-circuit probe output mismatch for case={}",
            case.label
        );

        if let Some(expected_scanned) = case.expected_scanned {
            assert_eq!(
                scanned, expected_scanned,
                "simple short-circuit probe scan budget mismatch for case={}",
                case.label
            );
        }
    }
}

#[test]
fn aggregate_last_unbounded_desc_large_dataset_scans_full_stream() {
    let ids: Vec<u128> = (0u128..128u128)
        .map(|i| 9701u128.saturating_add(i))
        .collect();
    seed_simple_entities(&ids);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (last_desc, scanned_last_desc) =
        capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
            load.aggregate_last(
                Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                    .order_by_desc("id")
                    .plan()
                    .map(crate::db::executor::ExecutablePlan::from)
                    .expect("last DESC large unbounded plan should build"),
            )
            .expect("last DESC large unbounded should succeed")
        });

    assert_eq!(
        last_desc.map(|id| id.key()),
        Some(Ulid::from_u128(9701)),
        "last DESC should return the last id in descending response order"
    );
    assert_eq!(
        scanned_last_desc, 128,
        "last DESC without limit should scan the full stream for large datasets"
    );
}

#[test]
fn aggregate_last_secondary_index_desc_mixed_direction_falls_back_safely() {
    let mut rows = Vec::new();
    for i in 0u32..64u32 {
        rows.push((
            9801u128.saturating_add(u128::from(i)),
            if i % 2 == 0 { 7 } else { 8 },
            i,
        ));
    }
    seed_pushdown_entities(rows.as_slice());
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    let (last_desc, scanned_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_last(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(group_seven.clone())
                    .order_by_desc("rank")
                    .plan()
                    .map(crate::db::executor::ExecutablePlan::from)
                    .expect("secondary last DESC unbounded plan should build"),
            )
            .expect("secondary last DESC unbounded should succeed")
        });

    assert_eq!(
        last_desc.map(|id| id.key()),
        Some(Ulid::from_u128(9801)),
        "secondary last DESC should return the final row in descending rank order"
    );
    assert_eq!(
        scanned_desc, 64,
        "mixed-direction secondary order should reject pushdown and fall back without under-scanning"
    );
}

#[test]
fn aggregate_last_index_range_ineligible_pushdown_shape_preserves_parity() {
    seed_unique_index_range_entities(&[
        (9811, 200),
        (9812, 201),
        (9813, 202),
        (9814, 203),
        (9815, 204),
        (9816, 205),
    ]);
    let load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let range_predicate = u32_range_predicate("code", 201, 206);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
                .filter(range_predicate.clone())
                .order_by("label")
                .offset(1)
                .limit(2)
        },
        "index-range ineligible pushdown shape",
    );
}

#[test]
fn aggregate_distinct_offset_probe_hint_suppression_preserves_parity() {
    seed_simple_entities(&[9501, 9502]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let duplicate_front_predicate = Predicate::Or(vec![
        id_in_predicate(&[9501]),
        id_in_predicate(&[9501, 9502]),
    ]);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .filter(duplicate_front_predicate.clone())
                .distinct()
                .order_by("id")
                .offset(1)
        },
        "distinct + offset probe-hint suppression",
    );
}

#[test]
fn aggregate_count_distinct_offset_window_disables_bounded_probe_hint() {
    seed_simple_entities(&[9511, 9512, 9513, 9514, 9515, 9516, 9517]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    let (count_asc, scanned_asc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .distinct()
                .order_by("id")
                .offset(2)
                .limit(2)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("count distinct+offset ASC plan should build"),
        )
        .expect("count distinct+offset ASC should succeed")
    });
    let (count_desc, scanned_desc) = capture_rows_scanned_for_entity(SimpleEntity::PATH, || {
        load.aggregate_count(
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .distinct()
                .order_by_desc("id")
                .offset(2)
                .limit(2)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("count distinct+offset DESC plan should build"),
        )
        .expect("count distinct+offset DESC should succeed")
    });

    assert_eq!(
        count_asc, 2,
        "ASC distinct+offset count should respect window"
    );
    assert_eq!(
        count_desc, 2,
        "DESC distinct+offset count should respect window"
    );
    assert_eq!(
        scanned_asc, 7,
        "ASC distinct+offset count should stay unbounded at access phase"
    );
    assert_eq!(
        scanned_desc, 7,
        "DESC distinct+offset count should stay unbounded at access phase"
    );
}

#[test]
fn aggregate_secondary_index_strict_prefilter_preserves_parity_across_window_shapes() {
    seed_pushdown_group_rank_fixture(10_101, 48, 10_301, 24);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let strict_filter = strict_group_rank_subset_filter(&[3, 7, 19, 23, 41]);

    for (direction_desc, distinct) in [(false, false), (false, true), (true, false), (true, true)] {
        assert_aggregate_parity_for_query(
            &load,
            || {
                let mut query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(strict_filter.clone());
                if distinct {
                    query = query.distinct();
                }
                if direction_desc {
                    query.order_by_desc("rank").offset(1).limit(3)
                } else {
                    query.order_by("rank").offset(1).limit(3)
                }
            },
            "secondary strict index-predicate prefilter parity",
        );
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StrictPrefilterAggregate {
    Exists,
    MinBy,
    MaxBy,
    First,
    Last,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StrictPrefilterOutput {
    Exists(bool),
    Id(Option<Id<PushdownParityEntity>>),
}

fn seed_pushdown_group_rank_fixture(
    group_seven_base: u128,
    group_seven_count: u32,
    group_eight_base: u128,
    group_eight_count: u32,
) {
    let mut rows = Vec::with_capacity(
        usize::try_from(group_seven_count.saturating_add(group_eight_count)).unwrap_or(0),
    );
    for rank in 0u32..group_seven_count {
        rows.push((group_seven_base.saturating_add(u128::from(rank)), 7, rank));
    }
    for rank in 0u32..group_eight_count {
        rows.push((group_eight_base.saturating_add(u128::from(rank)), 8, rank));
    }
    seed_pushdown_entities(rows.as_slice());
}

fn strict_group_rank_subset_filter(ranks: &'static [u32]) -> Predicate {
    Predicate::And(vec![
        u32_eq_predicate_strict("group", 7),
        u32_in_predicate_strict("rank", ranks),
    ])
}

fn uncertain_group_rank_subset_filter(ranks: &'static [u32]) -> Predicate {
    Predicate::And(vec![
        u32_eq_predicate("group", 7),
        u32_in_predicate("rank", ranks),
    ])
}

fn run_strict_prefilter_aggregate(
    load: &LoadExecutor<PushdownParityEntity>,
    aggregate: StrictPrefilterAggregate,
    filter: Predicate,
) -> Result<StrictPrefilterOutput, InternalError> {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore).filter(filter);
    let plan = match aggregate {
        StrictPrefilterAggregate::MaxBy => query
            .order_by_desc("rank")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("strict prefilter DESC aggregate plan should build"),
        StrictPrefilterAggregate::Exists
        | StrictPrefilterAggregate::MinBy
        | StrictPrefilterAggregate::First
        | StrictPrefilterAggregate::Last => query
            .order_by("rank")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("strict prefilter ASC aggregate plan should build"),
    };

    match aggregate {
        StrictPrefilterAggregate::Exists => load
            .aggregate_exists(plan)
            .map(StrictPrefilterOutput::Exists),
        StrictPrefilterAggregate::MinBy => load
            .aggregate_min_by(plan, "rank")
            .map(StrictPrefilterOutput::Id),
        StrictPrefilterAggregate::MaxBy => load
            .aggregate_max_by(plan, "rank")
            .map(StrictPrefilterOutput::Id),
        StrictPrefilterAggregate::First => {
            load.aggregate_first(plan).map(StrictPrefilterOutput::Id)
        }
        StrictPrefilterAggregate::Last => load.aggregate_last(plan).map(StrictPrefilterOutput::Id),
    }
}

fn assert_strict_prefilter_scan_reduction(
    load: &LoadExecutor<PushdownParityEntity>,
    aggregate: StrictPrefilterAggregate,
    label: &'static str,
) -> usize {
    const TARGET_RANKS: &[u32] = &[151, 152, 153];
    let strict_filter = strict_group_rank_subset_filter(TARGET_RANKS);
    let uncertain_filter = uncertain_group_rank_subset_filter(TARGET_RANKS);

    let (strict_output, strict_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            run_strict_prefilter_aggregate(load, aggregate, strict_filter.clone())
                .expect("strict prefilter aggregate should succeed")
        });
    let (fallback_output, fallback_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            run_strict_prefilter_aggregate(load, aggregate, uncertain_filter.clone())
                .expect("uncertain fallback aggregate should succeed")
        });

    assert_eq!(
        strict_output, fallback_output,
        "strict prefilter and uncertain fallback should preserve parity for terminal={label}",
    );
    assert!(
        strict_scanned < fallback_scanned,
        "strict prefilter should scan fewer rows than uncertain fallback for terminal={label}",
    );

    strict_scanned
}

#[test]
fn aggregate_strict_prefilter_reduces_scan_vs_uncertain_fallback() {
    seed_pushdown_group_rank_fixture(10_601, 160, 10_901, 40);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let strict_exists_scanned =
        assert_strict_prefilter_scan_reduction(&load, StrictPrefilterAggregate::Exists, "exists");
    assert!(
        strict_exists_scanned <= 3,
        "strict aggregate prefilter should bound scans to matching index candidates"
    );

    for (aggregate, label) in [
        (StrictPrefilterAggregate::MinBy, "min_by"),
        (StrictPrefilterAggregate::MaxBy, "max_by"),
        (StrictPrefilterAggregate::First, "first"),
        (StrictPrefilterAggregate::Last, "last"),
    ] {
        assert_strict_prefilter_scan_reduction(&load, aggregate, label);
    }
}

#[test]
fn aggregate_missing_ok_skips_leading_stale_secondary_keys_for_exists_min_max() {
    seed_pushdown_entities(&[
        (9601, 7, 10),
        (9602, 7, 20),
        (9603, 7, 30),
        (9604, 7, 40),
        (9605, 8, 50),
    ]);
    // Remove edge rows from primary data only, preserving index entries to
    // simulate stale leading secondary keys.
    remove_pushdown_row_data(9601);
    remove_pushdown_row_data(9604);

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let group_seven = u32_eq_predicate("group", 7);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(group_seven.clone())
                .order_by("rank")
        },
        "Ignore stale-leading ASC secondary path",
    );
    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(group_seven.clone())
                .order_by_desc("rank")
        },
        "Ignore stale-leading DESC secondary path",
    );

    let (exists_asc, scanned_asc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_exists(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(group_seven.clone())
                    .order_by("rank")
                    .plan()
                    .map(crate::db::executor::ExecutablePlan::from)
                    .expect("exists ASC stale-leading plan should build"),
            )
            .expect("exists ASC stale-leading should succeed")
        });
    let (exists_desc, scanned_desc) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_exists(
                Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                    .filter(group_seven.clone())
                    .order_by_desc("rank")
                    .plan()
                    .map(crate::db::executor::ExecutablePlan::from)
                    .expect("exists DESC stale-leading plan should build"),
            )
            .expect("exists DESC stale-leading should succeed")
        });

    assert!(
        exists_asc,
        "exists ASC should continue past stale leading key and find a row"
    );
    assert!(
        exists_desc,
        "exists DESC should continue past stale leading key and find a row"
    );
    assert!(
        scanned_asc >= 2,
        "exists ASC should scan beyond the first stale key"
    );
    assert!(
        scanned_desc >= 2,
        "exists DESC should scan beyond the first stale key"
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn aggregate_count_pushdown_contract_matrix_preserves_parity() {
    // Case A: full-scan ordered shape should be count-pushdown eligible.
    seed_simple_entities(&[9701, 9702, 9703, 9704, 9705]);
    let simple_load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let full_scan_query = || {
        Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .offset(1)
            .limit(2)
    };
    let full_scan_plan = full_scan_query()
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("full-scan count matrix plan should build");
    assert!(
        ExecutionKernel::is_streaming_access_shape_safe::<SimpleEntity, _>(
            full_scan_plan.as_inner(),
        ),
        "full-scan matrix shape should be streaming-safe"
    );
    assert!(
        count_pushdown_contract_eligible(&full_scan_plan),
        "full-scan matrix shape should be count-pushdown eligible by contract"
    );
    assert_count_parity_for_query(&simple_load, full_scan_query, "count matrix full-scan");

    // Case B: residual-filter full-scan is access-supported but not streaming-safe.
    seed_phase_entities(&[(9801, 1), (9802, 2), (9803, 2), (9804, 3)]);
    let phase_load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let residual_filter_query = || {
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("rank", 2))
            .order_by("id")
    };
    let residual_filter_plan = residual_filter_query()
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("residual-filter count matrix plan should build");
    assert!(
        !ExecutionKernel::is_streaming_access_shape_safe::<PhaseEntity, _>(
            residual_filter_plan.as_inner(),
        ),
        "residual-filter matrix shape should be streaming-unsafe"
    );
    assert!(
        explain_access_supports_count_pushdown(&residual_filter_plan.explain().access),
        "residual-filter matrix shape should still be access-supported for pushdown paths"
    );
    assert!(
        !count_pushdown_contract_eligible(&residual_filter_plan),
        "residual-filter matrix shape must not be count-pushdown eligible"
    );
    assert_count_parity_for_query(
        &phase_load,
        residual_filter_query,
        "count matrix residual-filter full-scan",
    );

    // Case C: secondary-order query with stale leading keys must remain ineligible
    // for count pushdown and preserve materialized count parity.
    seed_pushdown_entities(&[(9901, 7, 10), (9902, 7, 20), (9903, 7, 30), (9904, 7, 40)]);
    remove_pushdown_row_data(9901);
    let pushdown_load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let secondary_index_query = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
    };
    let secondary_index_plan = secondary_index_query()
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("secondary-index count matrix plan should build");
    assert!(
        !count_pushdown_contract_eligible(&secondary_index_plan),
        "secondary-index matrix shape must not be count-pushdown eligible"
    );
    assert_count_parity_for_query(
        &pushdown_load,
        secondary_index_query,
        "count matrix secondary-index",
    );

    // Case D: composite (OR) shape must remain ineligible for count pushdown.
    seed_simple_entities(&[9951, 9952, 9953, 9954, 9955, 9956]);
    let composite_load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let composite_predicate = Predicate::Or(vec![
        id_in_predicate(&[9951, 9952, 9953, 9954]),
        id_in_predicate(&[9953, 9954, 9955, 9956]),
    ]);
    let composite_query = || {
        Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .filter(composite_predicate.clone())
            .order_by("id")
    };
    let composite_plan = composite_query()
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("composite count matrix plan should build");
    assert!(
        matches!(
            composite_plan.explain().access,
            ExplainAccessPath::Union(_) | ExplainAccessPath::Intersection(_)
        ),
        "composite count matrix shape should compile to a composite access plan"
    );
    assert!(
        !count_pushdown_contract_eligible(&composite_plan),
        "composite count matrix shape must not be count-pushdown eligible"
    );
    assert_count_parity_for_query(
        &composite_load,
        composite_query,
        "count matrix composite OR",
    );
}

///
/// DescCursorResumeCase
///
/// One DESC cursor-resume matrix row for one access-shape family.
/// Each row binds a seed fixture and one query builder run function.
///

struct DescCursorResumeCase {
    label: &'static str,
    run: fn() -> (Vec<Ulid>, Vec<Ulid>),
    assert_strict_descending: bool,
}

macro_rules! collect_ulid_keys {
    ($response:expr) => {
        $response
            .ids()
            .into_iter()
            .map(|id| id.key())
            .collect::<Vec<_>>()
    };
}

macro_rules! paged_ids_and_cursor {
    ($execution:expr) => {{
        let execution = $execution;
        (
            collect_ulid_keys!(execution.response()),
            execution
                .continuation_cursor()
                .map(crate::db::encode_cursor),
        )
    }};
}

fn collect_desc_cursor_resume_ids(
    expected_desc_ids: Vec<Ulid>,
    mut fetch_page: impl FnMut(Option<&str>) -> (Vec<Ulid>, Option<String>),
) -> (Vec<Ulid>, Vec<Ulid>) {
    let mut resumed_desc_ids = Vec::new();
    let mut cursor_token = None::<String>;
    loop {
        let (page_ids, next_cursor) = fetch_page(cursor_token.as_deref());
        resumed_desc_ids.extend(page_ids);

        match next_cursor {
            Some(token) => {
                cursor_token = Some(token);
            }
            None => {
                break;
            }
        }
    }

    (resumed_desc_ids, expected_desc_ids)
}

fn run_desc_cursor_resume_simple_case() -> (Vec<Ulid>, Vec<Ulid>) {
    seed_simple_entities(&[9971, 9972, 9973, 9974, 9975, 9976, 9977, 9978, 9979, 9980]);
    let session = DbSession::new(DB);
    let expected_desc_ids: Vec<Ulid> = collect_ulid_keys!(
        session
            .load::<SimpleEntity>()
            .order_by_desc("id")
            .execute()
            .expect("unbounded DESC execute should succeed")
    );

    collect_desc_cursor_resume_ids(expected_desc_ids, |cursor_token| {
        let mut paged_query = session.load::<SimpleEntity>().order_by_desc("id").limit(3);
        if let Some(token) = cursor_token {
            paged_query = paged_query.cursor(token);
        }
        paged_ids_and_cursor!(
            paged_query
                .execute_paged()
                .expect("paged DESC execute should succeed")
        )
    })
}

fn run_desc_cursor_resume_secondary_index_case() -> (Vec<Ulid>, Vec<Ulid>) {
    seed_pushdown_entities(&[
        (9981, 7, 40),
        (9982, 7, 30),
        (9983, 7, 30),
        (9984, 7, 20),
        (9985, 7, 20),
        (9986, 7, 10),
        (9987, 8, 50),
    ]);
    let session = DbSession::new(DB);
    let group_seven = u32_eq_predicate("group", 7);
    let expected_desc_ids: Vec<Ulid> = collect_ulid_keys!(
        session
            .load::<PushdownParityEntity>()
            .filter(group_seven.clone())
            .order_by_desc("rank")
            .order_by_desc("id")
            .execute()
            .expect("unbounded DESC secondary-index execute should succeed")
    );

    collect_desc_cursor_resume_ids(expected_desc_ids, |cursor_token| {
        let mut paged_query = session
            .load::<PushdownParityEntity>()
            .filter(group_seven.clone())
            .order_by_desc("rank")
            .order_by_desc("id")
            .limit(2);
        if let Some(token) = cursor_token {
            paged_query = paged_query.cursor(token);
        }
        paged_ids_and_cursor!(
            paged_query
                .execute_paged()
                .expect("paged DESC secondary-index execute should succeed")
        )
    })
}

fn run_desc_cursor_resume_index_range_case() -> (Vec<Ulid>, Vec<Ulid>) {
    seed_unique_index_range_entities(&[
        (9991, 200),
        (9992, 201),
        (9993, 202),
        (9994, 203),
        (9995, 204),
        (9996, 205),
    ]);
    let session = DbSession::new(DB);
    let range_predicate = u32_range_predicate("code", 201, 206);
    let expected_desc_ids: Vec<Ulid> = collect_ulid_keys!(
        session
            .load::<UniqueIndexRangeEntity>()
            .filter(range_predicate.clone())
            .order_by_desc("code")
            .order_by_desc("id")
            .execute()
            .expect("unbounded DESC index-range execute should succeed")
    );

    collect_desc_cursor_resume_ids(expected_desc_ids, |cursor_token| {
        let mut paged_query = session
            .load::<UniqueIndexRangeEntity>()
            .filter(range_predicate.clone())
            .order_by_desc("code")
            .order_by_desc("id")
            .limit(2);
        if let Some(token) = cursor_token {
            paged_query = paged_query.cursor(token);
        }
        paged_ids_and_cursor!(
            paged_query
                .execute_paged()
                .expect("paged DESC index-range execute should succeed")
        )
    })
}

fn desc_cursor_resume_cases() -> [DescCursorResumeCase; 3] {
    [
        DescCursorResumeCase {
            label: "simple_desc_cursor_resume",
            run: run_desc_cursor_resume_simple_case,
            assert_strict_descending: true,
        },
        DescCursorResumeCase {
            label: "secondary_index_desc_cursor_resume",
            run: run_desc_cursor_resume_secondary_index_case,
            assert_strict_descending: false,
        },
        DescCursorResumeCase {
            label: "index_range_desc_cursor_resume",
            run: run_desc_cursor_resume_index_range_case,
            assert_strict_descending: false,
        },
    ]
}

#[test]
fn desc_cursor_resume_matrix_matches_unbounded_execution() {
    for case in desc_cursor_resume_cases() {
        let (resumed_desc_ids, expected_desc_ids) = (case.run)();
        assert_eq!(
            resumed_desc_ids, expected_desc_ids,
            "DESC cursor resume matrix mismatch for case={}",
            case.label
        );

        if case.assert_strict_descending {
            assert!(
                resumed_desc_ids
                    .windows(2)
                    .all(|window| window[0] > window[1]),
                "DESC cursor resume sequence should stay strictly descending without duplicates for case={}",
                case.label
            );
        }
    }
}
