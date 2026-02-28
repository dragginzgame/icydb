use super::*;

///
/// RankedDirectionCase
///
/// One ranked direction-invariance matrix row for session terminal behavior.
/// Each row binds one terminal runner and its behavior-matrix coordinates.
///

struct RankedDirectionCase {
    label: &'static str,
    cell: RankedDirectionBehaviorCell,
    run: fn(&DbSession<TestCanister>, OrderDirection) -> RankedDirectionResult,
}

fn run_session_top_k_by_direction(
    session: &DbSession<TestCanister>,
    direction: OrderDirection,
) -> RankedDirectionResult {
    let query = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7));
    let query = match direction {
        OrderDirection::Asc => query.order_by("id"),
        OrderDirection::Desc => query.order_by_desc("id"),
    };
    let ranked = query
        .top_k_by("rank", 3)
        .expect("session top_k_by(rank, 3) direction matrix query should succeed");

    RankedDirectionResult::Ids(ranked.ids())
}

fn run_session_bottom_k_by_direction(
    session: &DbSession<TestCanister>,
    direction: OrderDirection,
) -> RankedDirectionResult {
    let query = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7));
    let query = match direction {
        OrderDirection::Asc => query.order_by("id"),
        OrderDirection::Desc => query.order_by_desc("id"),
    };
    let ranked = query
        .bottom_k_by("rank", 3)
        .expect("session bottom_k_by(rank, 3) direction matrix query should succeed");

    RankedDirectionResult::Ids(ranked.ids())
}

fn run_session_top_k_by_values_direction(
    session: &DbSession<TestCanister>,
    direction: OrderDirection,
) -> RankedDirectionResult {
    let query = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7));
    let query = match direction {
        OrderDirection::Asc => query.order_by("id"),
        OrderDirection::Desc => query.order_by_desc("id"),
    };
    let ranked = query
        .top_k_by_values("rank", 3)
        .expect("session top_k_by_values(rank, 3) direction matrix query should succeed");

    RankedDirectionResult::Values(ranked)
}

fn run_session_bottom_k_by_values_direction(
    session: &DbSession<TestCanister>,
    direction: OrderDirection,
) -> RankedDirectionResult {
    let query = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7));
    let query = match direction {
        OrderDirection::Asc => query.order_by("id"),
        OrderDirection::Desc => query.order_by_desc("id"),
    };
    let ranked = query
        .bottom_k_by_values("rank", 3)
        .expect("session bottom_k_by_values(rank, 3) direction matrix query should succeed");

    RankedDirectionResult::Values(ranked)
}

fn run_session_top_k_by_with_ids_direction(
    session: &DbSession<TestCanister>,
    direction: OrderDirection,
) -> RankedDirectionResult {
    let query = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7));
    let query = match direction {
        OrderDirection::Asc => query.order_by("id"),
        OrderDirection::Desc => query.order_by_desc("id"),
    };
    let ranked = query
        .top_k_by_with_ids("rank", 3)
        .expect("session top_k_by_with_ids(rank, 3) direction matrix query should succeed");

    RankedDirectionResult::ValuesWithIds(ranked)
}

fn run_session_bottom_k_by_with_ids_direction(
    session: &DbSession<TestCanister>,
    direction: OrderDirection,
) -> RankedDirectionResult {
    let query = session
        .load::<PushdownParityEntity>()
        .filter(u32_eq_predicate("group", 7));
    let query = match direction {
        OrderDirection::Asc => query.order_by("id"),
        OrderDirection::Desc => query.order_by_desc("id"),
    };
    let ranked = query
        .bottom_k_by_with_ids("rank", 3)
        .expect("session bottom_k_by_with_ids(rank, 3) direction matrix query should succeed");

    RankedDirectionResult::ValuesWithIds(ranked)
}

fn session_ranked_direction_cases() -> [RankedDirectionCase; 6] {
    [
        RankedDirectionCase {
            label: "top_k_by",
            cell: RankedDirectionBehaviorCell {
                capability: "ranked_ids",
                path: "base_order_asc_desc",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            run: run_session_top_k_by_direction,
        },
        RankedDirectionCase {
            label: "bottom_k_by",
            cell: RankedDirectionBehaviorCell {
                capability: "ranked_ids",
                path: "base_order_asc_desc",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            run: run_session_bottom_k_by_direction,
        },
        RankedDirectionCase {
            label: "top_k_by_values",
            cell: RankedDirectionBehaviorCell {
                capability: "ranked_values",
                path: "base_order_asc_desc",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            run: run_session_top_k_by_values_direction,
        },
        RankedDirectionCase {
            label: "bottom_k_by_values",
            cell: RankedDirectionBehaviorCell {
                capability: "ranked_values",
                path: "base_order_asc_desc",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            run: run_session_bottom_k_by_values_direction,
        },
        RankedDirectionCase {
            label: "top_k_by_with_ids",
            cell: RankedDirectionBehaviorCell {
                capability: "ranked_values_with_ids",
                path: "base_order_asc_desc",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            run: run_session_top_k_by_with_ids_direction,
        },
        RankedDirectionCase {
            label: "bottom_k_by_with_ids",
            cell: RankedDirectionBehaviorCell {
                capability: "ranked_values_with_ids",
                path: "base_order_asc_desc",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            run: run_session_bottom_k_by_with_ids_direction,
        },
    ]
}

#[test]
fn session_load_ranked_terminals_direction_invariance_matrix() {
    seed_pushdown_entities(&[
        (8_3711, 7, 10),
        (8_3712, 7, 40),
        (8_3713, 7, 20),
        (8_3714, 7, 30),
        (8_3715, 7, 40),
        (8_3716, 8, 99),
    ]);
    let session = DbSession::new(DB);

    // Phase 1: execute every ranked terminal matrix row in ASC and DESC base-order modes.
    for case in session_ranked_direction_cases() {
        let asc = (case.run)(&session, OrderDirection::Asc);
        let desc = (case.run)(&session, OrderDirection::Desc);
        assert_eq!(
            asc, desc,
            "ranked direction invariance failed for terminal={} cell={:?}",
            case.label, case.cell
        );
    }
}

#[test]
fn session_load_ranked_terminals_direction_invariance_matrix_covers_all_rank_terminals() {
    let labels = session_ranked_direction_cases().map(|case| case.label);
    assert_eq!(
        labels,
        [
            "top_k_by",
            "bottom_k_by",
            "top_k_by_values",
            "bottom_k_by_values",
            "top_k_by_with_ids",
            "bottom_k_by_with_ids",
        ],
        "ranked direction matrix must enumerate all rank terminal projection forms"
    );
}

#[test]
fn aggregate_field_target_top_k_by_direction_invariance_across_forced_access_shapes() {
    // Phase 1: force a full-scan shape and assert ASC/DESC base-order invariance.
    seed_simple_entities(&[8_3941, 8_3942, 8_3943, 8_3944, 8_3945, 8_3946]);
    let simple_load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let full_scan_top_ids_for = |direction: OrderDirection| {
        let query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore);
        let query = match direction {
            OrderDirection::Asc => query.order_by("id"),
            OrderDirection::Desc => query.order_by_desc("id"),
        };
        let plan = query
            .plan()
            .expect("top_k_by full-scan direction-invariance plan should build");
        assert!(
            matches!(plan.explain().access, ExplainAccessPath::FullScan),
            "top_k_by full-scan direction invariance test must force FullScan"
        );

        simple_load
            .top_k_by(plan, "id", 3)
            .expect("top_k_by(id, 3) should succeed for full-scan direction matrix")
            .ids()
    };
    let full_scan_asc = full_scan_top_ids_for(OrderDirection::Asc);
    let full_scan_desc = full_scan_top_ids_for(OrderDirection::Desc);
    assert_eq!(
        full_scan_asc, full_scan_desc,
        "top_k_by(id, k) should be invariant to ASC/DESC base order under forced FullScan"
    );

    // Phase 2: force an index-range shape and assert ASC/DESC base-order invariance.
    seed_unique_index_range_entities(&[
        (8_3951, 100),
        (8_3952, 101),
        (8_3953, 102),
        (8_3954, 103),
        (8_3955, 104),
        (8_3956, 105),
    ]);
    let range_load = LoadExecutor::<UniqueIndexRangeEntity>::new(DB, false);
    let code_range = u32_range_predicate("code", 101, 106);
    let index_range_top_ids_for = |direction: OrderDirection| {
        let query = Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
            .filter(code_range.clone());
        let query = match direction {
            OrderDirection::Asc => query.order_by("code"),
            OrderDirection::Desc => query.order_by_desc("code"),
        };
        let plan = query
            .plan()
            .expect("top_k_by index-range direction-invariance plan should build");
        assert!(
            matches!(plan.explain().access, ExplainAccessPath::IndexRange { .. }),
            "top_k_by index-range direction invariance test must force IndexRange"
        );

        range_load
            .top_k_by(plan, "code", 3)
            .expect("top_k_by(code, 3) should succeed for index-range direction matrix")
            .ids()
    };
    let index_range_asc = index_range_top_ids_for(OrderDirection::Asc);
    let index_range_desc = index_range_top_ids_for(OrderDirection::Desc);
    assert_eq!(
        index_range_asc, index_range_desc,
        "top_k_by(code, k) should be invariant to ASC/DESC base order under forced IndexRange"
    );
}

#[test]
fn session_load_ranked_rows_are_invariant_to_insertion_order() {
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
    let ranked_ids_for = |rows: &[(u128, u32, u32)]| {
        seed_pushdown_entities(rows);
        let session = DbSession::new(DB);
        let top_ids = session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by("id")
            .top_k_by("rank", 3)
            .expect("top_k_by(rank, 3) insertion-order invariance query should succeed")
            .ids();
        let bottom_ids = session
            .load::<PushdownParityEntity>()
            .filter(u32_eq_predicate("group", 7))
            .order_by("id")
            .bottom_k_by("rank", 3)
            .expect("bottom_k_by(rank, 3) insertion-order invariance query should succeed")
            .ids();

        (top_ids, bottom_ids)
    };

    let (top_a, bottom_a) = ranked_ids_for(&rows_a);
    let (top_b, bottom_b) = ranked_ids_for(&rows_b);

    assert_eq!(
        top_a, top_b,
        "top_k_by(rank, k) should be invariant to seed insertion order for equivalent rows"
    );
    assert_eq!(
        bottom_a, bottom_b,
        "bottom_k_by(rank, k) should be invariant to seed insertion order for equivalent rows"
    );
}

///
/// RankedKOneTerminal
///
/// Declares which extrema terminal (`MIN`/`MAX`) anchors one k=1 parity row.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RankedKOneTerminal {
    Top,
    Bottom,
}

///
/// RankedKOneProjection
///
/// Declares the output projection shape for one k=1 ranked terminal row.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RankedKOneProjection {
    Ids,
    Values,
    ValuesWithIds,
}

///
/// RankedKOneCase
///
/// One matrix row for k=1 ranked-terminal parity against extrema terminals.
/// Encodes seed fixture, projection shape, and deterministic tie-break contracts.
///

struct RankedKOneCase {
    label: &'static str,
    cell: RankedDirectionBehaviorCell,
    rows: &'static [(u128, u32, u32)],
    terminal: RankedKOneTerminal,
    projection: RankedKOneProjection,
    expected_first_id_tie_break: Option<u128>,
}

const RANKED_K_ONE_TOP_IDS_ROWS: [(u128, u32, u32); 5] = [
    (8_3741, 7, 90),
    (8_3742, 7, 40),
    (8_3743, 7, 90),
    (8_3744, 7, 20),
    (8_3745, 8, 99),
];
const RANKED_K_ONE_TOP_VALUES_ROWS: [(u128, u32, u32); 5] = [
    (8_3811, 7, 90),
    (8_3812, 7, 40),
    (8_3813, 7, 90),
    (8_3814, 7, 20),
    (8_3815, 8, 99),
];
const RANKED_K_ONE_BOTTOM_IDS_ROWS: [(u128, u32, u32); 5] = [
    (8_3751, 7, 10),
    (8_3752, 7, 30),
    (8_3753, 7, 10),
    (8_3754, 7, 40),
    (8_3755, 8, 99),
];
const RANKED_K_ONE_BOTTOM_VALUES_ROWS: [(u128, u32, u32); 5] = [
    (8_3821, 7, 10),
    (8_3822, 7, 30),
    (8_3823, 7, 10),
    (8_3824, 7, 40),
    (8_3825, 8, 99),
];
const RANKED_K_ONE_TOP_VALUES_WITH_IDS_ROWS: [(u128, u32, u32); 5] = [
    (8_3831, 7, 90),
    (8_3832, 7, 40),
    (8_3833, 7, 90),
    (8_3834, 7, 20),
    (8_3835, 8, 99),
];
const RANKED_K_ONE_BOTTOM_VALUES_WITH_IDS_ROWS: [(u128, u32, u32); 5] = [
    (8_3836, 7, 10),
    (8_3837, 7, 30),
    (8_3838, 7, 10),
    (8_3839, 7, 40),
    (8_3840, 8, 99),
];

fn run_ranked_k_one_terminal(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
    terminal: RankedKOneTerminal,
    projection: RankedKOneProjection,
) -> Result<RankedDirectionResult, InternalError> {
    match (terminal, projection) {
        (RankedKOneTerminal::Top, RankedKOneProjection::Ids) => Ok(RankedDirectionResult::Ids(
            load.top_k_by(plan, "rank", 1)?.ids(),
        )),
        (RankedKOneTerminal::Top, RankedKOneProjection::Values) => Ok(
            RankedDirectionResult::Values(load.top_k_by_values(plan, "rank", 1)?),
        ),
        (RankedKOneTerminal::Top, RankedKOneProjection::ValuesWithIds) => Ok(
            RankedDirectionResult::ValuesWithIds(load.top_k_by_with_ids(plan, "rank", 1)?),
        ),
        (RankedKOneTerminal::Bottom, RankedKOneProjection::Ids) => Ok(RankedDirectionResult::Ids(
            load.bottom_k_by(plan, "rank", 1)?.ids(),
        )),
        (RankedKOneTerminal::Bottom, RankedKOneProjection::Values) => Ok(
            RankedDirectionResult::Values(load.bottom_k_by_values(plan, "rank", 1)?),
        ),
        (RankedKOneTerminal::Bottom, RankedKOneProjection::ValuesWithIds) => Ok(
            RankedDirectionResult::ValuesWithIds(load.bottom_k_by_with_ids(plan, "rank", 1)?),
        ),
    }
}

fn run_ranked_k_one_extrema(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
    terminal: RankedKOneTerminal,
) -> Result<Option<Id<PushdownParityEntity>>, InternalError> {
    match terminal {
        RankedKOneTerminal::Top => load.aggregate_max_by(plan, "rank"),
        RankedKOneTerminal::Bottom => load.aggregate_min_by(plan, "rank"),
    }
}

fn ranked_k_one_projection_from_extrema(
    load: &LoadExecutor<PushdownParityEntity>,
    plan: ExecutablePlan<PushdownParityEntity>,
    extrema_id: Option<Id<PushdownParityEntity>>,
    projection: RankedKOneProjection,
) -> Result<RankedDirectionResult, InternalError> {
    match projection {
        RankedKOneProjection::Ids => {
            Ok(RankedDirectionResult::Ids(extrema_id.into_iter().collect()))
        }
        RankedKOneProjection::Values => {
            let projected = if let Some(target_id) = extrema_id {
                load.execute(plan)?
                    .0
                    .into_iter()
                    .find(|(id, _)| *id == target_id)
                    .map(|(_, entity)| Value::Uint(u64::from(entity.rank)))
                    .into_iter()
                    .collect()
            } else {
                Vec::new()
            };
            Ok(RankedDirectionResult::Values(projected))
        }
        RankedKOneProjection::ValuesWithIds => {
            let projected = if let Some(target_id) = extrema_id {
                load.execute(plan)?
                    .0
                    .into_iter()
                    .find(|(id, _)| *id == target_id)
                    .map(|(_, entity)| (target_id, Value::Uint(u64::from(entity.rank))))
                    .into_iter()
                    .collect()
            } else {
                Vec::new()
            };
            Ok(RankedDirectionResult::ValuesWithIds(projected))
        }
    }
}

fn first_ranked_result_id(result: &RankedDirectionResult) -> Option<Id<PushdownParityEntity>> {
    match result {
        RankedDirectionResult::Ids(ids) => ids.first().copied(),
        RankedDirectionResult::Values(_) => None,
        RankedDirectionResult::ValuesWithIds(values_with_ids) => {
            values_with_ids.first().map(|(id, _)| *id)
        }
    }
}

fn ranked_k_one_cases() -> [RankedKOneCase; 6] {
    [
        RankedKOneCase {
            label: "top_k_by_ids",
            cell: RankedDirectionBehaviorCell {
                capability: "ranked_ids",
                path: "k_one_extrema_equivalence",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &RANKED_K_ONE_TOP_IDS_ROWS,
            terminal: RankedKOneTerminal::Top,
            projection: RankedKOneProjection::Ids,
            expected_first_id_tie_break: Some(8_3741),
        },
        RankedKOneCase {
            label: "top_k_by_values",
            cell: RankedDirectionBehaviorCell {
                capability: "ranked_values",
                path: "k_one_extrema_equivalence",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &RANKED_K_ONE_TOP_VALUES_ROWS,
            terminal: RankedKOneTerminal::Top,
            projection: RankedKOneProjection::Values,
            expected_first_id_tie_break: None,
        },
        RankedKOneCase {
            label: "bottom_k_by_ids",
            cell: RankedDirectionBehaviorCell {
                capability: "ranked_ids",
                path: "k_one_extrema_equivalence",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &RANKED_K_ONE_BOTTOM_IDS_ROWS,
            terminal: RankedKOneTerminal::Bottom,
            projection: RankedKOneProjection::Ids,
            expected_first_id_tie_break: Some(8_3751),
        },
        RankedKOneCase {
            label: "bottom_k_by_values",
            cell: RankedDirectionBehaviorCell {
                capability: "ranked_values",
                path: "k_one_extrema_equivalence",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &RANKED_K_ONE_BOTTOM_VALUES_ROWS,
            terminal: RankedKOneTerminal::Bottom,
            projection: RankedKOneProjection::Values,
            expected_first_id_tie_break: None,
        },
        RankedKOneCase {
            label: "top_k_by_with_ids",
            cell: RankedDirectionBehaviorCell {
                capability: "ranked_values_with_ids",
                path: "k_one_extrema_equivalence",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &RANKED_K_ONE_TOP_VALUES_WITH_IDS_ROWS,
            terminal: RankedKOneTerminal::Top,
            projection: RankedKOneProjection::ValuesWithIds,
            expected_first_id_tie_break: None,
        },
        RankedKOneCase {
            label: "bottom_k_by_with_ids",
            cell: RankedDirectionBehaviorCell {
                capability: "ranked_values_with_ids",
                path: "k_one_extrema_equivalence",
                nullability: "non_nullable_rank",
                grouping: "ungrouped",
            },
            rows: &RANKED_K_ONE_BOTTOM_VALUES_WITH_IDS_ROWS,
            terminal: RankedKOneTerminal::Bottom,
            projection: RankedKOneProjection::ValuesWithIds,
            expected_first_id_tie_break: None,
        },
    ]
}

#[test]
fn aggregate_field_target_rank_k_one_extrema_equivalence_matrix() {
    for case in ranked_k_one_cases() {
        seed_pushdown_entities(case.rows);
        let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
        let build_plan = || {
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("id")
                .limit(4)
                .plan()
                .expect("ranked k-one equivalence matrix plan should build")
        };

        // Phase 1: execute ranked terminal and extrema anchor while capturing scan budgets.
        let (actual, scanned_terminal) =
            capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
                run_ranked_k_one_terminal(&load, build_plan(), case.terminal, case.projection)
                    .expect("ranked k-one terminal matrix execution should succeed")
            });
        let (extrema_id, scanned_extrema) =
            capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
                run_ranked_k_one_extrema(&load, build_plan(), case.terminal)
                    .expect("ranked k-one extrema matrix execution should succeed")
            });

        // Phase 2: project extrema output into terminal shape and assert parity.
        let expected =
            ranked_k_one_projection_from_extrema(&load, build_plan(), extrema_id, case.projection)
                .expect("ranked k-one expected projection should succeed");
        assert_eq!(
            actual, expected,
            "ranked k-one extrema equivalence failed for case={} cell={:?}",
            case.label, case.cell
        );

        // Phase 3: assert deterministic tie-break and scan-budget dominance contracts.
        if let Some(expected_first_id_tie_break) = case.expected_first_id_tie_break {
            assert_eq!(
                first_ranked_result_id(&actual).map(|id| id.key()),
                Some(Ulid::from_u128(expected_first_id_tie_break)),
                "ranked k-one tie-break contract failed for case={}",
                case.label
            );
        }
        assert!(
            scanned_terminal >= scanned_extrema,
            "ranked k-one terminal scan budget must dominate extrema for case={} cell={:?}",
            case.label,
            case.cell
        );
    }
}

#[test]
fn aggregate_field_target_rank_k_one_extrema_equivalence_matrix_covers_all_projection_forms() {
    let labels = ranked_k_one_cases().map(|case| case.label);
    assert_eq!(
        labels,
        [
            "top_k_by_ids",
            "top_k_by_values",
            "bottom_k_by_ids",
            "bottom_k_by_values",
            "top_k_by_with_ids",
            "bottom_k_by_with_ids",
        ],
        "ranked k-one extrema equivalence matrix must enumerate all rank projection forms"
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn aggregate_field_target_take_and_rank_terminals_k_zero_return_empty_with_execute_scan_parity() {
    seed_pushdown_entities(&[
        (8_3761, 7, 10),
        (8_3762, 7, 20),
        (8_3763, 7, 30),
        (8_3764, 7, 40),
        (8_3765, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(3)
            .plan()
            .expect("k-zero terminal plan should build")
    };

    let (_, scanned_execute) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.execute(build_plan())
            .expect("execute baseline for k-zero terminal parity should succeed")
    });
    let (take_zero, scanned_take_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.take(build_plan(), 0)
                .expect("take(0) should succeed and return an empty response")
        });
    let (top_k_zero, scanned_top_k_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by(build_plan(), "rank", 0)
                .expect("top_k_by(rank, 0) should succeed and return an empty response")
        });
    let (bottom_k_zero, scanned_bottom_k_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by(build_plan(), "rank", 0)
                .expect("bottom_k_by(rank, 0) should succeed and return an empty response")
        });
    let (top_k_values_zero, scanned_top_k_values_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_values(build_plan(), "rank", 0)
                .expect("top_k_by_values(rank, 0) should succeed and return an empty response")
        });
    let (bottom_k_values_zero, scanned_bottom_k_values_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_values(build_plan(), "rank", 0)
                .expect("bottom_k_by_values(rank, 0) should succeed and return an empty response")
        });
    let (top_k_with_ids_zero, scanned_top_k_with_ids_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_with_ids(build_plan(), "rank", 0)
                .expect("top_k_by_with_ids(rank, 0) should succeed and return an empty response")
        });
    let (bottom_k_with_ids_zero, scanned_bottom_k_with_ids_zero) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_with_ids(build_plan(), "rank", 0)
                .expect("bottom_k_by_with_ids(rank, 0) should succeed and return an empty response")
        });

    assert!(
        take_zero.is_empty(),
        "take(0) should return an empty response"
    );
    assert!(
        top_k_zero.is_empty(),
        "top_k_by(rank, 0) should return an empty response"
    );
    assert!(
        bottom_k_zero.is_empty(),
        "bottom_k_by(rank, 0) should return an empty response"
    );
    assert!(
        top_k_values_zero.is_empty(),
        "top_k_by_values(rank, 0) should return an empty response"
    );
    assert!(
        bottom_k_values_zero.is_empty(),
        "bottom_k_by_values(rank, 0) should return an empty response"
    );
    assert!(
        top_k_with_ids_zero.is_empty(),
        "top_k_by_with_ids(rank, 0) should return an empty response"
    );
    assert!(
        bottom_k_with_ids_zero.is_empty(),
        "bottom_k_by_with_ids(rank, 0) should return an empty response"
    );
    assert_eq!(
        scanned_take_zero, scanned_execute,
        "take(0) should preserve execute() scan-budget consumption before truncation"
    );
    assert_eq!(
        scanned_top_k_zero, scanned_execute,
        "top_k_by(rank, 0) should preserve execute() scan-budget consumption before truncation"
    );
    assert_eq!(
        scanned_bottom_k_zero, scanned_execute,
        "bottom_k_by(rank, 0) should preserve execute() scan-budget consumption before truncation"
    );
    assert_eq!(
        scanned_top_k_values_zero, scanned_execute,
        "top_k_by_values(rank, 0) should preserve execute() scan-budget consumption before truncation"
    );
    assert_eq!(
        scanned_bottom_k_values_zero, scanned_execute,
        "bottom_k_by_values(rank, 0) should preserve execute() scan-budget consumption before truncation"
    );
    assert_eq!(
        scanned_top_k_with_ids_zero, scanned_execute,
        "top_k_by_with_ids(rank, 0) should preserve execute() scan-budget consumption before truncation"
    );
    assert_eq!(
        scanned_bottom_k_with_ids_zero, scanned_execute,
        "bottom_k_by_with_ids(rank, 0) should preserve execute() scan-budget consumption before truncation"
    );
}
