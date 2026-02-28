use super::*;

#[test]
fn aggregate_parity_ordered_page_window_asc() {
    seed_simple_entities(&[8101, 8102, 8103, 8104, 8105, 8106, 8107, 8108]);
    let load = LoadExecutor::<SimpleEntity>::new(DB, false);

    assert_aggregate_parity_for_query(
        &load,
        || {
            Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .offset(2)
                .limit(3)
        },
        "ordered ASC page window",
    );
}

#[test]
fn aggregate_parity_matrix_harness_covers_all_id_terminals() {
    let labels = aggregate_id_terminal_parity_cases::<SimpleEntity>().map(|case| case.label);

    assert_eq!(labels, ["count", "exists", "min", "max", "first", "last"]);
}

#[test]
fn aggregate_spec_field_target_non_extrema_surfaces_unsupported_taxonomy() {
    seed_pushdown_entities(&[(8_021, 7, 10), (8_022, 7, 20), (8_023, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("field-target non-extrema aggregate plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        ExecutionKernel::execute_aggregate_spec(
            &load,
            plan,
            AggregateSpec::for_target_field(AggregateKind::Count, "rank"),
        )
    });
    let Err(err) = result else {
        panic!("field-target COUNT should be rejected by unsupported taxonomy");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "unsupported field-target COUNT should fail before any scan-budget consumption"
    );
    assert!(
        err.message.contains("only supported for min/max terminals"),
        "field-target non-extrema taxonomy should be explicit: {err:?}"
    );
}

#[test]
fn aggregate_spec_field_target_extrema_selects_deterministic_ids() {
    seed_pushdown_entities(&[
        (8_031, 7, 20),
        (8_032, 7, 10),
        (8_033, 7, 10),
        (8_034, 7, 30),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("field-target extrema aggregate plan should build")
    };

    let (min_id, scanned_min) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_min_by(build_plan(), "rank")
            .expect("field-target MIN should execute")
    });
    let (max_id, scanned_max) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_max_by(build_plan(), "rank")
            .expect("field-target MAX should execute")
    });

    assert_eq!(
        min_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_032)),
        "field-target MIN should select the smallest field value with pk-asc tie-break"
    );
    assert_eq!(
        max_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_034)),
        "field-target MAX should select the largest field value"
    );
    assert!(
        scanned_min > 0 && scanned_max > 0,
        "field-target extrema execution should consume scan budget once supported"
    );
}

#[test]
fn aggregate_spec_field_target_unknown_field_surfaces_unsupported_without_scan() {
    seed_pushdown_entities(&[(8_041, 7, 10), (8_042, 7, 20), (8_043, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("field-target unknown-field aggregate plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        ExecutionKernel::execute_aggregate_spec(
            &load,
            plan,
            AggregateSpec::for_target_field(AggregateKind::Min, "missing_field"),
        )
    });
    let Err(err) = result else {
        panic!("field-target unknown field should be rejected until the 0.25 capability ships");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "field-target unknown-field MIN should fail before any scan-budget consumption"
    );
    assert!(
        err.message.contains("unknown aggregate target field"),
        "unknown field taxonomy should remain explicit: {err:?}"
    );
}

#[test]
fn aggregate_spec_field_target_non_orderable_field_surfaces_unsupported_without_scan() {
    seed_phase_entities(&[(8_051, 10), (8_052, 20), (8_053, 30)]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("field-target non-orderable aggregate plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
        ExecutionKernel::execute_aggregate_spec(
            &load,
            plan,
            AggregateSpec::for_target_field(AggregateKind::Min, "tags"),
        )
    });
    let Err(err) = result else {
        panic!("field-target MIN on list field should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "field-target non-orderable MIN should fail before any scan-budget consumption"
    );
    assert!(
        err.message.contains("does not support ordering"),
        "non-orderable field taxonomy should remain explicit: {err:?}"
    );
}

#[test]
fn aggregate_spec_field_target_tie_breaks_on_primary_key_ascending() {
    seed_pushdown_entities(&[
        (8_061, 7, 10),
        (8_062, 7, 10),
        (8_063, 7, 20),
        (8_064, 7, 20),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let min_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("field-target MIN tie-break plan should build");
    let max_plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("field-target MAX tie-break plan should build");

    let min_id = load
        .aggregate_min_by(min_plan, "rank")
        .expect("field-target MIN tie-break should succeed");
    let max_id = load
        .aggregate_max_by(max_plan, "rank")
        .expect("field-target MAX tie-break should succeed");

    assert_eq!(
        min_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_061)),
        "field-target MIN tie-break should pick primary key ascending when values tie"
    );
    assert_eq!(
        max_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_063)),
        "field-target MAX tie-break should pick primary key ascending when values tie"
    );
}

#[test]
fn aggregate_field_target_secondary_index_min_uses_index_leading_order() {
    seed_pushdown_entities(&[
        (8_071, 7, 30),
        (8_072, 7, 10),
        (8_073, 7, 20),
        (8_074, 8, 5),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = secondary_group_rank_order_plan(
        MissingRowPolicy::Ignore,
        crate::db::query::plan::OrderDirection::Asc,
        0,
    );

    let min_id = load
        .aggregate_min_by(plan, "rank")
        .expect("secondary-index field-target MIN should succeed");

    assert_eq!(
        min_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_072)),
        "secondary-index field-target MIN should return the lowest rank id"
    );
}

#[test]
fn aggregate_field_target_secondary_index_max_tie_breaks_primary_key_ascending() {
    seed_pushdown_entities(&[
        (8_081, 7, 20),
        (8_082, 7, 40),
        (8_083, 7, 40),
        (8_084, 7, 10),
        (8_085, 8, 50),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = secondary_group_rank_order_plan(
        MissingRowPolicy::Ignore,
        crate::db::query::plan::OrderDirection::Desc,
        0,
    );

    let max_id = load
        .aggregate_max_by(plan, "rank")
        .expect("secondary-index field-target MAX should succeed");

    assert_eq!(
        max_id.map(|id| id.key()),
        Some(Ulid::from_u128(8_082)),
        "secondary-index field-target MAX should pick primary key ascending within max-value ties"
    );
}

#[test]
fn aggregate_field_target_nth_selects_deterministic_position() {
    seed_pushdown_entities(&[
        (8_142, 7, 10),
        (8_141, 7, 10),
        (8_144, 7, 30),
        (8_143, 7, 20),
        (8_145, 8, 5),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("field-target nth plan should build")
    };

    let nth_0 = load
        .aggregate_nth_by(build_plan(), "rank", 0)
        .expect("nth_by(rank, 0) should succeed");
    let nth_1 = load
        .aggregate_nth_by(build_plan(), "rank", 1)
        .expect("nth_by(rank, 1) should succeed");
    let nth_2 = load
        .aggregate_nth_by(build_plan(), "rank", 2)
        .expect("nth_by(rank, 2) should succeed");
    let nth_3 = load
        .aggregate_nth_by(build_plan(), "rank", 3)
        .expect("nth_by(rank, 3) should succeed");
    let nth_4 = load
        .aggregate_nth_by(build_plan(), "rank", 4)
        .expect("nth_by(rank, 4) should succeed");

    assert_eq!(
        nth_0.map(|id| id.key()),
        Some(Ulid::from_u128(8_141)),
        "nth_by(rank, 0) should select the smallest rank with pk-asc tie-break"
    );
    assert_eq!(
        nth_1.map(|id| id.key()),
        Some(Ulid::from_u128(8_142)),
        "nth_by(rank, 1) should advance through equal-rank ties using pk-asc order"
    );
    assert_eq!(
        nth_2.map(|id| id.key()),
        Some(Ulid::from_u128(8_143)),
        "nth_by(rank, 2) should select the next field-ordered candidate"
    );
    assert_eq!(
        nth_3.map(|id| id.key()),
        Some(Ulid::from_u128(8_144)),
        "nth_by(rank, 3) should select the highest rank in-window candidate"
    );
    assert_eq!(
        nth_4, None,
        "nth_by(rank, 4) should return None when ordinal is outside the result window"
    );
}

#[test]
fn aggregate_field_target_nth_unknown_field_fails_without_scan() {
    seed_pushdown_entities(&[(8_151, 7, 10), (8_152, 7, 20)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("field-target nth unknown-field plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_nth_by(plan, "missing_field", 0)
    });
    let Err(err) = result else {
        panic!("nth_by(missing_field, 0) should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "unknown nth target should fail before scan-budget consumption"
    );
}

#[test]
fn aggregate_field_target_nth_non_orderable_field_fails_without_scan() {
    seed_phase_entities(&[(8_161, 10), (8_162, 20)]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("field-target nth non-orderable plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
        load.aggregate_nth_by(plan, "tags", 0)
    });
    let Err(err) = result else {
        panic!("nth_by(tags, 0) should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "non-orderable nth target should fail before scan-budget consumption"
    );
}

#[test]
fn aggregate_field_target_nth_boundary_matrix_respects_window_and_out_of_range() {
    seed_pushdown_entities(&[
        (8_171, 7, 10),
        (8_172, 7, 10),
        (8_173, 7, 20),
        (8_174, 7, 30),
        (8_175, 7, 40),
        (8_176, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let base_query = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(3)
    };
    let expected_response = load
        .execute(
            base_query()
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("nth boundary baseline plan should build"),
        )
        .expect("nth boundary baseline execute should succeed");
    let expected_len = expected_response.0.len();

    for nth in [0usize, 1, 2, 3, usize::MAX] {
        let actual = load
            .aggregate_nth_by(
                base_query()
                    .plan()
                    .map(crate::db::executor::ExecutablePlan::from)
                    .expect("nth boundary plan should build"),
                "rank",
                nth,
            )
            .expect("nth boundary aggregate should succeed");
        let expected = expected_nth_by_rank_id(&expected_response, nth);

        assert_eq!(actual, expected, "nth boundary parity failed for n={nth}");
    }

    let empty_window_nth_zero = load
        .aggregate_nth_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("id")
                .offset(50)
                .limit(3)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("empty-window nth plan should build"),
            "rank",
            0,
        )
        .expect("empty-window nth should succeed");

    assert_eq!(
        expected_len, 3,
        "baseline window length should lock nth boundary expectations"
    );
    assert_eq!(
        empty_window_nth_zero, None,
        "empty-window nth_by should return None"
    );
}

#[test]
fn aggregate_field_target_median_even_window_uses_lower_policy() {
    seed_pushdown_entities(&[
        (8_181, 7, 10),
        (8_182, 7, 20),
        (8_183, 7, 30),
        (8_184, 7, 40),
        (8_185, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .limit(4)
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("field-target median plan should build")
    };

    let expected_response = load
        .execute(build_plan())
        .expect("field-target median baseline execute should succeed");
    let median = load
        .aggregate_median_by(build_plan(), "rank")
        .expect("median_by(rank) should succeed");

    assert_eq!(
        median,
        expected_median_by_rank_id(&expected_response),
        "median_by(rank) should match deterministic parity projection"
    );
    assert_eq!(
        median.map(|id| id.key()),
        Some(Ulid::from_u128(8_182)),
        "median_by(rank) should use lower-median policy for even-length windows"
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn aggregate_field_target_new_terminals_unknown_field_fail_without_scan() {
    seed_pushdown_entities(&[(8_1981, 7, 10), (8_1982, 7, 20), (8_1983, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("unknown-field terminal plan should build")
    };

    let (median_result, median_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_median_by(build_plan(), "missing_field")
        });
    let Err(median_err) = median_result else {
        panic!("median_by(missing_field) should be rejected");
    };
    assert_eq!(median_err.class, ErrorClass::Unsupported);
    assert_eq!(median_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        median_scanned, 0,
        "median_by unknown-field target should fail before scan-budget consumption"
    );

    let (count_result, count_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_count_distinct_by(build_plan(), "missing_field")
        });
    let Err(count_err) = count_result else {
        panic!("count_distinct_by(missing_field) should be rejected");
    };
    assert_eq!(count_err.class, ErrorClass::Unsupported);
    assert_eq!(count_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        count_scanned, 0,
        "count_distinct_by unknown-field target should fail before scan-budget consumption"
    );

    let (min_max_result, min_max_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.aggregate_min_max_by(build_plan(), "missing_field")
        });
    let Err(min_max_err) = min_max_result else {
        panic!("min_max_by(missing_field) should be rejected");
    };
    assert_eq!(min_max_err.class, ErrorClass::Unsupported);
    assert_eq!(min_max_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        min_max_scanned, 0,
        "min_max_by unknown-field target should fail before scan-budget consumption"
    );

    let (values_result, values_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.values_by(build_plan(), "missing_field")
        });
    let Err(values_err) = values_result else {
        panic!("values_by(missing_field) should be rejected");
    };
    assert_eq!(values_err.class, ErrorClass::Unsupported);
    assert_eq!(values_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        values_scanned, 0,
        "values_by unknown-field target should fail before scan-budget consumption"
    );

    let (distinct_values_result, distinct_values_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.distinct_values_by(build_plan(), "missing_field")
        });
    let Err(distinct_values_err) = distinct_values_result else {
        panic!("distinct_values_by(missing_field) should be rejected");
    };
    assert_eq!(distinct_values_err.class, ErrorClass::Unsupported);
    assert_eq!(distinct_values_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        distinct_values_scanned, 0,
        "distinct_values_by unknown-field target should fail before scan-budget consumption"
    );

    let (values_with_ids_result, values_with_ids_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.values_by_with_ids(build_plan(), "missing_field")
        });
    let Err(values_with_ids_err) = values_with_ids_result else {
        panic!("values_by_with_ids(missing_field) should be rejected");
    };
    assert_eq!(values_with_ids_err.class, ErrorClass::Unsupported);
    assert_eq!(values_with_ids_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        values_with_ids_scanned, 0,
        "values_by_with_ids unknown-field target should fail before scan-budget consumption"
    );

    let (first_value_result, first_value_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.first_value_by(build_plan(), "missing_field")
        });
    let Err(first_value_err) = first_value_result else {
        panic!("first_value_by(missing_field) should be rejected");
    };
    assert_eq!(first_value_err.class, ErrorClass::Unsupported);
    assert_eq!(first_value_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        first_value_scanned, 0,
        "first_value_by unknown-field target should fail before scan-budget consumption"
    );

    let (last_value_result, last_value_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.last_value_by(build_plan(), "missing_field")
        });
    let Err(last_value_err) = last_value_result else {
        panic!("last_value_by(missing_field) should be rejected");
    };
    assert_eq!(last_value_err.class, ErrorClass::Unsupported);
    assert_eq!(last_value_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        last_value_scanned, 0,
        "last_value_by unknown-field target should fail before scan-budget consumption"
    );

    let (top_k_result, top_k_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by(build_plan(), "missing_field", 2)
        });
    let Err(top_k_err) = top_k_result else {
        panic!("top_k_by(missing_field, k) should be rejected");
    };
    assert_eq!(top_k_err.class, ErrorClass::Unsupported);
    assert_eq!(top_k_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        top_k_scanned, 0,
        "top_k_by unknown-field target should fail before scan-budget consumption"
    );

    let (bottom_k_result, bottom_k_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by(build_plan(), "missing_field", 2)
        });
    let Err(bottom_k_err) = bottom_k_result else {
        panic!("bottom_k_by(missing_field, k) should be rejected");
    };
    assert_eq!(bottom_k_err.class, ErrorClass::Unsupported);
    assert_eq!(bottom_k_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        bottom_k_scanned, 0,
        "bottom_k_by unknown-field target should fail before scan-budget consumption"
    );

    let (top_k_values_result, top_k_values_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_values(build_plan(), "missing_field", 2)
        });
    let Err(top_k_values_err) = top_k_values_result else {
        panic!("top_k_by_values(missing_field, k) should be rejected");
    };
    assert_eq!(top_k_values_err.class, ErrorClass::Unsupported);
    assert_eq!(top_k_values_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        top_k_values_scanned, 0,
        "top_k_by_values unknown-field target should fail before scan-budget consumption"
    );

    let (bottom_k_values_result, bottom_k_values_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_values(build_plan(), "missing_field", 2)
        });
    let Err(bottom_k_values_err) = bottom_k_values_result else {
        panic!("bottom_k_by_values(missing_field, k) should be rejected");
    };
    assert_eq!(bottom_k_values_err.class, ErrorClass::Unsupported);
    assert_eq!(bottom_k_values_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        bottom_k_values_scanned, 0,
        "bottom_k_by_values unknown-field target should fail before scan-budget consumption"
    );

    let (top_k_with_ids_result, top_k_with_ids_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.top_k_by_with_ids(build_plan(), "missing_field", 2)
        });
    let Err(top_k_with_ids_err) = top_k_with_ids_result else {
        panic!("top_k_by_with_ids(missing_field, k) should be rejected");
    };
    assert_eq!(top_k_with_ids_err.class, ErrorClass::Unsupported);
    assert_eq!(top_k_with_ids_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        top_k_with_ids_scanned, 0,
        "top_k_by_with_ids unknown-field target should fail before scan-budget consumption"
    );

    let (bottom_k_with_ids_result, bottom_k_with_ids_scanned) =
        capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
            load.bottom_k_by_with_ids(build_plan(), "missing_field", 2)
        });
    let Err(bottom_k_with_ids_err) = bottom_k_with_ids_result else {
        panic!("bottom_k_by_with_ids(missing_field, k) should be rejected");
    };
    assert_eq!(bottom_k_with_ids_err.class, ErrorClass::Unsupported);
    assert_eq!(bottom_k_with_ids_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        bottom_k_with_ids_scanned, 0,
        "bottom_k_by_with_ids unknown-field target should fail before scan-budget consumption"
    );
}

#[test]
fn aggregate_field_target_top_and_bottom_k_by_non_orderable_field_fail_without_scan() {
    seed_phase_entities(&[(8_1991, 10), (8_1992, 20), (8_1993, 30)]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let build_plan = || {
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .order_by("id")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("top/bottom non-orderable target plan should build")
    };

    let (top_k_result, top_k_scanned) = capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
        load.top_k_by(build_plan(), "tags", 2)
    });
    let Err(top_k_err) = top_k_result else {
        panic!("top_k_by(tags, 2) should be rejected");
    };
    assert_eq!(top_k_err.class, ErrorClass::Unsupported);
    assert_eq!(top_k_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        top_k_scanned, 0,
        "top_k_by non-orderable field target should fail before scan-budget consumption"
    );
    assert!(
        top_k_err.message.contains("does not support ordering"),
        "top_k_by(tags, 2) should preserve non-orderable field taxonomy: {top_k_err:?}"
    );

    let (bottom_k_result, bottom_k_scanned) =
        capture_rows_scanned_for_entity(PhaseEntity::PATH, || {
            load.bottom_k_by(build_plan(), "tags", 2)
        });
    let Err(bottom_k_err) = bottom_k_result else {
        panic!("bottom_k_by(tags, 2) should be rejected");
    };
    assert_eq!(bottom_k_err.class, ErrorClass::Unsupported);
    assert_eq!(bottom_k_err.origin, ErrorOrigin::Executor);
    assert_eq!(
        bottom_k_scanned, 0,
        "bottom_k_by non-orderable field target should fail before scan-budget consumption"
    );
    assert!(
        bottom_k_err.message.contains("does not support ordering"),
        "bottom_k_by(tags, 2) should preserve non-orderable field taxonomy: {bottom_k_err:?}"
    );
}

#[test]
fn aggregate_field_target_min_max_matches_individual_extrema() {
    seed_pushdown_entities(&[
        (8_2011, 7, 10),
        (8_2012, 7, 10),
        (8_2013, 7, 40),
        (8_2014, 7, 40),
        (8_2015, 7, 25),
        (8_2016, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("field-target min-max plan should build")
    };

    let min_max = load
        .aggregate_min_max_by(build_plan(), "rank")
        .expect("min_max_by(rank) should succeed");
    let min_by = load
        .aggregate_min_by(build_plan(), "rank")
        .expect("min_by(rank) should succeed");
    let max_by = load
        .aggregate_max_by(build_plan(), "rank")
        .expect("max_by(rank) should succeed");
    let expected_pair = min_by.zip(max_by);

    assert_eq!(
        min_max, expected_pair,
        "min_max_by(rank) should match individual min_by/max_by terminals"
    );
    assert_eq!(
        min_max.map(|(min_id, _)| min_id.key()),
        Some(Ulid::from_u128(8_2011)),
        "min_max_by(rank) min tie-break should use primary key ascending"
    );
    assert_eq!(
        min_max.map(|(_, max_id)| max_id.key()),
        Some(Ulid::from_u128(8_2013)),
        "min_max_by(rank) max tie-break should use primary key ascending"
    );
}

#[test]
fn aggregate_field_target_min_max_metamorphic_matrix_matches_individual_extrema() {
    seed_pushdown_entities(&[
        (8_2021, 7, 10),
        (8_2022, 7, 10),
        (8_2023, 7, 20),
        (8_2024, 7, 30),
        (8_2025, 7, 40),
        (8_2026, 7, 40),
        (8_2027, 8, 15),
        (8_2028, 8, 25),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let overlapping_predicate = Predicate::Or(vec![
        id_in_predicate(&[8_2021, 8_2022, 8_2023, 8_2024, 8_2025, 8_2026]),
        id_in_predicate(&[8_2022, 8_2023, 8_2026, 8_2027, 8_2028]),
    ]);

    for (label, distinct, desc, offset, limit) in [
        ("asc/no-distinct/unbounded", false, false, 0u32, None),
        ("asc/no-distinct/windowed", false, false, 1u32, Some(4u32)),
        ("asc/distinct/windowed", true, false, 1u32, Some(4u32)),
        ("desc/no-distinct/windowed", false, true, 1u32, Some(4u32)),
        ("desc/distinct/windowed", true, true, 2u32, Some(3u32)),
        ("desc/distinct/empty-window", true, true, 50u32, Some(3u32)),
    ] {
        let build_query = || {
            let mut query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(overlapping_predicate.clone());
            if distinct {
                query = query.distinct();
            }
            query = if desc {
                query.order_by_desc("id")
            } else {
                query.order_by("id")
            };
            query = query.offset(offset);
            if let Some(limit) = limit {
                query = query.limit(limit);
            }

            query
        };

        let min_max = load
            .aggregate_min_max_by(
                build_query()
                    .plan()
                    .map(crate::db::executor::ExecutablePlan::from)
                    .expect("metamorphic min_max plan should build"),
                "rank",
            )
            .expect("metamorphic min_max_by(rank) should succeed");
        let min_by = load
            .aggregate_min_by(
                build_query()
                    .plan()
                    .map(crate::db::executor::ExecutablePlan::from)
                    .expect("metamorphic min plan should build"),
                "rank",
            )
            .expect("metamorphic min_by(rank) should succeed");
        let max_by = load
            .aggregate_max_by(
                build_query()
                    .plan()
                    .map(crate::db::executor::ExecutablePlan::from)
                    .expect("metamorphic max plan should build"),
                "rank",
            )
            .expect("metamorphic max_by(rank) should succeed");

        assert_eq!(
            min_max,
            min_by.zip(max_by),
            "metamorphic min_max parity failed for case={label}"
        );
    }
}

#[test]
fn aggregate_field_target_min_max_empty_window_returns_none() {
    seed_pushdown_entities(&[(8_2031, 7, 10), (8_2032, 7, 20), (8_2033, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let min_max = load
        .aggregate_min_max_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("id")
                .offset(50)
                .limit(2)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("empty-window min_max plan should build"),
            "rank",
        )
        .expect("empty-window min_max_by(rank) should succeed");

    assert_eq!(min_max, None, "empty-window min_max_by should return None");
}

#[test]
fn aggregate_field_target_min_max_single_row_returns_same_id_pair() {
    seed_pushdown_entities(&[(8_2041, 7, 10), (8_2042, 7, 20), (8_2043, 7, 30)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let min_max = load
        .aggregate_min_max_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("id")
                .offset(1)
                .limit(1)
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("single-row min_max plan should build"),
            "rank",
        )
        .expect("single-row min_max_by(rank) should succeed");

    assert_eq!(
        min_max.map(|(min_id, max_id)| (min_id.key(), max_id.key())),
        Some((Ulid::from_u128(8_2042), Ulid::from_u128(8_2042))),
        "single-row min_max_by should return the same id for both extrema"
    );
}

#[test]
fn aggregate_field_target_median_order_direction_invariant_on_same_window() {
    seed_pushdown_entities(&[
        (8_2051, 7, 10),
        (8_2052, 7, 20),
        (8_2053, 7, 20),
        (8_2054, 7, 40),
        (8_2055, 7, 50),
        (8_2056, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let asc_median = load
        .aggregate_median_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("id")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("median ASC plan should build"),
            "rank",
        )
        .expect("median_by(rank) ASC should succeed");
    let desc_median = load
        .aggregate_median_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("id")
                .plan()
                .map(crate::db::executor::ExecutablePlan::from)
                .expect("median DESC plan should build"),
            "rank",
        )
        .expect("median_by(rank) DESC should succeed");

    assert_eq!(
        asc_median, desc_median,
        "median_by(rank) should be invariant to query order direction on the same row window"
    );
}

#[test]
fn aggregate_numeric_field_sum_and_avg_use_decimal_projection() {
    seed_pushdown_entities(&[
        (8_091, 7, 10),
        (8_092, 7, 20),
        (8_093, 7, 35),
        (8_094, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by("rank")
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("numeric field aggregate plan should build")
    };

    let sum = load
        .aggregate_sum_by(build_plan(), "rank")
        .expect("sum_by(rank) should succeed");
    let avg = load
        .aggregate_avg_by(build_plan(), "rank")
        .expect("avg_by(rank) should succeed");
    let expected_avg = Decimal::from_num(65u64).expect("sum decimal")
        / Decimal::from_num(3u64).expect("count decimal");

    assert_eq!(
        sum,
        Decimal::from_num(65u64),
        "sum_by(rank) should match row set"
    );
    assert_eq!(
        avg,
        Some(expected_avg),
        "avg_by(rank) should use decimal division semantics"
    );
}

#[test]
fn aggregate_numeric_field_unknown_target_fails_without_scan() {
    seed_pushdown_entities(&[(8_101, 7, 10), (8_102, 7, 20)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("numeric field unknown-target plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_sum_by(plan, "missing_field")
    });
    let Err(err) = result else {
        panic!("sum_by(missing_field) should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "unknown numeric target should fail before scan-budget consumption"
    );
}

#[test]
fn aggregate_numeric_field_non_numeric_target_fails_without_scan() {
    seed_pushdown_entities(&[(8_111, 7, 10), (8_112, 7, 20)]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("numeric field non-numeric target plan should build");
    let (result, scanned) = capture_rows_scanned_for_entity(PushdownParityEntity::PATH, || {
        load.aggregate_avg_by(plan, "label")
    });
    let Err(err) = result else {
        panic!("avg_by(label) should be rejected");
    };

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert_eq!(
        scanned, 0,
        "non-numeric target should fail before scan-budget consumption"
    );
}
