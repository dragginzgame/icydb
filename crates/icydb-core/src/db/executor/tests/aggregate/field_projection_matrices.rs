use super::*;

#[test]
fn aggregate_field_target_count_distinct_counts_window_values() {
    seed_pushdown_entities(&[
        (8_191, 7, 10),
        (8_192, 7, 10),
        (8_193, 7, 20),
        (8_194, 7, 30),
        (8_195, 7, 30),
        (8_196, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .limit(5)
            .plan()
            .expect("field-target count-distinct plan should build")
    };

    let expected_response = load
        .execute(build_plan())
        .expect("field-target count-distinct baseline execute should succeed");
    let distinct_count = load
        .aggregate_count_distinct_by(build_plan(), "rank")
        .expect("count_distinct_by(rank) should succeed");
    let empty_window_count = load
        .aggregate_count_distinct_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("id")
                .offset(50)
                .limit(5)
                .plan()
                .expect("empty-window count-distinct plan should build"),
            "rank",
        )
        .expect("empty-window count_distinct_by(rank) should succeed");

    assert_eq!(
        distinct_count,
        expected_count_distinct_by_rank(&expected_response),
        "count_distinct_by(rank) should match distinct values in the effective window"
    );
    assert_eq!(
        empty_window_count, 0,
        "count_distinct_by(rank) should return zero for empty windows"
    );
}

#[test]
fn aggregate_field_target_count_distinct_supports_non_orderable_fields() {
    seed_phase_entities(&[(8_197, 10), (8_198, 20), (8_199, 10)]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let distinct_count = load
        .aggregate_count_distinct_by(
            Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .plan()
                .expect("non-orderable count-distinct plan should build"),
            "tags",
        )
        .expect("count_distinct_by(tags) should succeed");

    assert_eq!(
        distinct_count, 2,
        "count_distinct_by(tags) should support structured field equality"
    );
}

#[test]
fn aggregate_field_target_count_distinct_list_order_semantics_are_stable() {
    seed_phase_entities_custom(vec![
        PhaseEntity {
            id: Ulid::from_u128(819_701),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![1, 2],
            label: "a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(819_702),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![2, 1],
            label: "b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(819_703),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![1, 2],
            label: "c".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(819_704),
            opt_rank: Some(40),
            rank: 40,
            tags: vec![1, 2, 3],
            label: "d".to_string(),
        },
    ]);
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let distinct_count = load
        .aggregate_count_distinct_by(
            Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
                .order_by("id")
                .plan()
                .expect("list-order count-distinct plan should build"),
            "tags",
        )
        .expect("count_distinct_by(tags) should succeed");

    assert_eq!(
        distinct_count, 3,
        "count_distinct_by(tags) should preserve list-order equality semantics"
    );
}

#[test]
fn aggregate_field_target_count_distinct_residual_retry_parity_and_scan_budget_match_execute() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();
    let save = SaveExecutor::<IndexedMetricsEntity>::new(DB, false);
    for (id, tag, label) in [
        (8_3101u128, 10u32, "drop-t10"),
        (8_3102, 11, "drop-t11"),
        (8_3103, 12, "drop-t12"),
        (8_3104, 13, "keep-t13"),
        (8_3105, 14, "keep-t14"),
        (8_3106, 15, "keep-t15"),
    ] {
        save.insert(IndexedMetricsEntity {
            id: Ulid::from_u128(id),
            tag,
            label: label.to_string(),
        })
        .expect("indexed metrics seed row save should succeed");
    }
    let load = LoadExecutor::<IndexedMetricsEntity>::new(DB, false);
    let build_plan = || {
        let mut logical = AccessPlannedQuery::new(
            AccessPath::index_range(
                INDEXED_METRICS_INDEX_MODELS[0],
                Vec::new(),
                Bound::Included(Value::Uint(10)),
                Bound::Excluded(Value::Uint(16)),
            ),
            MissingRowPolicy::Ignore,
        );
        logical.predicate = Some(Predicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("keep".to_string()),
        });
        logical.order = Some(OrderSpec {
            fields: vec![
                ("tag".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        logical.page = Some(PageSpec {
            limit: Some(2),
            offset: 0,
        });

        ExecutablePlan::<IndexedMetricsEntity>::new(logical)
    };

    let (distinct_count, scanned_count_distinct) =
        capture_rows_scanned_for_entity(IndexedMetricsEntity::PATH, || {
            load.aggregate_count_distinct_by(build_plan(), "tag")
                .expect("residual-retry count_distinct_by(tag) should succeed")
        });
    let (response, scanned_execute) =
        capture_rows_scanned_for_entity(IndexedMetricsEntity::PATH, || {
            load.execute(build_plan())
                .expect("residual-retry execute baseline should succeed")
        });
    let expected_count = {
        let mut seen_values: Vec<Value> = Vec::new();
        let mut count = 0u32;
        for (_, entity) in &response.0 {
            let value = Value::Uint(u64::from(entity.tag));
            if seen_values.iter().any(|existing| existing == &value) {
                continue;
            }
            seen_values.push(value);
            count = count.saturating_add(1);
        }
        count
    };

    assert_eq!(
        distinct_count, expected_count,
        "count_distinct_by(tag) should preserve canonical fallback parity for residual-retry index-range shapes"
    );
    assert_eq!(
        scanned_count_distinct, scanned_execute,
        "count_distinct_by(tag) should preserve scan-budget parity with execute() on residual-retry index-range shapes"
    );
}

#[test]
fn aggregate_field_target_count_distinct_is_direction_invariant() {
    seed_pushdown_entities(&[
        (8_3201, 7, 10),
        (8_3202, 7, 20),
        (8_3203, 7, 20),
        (8_3204, 7, 30),
        (8_3205, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let asc_count = load
        .aggregate_count_distinct_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by("rank")
                .plan()
                .expect("direction-invariant ASC plan should build"),
            "rank",
        )
        .expect("direction-invariant ASC count_distinct_by(rank) should succeed");
    let desc_count = load
        .aggregate_count_distinct_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .order_by_desc("rank")
                .order_by_desc("id")
                .plan()
                .expect("direction-invariant DESC plan should build"),
            "rank",
        )
        .expect("direction-invariant DESC count_distinct_by(rank) should succeed");

    assert_eq!(
        asc_count, desc_count,
        "count_distinct_by(rank) should be invariant to traversal direction over the same effective window"
    );
}

#[test]
fn aggregate_field_target_count_distinct_optional_field_null_values_are_rejected_consistently() {
    seed_optional_field_null_values_fixture();
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let build_plan_asc = || {
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .order_by("rank")
            .plan()
            .expect("optional-field null-semantics ASC plan should build")
    };
    let build_plan_desc = || {
        Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
            .order_by_desc("rank")
            .order_by_desc("id")
            .plan()
            .expect("optional-field null-semantics DESC plan should build")
    };
    let asc_err = load
        .aggregate_count_distinct_by(build_plan_asc(), "opt_rank")
        .expect_err("count_distinct_by(opt_rank) ASC should reject null field values");
    let desc_err = load
        .aggregate_count_distinct_by(build_plan_desc(), "opt_rank")
        .expect_err("count_distinct_by(opt_rank) DESC should reject null field values");

    assert_eq!(
        asc_err.class,
        ErrorClass::InvariantViolation,
        "count_distinct_by(opt_rank) should classify null-value mismatch as invariant violation"
    );
    assert_eq!(
        desc_err.class,
        ErrorClass::InvariantViolation,
        "descending count_distinct_by(opt_rank) should classify null-value mismatch as invariant violation"
    );
    assert!(
        asc_err
            .message
            .contains("aggregate target field value type mismatch"),
        "count_distinct_by(opt_rank) should expose type-mismatch reason for null values"
    );
    assert!(
        desc_err
            .message
            .contains("aggregate target field value type mismatch"),
        "descending count_distinct_by(opt_rank) should expose type-mismatch reason for null values"
    );
    assert!(
        asc_err.message.contains("value=Null") && desc_err.message.contains("value=Null"),
        "count_distinct_by(opt_rank) should report null payload mismatch consistently across directions"
    );
}

// Shared terminal-kind selector for optional-field null-value parity coverage.
#[derive(Clone, Copy)]
enum OptionalFieldNullTerminal {
    TopKBy,
    BottomKBy,
    TopKByValues,
    BottomKByValues,
    TopKByWithIds,
    BottomKByWithIds,
}

// Shared terminal-kind selector for missing-field projection-parity coverage.
#[derive(Clone, Copy)]
enum MissingFieldTerminal {
    TopKBy,
    TopKByValues,
    BottomKBy,
    BottomKByValues,
    TopKByWithIds,
    BottomKByWithIds,
}

fn seed_optional_field_null_values_fixture() {
    seed_phase_entities_custom(vec![
        PhaseEntity {
            id: Ulid::from_u128(8_3301),
            opt_rank: None,
            rank: 1,
            tags: vec![1],
            label: "phase-1".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3302),
            opt_rank: Some(10),
            rank: 2,
            tags: vec![2],
            label: "phase-2".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(8_3303),
            opt_rank: Some(20),
            rank: 3,
            tags: vec![3],
            label: "phase-3".to_string(),
        },
    ]);
}

fn optional_field_null_plan() -> ExecutablePlan<PhaseEntity> {
    Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .plan()
        .expect("optional-field null-semantics plan should build")
}

fn optional_field_null_baseline_error(
    load: &LoadExecutor<PhaseEntity>,
    terminal: OptionalFieldNullTerminal,
) -> InternalError {
    match terminal {
        OptionalFieldNullTerminal::TopKByWithIds | OptionalFieldNullTerminal::BottomKByWithIds => {
            load.values_by_with_ids(optional_field_null_plan(), "opt_rank")
                .expect_err("values_by_with_ids(opt_rank) should reject null field values")
        }
        _ => load
            .values_by(optional_field_null_plan(), "opt_rank")
            .expect_err("values_by(opt_rank) should reject null field values"),
    }
}

fn optional_field_null_terminal_error(
    load: &LoadExecutor<PhaseEntity>,
    terminal: OptionalFieldNullTerminal,
) -> InternalError {
    match terminal {
        OptionalFieldNullTerminal::TopKBy => load
            .top_k_by(optional_field_null_plan(), "opt_rank", 2)
            .expect_err("top_k_by(opt_rank, 2) should reject null field values"),
        OptionalFieldNullTerminal::BottomKBy => load
            .bottom_k_by(optional_field_null_plan(), "opt_rank", 2)
            .expect_err("bottom_k_by(opt_rank, 2) should reject null field values"),
        OptionalFieldNullTerminal::TopKByValues => load
            .top_k_by_values(optional_field_null_plan(), "opt_rank", 2)
            .expect_err("top_k_by_values(opt_rank, 2) should reject null field values"),
        OptionalFieldNullTerminal::BottomKByValues => load
            .bottom_k_by_values(optional_field_null_plan(), "opt_rank", 2)
            .expect_err("bottom_k_by_values(opt_rank, 2) should reject null field values"),
        OptionalFieldNullTerminal::TopKByWithIds => load
            .top_k_by_with_ids(optional_field_null_plan(), "opt_rank", 2)
            .expect_err("top_k_by_with_ids(opt_rank, 2) should reject null field values"),
        OptionalFieldNullTerminal::BottomKByWithIds => load
            .bottom_k_by_with_ids(optional_field_null_plan(), "opt_rank", 2)
            .expect_err("bottom_k_by_with_ids(opt_rank, 2) should reject null field values"),
    }
}

fn assert_optional_field_null_parity(terminal: OptionalFieldNullTerminal, label: &str) {
    seed_optional_field_null_values_fixture();
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let baseline_err = optional_field_null_baseline_error(&load, terminal);
    let terminal_err = optional_field_null_terminal_error(&load, terminal);

    assert_eq!(
        baseline_err.class,
        ErrorClass::InvariantViolation,
        "{label} baseline projection should classify null-value mismatch as invariant violation"
    );
    assert_eq!(
        terminal_err.class,
        ErrorClass::InvariantViolation,
        "{label} should classify null-value mismatch as invariant violation"
    );
    assert!(
        baseline_err
            .message
            .contains("aggregate target field value type mismatch"),
        "{label} baseline projection should expose type-mismatch reason for null values"
    );
    assert!(
        terminal_err
            .message
            .contains("aggregate target field value type mismatch"),
        "{label} should expose type-mismatch reason for null values"
    );
    assert!(
        baseline_err.message.contains("value=Null") && terminal_err.message.contains("value=Null"),
        "{label} should report null payload mismatch consistently with baseline projection"
    );
}

fn seed_missing_field_parity_fixture() {
    seed_pushdown_entities(&[(8_3381, 7, 10), (8_3382, 7, 20), (8_3383, 7, 30)]);
}

fn missing_field_parity_plan() -> ExecutablePlan<PushdownParityEntity> {
    Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .expect("missing-field parity plan should build")
}

fn missing_field_baseline_error(
    load: &LoadExecutor<PushdownParityEntity>,
    terminal: MissingFieldTerminal,
) -> InternalError {
    match terminal {
        MissingFieldTerminal::TopKByWithIds | MissingFieldTerminal::BottomKByWithIds => load
            .values_by_with_ids(missing_field_parity_plan(), "missing_field")
            .expect_err("values_by_with_ids(missing_field) should be rejected"),
        _ => load
            .values_by(missing_field_parity_plan(), "missing_field")
            .expect_err("values_by(missing_field) should be rejected"),
    }
}

fn missing_field_terminal_error(
    load: &LoadExecutor<PushdownParityEntity>,
    terminal: MissingFieldTerminal,
) -> InternalError {
    match terminal {
        MissingFieldTerminal::TopKBy => load
            .top_k_by(missing_field_parity_plan(), "missing_field", 2)
            .expect_err("top_k_by(missing_field, 2) should be rejected"),
        MissingFieldTerminal::TopKByValues => load
            .top_k_by_values(missing_field_parity_plan(), "missing_field", 2)
            .expect_err("top_k_by_values(missing_field, 2) should be rejected"),
        MissingFieldTerminal::BottomKBy => load
            .bottom_k_by(missing_field_parity_plan(), "missing_field", 2)
            .expect_err("bottom_k_by(missing_field, 2) should be rejected"),
        MissingFieldTerminal::BottomKByValues => load
            .bottom_k_by_values(missing_field_parity_plan(), "missing_field", 2)
            .expect_err("bottom_k_by_values(missing_field, 2) should be rejected"),
        MissingFieldTerminal::TopKByWithIds => load
            .top_k_by_with_ids(missing_field_parity_plan(), "missing_field", 2)
            .expect_err("top_k_by_with_ids(missing_field, 2) should be rejected"),
        MissingFieldTerminal::BottomKByWithIds => load
            .bottom_k_by_with_ids(missing_field_parity_plan(), "missing_field", 2)
            .expect_err("bottom_k_by_with_ids(missing_field, 2) should be rejected"),
    }
}

fn assert_missing_field_terminal_parity(terminal: MissingFieldTerminal, label: &str) {
    seed_missing_field_parity_fixture();
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);

    let baseline_err = missing_field_baseline_error(&load, terminal);
    let terminal_err = missing_field_terminal_error(&load, terminal);

    assert_eq!(
        terminal_err.class, baseline_err.class,
        "{label} should classify unknown-field failures the same way as baseline projection"
    );
    assert_eq!(
        terminal_err.origin, baseline_err.origin,
        "{label} should preserve unknown-field origin parity with baseline projection"
    );
    assert!(
        terminal_err
            .message
            .contains("unknown aggregate target field"),
        "{label} should surface the same unknown-field reason"
    );
}

#[test]
fn aggregate_field_target_optional_field_null_value_terminal_parity_matrix() {
    for terminal in [
        OptionalFieldNullTerminal::TopKBy,
        OptionalFieldNullTerminal::BottomKBy,
        OptionalFieldNullTerminal::TopKByValues,
        OptionalFieldNullTerminal::BottomKByValues,
        OptionalFieldNullTerminal::TopKByWithIds,
        OptionalFieldNullTerminal::BottomKByWithIds,
    ] {
        assert_optional_field_null_parity(terminal, "optional-field null-value parity");
    }
}

#[test]
fn aggregate_field_target_missing_field_ranked_projection_parity_matrix() {
    for terminal in [
        MissingFieldTerminal::TopKBy,
        MissingFieldTerminal::TopKByValues,
        MissingFieldTerminal::BottomKBy,
        MissingFieldTerminal::BottomKByValues,
        MissingFieldTerminal::TopKByWithIds,
        MissingFieldTerminal::BottomKByWithIds,
    ] {
        assert_missing_field_terminal_parity(terminal, "missing-field ranked projection parity");
    }
}

#[test]
fn aggregate_field_target_count_distinct_distinct_modifier_tracks_effective_window_rows() {
    seed_pushdown_entities(&[
        (8_1971, 7, 10),
        (8_1972, 7, 20),
        (8_1973, 7, 30),
        (8_1974, 7, 40),
        (8_1975, 8, 50),
        (8_1976, 8, 60),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let overlapping_predicate = Predicate::Or(vec![
        id_in_predicate(&[8_1971, 8_1972, 8_1973, 8_1974]),
        id_in_predicate(&[8_1972, 8_1973, 8_1975, 8_1976]),
    ]);
    let build_query = |distinct: bool| {
        let mut query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(overlapping_predicate.clone());
        if distinct {
            query = query.distinct();
        }

        query.order_by_desc("id").offset(1).limit(4)
    };

    let non_distinct_response = load
        .execute(
            build_query(false)
                .plan()
                .expect("non-distinct count-distinct baseline plan should build"),
        )
        .expect("non-distinct count-distinct baseline execute should succeed");
    let distinct_response = load
        .execute(
            build_query(true)
                .plan()
                .expect("distinct count-distinct baseline plan should build"),
        )
        .expect("distinct count-distinct baseline execute should succeed");

    let non_distinct_count = load
        .aggregate_count_distinct_by(
            build_query(false)
                .plan()
                .expect("non-distinct count-distinct plan should build"),
            "rank",
        )
        .expect("non-distinct count_distinct_by(rank) should succeed");
    let distinct_count = load
        .aggregate_count_distinct_by(
            build_query(true)
                .plan()
                .expect("distinct count-distinct plan should build"),
            "rank",
        )
        .expect("distinct count_distinct_by(rank) should succeed");

    assert_eq!(
        non_distinct_count,
        expected_count_distinct_by_rank(&non_distinct_response),
        "non-distinct count_distinct_by(rank) should match effective-window field distinct count"
    );
    assert_eq!(
        distinct_count,
        expected_count_distinct_by_rank(&distinct_response),
        "distinct count_distinct_by(rank) should match effective-window field distinct count"
    );
}

#[test]
fn aggregate_field_target_values_by_distinct_remains_row_level() {
    seed_pushdown_entities(&[
        (8_1971, 7, 10),
        (8_1972, 7, 10),
        (8_1973, 7, 20),
        (8_1974, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let values = load
        .values_by(
            Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
                .filter(u32_eq_predicate("group", 7))
                .distinct()
                .order_by("id")
                .plan()
                .expect("values_by distinct plan should build"),
            "rank",
        )
        .expect("values_by(rank) should succeed");

    assert_eq!(
        values,
        vec![Value::Uint(10), Value::Uint(10), Value::Uint(20)],
        "query-level DISTINCT must remain row-level; equal projected values may repeat"
    );
}

#[test]
fn aggregate_field_target_distinct_values_by_matches_effective_window_projection() {
    seed_pushdown_entities(&[
        (8_1971, 7, 10),
        (8_1972, 7, 10),
        (8_1973, 7, 20),
        (8_1974, 7, 30),
        (8_1975, 7, 20),
        (8_1976, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
            .plan()
            .expect("distinct_values_by plan should build")
    };

    let expected = load
        .execute(build_plan())
        .expect("baseline execute for distinct_values_by should succeed");
    let actual = load
        .distinct_values_by(build_plan(), "rank")
        .expect("distinct_values_by(rank) should succeed");

    assert_eq!(
        actual,
        expected_distinct_values_by_rank(&expected),
        "distinct_values_by(rank) should match effective-window first-observed distinct projection"
    );
}

#[test]
fn aggregate_field_target_distinct_values_by_matches_values_by_first_observed_dedup() {
    seed_pushdown_entities(&[
        (8_1971, 7, 10),
        (8_1972, 7, 10),
        (8_1973, 7, 20),
        (8_1974, 7, 30),
        (8_1975, 7, 20),
        (8_1976, 8, 99),
    ]);
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let build_plan = || {
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(u32_eq_predicate("group", 7))
            .order_by_desc("id")
            .offset(1)
            .limit(4)
            .plan()
            .expect("distinct-values invariant plan should build")
    };

    let values = load
        .values_by(build_plan(), "rank")
        .expect("values_by(rank) should succeed");
    let distinct_values = load
        .distinct_values_by(build_plan(), "rank")
        .expect("distinct_values_by(rank) should succeed");

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
        "values_by(field).len() must be >= distinct_values_by(field).len()"
    );
    assert_eq!(
        distinct_values, expected_distinct_from_values,
        "distinct_values_by(field) must equal values_by(field) deduped by first occurrence"
    );
}
