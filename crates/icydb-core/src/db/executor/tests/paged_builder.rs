use super::*;
use proptest::prelude::*;
use std::cmp::Ordering;

fn seed_grouped_phase_entities() {
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for (rank, label) in [(1_u32, "alpha"), (1_u32, "beta"), (2_u32, "gamma")] {
        save.insert(PhaseEntity {
            id: Ulid::generate(),
            opt_rank: Some(rank),
            rank,
            tags: vec![rank],
            label: label.to_string(),
        })
        .expect("grouped seed insert should succeed");
    }
}

fn seed_grouped_phase_entities_with_fixed_ids() -> (Ulid, Ulid, Ulid) {
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    let id_a = Ulid::generate();
    let id_b = Ulid::generate();
    let id_c = Ulid::generate();
    for (id, rank, label) in [
        (id_a, 1_u32, "alpha"),
        (id_b, 1_u32, "beta"),
        (id_c, 2_u32, "gamma"),
    ] {
        save.insert(PhaseEntity {
            id,
            opt_rank: Some(rank),
            rank,
            tags: vec![rank],
            label: label.to_string(),
        })
        .expect("grouped seed insert should succeed");
    }

    (id_a, id_b, id_c)
}

fn seed_single_group_phase_entities() {
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for label in ["alpha", "beta", "gamma"] {
        save.insert(PhaseEntity {
            id: Ulid::generate(),
            opt_rank: Some(1_u32),
            rank: 1_u32,
            tags: vec![1_u32],
            label: label.to_string(),
        })
        .expect("single-group seed insert should succeed");
    }
}

fn seed_grouped_phase_entities_with_filtered_middle_group() {
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for (rank, label) in [
        (1_u32, "alpha"),
        (1_u32, "beta"),
        (2_u32, "gamma"),
        (3_u32, "delta"),
        (3_u32, "epsilon"),
    ] {
        save.insert(PhaseEntity {
            id: Ulid::generate(),
            opt_rank: Some(rank),
            rank,
            tags: vec![rank],
            label: label.to_string(),
        })
        .expect("grouped seed insert should succeed");
    }
}

// Compare grouped keys using the same canonical grouped-key comparator used by grouped execution.
fn grouped_key_ordering(left: &[Value], right: &[Value]) -> Ordering {
    crate::db::contracts::canonical_value_compare(
        &Value::List(left.to_vec()),
        &Value::List(right.to_vec()),
    )
}

fn grouped_rank_strategy() -> impl Strategy<Value = u32> {
    prop_oneof![Just(0_u32), Just(1_u32), Just(u32::MAX), 2_u32..2048_u32,]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn grouped_continuation_property_preserves_monotonicity_no_duplicates_and_full_union(
        ranks in proptest::collection::vec(grouped_rank_strategy(), 5..40),
        limit in 1_u32..7_u32,
        descending in any::<bool>(),
    ) {
        // Phase 1: seed a synthetic grouped domain with duplicate and edge-value group keys.
        reset_store();
        let save = SaveExecutor::<PhaseEntity>::new(DB, false);
        for (idx, rank) in ranks.iter().copied().enumerate() {
            save.insert(PhaseEntity {
                id: Ulid::generate(),
                opt_rank: Some(rank),
                rank,
                tags: vec![rank],
                label: format!("grouped-property-{idx}-{rank}"),
            })
            .expect("grouped property seed insert should succeed");
        }

        let session = DbSession::new(DB);

        // Phase 2: execute one full grouped baseline to define expected grouped-domain output.
        let baseline = if descending {
            session
                .load::<PhaseEntity>()
                .order_by_desc("rank")
                .group_by("rank")
                .expect("group field should resolve")
                .aggregate(crate::db::count())
                .limit(u32::MAX)
                .execute_grouped()
                .expect("grouped property descending baseline should execute")
        } else {
            session
                .load::<PhaseEntity>()
                .order_by("rank")
                .group_by("rank")
                .expect("group field should resolve")
                .aggregate(crate::db::count())
                .limit(u32::MAX)
                .execute_grouped()
                .expect("grouped property ascending baseline should execute")
        };
        let expected_rows: Vec<(Vec<Value>, Vec<Value>)> = baseline
            .rows()
            .iter()
            .map(|row| (row.group_key().to_vec(), row.aggregate_values().to_vec()))
            .collect();
        let expected_progression = expected_rows
            .windows(2)
            .next()
            .map_or(Ordering::Less, |pair| {
                grouped_key_ordering(pair[0].0.as_slice(), pair[1].0.as_slice())
            });
        prop_assert!(
            expected_rows.windows(2).all(|pair| {
                let ordering = grouped_key_ordering(pair[0].0.as_slice(), pair[1].0.as_slice());
                ordering != Ordering::Equal && ordering == expected_progression
            }),
            "baseline grouped output must remain strictly monotonic for descending={}",
            descending,
        );

        // Phase 3: page through grouped results with continuation tokens and collect emitted rows.
        let mut observed_rows = Vec::<(Vec<Value>, Vec<Value>)>::new();
        let mut continuation_hex: Option<String> = None;
        let mut page_count = 0usize;
        loop {
            let query = if descending {
                session
                    .load::<PhaseEntity>()
                    .order_by_desc("rank")
                    .group_by("rank")
                    .expect("group field should resolve")
                    .aggregate(crate::db::count())
                    .limit(limit)
            } else {
                session
                    .load::<PhaseEntity>()
                    .order_by("rank")
                    .group_by("rank")
                    .expect("group field should resolve")
                    .aggregate(crate::db::count())
                    .limit(limit)
            };
            let page = if let Some(cursor_hex) = continuation_hex.as_deref() {
                query
                    .cursor(cursor_hex)
                    .execute_grouped()
                    .expect("grouped property continuation page should execute")
            } else {
                query
                    .execute_grouped()
                    .expect("grouped property initial page should execute")
            };

            for row in page.rows() {
                let emitted = (row.group_key().to_vec(), row.aggregate_values().to_vec());
                if let Some((previous_key, _)) = observed_rows.last() {
                    prop_assert_eq!(
                        grouped_key_ordering(previous_key, emitted.0.as_slice()),
                        expected_progression,
                        "grouped continuation must preserve baseline strict grouped-key progression for descending={}",
                        descending,
                    );
                }
                observed_rows.push(emitted);
            }

            page_count = page_count.saturating_add(1);
            prop_assert!(
                page_count <= expected_rows.len().saturating_add(1),
                "grouped continuation pagination must terminate without replay loops",
            );

            continuation_hex = page.continuation_cursor().map(crate::db::encode_cursor);
            if continuation_hex.is_none() {
                break;
            }
        }

        // Phase 4: assert union parity against the full grouped baseline.
        prop_assert_eq!(
            observed_rows,
            expected_rows,
            "grouped continuation pages must cover the full grouped output without omission or duplication",
        );
    }
}

#[test]
fn paged_query_builder_requires_explicit_limit() {
    let session = DbSession::new(DB);

    let Err(err) = session.load::<PhaseEntity>().order_by("rank").page() else {
        panic!("paged builder should require explicit limit")
    };

    assert!(
        matches!(err, QueryError::Intent(IntentError::CursorRequiresLimit)),
        "missing limit should be rejected at page-builder boundary"
    );
}

#[test]
fn paged_query_builder_accepts_offset() {
    let session = DbSession::new(DB);

    session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(10)
        .offset(2)
        .page()
        .expect("paged builder should accept offset usage");
}

#[test]
fn paged_query_builder_accepts_order_and_limit() {
    let session = DbSession::new(DB);

    session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .page()
        .expect("paged builder should accept canonical cursor pagination intent");
}

#[test]
fn paged_query_rejects_invalid_hex_cursor_token() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .page()
        .expect("paged builder should accept order+limit")
        .cursor("zz")
        .execute()
        .expect_err("invalid hex cursor should fail at API boundary");

    let QueryError::Plan(plan_err) = err else {
        panic!("invalid cursor token should map to plan error");
    };
    let crate::db::query::plan::PlanError::Cursor(inner) = &*plan_err else {
        panic!("invalid cursor token should be classified as invalid continuation cursor");
    };
    let crate::db::cursor::CursorPlanError::InvalidContinuationCursor { reason } = inner.as_ref()
    else {
        panic!("invalid cursor token should be classified as invalid continuation cursor");
    };
    assert!(
        matches!(
            reason,
            crate::db::codec::cursor::CursorDecodeError::InvalidHex { position: 1 }
        ),
        "unexpected cursor decode reason: {reason}"
    );
}

#[test]
fn paged_query_rejects_odd_length_hex_cursor_token() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .page()
        .expect("paged builder should accept order+limit")
        .cursor("abc")
        .execute()
        .expect_err("odd-length hex cursor should fail at API boundary");

    let QueryError::Plan(plan_err) = err else {
        panic!("odd-length cursor token should map to plan error");
    };
    let crate::db::query::plan::PlanError::Cursor(inner) = &*plan_err else {
        panic!("odd-length cursor token should be classified as invalid continuation cursor");
    };
    let crate::db::cursor::CursorPlanError::InvalidContinuationCursor { reason } = inner.as_ref()
    else {
        panic!("odd-length cursor token should be classified as invalid continuation cursor");
    };
    assert!(
        matches!(
            reason,
            crate::db::codec::cursor::CursorDecodeError::OddLength
        ),
        "unexpected cursor decode reason: {reason}"
    );
}

#[test]
fn paged_query_rejects_empty_cursor_token() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .page()
        .expect("paged builder should accept order+limit")
        .cursor("   ")
        .execute()
        .expect_err("empty cursor should fail at API boundary");

    let QueryError::Plan(plan_err) = err else {
        panic!("empty cursor token should map to plan error");
    };
    let crate::db::query::plan::PlanError::Cursor(inner) = &*plan_err else {
        panic!("empty cursor token should be classified as invalid continuation cursor");
    };
    let crate::db::cursor::CursorPlanError::InvalidContinuationCursor { reason } = inner.as_ref()
    else {
        panic!("empty cursor token should be classified as invalid continuation cursor");
    };
    assert!(
        matches!(reason, crate::db::codec::cursor::CursorDecodeError::Empty),
        "unexpected cursor decode reason: {reason}"
    );
}

#[test]
fn paged_query_rejects_oversized_cursor_token() {
    let session = DbSession::new(DB);
    let oversized = "aa".repeat(5_000);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .page()
        .expect("paged builder should accept order+limit")
        .cursor(&oversized)
        .execute()
        .expect_err("oversized cursor token should fail at API boundary");

    let QueryError::Plan(plan_err) = err else {
        panic!("oversized cursor token should map to plan error");
    };
    let crate::db::query::plan::PlanError::Cursor(inner) = &*plan_err else {
        panic!("oversized cursor token should be classified as invalid continuation cursor");
    };
    let crate::db::cursor::CursorPlanError::InvalidContinuationCursor { reason } = inner.as_ref()
    else {
        panic!("oversized cursor token should be classified as invalid continuation cursor");
    };
    assert!(
        matches!(
            reason,
            crate::db::codec::cursor::CursorDecodeError::TooLong { .. }
        ),
        "unexpected cursor decode reason: {reason}"
    );
}

#[test]
fn paged_query_rejects_non_token_cursor_payload_as_payload_error() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .page()
        .expect("paged builder should accept order+limit")
        .cursor("00")
        .execute()
        .expect_err("non-token cursor payload should fail at API boundary");

    let QueryError::Plan(plan_err) = err else {
        panic!("non-token cursor payload should map to plan error");
    };
    let crate::db::query::plan::PlanError::Cursor(inner) = &*plan_err else {
        panic!("non-token payload should be classified as invalid continuation cursor payload");
    };
    let crate::db::cursor::CursorPlanError::InvalidContinuationCursorPayload { reason } =
        inner.as_ref()
    else {
        panic!("non-token payload should be classified as invalid continuation cursor payload");
    };
    assert!(
        !reason.is_empty(),
        "payload decode reason should provide context for debugging"
    );
}

#[test]
fn paged_query_execute_with_trace_is_none_without_debug_mode() {
    let session = DbSession::new(DB);

    let execution = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(2)
        .page()
        .expect("paged builder should accept order+limit")
        .execute_with_trace()
        .expect("paged execute_with_trace should succeed");

    assert!(
        execution.execution_trace().is_none(),
        "execution trace should be disabled unless session debug mode is enabled"
    );
}

#[test]
fn paged_query_execute_with_trace_is_present_in_debug_mode() {
    let session = DbSession::new(DB).debug();

    let execution = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(2)
        .page()
        .expect("paged builder should accept order+limit")
        .execute_with_trace()
        .expect("paged execute_with_trace should succeed");

    assert!(
        execution.execution_trace().is_some(),
        "execution trace should be present when session debug mode is enabled"
    );
}

#[test]
fn grouped_fluent_execute_rejects_scalar_query_shape() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .execute_grouped()
        .expect_err("grouped execution should reject non-grouped query plans");

    assert!(
        matches!(
            err,
            QueryError::Execute(crate::db::QueryExecuteError::InvariantViolation(
                crate::error::InternalError {
                    class: crate::error::ErrorClass::InvariantViolation,
                    origin: crate::error::ErrorOrigin::Query,
                    ..
                }
            ))
        ),
        "non-grouped execute_grouped should preserve query invariant classification"
    );
}

#[test]
fn grouped_fluent_execute_supports_cursor_continuation() {
    seed_grouped_phase_entities();
    let session = DbSession::new(DB);

    let page_1 = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .execute_grouped()
        .expect("first grouped page should execute");

    assert_eq!(
        page_1.rows().len(),
        1,
        "first grouped page should be limited"
    );
    assert_eq!(
        page_1.rows()[0].group_key(),
        &[Value::Uint(1)],
        "grouped rows should preserve canonical key ordering"
    );
    assert_eq!(
        page_1.rows()[0].aggregate_values(),
        &[Value::Uint(2)],
        "grouped count terminal should return grouped cardinality for rank=1"
    );

    let continuation = page_1
        .continuation_cursor()
        .map(crate::db::encode_cursor)
        .expect("first grouped page should emit continuation cursor");

    let page_2 = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .cursor(continuation)
        .execute_grouped()
        .expect("second grouped page should execute from continuation");

    assert_eq!(
        page_2.rows().len(),
        1,
        "second grouped page should contain remaining group"
    );
    assert_eq!(
        page_2.rows()[0].group_key(),
        &[Value::Uint(2)],
        "grouped continuation should resume at next canonical group key"
    );
    assert_eq!(
        page_2.rows()[0].aggregate_values(),
        &[Value::Uint(1)],
        "grouped count terminal should return grouped cardinality for rank=2"
    );
    assert!(
        page_2.continuation_cursor().is_none(),
        "terminal grouped page should not emit continuation cursor"
    );
}

#[test]
fn grouped_fluent_execute_initial_to_continuation_matrix_covers_offset_and_limit() {
    seed_grouped_phase_entities_with_filtered_middle_group();
    let session = DbSession::new(DB);

    for (case_name, offset, expected_page_1_rank, expected_page_2_rank) in [
        ("offset0_limit1", 0_u32, 1_u64, 2_u64),
        ("offset1_limit1", 1_u32, 2_u64, 3_u64),
    ] {
        // Phase 1: execute initial grouped page for the case-specific offset window.
        let page_1 = session
            .load::<PhaseEntity>()
            .group_by("rank")
            .expect("group field should resolve")
            .aggregate(crate::db::count())
            .offset(offset)
            .limit(1)
            .execute_grouped()
            .expect("first grouped matrix page should execute");
        assert_eq!(
            page_1.rows().len(),
            1,
            "first grouped matrix page should emit one row for case={case_name}",
        );
        assert_eq!(
            page_1.rows()[0].group_key(),
            &[Value::Uint(expected_page_1_rank)],
            "first grouped matrix page should preserve canonical grouped key for case={case_name}",
        );

        // Phase 2: resume using continuation and verify strict suffix progression.
        let continuation = page_1
            .continuation_cursor()
            .map(crate::db::encode_cursor)
            .expect("non-terminal grouped matrix first page should emit continuation cursor");
        let page_2 = session
            .load::<PhaseEntity>()
            .group_by("rank")
            .expect("group field should resolve")
            .aggregate(crate::db::count())
            .offset(offset)
            .limit(1)
            .cursor(continuation)
            .execute_grouped()
            .expect("second grouped matrix page should execute from continuation");
        assert_eq!(
            page_2.rows().len(),
            1,
            "second grouped matrix page should emit one row for case={case_name}",
        );
        assert_eq!(
            page_2.rows()[0].group_key(),
            &[Value::Uint(expected_page_2_rank)],
            "grouped continuation should resume at the next canonical group key for case={case_name}",
        );
    }
}

#[test]
fn grouped_fluent_execute_continuation_payload_bytes_are_stable() {
    seed_grouped_phase_entities();
    let session = DbSession::new(DB);

    let page_1 = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .execute_grouped()
        .expect("first grouped page should execute");
    let continuation = page_1
        .continuation_cursor()
        .expect("first grouped page should emit continuation cursor");
    let actual_hex = crate::db::encode_cursor(continuation);
    assert_eq!(
        actual_hex,
        "a56776657273696f6e01697369676e61747572659820183418a418de18e8187b182f186b18c01870181c1899183c18c718251118b00f184218c118a018b818a6189818a4189418e2184818f118e71857187e18a96e6c6173745f67726f75705f6b657981a16455696e740169646972656374696f6e634173636e696e697469616c5f6f666673657400",
        "grouped execution continuation cursor wire encoding must remain stable",
    );
}

#[test]
fn grouped_fluent_execute_having_filters_groups_without_extra_continuation() {
    seed_grouped_phase_entities();
    let session = DbSession::new(DB);

    let page = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .having_aggregate(0, CompareOp::Gt, Value::Uint(1))
        .expect("having clause should append on grouped query")
        .limit(1)
        .execute_grouped()
        .expect("grouped having execution should succeed");

    assert_eq!(
        page.rows().len(),
        1,
        "having should keep only one grouped row"
    );
    assert_eq!(
        page.rows()[0].group_key(),
        &[Value::Uint(1)],
        "having over count should keep rank=1 only"
    );
    assert_eq!(
        page.rows()[0].aggregate_values(),
        &[Value::Uint(2)],
        "having-filtered group should preserve aggregate payload"
    );
    assert!(
        page.continuation_cursor().is_none(),
        "having-filtered grouped page should not emit continuation when no additional matching groups exist"
    );
}

#[test]
fn grouped_fluent_execute_having_supports_group_key_symbol_filtering() {
    seed_grouped_phase_entities();
    let session = DbSession::new(DB);

    let page = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .having_group("rank", CompareOp::Eq, Value::Uint(2))
        .expect("group-key having clause should append on grouped query")
        .execute_grouped()
        .expect("grouped having execution should succeed");

    assert_eq!(
        page.rows().len(),
        1,
        "group-key having should emit one matching group"
    );
    assert_eq!(
        page.rows()[0].group_key(),
        &[Value::Uint(2)],
        "group-key having should keep only rank=2 group"
    );
    assert_eq!(
        page.rows()[0].aggregate_values(),
        &[Value::Uint(1)],
        "group-key having should preserve grouped count payload"
    );
}

#[test]
fn grouped_fluent_execute_count_distinct_matches_group_count_for_id_terminals() {
    seed_grouped_phase_entities();
    let session = DbSession::new(DB);

    let grouped_count = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .execute_grouped()
        .expect("grouped count should execute");
    let grouped_count_distinct = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count().distinct())
        .execute_grouped()
        .expect("grouped count distinct should execute");

    assert_eq!(
        grouped_count_distinct.rows().len(),
        grouped_count.rows().len(),
        "grouped distinct count should emit one row per canonical group",
    );
    for (count_row, distinct_row) in grouped_count
        .rows()
        .iter()
        .zip(grouped_count_distinct.rows().iter())
    {
        assert_eq!(
            distinct_row.group_key(),
            count_row.group_key(),
            "grouped distinct count should preserve canonical group ordering",
        );
        assert_eq!(
            distinct_row.aggregate_values(),
            count_row.aggregate_values(),
            "grouped distinct count should match grouped count for id-target terminals",
        );
    }
}

#[test]
fn grouped_fluent_execute_count_distinct_pagination_does_not_split_single_group() {
    seed_single_group_phase_entities();
    let session = DbSession::new(DB);

    let page = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count().distinct())
        .limit(1)
        .execute_grouped()
        .expect("single-group grouped distinct page should execute");

    assert_eq!(
        page.rows().len(),
        1,
        "single-group distinct page should emit one group",
    );
    assert_eq!(
        page.rows()[0].group_key(),
        &[Value::Uint(1)],
        "single-group distinct page should preserve canonical grouped key",
    );
    assert_eq!(
        page.rows()[0].aggregate_values(),
        &[Value::Uint(3)],
        "single group distinct count should reflect all unique id rows for that group",
    );
    assert!(
        page.continuation_cursor().is_none(),
        "single grouped distinct result must not emit continuation cursor that could split one group",
    );
}

#[test]
fn grouped_fluent_execute_global_count_distinct_field_emits_single_row() {
    seed_grouped_phase_entities();
    let session = DbSession::new(DB);

    let page = session
        .load::<PhaseEntity>()
        .aggregate(crate::db::count_by("rank").distinct())
        .execute_grouped()
        .expect("global grouped count(distinct rank) should execute");

    assert_eq!(
        page.rows().len(),
        1,
        "global grouped distinct should emit one row"
    );
    assert_eq!(
        page.rows()[0].group_key(),
        &[] as &[Value],
        "global grouped distinct should use empty grouped key",
    );
    assert_eq!(
        page.rows()[0].aggregate_values(),
        &[Value::Uint(2)],
        "global grouped count(distinct rank) should count unique rank values",
    );
    assert!(
        page.continuation_cursor().is_none(),
        "global grouped distinct aggregates must not emit continuation cursors",
    );
}

#[test]
fn grouped_fluent_execute_global_sum_distinct_field_emits_single_row() {
    seed_grouped_phase_entities();
    let session = DbSession::new(DB);

    let page = session
        .load::<PhaseEntity>()
        .aggregate(crate::db::sum("rank").distinct())
        .execute_grouped()
        .expect("global grouped sum(distinct rank) should execute");

    assert_eq!(
        page.rows().len(),
        1,
        "global grouped distinct should emit one row"
    );
    assert_eq!(
        page.rows()[0].group_key(),
        &[] as &[Value],
        "global grouped distinct should use empty grouped key",
    );
    assert_eq!(
        page.rows()[0].aggregate_values(),
        &[Value::Decimal(
            crate::types::Decimal::from_num(3_u64).expect("sum(distinct rank) decimal conversion")
        )],
        "global grouped sum(distinct rank) should sum unique rank values",
    );
    assert!(
        page.continuation_cursor().is_none(),
        "global grouped distinct aggregates must not emit continuation cursors",
    );
}

#[test]
fn grouped_fluent_execute_global_distinct_sum_rejects_continuation_cursor() {
    seed_grouped_phase_entities();
    let signature = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("rank").distinct())
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("global grouped sum(distinct rank) plan should build")
        .continuation_signature();
    let cursor = crate::db::cursor::GroupedContinuationToken::new_with_direction(
        signature,
        Vec::new(),
        crate::db::direction::Direction::Asc,
        0,
    )
    .encode()
    .expect("global grouped cursor should encode");
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .aggregate(crate::db::sum("rank").distinct())
        .limit(1)
        .cursor(crate::db::encode_cursor(cursor.as_slice()))
        .execute_grouped()
        .expect_err("global grouped distinct aggregates must reject continuation cursors");

    let QueryError::Plan(plan_err) = err else {
        panic!("global grouped continuation rejection should surface as plan-layer cursor error");
    };
    assert!(
        plan_err
            .to_string()
            .contains("do not support continuation cursors"),
        "global grouped continuation rejection reason should mention cursor incompatibility",
    );
}

#[test]
fn grouped_fluent_execute_global_distinct_count_rejects_continuation_cursor() {
    seed_grouped_phase_entities();
    let signature = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("rank").distinct())
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("global grouped count(distinct rank) plan should build")
        .continuation_signature();
    let cursor = crate::db::cursor::GroupedContinuationToken::new_with_direction(
        signature,
        Vec::new(),
        crate::db::direction::Direction::Asc,
        0,
    )
    .encode()
    .expect("global grouped cursor should encode");
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .aggregate(crate::db::count_by("rank").distinct())
        .limit(1)
        .cursor(crate::db::encode_cursor(cursor.as_slice()))
        .execute_grouped()
        .expect_err("global grouped distinct aggregates must reject continuation cursors");

    let QueryError::Plan(plan_err) = err else {
        panic!("global grouped continuation rejection should surface as plan-layer cursor error");
    };
    assert!(
        plan_err
            .to_string()
            .contains("do not support continuation cursors"),
        "global grouped continuation rejection reason should mention cursor incompatibility",
    );
}

#[test]
fn grouped_fluent_execute_rejects_cursor_without_explicit_limit() {
    seed_grouped_phase_entities();
    let signature = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .group_by("rank")
        .expect("grouped cursor-without-limit query should build")
        .aggregate(crate::db::count())
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("grouped cursor-without-limit plan should build")
        .continuation_signature();
    let cursor = crate::db::cursor::GroupedContinuationToken::new_with_direction(
        signature,
        vec![Value::Uint(1)],
        crate::db::direction::Direction::Asc,
        0,
    )
    .encode()
    .expect("grouped cursor-without-limit token should encode");
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .cursor(crate::db::encode_cursor(cursor.as_slice()))
        .execute_grouped()
        .expect_err("grouped continuation cursor should require explicit LIMIT");

    let QueryError::Plan(plan_err) = err else {
        panic!("grouped cursor-without-limit rejection should surface as plan-layer cursor error");
    };
    assert!(
        plan_err.to_string().contains("require an explicit LIMIT"),
        "grouped cursor-without-limit rejection should mention explicit LIMIT requirement",
    );
}

#[test]
fn grouped_fluent_execute_global_distinct_count_enforces_total_distinct_cap() {
    seed_grouped_phase_entities_with_filtered_middle_group();
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .aggregate(crate::db::count_by("label").distinct())
        .grouped_limits(1, 256)
        .execute_grouped()
        .expect_err("global grouped distinct should fail when total distinct cap is exceeded");

    let QueryError::Execute(err) = err else {
        panic!("global grouped distinct cap failure should surface as execution error");
    };
    assert!(
        err.as_internal().message.contains("distinct_values_total"),
        "global grouped distinct cap failure should report total distinct budget resource",
    );
}

#[test]
fn grouped_fluent_execute_rejects_cross_shape_cursor_when_only_distinct_changes() {
    seed_grouped_phase_entities();
    let session = DbSession::new(DB);

    let first_page = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .execute_grouped()
        .expect("seed grouped page should execute");
    let continuation = first_page
        .continuation_cursor()
        .map(crate::db::encode_cursor)
        .expect("seed grouped page should emit continuation cursor");

    let err = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count().distinct())
        .limit(1)
        .cursor(continuation)
        .execute_grouped()
        .expect_err("grouped continuation should fail when DISTINCT shape changes");

    let QueryError::Plan(plan_err) = err else {
        panic!("cross-shape grouped cursor rejection should surface as plan-layer cursor error");
    };
    assert!(
        plan_err
            .to_string()
            .contains("does not match query plan signature"),
        "cross-shape grouped cursor rejection should mention signature mismatch",
    );
}

#[test]
fn grouped_fluent_execute_having_pagination_skips_filtered_middle_group() {
    seed_grouped_phase_entities_with_filtered_middle_group();
    let session = DbSession::new(DB);

    let page_1 = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .having_aggregate(0, CompareOp::Gt, Value::Uint(1))
        .expect("having aggregate clause should append on grouped query")
        .limit(1)
        .execute_grouped()
        .expect("first grouped having page should execute");

    assert_eq!(
        page_1.rows().len(),
        1,
        "first grouped having page should emit one group",
    );
    assert_eq!(
        page_1.rows()[0].group_key(),
        &[Value::Uint(1)],
        "first grouped having page should emit the first matching group",
    );
    let continuation = page_1
        .continuation_cursor()
        .map(crate::db::encode_cursor)
        .expect("first grouped having page should emit continuation cursor");

    let page_2 = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .having_aggregate(0, CompareOp::Gt, Value::Uint(1))
        .expect("having aggregate clause should append on grouped query")
        .limit(1)
        .cursor(continuation)
        .execute_grouped()
        .expect("second grouped having page should execute from continuation");

    assert_eq!(
        page_2.rows().len(),
        1,
        "second grouped having page should emit one remaining matching group",
    );
    assert_eq!(
        page_2.rows()[0].group_key(),
        &[Value::Uint(3)],
        "continuation should skip filtered middle group and resume at next matching group",
    );
    assert_eq!(
        page_2.rows()[0].aggregate_values(),
        &[Value::Uint(2)],
        "second matching group should preserve grouped aggregate payload",
    );
    assert!(
        page_2.continuation_cursor().is_none(),
        "terminal grouped having page should not emit continuation cursor",
    );
}

#[test]
fn grouped_fluent_execute_does_not_split_single_group_across_pages() {
    seed_single_group_phase_entities();
    let session = DbSession::new(DB);

    let page = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .execute_grouped()
        .expect("single-group grouped page should execute");

    assert_eq!(
        page.rows().len(),
        1,
        "single-group page should emit one group"
    );
    assert_eq!(
        page.rows()[0].group_key(),
        &[Value::Uint(1)],
        "single-group page should preserve canonical grouped key"
    );
    assert_eq!(
        page.rows()[0].aggregate_values(),
        &[Value::Uint(3)],
        "single group count should reflect all rows for that group"
    );
    assert!(
        page.continuation_cursor().is_none(),
        "single grouped result must not emit continuation cursor that could split one group",
    );
}

#[test]
fn grouped_fluent_execute_supports_min_max_id_terminals() {
    let (id_a, id_b, id_c) = seed_grouped_phase_entities_with_fixed_ids();
    let (rank_1_min, rank_1_max) = if id_a <= id_b {
        (id_a, id_b)
    } else {
        (id_b, id_a)
    };
    let session = DbSession::new(DB);
    let execution = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::min())
        .aggregate(crate::db::max())
        .execute_grouped()
        .expect("grouped min/max terminals should execute");

    assert_eq!(
        execution.rows().len(),
        2,
        "grouped min/max should emit one row per canonical group"
    );
    assert_eq!(
        execution.rows()[0].group_key(),
        &[Value::Uint(1)],
        "rank=1 group should be first in canonical grouped-key order"
    );
    assert_eq!(
        execution.rows()[0].aggregate_values(),
        &[Value::Ulid(rank_1_min), Value::Ulid(rank_1_max)],
        "grouped min/max terminal outputs should preserve declaration order for rank=1",
    );
    assert_eq!(
        execution.rows()[1].group_key(),
        &[Value::Uint(2)],
        "rank=2 group should follow rank=1"
    );
    assert_eq!(
        execution.rows()[1].aggregate_values(),
        &[Value::Ulid(id_c), Value::Ulid(id_c)],
        "single-row groups should return same id for grouped min/max terminals",
    );
}

#[test]
fn grouped_fluent_execute_supports_exists_first_last_terminals() {
    let (id_a, id_b, id_c) = seed_grouped_phase_entities_with_fixed_ids();
    let session = DbSession::new(DB);
    let execution = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::exists())
        .aggregate(crate::db::first())
        .aggregate(crate::db::last())
        .execute_grouped()
        .expect("grouped exists/first/last terminals should execute");

    assert_eq!(
        execution.rows().len(),
        2,
        "grouped exists/first/last should emit one row per canonical group"
    );

    let rank_1_row = &execution.rows()[0];
    assert_eq!(
        rank_1_row.group_key(),
        &[Value::Uint(1)],
        "rank=1 group should be first in canonical grouped-key order"
    );
    let rank_1_values = rank_1_row.aggregate_values();
    let [rank_1_exists, rank_1_first, rank_1_last] = rank_1_values else {
        panic!("rank=1 grouped exists/first/last should produce exactly three aggregate values")
    };
    assert_eq!(
        rank_1_exists,
        &Value::Bool(true),
        "grouped exists terminal should report true for non-empty groups"
    );
    let rank_1_first = match rank_1_first {
        Value::Ulid(id) => id,
        other => panic!("grouped first terminal should return Ulid, found {other:?}"),
    };
    let rank_1_last = match rank_1_last {
        Value::Ulid(id) => id,
        other => panic!("grouped last terminal should return Ulid, found {other:?}"),
    };
    assert!(
        (rank_1_first == &id_a || rank_1_first == &id_b)
            && (rank_1_last == &id_a || rank_1_last == &id_b),
        "grouped first/last ids should come from the rank=1 group"
    );
    assert_ne!(
        rank_1_first, rank_1_last,
        "grouped first/last should differ for the two-row rank=1 group"
    );

    let rank_2_row = &execution.rows()[1];
    assert_eq!(
        rank_2_row.group_key(),
        &[Value::Uint(2)],
        "rank=2 group should follow rank=1"
    );
    assert_eq!(
        rank_2_row.aggregate_values(),
        &[Value::Bool(true), Value::Ulid(id_c), Value::Ulid(id_c)],
        "single-row groups should return the same id for grouped first/last with exists=true"
    );
}

#[test]
fn grouped_query_page_builder_rejects_grouped_shape() {
    let session = DbSession::new(DB);

    let Err(err) = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(1)
        .page()
    else {
        panic!("grouped query should not use scalar page builder");
    };

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::GroupedRequiresExecuteGrouped)
        ),
        "grouped page builder misuse should fail as intent error"
    );
}

#[test]
fn grouped_query_scalar_execute_rejects_grouped_shape() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .execute()
        .expect_err("grouped query should not execute through scalar load path");

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::GroupedRequiresExecuteGrouped)
        ),
        "grouped scalar execute misuse should fail as intent error"
    );
}

#[test]
fn grouped_field_target_min_by_is_rejected_in_grouped_v1() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::min_by("rank"))
        .execute_grouped()
        .expect_err("grouped field-target min should be deferred in grouped v1");

    assert!(
        matches!(
            err,
            QueryError::Plan(plan_err)
                if matches!(
                    plan_err.as_ref(),
                    crate::db::query::plan::PlanError::Semantic(inner)
                        if matches!(
                            inner.as_ref(),
                            crate::db::query::plan::SemanticPlanError::Group(group_err)
                                if matches!(
                                    group_err.as_ref(),
                                    crate::db::query::plan::GroupPlanError::FieldTargetAggregatesUnsupported { .. }
                                )
                        )
                )
        ),
        "grouped field-target min should fail with grouped field-target policy error"
    );
}

#[test]
fn grouped_field_target_max_by_is_rejected_in_grouped_v1() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .aggregate(crate::db::max_by("rank"))
        .execute_grouped()
        .expect_err("grouped field-target max should be deferred in grouped v1");

    assert!(
        matches!(
            err,
            QueryError::Plan(plan_err)
                if matches!(
                    plan_err.as_ref(),
                    crate::db::query::plan::PlanError::Semantic(inner)
                        if matches!(
                            inner.as_ref(),
                            crate::db::query::plan::SemanticPlanError::Group(group_err)
                                if matches!(
                                    group_err.as_ref(),
                                    crate::db::query::plan::GroupPlanError::FieldTargetAggregatesUnsupported { .. }
                                )
                        )
                )
        ),
        "grouped field-target max should fail with grouped field-target policy error"
    );
}

#[test]
fn non_paged_execute_rejects_cursor_token() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .cursor("00")
        .execute()
        .expect_err("non-paged execute should reject cursor tokens");

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::CursorRequiresPagedExecution)
        ),
        "non-paged execute should reject cursor tokens as intent misuse"
    );
}

#[test]
fn non_paged_aggregate_terminal_rejects_cursor_token() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .cursor("00")
        .last()
        .expect_err("non-paged aggregate terminals should reject cursor tokens");

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::CursorRequiresPagedExecution)
        ),
        "non-paged aggregate terminal should reject cursor tokens as intent misuse"
    );
}

#[test]
fn non_paged_take_terminal_rejects_cursor_token() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .cursor("00")
        .take(1)
        .expect_err("non-paged take terminal should reject cursor tokens");

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::CursorRequiresPagedExecution)
        ),
        "non-paged take terminal should reject cursor tokens as intent misuse"
    );
}

#[test]
fn non_paged_top_k_by_terminal_rejects_cursor_token() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .cursor("00")
        .top_k_by("rank", 1)
        .expect_err("non-paged top_k_by terminal should reject cursor tokens");

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::CursorRequiresPagedExecution)
        ),
        "non-paged top_k_by terminal should reject cursor tokens as intent misuse"
    );
}

#[test]
fn non_paged_bottom_k_by_terminal_rejects_cursor_token() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .cursor("00")
        .bottom_k_by("rank", 1)
        .expect_err("non-paged bottom_k_by terminal should reject cursor tokens");

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::CursorRequiresPagedExecution)
        ),
        "non-paged bottom_k_by terminal should reject cursor tokens as intent misuse"
    );
}

#[test]
fn non_paged_top_k_by_values_terminal_rejects_cursor_token() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .cursor("00")
        .top_k_by_values("rank", 1)
        .expect_err("non-paged top_k_by_values terminal should reject cursor tokens");

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::CursorRequiresPagedExecution)
        ),
        "non-paged top_k_by_values terminal should reject cursor tokens as intent misuse"
    );
}

#[test]
fn non_paged_bottom_k_by_values_terminal_rejects_cursor_token() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .cursor("00")
        .bottom_k_by_values("rank", 1)
        .expect_err("non-paged bottom_k_by_values terminal should reject cursor tokens");

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::CursorRequiresPagedExecution)
        ),
        "non-paged bottom_k_by_values terminal should reject cursor tokens as intent misuse"
    );
}

#[test]
fn non_paged_top_k_by_with_ids_terminal_rejects_cursor_token() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .cursor("00")
        .top_k_by_with_ids("rank", 1)
        .expect_err("non-paged top_k_by_with_ids terminal should reject cursor tokens");

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::CursorRequiresPagedExecution)
        ),
        "non-paged top_k_by_with_ids terminal should reject cursor tokens as intent misuse"
    );
}

#[test]
fn non_paged_bottom_k_by_with_ids_terminal_rejects_cursor_token() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(1)
        .cursor("00")
        .bottom_k_by_with_ids("rank", 1)
        .expect_err("non-paged bottom_k_by_with_ids terminal should reject cursor tokens");

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::CursorRequiresPagedExecution)
        ),
        "non-paged bottom_k_by_with_ids terminal should reject cursor tokens as intent misuse"
    );
}

#[test]
fn invalid_order_field_remains_plan_error_not_execute_error() {
    let session = DbSession::new(DB);

    let err = session
        .load::<PhaseEntity>()
        .order_by("definitely_not_a_field")
        .execute()
        .expect_err("unknown order field should fail during planning");

    let QueryError::Plan(plan_err) = err else {
        panic!("unknown order field must be classified as plan error");
    };

    assert!(
        matches!(
            *plan_err,
            crate::db::query::plan::PlanError::Semantic(ref inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::SemanticPlanError::Order(inner)
                        if matches!(
                            inner.as_ref(),
                            crate::db::query::plan::validate::OrderPlanError::UnknownField { field }
                                if field == "definitely_not_a_field"
                        )
                )
        ),
        "unknown order field must preserve order-plan classification"
    );
}
