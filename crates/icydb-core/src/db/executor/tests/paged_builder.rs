use super::*;

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
            QueryError::Execute(crate::error::InternalError {
                class: crate::error::ErrorClass::InvariantViolation,
                origin: crate::error::ErrorOrigin::Query,
                ..
            })
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
        .group_count()
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
        .group_count()
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
fn grouped_fluent_execute_supports_min_max_id_terminals() {
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
        .group_min()
        .group_max()
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
fn grouped_query_page_builder_rejects_grouped_shape() {
    let session = DbSession::new(DB);

    let Err(err) = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .group_count()
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
        .group_count()
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

    let Err(err) = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .group_min_by("rank")
    else {
        panic!("grouped field-target min should be deferred in grouped v1");
    };

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::GroupedFieldTargetExtremaUnsupported)
        ),
        "grouped field-target min should fail fast at intent boundary"
    );
}

#[test]
fn grouped_field_target_max_by_is_rejected_in_grouped_v1() {
    let session = DbSession::new(DB);

    let Err(err) = session
        .load::<PhaseEntity>()
        .group_by("rank")
        .expect("group field should resolve")
        .group_max_by("rank")
    else {
        panic!("grouped field-target max should be deferred in grouped v1");
    };

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::GroupedFieldTargetExtremaUnsupported)
        ),
        "grouped field-target max should fail fast at intent boundary"
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
            crate::db::query::plan::PlanError::Order(ref inner)
                if matches!(
                    inner.as_ref(),
                    crate::db::query::plan::validate::OrderPlanError::UnknownField { field }
                        if field == "definitely_not_a_field"
                )
        ),
        "unknown order field must preserve order-plan classification"
    );
}
