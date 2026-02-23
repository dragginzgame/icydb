use super::*;

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
    let crate::db::query::plan::CursorPlanError::InvalidContinuationCursor { reason } =
        inner.as_ref()
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
    let crate::db::query::plan::CursorPlanError::InvalidContinuationCursor { reason } =
        inner.as_ref()
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
    let crate::db::query::plan::CursorPlanError::InvalidContinuationCursor { reason } =
        inner.as_ref()
    else {
        panic!("empty cursor token should be classified as invalid continuation cursor");
    };
    assert!(
        matches!(reason, crate::db::codec::cursor::CursorDecodeError::Empty),
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
    let crate::db::query::plan::CursorPlanError::InvalidContinuationCursorPayload { reason } =
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
                    crate::db::query::plan::OrderPlanError::UnknownField { field }
                        if field == "definitely_not_a_field"
                )
        ),
        "unknown order field must preserve order-plan classification"
    );
}
