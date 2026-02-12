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
fn paged_query_builder_rejects_offset() {
    let session = DbSession::new(DB);

    let Err(err) = session
        .load::<PhaseEntity>()
        .order_by("rank")
        .limit(10)
        .offset(2)
        .page()
    else {
        panic!("paged builder should reject offset usage")
    };

    assert!(
        matches!(
            err,
            QueryError::Intent(IntentError::CursorWithOffsetUnsupported)
        ),
        "offset should be rejected at page-builder boundary"
    );
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

    assert!(
        matches!(
            err,
            QueryError::Plan(ref plan_err)
                if matches!(
                    **plan_err,
                    crate::db::query::plan::PlanError::InvalidContinuationCursor { .. }
                )
        ),
        "invalid cursor token should be classified as plan-time cursor error"
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

    assert!(
        matches!(
            err,
            QueryError::Plan(ref plan_err)
                if matches!(
                    **plan_err,
                    crate::db::query::plan::PlanError::InvalidContinuationCursor { .. }
                )
        ),
        "odd-length cursor token should be classified as plan-time cursor error"
    );
}
