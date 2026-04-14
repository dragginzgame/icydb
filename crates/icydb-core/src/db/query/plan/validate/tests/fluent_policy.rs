//! Module: db::query::plan::validate::tests::fluent_policy
//! Covers owner-level fluent planner policy checks around paged and non-paged
//! query entrypoints.
//! Does not own: leaf-local fluent policy helper implementation details.
//! Boundary: keeps fluent planner policy regressions in the validate
//! subsystem `tests/` boundary.

use crate::db::query::plan::{
    LoadSpec,
    validate::{
        FluentLoadPolicyViolation, validate_fluent_non_paged_mode, validate_fluent_paged_mode,
    },
};

fn cursor_spec(limit: Option<u32>) -> LoadSpec {
    LoadSpec { limit, offset: 0 }
}

#[test]
fn fluent_non_paged_rejects_cursor_tokens() {
    let err = validate_fluent_non_paged_mode(true, false)
        .expect_err("non-paged fluent mode must reject cursor tokens");
    assert!(matches!(
        err,
        FluentLoadPolicyViolation::CursorRequiresPagedExecution
    ));
}

#[test]
fn fluent_non_paged_grouped_shapes_pass() {
    validate_fluent_non_paged_mode(false, true)
        .expect("non-paged fluent mode should admit grouped shapes");
}

#[test]
fn fluent_non_paged_prefers_cursor_error_when_both_violate() {
    validate_fluent_non_paged_mode(true, true)
        .expect("grouped fluent mode should admit direct cursor execution");
}

#[test]
fn fluent_paged_rejects_grouped_shapes() {
    let err = validate_fluent_paged_mode(true, true, Some(cursor_spec(Some(10))))
        .expect_err("paged fluent mode must reject grouped shapes");
    assert!(matches!(
        err,
        FluentLoadPolicyViolation::GroupedRequiresDirectExecute
    ));
}

#[test]
fn fluent_paged_without_cursor_spec_passes_without_order_requirement() {
    validate_fluent_paged_mode(false, false, None)
        .expect("paged fluent mode without cursor spec should pass");
}

#[test]
fn fluent_paged_cursor_requires_order() {
    let err = validate_fluent_paged_mode(false, false, Some(cursor_spec(Some(10))))
        .expect_err("cursor pagination must require explicit ORDER BY");
    assert!(matches!(
        err,
        FluentLoadPolicyViolation::CursorRequiresOrder
    ));
}

#[test]
fn fluent_paged_cursor_requires_limit() {
    let err = validate_fluent_paged_mode(false, true, Some(cursor_spec(None)))
        .expect_err("cursor pagination must require explicit LIMIT");
    assert!(matches!(
        err,
        FluentLoadPolicyViolation::CursorRequiresLimit
    ));
}

#[test]
fn fluent_paged_cursor_with_order_and_limit_passes() {
    validate_fluent_paged_mode(false, true, Some(cursor_spec(Some(25))))
        .expect("cursor pagination with ORDER BY + LIMIT should pass");
}
