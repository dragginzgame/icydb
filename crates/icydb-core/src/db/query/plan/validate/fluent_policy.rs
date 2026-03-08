use crate::db::query::plan::{
    LoadSpec,
    validate::cursor_policy::validate_cursor_paging_requirements,
    validate::{CursorPagingPolicyError, FluentLoadPolicyViolation},
};

/// Validate fluent non-paged load entry policy.
pub(crate) const fn validate_fluent_non_paged_mode(
    has_cursor_token: bool,
    has_grouping: bool,
) -> Result<(), FluentLoadPolicyViolation> {
    if has_cursor_token {
        return Err(FluentLoadPolicyViolation::CursorRequiresPagedExecution);
    }
    if has_grouping {
        return Err(FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped);
    }

    Ok(())
}

/// Validate fluent paged load entry policy.
pub(crate) fn validate_fluent_paged_mode(
    has_grouping: bool,
    has_explicit_order: bool,
    spec: Option<LoadSpec>,
) -> Result<(), FluentLoadPolicyViolation> {
    if has_grouping {
        return Err(FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped);
    }

    let Some(spec) = spec else {
        return Ok(());
    };

    validate_cursor_paging_requirements(has_explicit_order, spec).map_err(|err| match err {
        CursorPagingPolicyError::CursorRequiresOrder => {
            FluentLoadPolicyViolation::CursorRequiresOrder
        }
        CursorPagingPolicyError::CursorRequiresLimit => {
            FluentLoadPolicyViolation::CursorRequiresLimit
        }
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
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
    fn fluent_non_paged_rejects_grouped_shapes() {
        let err = validate_fluent_non_paged_mode(false, true)
            .expect_err("non-paged fluent mode must reject grouped shapes");
        assert!(matches!(
            err,
            FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped
        ));
    }

    #[test]
    fn fluent_non_paged_prefers_cursor_error_when_both_violate() {
        let err = validate_fluent_non_paged_mode(true, true)
            .expect_err("cursor token + grouped in non-paged mode must fail");
        assert!(matches!(
            err,
            FluentLoadPolicyViolation::CursorRequiresPagedExecution
        ));
    }

    #[test]
    fn fluent_paged_rejects_grouped_shapes() {
        let err = validate_fluent_paged_mode(true, true, Some(cursor_spec(Some(10))))
            .expect_err("paged fluent mode must reject grouped shapes");
        assert!(matches!(
            err,
            FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped
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
}
