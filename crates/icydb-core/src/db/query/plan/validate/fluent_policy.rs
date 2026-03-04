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
