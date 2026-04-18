//! Module: query::plan::validate::fluent_policy
//! Responsibility: fluent API policy validation for paged/non-paged grouped and cursor usage.
//! Does not own: core planner semantic validation outside fluent policy constraints.
//! Boundary: evaluates fluent-entrypoint policy rules into planner policy violations.

use crate::db::query::plan::{
    LoadSpec, validate::FluentLoadPolicyViolation,
    validate::cursor_policy::validate_cursor_paging_requirements,
};

/// Validate fluent non-paged load entry policy.
pub(crate) const fn validate_fluent_non_paged_mode(
    has_cursor_token: bool,
    has_grouping: bool,
) -> Result<(), FluentLoadPolicyViolation> {
    if has_cursor_token && !has_grouping {
        return Err(FluentLoadPolicyViolation::cursor_requires_paged_execution());
    }

    Ok(())
}

/// Validate fluent paged load entry policy.
pub(crate) fn validate_fluent_paged_mode(
    has_grouping: bool,
    has_explicit_order: bool,
    spec: Option<LoadSpec>,
) -> Result<(), FluentLoadPolicyViolation> {
    // Grouped fluent queries still require the direct execution lane even when
    // pagination helpers are in play, so reject that shape before cursor checks.
    if has_grouping {
        return Err(FluentLoadPolicyViolation::grouped_requires_direct_execute());
    }

    match spec {
        Some(spec) => validate_cursor_paging_requirements(has_explicit_order, spec)
            .map_err(FluentLoadPolicyViolation::from),
        None => Ok(()),
    }
}
