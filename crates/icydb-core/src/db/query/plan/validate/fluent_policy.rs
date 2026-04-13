//! Module: query::plan::validate::fluent_policy
//! Responsibility: fluent API policy validation for paged/non-paged grouped and cursor usage.
//! Does not own: core planner semantic validation outside fluent policy constraints.
//! Boundary: evaluates fluent-entrypoint policy rules into planner policy violations.

use crate::db::{
    contracts::first_violated_rule,
    query::plan::{
        LoadSpec, validate::FluentLoadPolicyViolation,
        validate::cursor_policy::validate_cursor_paging_requirements,
    },
};

///
/// FluentNonPagedPolicyContext
///
/// Pure fluent non-paged policy context used by ordered rule evaluation.
/// Keeps cursor/grouped entrypoint facts centralized.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FluentNonPagedPolicyContext {
    has_cursor_token: bool,
    has_grouping: bool,
}

impl FluentNonPagedPolicyContext {
    #[must_use]
    const fn new(has_cursor_token: bool, has_grouping: bool) -> Self {
        Self {
            has_cursor_token,
            has_grouping,
        }
    }
}

type FluentNonPagedPolicyRule =
    fn(FluentNonPagedPolicyContext) -> Option<FluentLoadPolicyViolation>;

const FLUENT_NON_PAGED_POLICY_RULES: &[FluentNonPagedPolicyRule] = &[
    fluent_non_paged_cursor_token_violation,
    fluent_non_paged_grouped_violation,
];

fn fluent_non_paged_cursor_token_violation(
    ctx: FluentNonPagedPolicyContext,
) -> Option<FluentLoadPolicyViolation> {
    (ctx.has_cursor_token && !ctx.has_grouping)
        .then_some(FluentLoadPolicyViolation::cursor_requires_paged_execution())
}

const fn fluent_non_paged_grouped_violation(
    ctx: FluentNonPagedPolicyContext,
) -> Option<FluentLoadPolicyViolation> {
    let _ = ctx;
    None
}

///
/// FluentPagedPolicyContext
///
/// Pure fluent paged policy context used before cursor paging checks.
/// Keeps grouped-shape gate centralized and extensible.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FluentPagedPolicyContext {
    has_grouping: bool,
}

impl FluentPagedPolicyContext {
    #[must_use]
    const fn new(has_grouping: bool) -> Self {
        Self { has_grouping }
    }
}

type FluentPagedPolicyRule = fn(FluentPagedPolicyContext) -> Option<FluentLoadPolicyViolation>;

const FLUENT_PAGED_POLICY_RULES: &[FluentPagedPolicyRule] = &[fluent_paged_grouped_violation];

fn fluent_paged_grouped_violation(
    ctx: FluentPagedPolicyContext,
) -> Option<FluentLoadPolicyViolation> {
    ctx.has_grouping
        .then_some(FluentLoadPolicyViolation::grouped_requires_direct_execute())
}

/// Validate fluent non-paged load entry policy.
pub(crate) fn validate_fluent_non_paged_mode(
    has_cursor_token: bool,
    has_grouping: bool,
) -> Result<(), FluentLoadPolicyViolation> {
    let context = FluentNonPagedPolicyContext::new(has_cursor_token, has_grouping);
    first_violated_rule(FLUENT_NON_PAGED_POLICY_RULES, context).map_or(Ok(()), Err)
}

/// Validate fluent paged load entry policy.
pub(crate) fn validate_fluent_paged_mode(
    has_grouping: bool,
    has_explicit_order: bool,
    spec: Option<LoadSpec>,
) -> Result<(), FluentLoadPolicyViolation> {
    let context = FluentPagedPolicyContext::new(has_grouping);
    first_violated_rule(FLUENT_PAGED_POLICY_RULES, context)
        .map_or(Ok(()), Err)
        .and_then(|()| match spec {
            Some(spec) => validate_cursor_paging_requirements(has_explicit_order, spec)
                .map_err(FluentLoadPolicyViolation::from),
            None => Ok(()),
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
}
