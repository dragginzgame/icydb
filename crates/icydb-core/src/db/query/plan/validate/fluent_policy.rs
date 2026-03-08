use crate::db::query::plan::{
    LoadSpec,
    validate::cursor_policy::validate_cursor_paging_requirements,
    validate::{CursorPagingPolicyError, FluentLoadPolicyViolation},
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

// Declarative fluent non-paged policy rule evaluated in order.
#[derive(Clone, Copy)]
struct FluentNonPagedPolicyRule {
    reason: FluentLoadPolicyViolation,
    violated: fn(FluentNonPagedPolicyContext) -> bool,
}

impl FluentNonPagedPolicyRule {
    #[must_use]
    const fn new(
        reason: FluentLoadPolicyViolation,
        violated: fn(FluentNonPagedPolicyContext) -> bool,
    ) -> Self {
        Self { reason, violated }
    }
}

const FLUENT_NON_PAGED_POLICY_RULES: &[FluentNonPagedPolicyRule] = &[
    FluentNonPagedPolicyRule::new(
        FluentLoadPolicyViolation::CursorRequiresPagedExecution,
        fluent_non_paged_cursor_token_violated,
    ),
    FluentNonPagedPolicyRule::new(
        FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped,
        fluent_non_paged_grouped_violated,
    ),
];

const fn fluent_non_paged_cursor_token_violated(ctx: FluentNonPagedPolicyContext) -> bool {
    ctx.has_cursor_token
}

const fn fluent_non_paged_grouped_violated(ctx: FluentNonPagedPolicyContext) -> bool {
    ctx.has_grouping
}

fn first_fluent_non_paged_policy_violation(
    ctx: FluentNonPagedPolicyContext,
) -> Option<FluentLoadPolicyViolation> {
    for rule in FLUENT_NON_PAGED_POLICY_RULES {
        if (rule.violated)(ctx) {
            return Some(rule.reason);
        }
    }

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

// Declarative fluent paged policy rule evaluated before cursor paging checks.
#[derive(Clone, Copy)]
struct FluentPagedPolicyRule {
    reason: FluentLoadPolicyViolation,
    violated: fn(FluentPagedPolicyContext) -> bool,
}

impl FluentPagedPolicyRule {
    #[must_use]
    const fn new(
        reason: FluentLoadPolicyViolation,
        violated: fn(FluentPagedPolicyContext) -> bool,
    ) -> Self {
        Self { reason, violated }
    }
}

const FLUENT_PAGED_POLICY_RULES: &[FluentPagedPolicyRule] = &[FluentPagedPolicyRule::new(
    FluentLoadPolicyViolation::GroupedRequiresExecuteGrouped,
    fluent_paged_grouped_violated,
)];

const fn fluent_paged_grouped_violated(ctx: FluentPagedPolicyContext) -> bool {
    ctx.has_grouping
}

fn first_fluent_paged_policy_violation(
    ctx: FluentPagedPolicyContext,
) -> Option<FluentLoadPolicyViolation> {
    for rule in FLUENT_PAGED_POLICY_RULES {
        if (rule.violated)(ctx) {
            return Some(rule.reason);
        }
    }

    None
}

/// Validate fluent non-paged load entry policy.
pub(crate) fn validate_fluent_non_paged_mode(
    has_cursor_token: bool,
    has_grouping: bool,
) -> Result<(), FluentLoadPolicyViolation> {
    let context = FluentNonPagedPolicyContext::new(has_cursor_token, has_grouping);
    if let Some(reason) = first_fluent_non_paged_policy_violation(context) {
        return Err(reason);
    }

    Ok(())
}

/// Validate fluent paged load entry policy.
pub(crate) fn validate_fluent_paged_mode(
    has_grouping: bool,
    has_explicit_order: bool,
    spec: Option<LoadSpec>,
) -> Result<(), FluentLoadPolicyViolation> {
    let context = FluentPagedPolicyContext::new(has_grouping);
    if let Some(reason) = first_fluent_paged_policy_violation(context) {
        return Err(reason);
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
