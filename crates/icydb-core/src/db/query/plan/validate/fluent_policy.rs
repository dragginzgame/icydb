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

const FLUENT_NON_PAGED_POLICY_RULES: &[FluentNonPagedPolicyRule] =
    &[fluent_non_paged_cursor_token_violation];

fn fluent_non_paged_cursor_token_violation(
    ctx: FluentNonPagedPolicyContext,
) -> Option<FluentLoadPolicyViolation> {
    (ctx.has_cursor_token && !ctx.has_grouping)
        .then_some(FluentLoadPolicyViolation::cursor_requires_paged_execution())
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

// Evaluate one ordered fluent policy rule set and lift the first violation
// into the conventional `Result<(), _>` shell used by planner validation.
fn validate_fluent_policy_rules<C>(
    rules: &[fn(C) -> Option<FluentLoadPolicyViolation>],
    context: C,
) -> Result<(), FluentLoadPolicyViolation>
where
    C: Copy,
{
    first_violated_rule(rules, context).map_or(Ok(()), Err)
}

/// Validate fluent non-paged load entry policy.
pub(crate) fn validate_fluent_non_paged_mode(
    has_cursor_token: bool,
    has_grouping: bool,
) -> Result<(), FluentLoadPolicyViolation> {
    let context = FluentNonPagedPolicyContext::new(has_cursor_token, has_grouping);
    validate_fluent_policy_rules(FLUENT_NON_PAGED_POLICY_RULES, context)
}

/// Validate fluent paged load entry policy.
pub(crate) fn validate_fluent_paged_mode(
    has_grouping: bool,
    has_explicit_order: bool,
    spec: Option<LoadSpec>,
) -> Result<(), FluentLoadPolicyViolation> {
    let context = FluentPagedPolicyContext::new(has_grouping);
    validate_fluent_policy_rules(FLUENT_PAGED_POLICY_RULES, context).and_then(|()| match spec {
        Some(spec) => validate_cursor_paging_requirements(has_explicit_order, spec)
            .map_err(FluentLoadPolicyViolation::from),
        None => Ok(()),
    })
}
