//! Module: db::executor::planning::route::planner::feasibility::gates
//! Defines feasibility gates that reject route plans before executor shaping.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::{contracts::first_violated_rule, executor::aggregate::AggregateKind};

///
/// IndexRangeLimitGateReason
///
/// Route-owned reasons why index-range limit pushdown derivation is skipped.
/// Keeps feasibility policy explicit and additive as route rules evolve.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IndexRangeLimitGateReason {
    GroupedIntent,
}

///
/// IndexRangeLimitGateContext
///
/// Minimal policy context for index-range limit pushdown pre-gates.
/// This context is intentionally pure and independent from execution mechanics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IndexRangeLimitGateContext {
    grouped: bool,
}

impl IndexRangeLimitGateContext {
    #[must_use]
    const fn new(grouped: bool) -> Self {
        Self { grouped }
    }
}

type IndexRangeLimitFeasibilityRule =
    fn(IndexRangeLimitGateContext) -> Option<IndexRangeLimitGateReason>;

const INDEX_RANGE_LIMIT_FEASIBILITY_RULES: &[IndexRangeLimitFeasibilityRule] =
    &[index_range_limit_gate_grouped_violation];

fn index_range_limit_gate_grouped_violation(
    ctx: IndexRangeLimitGateContext,
) -> Option<IndexRangeLimitGateReason> {
    ctx.grouped
        .then_some(IndexRangeLimitGateReason::GroupedIntent)
}

///
/// LoadScanHintGateReason
///
/// Route-owned reasons why load-bound scan hints are suppressed.
/// Applies to load probe fetch hints and load scan-budget hints uniformly.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LoadScanHintGateReason {
    AggregateIntent,
    GroupedIntent,
}

///
/// LoadScanHintGateContext
///
/// Pure policy context for load-bound scan hint eligibility.
/// This remains independent from hint derivation mechanics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LoadScanHintGateContext {
    has_aggregate: bool,
    grouped: bool,
}

impl LoadScanHintGateContext {
    #[must_use]
    const fn new(has_aggregate: bool, grouped: bool) -> Self {
        Self {
            has_aggregate,
            grouped,
        }
    }
}

type LoadScanHintFeasibilityRule = fn(LoadScanHintGateContext) -> Option<LoadScanHintGateReason>;

const LOAD_SCAN_HINT_FEASIBILITY_RULES: &[LoadScanHintFeasibilityRule] = &[
    load_scan_hint_gate_aggregate_intent_violation,
    load_scan_hint_gate_grouped_intent_violation,
];

fn load_scan_hint_gate_aggregate_intent_violation(
    ctx: LoadScanHintGateContext,
) -> Option<LoadScanHintGateReason> {
    ctx.has_aggregate
        .then_some(LoadScanHintGateReason::AggregateIntent)
}

fn load_scan_hint_gate_grouped_intent_violation(
    ctx: LoadScanHintGateContext,
) -> Option<LoadScanHintGateReason> {
    ctx.grouped.then_some(LoadScanHintGateReason::GroupedIntent)
}

#[must_use]
pub(super) fn index_range_limit_pushdown_allowed_for_grouped(grouped: bool) -> bool {
    let gate = IndexRangeLimitGateContext::new(grouped);
    let rejection = first_violated_rule(INDEX_RANGE_LIMIT_FEASIBILITY_RULES, gate);

    rejection.is_none()
}

#[must_use]
pub(super) fn load_scan_hints_allowed_for_intent(
    kind: Option<AggregateKind>,
    grouped: bool,
) -> bool {
    let gate = LoadScanHintGateContext::new(kind.is_some(), grouped);
    let rejection = first_violated_rule(LOAD_SCAN_HINT_FEASIBILITY_RULES, gate);

    kind.is_none() && rejection.is_none()
}
