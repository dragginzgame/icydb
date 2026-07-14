//! Planned route and execution-shape counter mutation helpers.
//! Does not own planning, route selection, or metrics event dispatch.

use crate::metrics::{
    sink::{GroupedPlanExecutionMode, PlanChoiceReason, PlanKind},
    state as metrics,
};

// Keep the legacy coarse global plan groups in lockstep with the detailed
// route counters so existing dashboards and newer diagnostics can agree.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_plan_kind(
    ops: &mut metrics::EventOps,
    kind: PlanKind,
) {
    #[remain::sorted]
    match kind {
        PlanKind::ByKey => {
            ops.plan_keys = ops.plan_keys.saturating_add(1);
            ops.plan_by_key = ops.plan_by_key.saturating_add(1);
        }
        PlanKind::ByKeys => {
            ops.plan_keys = ops.plan_keys.saturating_add(1);
            ops.plan_by_keys = ops.plan_by_keys.saturating_add(1);
        }
        PlanKind::FullScan => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_explicit_full_scan = ops.plan_explicit_full_scan.saturating_add(1);
        }
        PlanKind::IndexBranchSet => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_branch_set = ops.plan_index_branch_set.saturating_add(1);
        }
        PlanKind::IndexMultiLookup => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_multi_lookup = ops.plan_index_multi_lookup.saturating_add(1);
        }
        PlanKind::IndexPrefix => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_prefix = ops.plan_index_prefix.saturating_add(1);
        }
        PlanKind::IndexRange => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_range = ops.plan_index_range.saturating_add(1);
        }
        PlanKind::Intersection => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_intersection = ops.plan_intersection.saturating_add(1);
        }
        PlanKind::KeyRange => {
            ops.plan_range = ops.plan_range.saturating_add(1);
            ops.plan_key_range = ops.plan_key_range.saturating_add(1);
        }
        PlanKind::Union => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_union = ops.plan_union.saturating_add(1);
        }
    }
}

// Plan choice reasons explain selected non-index and primary-key route families
// at execution time, complementing the coarse route kind counters.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_plan_choice_reason(
    ops: &mut metrics::EventOps,
    reason: PlanChoiceReason,
) {
    #[remain::sorted]
    match reason {
        PlanChoiceReason::ConflictingPrimaryKeyChildrenAccessPreferred => {
            ops.plan_choice_conflicting_primary_key_children_access_preferred = ops
                .plan_choice_conflicting_primary_key_children_access_preferred
                .saturating_add(1);
        }
        PlanChoiceReason::ConstantFalsePredicate => {
            ops.plan_choice_constant_false_predicate =
                ops.plan_choice_constant_false_predicate.saturating_add(1);
        }
        PlanChoiceReason::EmptyChildAccessPreferred => {
            ops.plan_choice_empty_child_access_preferred = ops
                .plan_choice_empty_child_access_preferred
                .saturating_add(1);
        }
        PlanChoiceReason::FullScanAccess => {
            ops.plan_choice_full_scan_access = ops.plan_choice_full_scan_access.saturating_add(1);
        }
        PlanChoiceReason::IntentKeyAccessOverride => {
            ops.plan_choice_intent_key_access_override =
                ops.plan_choice_intent_key_access_override.saturating_add(1);
        }
        PlanChoiceReason::LimitZeroWindow => {
            ops.plan_choice_limit_zero_window = ops.plan_choice_limit_zero_window.saturating_add(1);
        }
        PlanChoiceReason::NonIndexAccess => {
            ops.plan_choice_non_index_access = ops.plan_choice_non_index_access.saturating_add(1);
        }
        PlanChoiceReason::PlannerCompositeNonIndex => {
            ops.plan_choice_planner_composite_non_index = ops
                .plan_choice_planner_composite_non_index
                .saturating_add(1);
        }
        PlanChoiceReason::PlannerFullScanFallback => {
            ops.plan_choice_planner_full_scan_fallback =
                ops.plan_choice_planner_full_scan_fallback.saturating_add(1);
        }
        PlanChoiceReason::PlannerKeySetAccess => {
            ops.plan_choice_planner_key_set_access =
                ops.plan_choice_planner_key_set_access.saturating_add(1);
        }
        PlanChoiceReason::PlannerPrimaryKeyLookup => {
            ops.plan_choice_planner_primary_key_lookup =
                ops.plan_choice_planner_primary_key_lookup.saturating_add(1);
        }
        PlanChoiceReason::PlannerPrimaryKeyRange => {
            ops.plan_choice_planner_primary_key_range =
                ops.plan_choice_planner_primary_key_range.saturating_add(1);
        }
        PlanChoiceReason::RequiredOrderPrimaryKeyRangePreferred => {
            ops.plan_choice_required_order_primary_key_range_preferred = ops
                .plan_choice_required_order_primary_key_range_preferred
                .saturating_add(1);
        }
        PlanChoiceReason::SingletonPrimaryKeyChildAccessPreferred => {
            ops.plan_choice_singleton_primary_key_child_access_preferred = ops
                .plan_choice_singleton_primary_key_child_access_preferred
                .saturating_add(1);
        }
    }
}

// Grouped plan modes are orthogonal to access shape, so count them beside the
// route counters instead of deriving them from a single access kind.
#[remain::check]
pub(in crate::metrics::sink) const fn record_global_grouped_plan_mode(
    ops: &mut metrics::EventOps,
    grouped_execution_mode: Option<GroupedPlanExecutionMode>,
) {
    #[remain::sorted]
    match grouped_execution_mode {
        None => {}
        Some(GroupedPlanExecutionMode::HashMaterialized) => {
            ops.plan_grouped_hash_materialized =
                ops.plan_grouped_hash_materialized.saturating_add(1);
        }
        Some(GroupedPlanExecutionMode::OrderedMaterialized) => {
            ops.plan_grouped_ordered_materialized =
                ops.plan_grouped_ordered_materialized.saturating_add(1);
        }
    }
}

// Mirror global plan attribution into the owning entity summary so operators
// can identify which model is causing full scans, unions, or expensive grouped
// routes without correlating separate counters.
#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_plan_kind(
    ops: &mut metrics::EntityCounters,
    kind: PlanKind,
) {
    #[remain::sorted]
    match kind {
        PlanKind::ByKey => {
            ops.plan_keys = ops.plan_keys.saturating_add(1);
            ops.plan_by_key = ops.plan_by_key.saturating_add(1);
        }
        PlanKind::ByKeys => {
            ops.plan_keys = ops.plan_keys.saturating_add(1);
            ops.plan_by_keys = ops.plan_by_keys.saturating_add(1);
        }
        PlanKind::FullScan => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_explicit_full_scan = ops.plan_explicit_full_scan.saturating_add(1);
        }
        PlanKind::IndexBranchSet => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_branch_set = ops.plan_index_branch_set.saturating_add(1);
        }
        PlanKind::IndexMultiLookup => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_multi_lookup = ops.plan_index_multi_lookup.saturating_add(1);
        }
        PlanKind::IndexPrefix => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_prefix = ops.plan_index_prefix.saturating_add(1);
        }
        PlanKind::IndexRange => {
            ops.plan_index = ops.plan_index.saturating_add(1);
            ops.plan_index_range = ops.plan_index_range.saturating_add(1);
        }
        PlanKind::Intersection => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_intersection = ops.plan_intersection.saturating_add(1);
        }
        PlanKind::KeyRange => {
            ops.plan_range = ops.plan_range.saturating_add(1);
            ops.plan_key_range = ops.plan_key_range.saturating_add(1);
        }
        PlanKind::Union => {
            ops.plan_full_scan = ops.plan_full_scan.saturating_add(1);
            ops.plan_union = ops.plan_union.saturating_add(1);
        }
    }
}

// Mirror selected route-choice reasons to per-entity summaries so one model's
// fallback behavior is visible without correlating global counters manually.
#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_plan_choice_reason(
    ops: &mut metrics::EntityCounters,
    reason: PlanChoiceReason,
) {
    #[remain::sorted]
    match reason {
        PlanChoiceReason::ConflictingPrimaryKeyChildrenAccessPreferred => {
            ops.plan_choice_conflicting_primary_key_children_access_preferred = ops
                .plan_choice_conflicting_primary_key_children_access_preferred
                .saturating_add(1);
        }
        PlanChoiceReason::ConstantFalsePredicate => {
            ops.plan_choice_constant_false_predicate =
                ops.plan_choice_constant_false_predicate.saturating_add(1);
        }
        PlanChoiceReason::EmptyChildAccessPreferred => {
            ops.plan_choice_empty_child_access_preferred = ops
                .plan_choice_empty_child_access_preferred
                .saturating_add(1);
        }
        PlanChoiceReason::FullScanAccess => {
            ops.plan_choice_full_scan_access = ops.plan_choice_full_scan_access.saturating_add(1);
        }
        PlanChoiceReason::IntentKeyAccessOverride => {
            ops.plan_choice_intent_key_access_override =
                ops.plan_choice_intent_key_access_override.saturating_add(1);
        }
        PlanChoiceReason::LimitZeroWindow => {
            ops.plan_choice_limit_zero_window = ops.plan_choice_limit_zero_window.saturating_add(1);
        }
        PlanChoiceReason::NonIndexAccess => {
            ops.plan_choice_non_index_access = ops.plan_choice_non_index_access.saturating_add(1);
        }
        PlanChoiceReason::PlannerCompositeNonIndex => {
            ops.plan_choice_planner_composite_non_index = ops
                .plan_choice_planner_composite_non_index
                .saturating_add(1);
        }
        PlanChoiceReason::PlannerFullScanFallback => {
            ops.plan_choice_planner_full_scan_fallback =
                ops.plan_choice_planner_full_scan_fallback.saturating_add(1);
        }
        PlanChoiceReason::PlannerKeySetAccess => {
            ops.plan_choice_planner_key_set_access =
                ops.plan_choice_planner_key_set_access.saturating_add(1);
        }
        PlanChoiceReason::PlannerPrimaryKeyLookup => {
            ops.plan_choice_planner_primary_key_lookup =
                ops.plan_choice_planner_primary_key_lookup.saturating_add(1);
        }
        PlanChoiceReason::PlannerPrimaryKeyRange => {
            ops.plan_choice_planner_primary_key_range =
                ops.plan_choice_planner_primary_key_range.saturating_add(1);
        }
        PlanChoiceReason::RequiredOrderPrimaryKeyRangePreferred => {
            ops.plan_choice_required_order_primary_key_range_preferred = ops
                .plan_choice_required_order_primary_key_range_preferred
                .saturating_add(1);
        }
        PlanChoiceReason::SingletonPrimaryKeyChildAccessPreferred => {
            ops.plan_choice_singleton_primary_key_child_access_preferred = ops
                .plan_choice_singleton_primary_key_child_access_preferred
                .saturating_add(1);
        }
    }
}

// Grouped execution counters stay per entity for the same reason as access
// route counters: global counts show shape drift, but entity counts show owner.
#[remain::check]
pub(in crate::metrics::sink) const fn record_entity_grouped_plan_mode(
    ops: &mut metrics::EntityCounters,
    grouped_execution_mode: Option<GroupedPlanExecutionMode>,
) {
    #[remain::sorted]
    match grouped_execution_mode {
        None => {}
        Some(GroupedPlanExecutionMode::HashMaterialized) => {
            ops.plan_grouped_hash_materialized =
                ops.plan_grouped_hash_materialized.saturating_add(1);
        }
        Some(GroupedPlanExecutionMode::OrderedMaterialized) => {
            ops.plan_grouped_ordered_materialized =
                ops.plan_grouped_ordered_materialized.saturating_add(1);
        }
    }
}
