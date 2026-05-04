//! Module: executor::plan_metrics
//! Responsibility: lightweight execution observability event helpers.
//! Does not own: execution behavior or route-selection logic.
//! Boundary: metric projection utilities for executor call sites.

use crate::{
    db::access::{AccessPathKind, AccessPlan},
    db::executor::planning::route::GroupedExecutionMode,
    db::query::plan::{AccessChoiceSelectedReason, AccessPlannedQuery},
    metrics::sink::{
        GroupedPlanExecutionMode, MetricsEvent, PlanChoiceReason, PlanKind, Span, record,
    },
    traits::EntityKind,
};

/// Records metrics for the chosen execution plan.
/// Must be called exactly once per execution.
pub(super) fn record_plan_metrics(entity_path: &'static str, plan: &AccessPlannedQuery) {
    let kind = access_plan_kind(&plan.access);

    record(MetricsEvent::Plan {
        entity_path,
        kind,
        grouped_execution_mode: None,
    });
    record_plan_choice_reason(entity_path, plan);
}

/// Records metrics for one grouped execution plan with explicit grouped execution mode.
/// Must be called exactly once per grouped execution.
pub(super) fn record_grouped_plan_metrics(
    entity_path: &'static str,
    plan: &AccessPlannedQuery,
    grouped_execution_mode: GroupedExecutionMode,
) {
    let kind = access_plan_kind(&plan.access);
    let grouped_execution_mode = match grouped_execution_mode {
        GroupedExecutionMode::HashMaterialized => GroupedPlanExecutionMode::HashMaterialized,
        GroupedExecutionMode::OrderedMaterialized => GroupedPlanExecutionMode::OrderedMaterialized,
    };

    record(MetricsEvent::Plan {
        entity_path,
        kind,
        grouped_execution_mode: Some(grouped_execution_mode),
    });
    record_plan_choice_reason(entity_path, plan);
}

// Project the exact top-level access shape while the metrics sink keeps the
// older coarse counter groups populated for existing dashboards.
fn access_plan_kind<K>(access: &AccessPlan<K>) -> PlanKind {
    match access {
        AccessPlan::Path(path) => match path.kind() {
            AccessPathKind::ByKey => PlanKind::ByKey,
            AccessPathKind::ByKeys => PlanKind::ByKeys,
            AccessPathKind::KeyRange => PlanKind::KeyRange,
            AccessPathKind::IndexPrefix => PlanKind::IndexPrefix,
            AccessPathKind::IndexMultiLookup => PlanKind::IndexMultiLookup,
            AccessPathKind::IndexRange => PlanKind::IndexRange,
            AccessPathKind::FullScan => PlanKind::FullScan,
        },
        AccessPlan::Union(_) => PlanKind::Union,
        AccessPlan::Intersection(_) => PlanKind::Intersection,
    }
}

// Record selected non-index and primary-key route explanations without
// emitting one metric for normal secondary-index winner ranking.
fn record_plan_choice_reason(entity_path: &'static str, plan: &AccessPlannedQuery) {
    let Some(reason) = plan_choice_reason(plan.access_choice().chosen_reason()) else {
        return;
    };

    record(MetricsEvent::PlanChoice {
        entity_path,
        reason,
    });
}

// Map explain-owned selected-route reasons into the low-cardinality metrics
// subset that explains non-index/primary-key route selection.
const fn plan_choice_reason(reason: AccessChoiceSelectedReason) -> Option<PlanChoiceReason> {
    match reason {
        AccessChoiceSelectedReason::ConflictingPrimaryKeyChildrenAccessPreferred => {
            Some(PlanChoiceReason::ConflictingPrimaryKeyChildrenAccessPreferred)
        }
        AccessChoiceSelectedReason::ConstantFalsePredicate => {
            Some(PlanChoiceReason::ConstantFalsePredicate)
        }
        AccessChoiceSelectedReason::EmptyChildAccessPreferred => {
            Some(PlanChoiceReason::EmptyChildAccessPreferred)
        }
        AccessChoiceSelectedReason::FullScanAccess => Some(PlanChoiceReason::FullScanAccess),
        AccessChoiceSelectedReason::IntentKeyAccessOverride => {
            Some(PlanChoiceReason::IntentKeyAccessOverride)
        }
        AccessChoiceSelectedReason::LimitZeroWindow => Some(PlanChoiceReason::LimitZeroWindow),
        AccessChoiceSelectedReason::NonIndexAccess => Some(PlanChoiceReason::NonIndexAccess),
        AccessChoiceSelectedReason::PlannerCompositeNonIndex => {
            Some(PlanChoiceReason::PlannerCompositeNonIndex)
        }
        AccessChoiceSelectedReason::PlannerFullScanFallback => {
            Some(PlanChoiceReason::PlannerFullScanFallback)
        }
        AccessChoiceSelectedReason::PlannerKeySetAccess => {
            Some(PlanChoiceReason::PlannerKeySetAccess)
        }
        AccessChoiceSelectedReason::PlannerPrimaryKeyLookup => {
            Some(PlanChoiceReason::PlannerPrimaryKeyLookup)
        }
        AccessChoiceSelectedReason::PlannerPrimaryKeyRange => {
            Some(PlanChoiceReason::PlannerPrimaryKeyRange)
        }
        AccessChoiceSelectedReason::RequiredOrderPrimaryKeyRangePreferred => {
            Some(PlanChoiceReason::RequiredOrderPrimaryKeyRangePreferred)
        }
        AccessChoiceSelectedReason::SingletonPrimaryKeyChildAccessPreferred => {
            Some(PlanChoiceReason::SingletonPrimaryKeyChildAccessPreferred)
        }
        AccessChoiceSelectedReason::BestPrefixLen
        | AccessChoiceSelectedReason::ByKeyAccess
        | AccessChoiceSelectedReason::ByKeysAccess
        | AccessChoiceSelectedReason::PrimaryKeyRangeAccess
        | AccessChoiceSelectedReason::Ranked(_)
        | AccessChoiceSelectedReason::SelectedIndexNotProjected
        | AccessChoiceSelectedReason::SingleCandidate => None,
    }
}

/// Convenience: set span rows from a usize length.
pub(super) const fn set_rows_from_len<E: EntityKind>(span: &mut Span<E>, len: usize) {
    span.set_rows(len as u64);
}

/// Record per-request rows scanned metrics for one structural entity path.
pub(super) fn record_rows_scanned_for_path(entity_path: &'static str, rows_scanned: usize) {
    record(MetricsEvent::RowsScanned {
        entity_path,
        rows_scanned: u64::try_from(rows_scanned).unwrap_or(u64::MAX),
    });
}

/// Record per-request rows filtered metrics for one structural entity path.
pub(super) fn record_rows_filtered_for_path(entity_path: &'static str, rows_filtered: usize) {
    record(MetricsEvent::RowsFiltered {
        entity_path,
        rows_filtered: u64::try_from(rows_filtered).unwrap_or(u64::MAX),
    });
}

/// Record per-request rows aggregated metrics for one structural entity path.
pub(super) fn record_rows_aggregated_for_path(entity_path: &'static str, rows_aggregated: usize) {
    record(MetricsEvent::RowsAggregated {
        entity_path,
        rows_aggregated: u64::try_from(rows_aggregated).unwrap_or(u64::MAX),
    });
}

/// Record per-request rows emitted metrics for one structural entity path.
pub(super) fn record_rows_emitted_for_path(entity_path: &'static str, rows_emitted: usize) {
    record(MetricsEvent::RowsEmitted {
        entity_path,
        rows_emitted: u64::try_from(rows_emitted).unwrap_or(u64::MAX),
    });
}

/// Record read-efficiency totals for one finalized load path.
pub(super) fn record_load_row_efficiency_for_path(
    entity_path: &'static str,
    candidate_rows_scanned: usize,
    candidate_rows_filtered: usize,
    result_rows_emitted: usize,
) {
    record(MetricsEvent::LoadRowEfficiency {
        entity_path,
        candidate_rows_scanned: u64::try_from(candidate_rows_scanned).unwrap_or(u64::MAX),
        candidate_rows_filtered: u64::try_from(candidate_rows_filtered).unwrap_or(u64::MAX),
        result_rows_emitted: u64::try_from(result_rows_emitted).unwrap_or(u64::MAX),
    });
}
