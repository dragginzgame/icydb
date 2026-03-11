//! Module: executor::plan_metrics
//! Responsibility: lightweight execution observability event helpers.
//! Does not own: execution behavior or route-selection logic.
//! Boundary: metric projection utilities for executor call sites.

use crate::{
    db::access::AccessPlan,
    metrics::sink::{GroupedPlanStrategy, MetricsEvent, PlanKind, Span, record},
    traits::EntityKind,
};

///
/// GroupedPlanMetricsStrategy
///
/// Deterministic grouped strategy dimension emitted alongside plan metrics.
/// Strategy labels reflect grouped ordering feasibility while execution mode
/// remains materialized for grouped routes in `0.36.x`.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum GroupedPlanMetricsStrategy {
    HashMaterialized,
    OrderedMaterialized,
}

impl From<GroupedPlanMetricsStrategy> for GroupedPlanStrategy {
    fn from(value: GroupedPlanMetricsStrategy) -> Self {
        match value {
            GroupedPlanMetricsStrategy::HashMaterialized => Self::HashMaterialized,
            GroupedPlanMetricsStrategy::OrderedMaterialized => Self::OrderedMaterialized,
        }
    }
}

/// Records metrics for the chosen execution plan.
/// Must be called exactly once per execution.
pub(super) fn record_plan_metrics<K>(access: &AccessPlan<K>) {
    let kind = access_plan_kind(access);

    record(MetricsEvent::Plan {
        kind,
        grouped_strategy: None,
    });
}

/// Records metrics for one grouped execution plan with explicit grouped strategy.
/// Must be called exactly once per grouped execution.
pub(super) fn record_grouped_plan_metrics<K>(
    access: &AccessPlan<K>,
    grouped_strategy: GroupedPlanMetricsStrategy,
) {
    let kind = access_plan_kind(access);

    record(MetricsEvent::Plan {
        kind,
        grouped_strategy: Some(grouped_strategy.into()),
    });
}

// This metric is intentionally coarse and only reflects the top-level access shape.
fn access_plan_kind<K>(access: &AccessPlan<K>) -> PlanKind {
    let executable = access.resolve_strategy();

    executable.executable().metrics_kind()
}

/// Convenience: set span rows from a usize length.
pub(super) const fn set_rows_from_len<E: EntityKind>(span: &mut Span<E>, len: usize) {
    span.set_rows(len as u64);
}

/// Record per-request rows scanned metrics with saturated diagnostics counts.
pub(super) fn record_rows_scanned<E: EntityKind>(rows_scanned: usize) {
    record(MetricsEvent::RowsScanned {
        entity_path: E::PATH,
        rows_scanned: u64::try_from(rows_scanned).unwrap_or(u64::MAX),
    });
}

/// Record per-request rows filtered metrics.
pub(super) fn record_rows_filtered<E: EntityKind>(rows_filtered: usize) {
    record(MetricsEvent::RowsFiltered {
        entity_path: E::PATH,
        rows_filtered: u64::try_from(rows_filtered).unwrap_or(u64::MAX),
    });
}

/// Record per-request rows aggregated metrics.
pub(super) fn record_rows_aggregated<E: EntityKind>(rows_aggregated: usize) {
    record(MetricsEvent::RowsAggregated {
        entity_path: E::PATH,
        rows_aggregated: u64::try_from(rows_aggregated).unwrap_or(u64::MAX),
    });
}

/// Record per-request rows emitted metrics.
pub(super) fn record_rows_emitted<E: EntityKind>(rows_emitted: usize) {
    record(MetricsEvent::RowsEmitted {
        entity_path: E::PATH,
        rows_emitted: u64::try_from(rows_emitted).unwrap_or(u64::MAX),
    });
}
