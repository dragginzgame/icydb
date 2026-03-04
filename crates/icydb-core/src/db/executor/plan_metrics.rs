//! Module: executor::plan_metrics
//! Responsibility: lightweight execution observability event helpers.
//! Does not own: execution behavior or route-selection logic.
//! Boundary: metric projection utilities for executor call sites.

use crate::{
    db::{
        access::{AccessPlan, lower_executable_access_plan},
        executor::access_plan_metrics_kind,
    },
    obs::sink::{GroupedPlanStrategy, MetricsEvent, PlanKind, Span, record},
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
    let executable = lower_executable_access_plan(access);

    access_plan_metrics_kind(&executable)
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
