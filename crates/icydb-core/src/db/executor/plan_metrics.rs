//! Module: executor::plan_metrics
//! Responsibility: lightweight execution observability event helpers.
//! Does not own: execution behavior or route-selection logic.
//! Boundary: metric projection utilities for executor call sites.

use crate::{
    db::access::AccessPlan,
    db::executor::planning::route::GroupedExecutionMode,
    metrics::sink::{GroupedPlanExecutionMode, MetricsEvent, PlanKind, Span, record},
    traits::EntityKind,
};

/// Records metrics for the chosen execution plan.
/// Must be called exactly once per execution.
pub(super) fn record_plan_metrics<K>(access: &AccessPlan<K>) {
    let kind = access_plan_kind(access);

    record(MetricsEvent::Plan {
        kind,
        grouped_execution_mode: None,
    });
}

/// Records metrics for one grouped execution plan with explicit grouped execution mode.
/// Must be called exactly once per grouped execution.
pub(super) fn record_grouped_plan_metrics<K>(
    access: &AccessPlan<K>,
    grouped_execution_mode: GroupedExecutionMode,
) {
    let kind = access_plan_kind(access);
    let grouped_execution_mode = match grouped_execution_mode {
        GroupedExecutionMode::HashMaterialized => GroupedPlanExecutionMode::HashMaterialized,
        GroupedExecutionMode::OrderedMaterialized => GroupedPlanExecutionMode::OrderedMaterialized,
    };

    record(MetricsEvent::Plan {
        kind,
        grouped_execution_mode: Some(grouped_execution_mode),
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
