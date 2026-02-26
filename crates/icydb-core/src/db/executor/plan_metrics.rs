use crate::{
    db::query::plan::{AccessPath, AccessPlan},
    obs::sink::{MetricsEvent, PlanKind, Span, record},
    traits::EntityKind,
};

/// Records metrics for the chosen execution plan.
/// Must be called exactly once per execution.
pub(super) fn record_plan_metrics<K>(access: &AccessPlan<K>) {
    let kind = access_plan_kind(access);

    record(MetricsEvent::Plan { kind });
}

// This metric is intentionally coarse and only reflects the top-level access shape.
fn access_plan_kind<K>(access: &AccessPlan<K>) -> PlanKind {
    match access {
        AccessPlan::Path(path) => match path.as_ref() {
            AccessPath::ByKey(_) | AccessPath::ByKeys(_) => PlanKind::Keys,
            AccessPath::KeyRange { .. } => PlanKind::Range,
            AccessPath::IndexPrefix { .. } | AccessPath::IndexRange { .. } => PlanKind::Index,
            AccessPath::FullScan => PlanKind::FullScan,
        },
        AccessPlan::Union(_) | AccessPlan::Intersection(_) => PlanKind::FullScan,
    }
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
