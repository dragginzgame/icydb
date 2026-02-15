use crate::{
    db::query::plan::{AccessPath, AccessPlan},
    obs::sink::{self, MetricsEvent, PlanKind, Span},
    traits::EntityKind,
};

/// Records metrics for the chosen execution plan.
/// Must be called exactly once per execution.
pub fn record_plan_metrics<K>(access: &AccessPlan<K>) {
    let kind = match access {
        AccessPlan::Path(path) => match path {
            AccessPath::ByKey(_) | AccessPath::ByKeys(_) => PlanKind::Keys,
            AccessPath::IndexPrefix { .. } => PlanKind::Index,
            AccessPath::KeyRange { .. } => PlanKind::Range,
            AccessPath::FullScan => PlanKind::FullScan,
        },
        AccessPlan::Union(_) | AccessPlan::Intersection(_) => PlanKind::FullScan,
    };

    sink::record(MetricsEvent::Plan { kind });
}

/// Convenience: set span rows from a usize length.
pub const fn set_rows_from_len<E: EntityKind>(span: &mut Span<E>, len: usize) {
    span.set_rows(len as u64);
}

/// Record per-request rows scanned metrics with saturated diagnostics counts.
pub fn record_rows_scanned<E: EntityKind>(rows_scanned: usize) {
    sink::record(MetricsEvent::RowsScanned {
        entity_path: E::PATH,
        rows_scanned: u64::try_from(rows_scanned).unwrap_or(u64::MAX),
    });
}
