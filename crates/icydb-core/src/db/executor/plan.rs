use crate::{
    db::query::plan::AccessPath,
    obs::sink::{self, MetricsEvent, PlanKind, Span},
    traits::EntityKind,
};

/// Records metrics for the chosen execution plan.
/// Must be called exactly once per execution.
pub fn record_plan_metrics(access: &AccessPath) {
    let kind = match access {
        AccessPath::ByKey(_) | AccessPath::ByKeys(_) => PlanKind::Keys,
        AccessPath::IndexPrefix { .. } => PlanKind::Index,
        AccessPath::KeyRange { .. } => PlanKind::Range,
        AccessPath::FullScan => PlanKind::FullScan,
    };

    sink::record(MetricsEvent::Plan { kind });
}

/// Convenience: set span rows from a usize length.
pub const fn set_rows_from_len<E: EntityKind>(span: &mut Span<E>, len: usize) {
    span.set_rows(len as u64);
}
