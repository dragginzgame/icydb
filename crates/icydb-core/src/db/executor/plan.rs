use crate::{
    db::query::plan::{AccessPlan, AccessPlanProjection, project_access_plan},
    obs::sink::{self, MetricsEvent, PlanKind, Span},
    traits::EntityKind,
    value::Value,
};

/// Records metrics for the chosen execution plan.
/// Must be called exactly once per execution.
pub fn record_plan_metrics<K>(access: &AccessPlan<K>) {
    let mut projection = PlanKindProjection;
    let kind = project_access_plan(access, &mut projection);

    sink::record(MetricsEvent::Plan { kind });
}

struct PlanKindProjection;

impl<K> AccessPlanProjection<K> for PlanKindProjection {
    type Output = PlanKind;

    fn by_key(&mut self, _key: &K) -> Self::Output {
        PlanKind::Keys
    }

    fn by_keys(&mut self, _keys: &[K]) -> Self::Output {
        PlanKind::Keys
    }

    fn key_range(&mut self, _start: &K, _end: &K) -> Self::Output {
        PlanKind::Range
    }

    fn index_prefix(
        &mut self,
        _index_name: &'static str,
        _index_fields: &[&'static str],
        _prefix_len: usize,
        _values: &[Value],
    ) -> Self::Output {
        PlanKind::Index
    }

    fn full_scan(&mut self) -> Self::Output {
        PlanKind::FullScan
    }

    fn union(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        PlanKind::FullScan
    }

    fn intersection(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        PlanKind::FullScan
    }
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
