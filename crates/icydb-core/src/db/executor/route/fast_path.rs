use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        executor::{ExecutionPreparation, aggregate::AggregateKind, load::LoadExecutor},
        index::{IndexCompilePolicy, compile_index_program},
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    FastPathOrder, RouteCapabilities, supports_pk_stream_access_path,
};

/// Iterate route-owned fast-path precedence through a shared verify+execute gate.
///
/// Verification runs first for each route; execution is attempted only when
/// verification returns a marker. Returns the first successful execution hit.
pub(in crate::db::executor) fn try_first_verified_fast_path_hit<T, M, V, E>(
    fast_path_order: &[FastPathOrder],
    mut verify_route: V,
    mut execute_verified_route: E,
) -> Result<Option<T>, InternalError>
where
    V: FnMut(FastPathOrder) -> Result<Option<M>, InternalError>,
    E: FnMut(M) -> Result<Option<T>, InternalError>,
{
    for route in fast_path_order.iter().copied() {
        let Some(marker) = verify_route(route)? else {
            continue;
        };
        if let Some(hit) = execute_verified_route(marker)? {
            return Ok(Some(hit));
        }
    }

    Ok(None)
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(super) const fn count_pushdown_path_shape_supported(path: &AccessPath<E::Key>) -> bool {
        matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. })
    }

    pub(super) fn count_pushdown_access_shape_supported(access: &AccessPlan<E::Key>) -> bool {
        match access {
            AccessPlan::Path(path) => Self::count_pushdown_path_shape_supported(path),
            AccessPlan::Union(_) | AccessPlan::Intersection(_) => false,
        }
    }

    // Route-owned gate for PK full-scan/key-range ordered fast-path eligibility.
    pub(in crate::db::executor) fn pk_order_stream_fast_path_shape_supported(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> bool {
        let logical = plan.scalar_plan();
        if !logical.mode.is_load() {
            return false;
        }

        let supports_pk_stream_access = plan
            .access
            .as_path()
            .is_some_and(supports_pk_stream_access_path);
        if !supports_pk_stream_access {
            return false;
        }

        let Some(order) = logical.order.as_ref() else {
            return false;
        };

        order.fields.len() == 1 && order.fields[0].0 == E::MODEL.primary_key.name
    }

    pub(super) const fn is_count_pushdown_eligible(
        kind: AggregateKind,
        capabilities: RouteCapabilities,
    ) -> bool {
        kind.is_count()
            && capabilities.streaming_access_shape_safe
            && capabilities.count_pushdown_access_shape_supported
    }

    pub(super) fn aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
        execution_preparation: &ExecutionPreparation,
    ) -> bool {
        let Some(compiled_predicate) = execution_preparation.compiled_predicate() else {
            return false;
        };
        let Some(slot_map) = execution_preparation.slot_map() else {
            return false;
        };

        // Route strict-mode uncertainty must remain aligned with the shared
        // kernel predicate compiler boundary.
        execution_preparation.strict_mode().is_none()
            && compile_index_program(
                compiled_predicate.resolved(),
                slot_map,
                IndexCompilePolicy::StrictAllOrNone,
            )
            .is_none()
    }
}
