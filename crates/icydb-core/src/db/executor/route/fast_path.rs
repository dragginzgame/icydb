//! Module: executor::route::fast_path
//! Responsibility: route-owned fast-path verification/dispatch scaffolding.
//! Does not own: route capability derivation or stream materialization behavior.
//! Boundary: precedence runner and fast-path eligibility helpers for route planning.

use crate::{
    db::{
        access::AccessPlan,
        executor::{AccessPathRuntimeStrategy, derive_access_capabilities, dispatch_access_path},
        executor::{ExecutionPreparation, aggregate::AggregateKind, load::LoadExecutor},
        index::{IndexCompilePolicy, compile_index_program},
        query::plan::{AccessPlannedQuery, lower_executable_access_plan},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

use crate::db::executor::route::{
    FastPathOrder, RouteCapabilities, supports_pk_stream_access_executable_path,
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
    /// Return whether count pushdown is supported for one access plan.
    pub(super) fn count_pushdown_access_shape_supported(access: &AccessPlan<E::Key>) -> bool {
        let executable = lower_executable_access_plan(access);
        let access_capabilities = derive_access_capabilities(&executable);
        let Some(single_path) = access_capabilities.single_path() else {
            return false;
        };

        single_path.supports_count_pushdown_shape()
    }

    // Route-owned gate for PK full-scan/key-range ordered fast-path eligibility.
    pub(in crate::db::executor) fn pk_order_stream_fast_path_shape_supported(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> bool {
        let logical = plan.scalar_plan();
        if !logical.mode.is_load() {
            return false;
        }

        let executable = lower_executable_access_plan(&plan.access);
        let supports_pk_stream_access = executable
            .as_path()
            .is_some_and(supports_pk_stream_access_executable_path);
        if !supports_pk_stream_access {
            return false;
        }

        let Some(order) = logical.order.as_ref() else {
            return false;
        };

        order.fields.len() == 1 && order.fields[0].0 == E::MODEL.primary_key.name
    }

    /// Validate routed access-path shape for PK stream fast-path execution.
    pub(in crate::db::executor) fn verify_pk_stream_fast_path_access(
        plan: &AccessPlannedQuery<E::Key>,
    ) -> Result<(), InternalError> {
        let executable = lower_executable_access_plan(&plan.access);
        let access_capabilities = derive_access_capabilities(&executable);
        let Some(single_path) = access_capabilities.single_path() else {
            return Err(invariant(
                "pk stream fast-path requires direct access-path execution",
            ));
        };
        if !single_path.supports_pk_stream_access() {
            return Err(invariant(
                "pk stream fast-path requires full-scan/key-range access path",
            ));
        }

        let access = executable.as_path().ok_or_else(|| {
            invariant("pk stream fast-path requires direct access-path execution")
        })?;
        let dispatched = dispatch_access_path(access);
        let strategy: &dyn AccessPathRuntimeStrategy<E::Key> = dispatched;
        debug_assert_eq!(
            strategy.supports_pk_stream_access(),
            single_path.supports_pk_stream_access(),
            "route invariant: descriptor and strategy pk-stream capability must stay aligned",
        );

        Ok(())
    }

    pub(super) const fn is_count_pushdown_eligible(
        kind: AggregateKind,
        capabilities: RouteCapabilities,
    ) -> bool {
        kind.is_count()
            && capabilities.streaming_access_shape_safe
            && capabilities.count_pushdown_access_shape_supported
    }

    /// Return whether aggregate routing must force materialized mode due to predicate uncertainty.
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

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
