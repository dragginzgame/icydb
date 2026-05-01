//! Module: db::executor::planning::route::fast_path
//! Responsibility: route-owned fast-path verification/dispatch scaffolding.
//! Does not own: route capability derivation or stream materialization behavior.
//! Boundary: precedence runner and fast-path eligibility helpers for route planning.

use crate::{
    db::{
        access::ExecutableAccessPlan,
        executor::{ExecutionPreparation, route::primary_key_stream_window_shape_supported},
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    value::Value,
};

use crate::db::executor::planning::route::FastPathOrder;

///
/// try_first_verified_fast_path_hit
///
/// Iterate route-owned fast-path precedence through one shared verify+execute
/// gate. This keeps the fast-path runner separate from route-specific path
/// guards and execution payload construction.
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
        let Some(hit) = execute_verified_route(marker)? else {
            continue;
        };

        return Ok(Some(hit));
    }

    Ok(None)
}

/// Validate routed access-path shape for PK stream fast-path execution.
pub(in crate::db::executor) fn verify_pk_stream_fast_path_access(
    access_strategy: &ExecutableAccessPlan<'_, Value>,
) -> Result<(), InternalError> {
    let access_capabilities = access_strategy.capabilities();
    let Some(path_capabilities) = access_capabilities.single_path_capabilities() else {
        return Err(InternalError::query_executor_invariant(
            "pk stream fast-path requires direct access-path execution",
        ));
    };
    primary_key_stream_window_shape_supported(path_capabilities)
        .then_some(())
        .ok_or_else(|| {
            InternalError::query_executor_invariant(
                "pk stream fast-path requires full-scan/key-range access path",
            )
        })?;

    let access = access_strategy.as_path().ok_or_else(|| {
        InternalError::query_executor_invariant(
            "pk stream fast-path requires direct access-path execution",
        )
    })?;
    debug_assert_eq!(
        primary_key_stream_window_shape_supported(access.capabilities()),
        primary_key_stream_window_shape_supported(path_capabilities),
        "route invariant: descriptor and path capability snapshots must stay aligned",
    );

    Ok(())
}

/// Return whether aggregate routing must force materialized mode due to predicate uncertainty.
#[must_use]
pub(super) const fn aggregate_force_materialized_due_to_predicate_uncertainty_with_preparation(
    execution_preparation: &ExecutionPreparation,
) -> bool {
    execution_preparation.compiled_predicate().is_some()
        &&
        // Route strict-mode uncertainty must remain aligned with the shared
        // kernel predicate compiler boundary.
        execution_preparation.strict_mode().is_none()
}

/// Return whether one plan shape supports primary-key ordered stream execution.
#[must_use]
pub(super) fn pk_order_stream_fast_path_shape_supported(plan: &AccessPlannedQuery) -> bool {
    let logical = plan.scalar_plan();
    let access_capabilities = plan.access_capabilities();
    let has_primary_key_stream_window = match access_capabilities.single_path_capabilities() {
        Some(path) => primary_key_stream_window_shape_supported(path),
        None => false,
    };

    let Some(order) = logical.order.as_ref() else {
        return false;
    };

    logical.mode.is_load()
        && has_primary_key_stream_window
        && order.is_primary_key_only(plan.primary_key_name())
}
