//! Module: executor::mutation
//! Responsibility: mutation execution preflight and shared commit-window entry helpers.
//! Does not own: relation semantics or logical-plan construction.
//! Boundary: write-path setup shared by save/delete executors.

pub(super) mod commit_window;
pub(super) mod save;
mod save_validation;

use crate::{
    db::{
        Db,
        commit::ensure_recovered,
        executor::{
            Context, EntityAuthority,
            route::{RoutePlanRequest, build_execution_route_plan},
            validate_executor_plan_for_authority,
        },
        query::plan::AccessPlannedQuery,
    },
    entity::{EntityKind, EntityValue},
    error::InternalError,
};

pub(super) use commit_window::{
    PreparedRowOpDelta, affected_store_handles_for_prepared_row_ops, classify_mutation_commit_plan,
    commit_delete_row_ops_with_window_for_path, commit_prepared_single_save_row_op_with_window,
    commit_save_row_ops_with_window_and_schema_fingerprint, emit_index_delta_metrics,
    record_mutation_commit_plan, synchronized_store_handles_for_prepared_row_ops,
};

/// Run mutation write-entry recovery checks and return a write-ready context.
pub(in crate::db::executor) fn mutation_write_context<E>(
    db: &'_ Db<E::Canister>,
) -> Result<Context<'_, E>, InternalError>
where
    E: EntityKind + EntityValue,
{
    ensure_recovered(db)?;

    Ok(db.context::<E>())
}

/// Validate mutation-plan executor contracts using authority only.
pub(in crate::db::executor) fn preflight_mutation_plan_for_authority(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<(), InternalError> {
    validate_executor_plan_for_authority(&authority, plan)?;
    build_execution_route_plan(plan, RoutePlanRequest::MutationDelete)?;

    Ok(())
}
