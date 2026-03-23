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
            Context, EntityAuthority, route::build_execution_route_plan_for_mutation_with_model,
            validate_executor_plan_for_authority,
        },
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

pub(super) use commit_window::{
    commit_delete_row_ops_with_window, commit_delete_row_ops_with_window_for_path,
    commit_save_row_ops_with_window,
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

/// Validate mutation-plan executor contracts using structural authority only.
pub(in crate::db::executor) fn preflight_mutation_plan_for_authority(
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
) -> Result<(), InternalError> {
    validate_executor_plan_for_authority(authority, plan)?;
    let _ = build_execution_route_plan_for_mutation_with_model(authority.model(), plan)?;

    Ok(())
}
