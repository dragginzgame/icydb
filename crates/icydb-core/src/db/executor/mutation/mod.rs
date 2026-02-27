pub(super) mod commit_window;
pub(super) mod save;
mod save_validation;

use crate::{
    db::{
        Db,
        commit::ensure_recovered_for_write,
        executor::{Context, load::LoadExecutor, validate_executor_plan},
        query::plan::AccessPlannedQuery,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

pub(super) use commit_window::{
    commit_delete_row_ops_with_window, commit_save_row_ops_with_window,
};

/// Run mutation write-entry recovery checks and return a write-ready context.
pub(in crate::db::executor) fn mutation_write_context<E>(
    db: &'_ Db<E::Canister>,
) -> Result<Context<'_, E>, InternalError>
where
    E: EntityKind + EntityValue,
{
    ensure_recovered_for_write(db)?;

    Ok(db.context::<E>())
}

/// Validate mutation-plan executor contracts before write-phase execution.
pub(in crate::db::executor) fn preflight_mutation_plan<E>(
    plan: &AccessPlannedQuery<E::Key>,
) -> Result<(), InternalError>
where
    E: EntityKind + EntityValue,
{
    validate_executor_plan::<E>(plan)?;
    let _ = LoadExecutor::<E>::build_execution_route_plan_for_mutation(plan)?;

    Ok(())
}
