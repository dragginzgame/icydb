//! Module: executor::delete::api
//! Responsibility: public delete executor entrypoints over the shared delete
//! core.
//! Does not own: row resolution, rollback packaging, or commit preparation.
//! Boundary: wraps prepared delete plans with metrics and final response shaping.

use crate::{
    db::{
        Db, PersistedRow,
        executor::{
            PreparedExecutionPlan,
            delete::{
                DeleteProjection, apply_delete_commit_window_for_type,
                execute_structural_delete_projection_core, package_typed_delete_count,
                package_typed_delete_rows, prepare_delete_runtime, prepare_typed_delete_core,
            },
            plan_metrics::{record_plan_metrics, set_rows_from_len},
        },
        response::EntityResponse,
    },
    error::InternalError,
    metrics::sink::{ExecKind, Span},
    traits::EntityValue,
};

///
/// DeleteExecutor
///
/// Atomicity invariant:
/// All fallible validation and planning completes before the commit boundary.
/// After `begin_commit`, mutations are applied mechanically from a
/// prevalidated commit marker. Rollback exists as a safety net but is
/// not relied upon for correctness.
///

#[derive(Clone, Copy)]
pub(in crate::db) struct DeleteExecutor<E>
where
    E: PersistedRow,
{
    db: Db<E::Canister>,
}

impl<E> DeleteExecutor<E>
where
    E: PersistedRow + EntityValue,
{
    /// Construct one delete executor bound to a database handle.
    #[must_use]
    pub(in crate::db) const fn new(db: Db<E::Canister>) -> Self {
        Self { db }
    }

    /// Execute one delete plan and return deleted entities in response order.
    pub(in crate::db) fn execute(
        self,
        plan: PreparedExecutionPlan<E>,
    ) -> Result<EntityResponse<E>, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Delete);
        let result = (|| {
            // Phase 1: prepare authority, store access, and delete execution inputs once.
            let (prepared, store) = prepare_delete_runtime(&self.db, plan)?;
            record_plan_metrics(
                prepared.authority.entity.entity_path(),
                &prepared.logical_plan.access,
            );

            // Phase 2: run the shared typed delete core and package response rows.
            let Some(typed) = prepare_typed_delete_core(
                &self.db,
                store,
                &prepared,
                package_typed_delete_rows::<E>,
            )?
            else {
                set_rows_from_len(&mut span, 0);
                return Ok(EntityResponse::new(Vec::new()));
            };

            // Phase 3: apply the already prepared delete commit payload.
            apply_delete_commit_window_for_type::<E>(
                &self.db,
                prepared.authority.entity,
                typed.commit.row_ops,
                "delete_row_apply",
            )?;

            // Phase 4: return the already-prepared typed delete response rows.
            set_rows_from_len(&mut span, typed.row_count);

            Ok(EntityResponse::new(typed.output))
        })();
        if let Err(err) = &result {
            span.set_error(err);
        }

        result
    }

    /// Execute one structural delete projection plan and return structural row
    /// values for one outer projection/rendering surface.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn execute_structural_projection(
        self,
        plan: PreparedExecutionPlan<E>,
    ) -> Result<DeleteProjection, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Delete);
        let result = (|| {
            // Phase 1: prepare authority, store access, and delete execution inputs once.
            let (prepared, store) = prepare_delete_runtime(&self.db, plan)?;
            record_plan_metrics(
                prepared.authority.entity.entity_path(),
                &prepared.logical_plan.access,
            );

            // Phase 2: run the shared structural delete core and apply the
            // final typed commit-window bridge only at the boundary.
            let projection = execute_structural_delete_projection_core(
                &self.db,
                store,
                &prepared,
                apply_delete_commit_window_for_type::<E>,
            )?;
            if projection.row_count() == 0 {
                set_rows_from_len(&mut span, 0);
                return Ok(projection);
            }

            // Phase 3: return the already prepared structural delete projection.
            set_rows_from_len(
                &mut span,
                usize::try_from(projection.row_count()).unwrap_or(usize::MAX),
            );

            Ok(projection)
        })();
        if let Err(err) = &result {
            span.set_error(err);
        }

        result
    }

    /// Execute one delete plan and return only the affected-row count.
    pub(in crate::db) fn execute_count(
        self,
        plan: PreparedExecutionPlan<E>,
    ) -> Result<u32, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Delete);
        let result = (|| {
            // Phase 1: prepare authority, store access, and delete execution inputs once.
            let (prepared, store) = prepare_delete_runtime(&self.db, plan)?;
            record_plan_metrics(
                prepared.authority.entity.entity_path(),
                &prepared.logical_plan.access,
            );

            // Phase 2: run the shared typed delete core while skipping response
            // row materialization.
            let Some(counted) = prepare_typed_delete_core(
                &self.db,
                store,
                &prepared,
                package_typed_delete_count::<E>,
            )?
            else {
                set_rows_from_len(&mut span, 0);
                return Ok(0);
            };

            // Phase 3: apply the already prepared delete commit payload.
            apply_delete_commit_window_for_type::<E>(
                &self.db,
                prepared.authority.entity,
                counted.commit.row_ops,
                "delete_row_apply",
            )?;

            // Phase 4: return only the final affected-row count.
            set_rows_from_len(&mut span, counted.row_count);

            Ok(counted.output)
        })();
        if let Err(err) = &result {
            span.set_error(err);
        }

        result
    }
}
