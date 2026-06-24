//! Module: executor::delete::api
//! Responsibility: public delete executor entrypoints over the shared delete
//! core.
//! Does not own: row resolution, rollback packaging, or commit preparation.
//! Boundary: wraps prepared delete plans with metrics and final response shaping.

#[cfg(feature = "sql")]
use crate::db::executor::delete::{
    DeleteProjection, DeleteProjectionBounds, prepare_structural_delete_count_core_with_bounds,
    prepare_structural_delete_projection_core,
};
use crate::{
    db::{
        Db, PersistedRow,
        commit::CommitRowOp,
        executor::{
            PreparedExecutionPlan,
            delete::{
                apply_delete_commit_window_for_type, package_typed_delete_rows,
                prepare_delete_runtime, prepare_structural_delete_count_core,
                prepare_typed_delete_core, types::PreparedDeleteExecutionState,
            },
            plan_metrics::{record_plan_metrics, set_rows_from_len},
        },
        registry::StoreHandle,
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

    fn apply_prepared_delete_commit(
        db: &Db<E::Canister>,
        prepared: &PreparedDeleteExecutionState,
        row_ops: Vec<CommitRowOp>,
    ) -> Result<(), InternalError> {
        apply_delete_commit_window_for_type::<E>(
            db,
            prepared.authority.entity.clone(),
            row_ops,
            "delete_row_apply",
        )
    }

    fn structural_count_row_count(
        prepared: &crate::db::executor::delete::types::PreparedDeleteOutput<()>,
    ) -> u32 {
        u32::try_from(prepared.row_count).unwrap_or(u32::MAX)
    }

    // Run one delete plan through the shared outer shell: span setup, runtime
    // preparation, plan metrics, row-count attribution, and error recording.
    fn execute_with_delete_runtime<T>(
        self,
        plan: PreparedExecutionPlan<E>,
        run: impl FnOnce(
            &Db<E::Canister>,
            PreparedDeleteExecutionState,
            StoreHandle,
        ) -> Result<(T, usize), InternalError>,
    ) -> Result<T, InternalError> {
        let mut span = Span::<E>::new(ExecKind::Delete);
        let result = (|| {
            let (prepared, store) = prepare_delete_runtime(&self.db, plan)?;
            record_plan_metrics(
                prepared.authority.entity.entity_path(),
                &prepared.logical_plan,
            );
            let (output, row_count) = run(&self.db, prepared, store)?;
            set_rows_from_len(&mut span, row_count);

            Ok(output)
        })();
        if let Err(err) = &result {
            span.set_error(err);
        }

        result
    }

    /// Execute one delete plan and return deleted entities in response order.
    pub(in crate::db) fn execute(
        self,
        plan: PreparedExecutionPlan<E>,
    ) -> Result<EntityResponse<E>, InternalError> {
        self.execute_with_delete_runtime(plan, |db, prepared, store| {
            // Phase 1: run the shared typed delete core and package response rows.
            let Some(typed) =
                prepare_typed_delete_core(db, store, &prepared, package_typed_delete_rows::<E>)?
            else {
                return Ok((EntityResponse::new(Vec::new()), 0));
            };
            let row_count = typed.row_count;

            // Phase 2: apply the already prepared delete commit payload.
            Self::apply_prepared_delete_commit(db, &prepared, typed.commit.row_ops)?;

            // Phase 3: return the already-prepared typed delete response rows.
            Ok((EntityResponse::new(typed.output), row_count))
        })
    }

    /// Execute one structural delete projection plan with an optional
    /// pre-commit row bound for bounded SQL exposure policies.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn execute_structural_projection_with_bounds(
        self,
        plan: PreparedExecutionPlan<E>,
        bounds: DeleteProjectionBounds,
        validate_precommit: impl FnOnce(&DeleteProjection) -> Result<(), InternalError>,
    ) -> Result<DeleteProjection, InternalError> {
        self.execute_with_delete_runtime(plan, |db, prepared, store| {
            // Phase 1: run the shared structural delete core and apply the
            // final typed commit-window bridge only at the boundary.
            let Some(projection) = prepare_structural_delete_projection_core(
                db,
                store,
                &prepared,
                bounds,
                validate_precommit,
            )?
            else {
                return Ok((DeleteProjection::empty(), 0));
            };
            let row_count = usize::try_from(projection.output.row_count()).unwrap_or(usize::MAX);

            Self::apply_prepared_delete_commit(db, &prepared, projection.commit.row_ops)?;

            // Phase 2: return the already prepared structural delete projection.
            Ok((projection.output, row_count))
        })
    }

    /// Execute one delete plan and return only the affected-row count.
    pub(in crate::db) fn execute_count(
        self,
        plan: PreparedExecutionPlan<E>,
    ) -> Result<u32, InternalError> {
        self.execute_with_delete_runtime(plan, |db, prepared, store| {
            // Phase 1: run the structural delete-count core so accepted-schema
            // row layouts are preserved for old physical rows. Count-only
            // deletes do not need typed entity materialization.
            let Some(count) = prepare_structural_delete_count_core(db, store, &prepared)? else {
                return Ok((0, 0));
            };
            let row_count = Self::structural_count_row_count(&count);
            let row_count_len = count.row_count;

            Self::apply_prepared_delete_commit(db, &prepared, count.commit.row_ops)?;

            // Phase 2: return only the final affected-row count.
            Ok((row_count, row_count_len))
        })
    }

    /// Execute one delete plan and return only the affected-row count while
    /// checking the selected-row bound before the commit window.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn execute_count_with_bounds(
        self,
        plan: PreparedExecutionPlan<E>,
        bounds: DeleteProjectionBounds,
    ) -> Result<u32, InternalError> {
        self.execute_with_delete_runtime(plan, |db, prepared, store| {
            // Phase 1: run the structural delete-count core with the SQL
            // exposure-policy bound before applying the commit payload.
            let Some(count) =
                prepare_structural_delete_count_core_with_bounds(db, store, &prepared, bounds)?
            else {
                return Ok((0, 0));
            };
            let row_count = Self::structural_count_row_count(&count);
            let row_count_len = count.row_count;

            Self::apply_prepared_delete_commit(db, &prepared, count.commit.row_ops)?;

            // Phase 2: return only the final affected-row count.
            Ok((row_count, row_count_len))
        })
    }
}
