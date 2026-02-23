mod helpers;

use crate::{
    db::{
        Db,
        commit::{CommitRowOp, ensure_recovered_for_write},
        executor::{
            mutation::{
                OpenCommitWindow, apply_prepared_row_ops, emit_index_delta_metrics,
                open_commit_window,
            },
            plan::{record_plan_metrics, record_rows_scanned, set_rows_from_len},
        },
        query::plan::{ExecutablePlan, validate::validate_executor_plan},
        response::Response,
    },
    error::InternalError,
    obs::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::{collections::BTreeSet, marker::PhantomData};

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
pub(crate) struct DeleteExecutor<E>
where
    E: EntityKind,
{
    db: Db<E::Canister>,
    _marker: PhantomData<E>,
}

impl<E> DeleteExecutor<E>
where
    E: EntityKind + EntityValue,
{
    #[must_use]
    pub(crate) const fn new(db: Db<E::Canister>, _debug: bool) -> Self {
        Self {
            db,
            _marker: PhantomData,
        }
    }

    // ─────────────────────────────────────────────
    // Plan-based delete
    // ─────────────────────────────────────────────

    pub(crate) fn execute(self, plan: ExecutablePlan<E>) -> Result<Response<E>, InternalError> {
        if !plan.mode().is_delete() {
            return Err(InternalError::query_executor_invariant(
                "delete executor requires delete plans",
            ));
        }
        (|| {
            // Recovery is mandatory before mutations; read paths recover separately.
            ensure_recovered_for_write(&self.db)?;
            let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
            let index_range_specs = plan.index_range_specs()?.to_vec();
            let (plan, predicate_slots) = plan.into_parts();
            validate_executor_plan::<E>(&plan)?;
            let ctx = self.db.recovered_context::<E>()?;

            let mut span = Span::<E>::new(ExecKind::Delete);
            record_plan_metrics(&plan.access);

            // Access phase: resolve candidate rows before delete filtering.
            let data_rows = ctx.rows_from_access_plan(
                &plan.access,
                index_prefix_specs.as_slice(),
                index_range_specs.as_slice(),
                plan.consistency,
            )?;
            record_rows_scanned::<E>(data_rows.len());

            // Decode rows into entities before post-access filtering.
            let mut rows = helpers::decode_rows::<E>(data_rows)?;

            // Post-access phase: filter, order, and apply delete limits.
            let stats = plan.apply_post_access_with_compiled_predicate::<E, _>(
                &mut rows,
                predicate_slots.as_ref(),
            )?;
            let _ = stats.delete_was_limited;

            if rows.is_empty() {
                set_rows_from_len(&mut span, 0);
                return Ok(Response(Vec::new()));
            }

            // Relation phase: reject target deletes that are still strongly referenced.
            let deleted_target_keys = rows
                .iter()
                .map(|row| row.key.to_raw())
                .collect::<Result<BTreeSet<_>, InternalError>>()?;
            self.db
                .validate_delete_strong_relations(E::PATH, &deleted_target_keys)?;
            let response_ids = rows
                .iter()
                .map(|row| Ok(Id::from_key(row.key.try_key::<E>()?)))
                .collect::<Result<Vec<_>, InternalError>>()?;

            // Preflight store access to ensure no fallible work remains post-commit.
            ctx.with_store(|_| ())?;
            let row_ops = rows
                .iter_mut()
                .map(|row| {
                    let raw_key = row.key.to_raw()?;
                    let raw_row = row.raw.take().ok_or_else(|| {
                        InternalError::store_internal(
                            "missing raw row for delete rollback".to_string(),
                        )
                    })?;
                    Ok(CommitRowOp::new(
                        E::PATH,
                        raw_key.as_bytes().to_vec(),
                        Some(raw_row.as_bytes().to_vec()),
                        None,
                    ))
                })
                .collect::<Result<Vec<_>, InternalError>>()?;
            let OpenCommitWindow {
                commit,
                prepared_row_ops,
                index_store_guards,
                delta,
            } = open_commit_window::<E>(&self.db, row_ops)?;

            apply_prepared_row_ops(
                commit,
                "delete_row_apply",
                prepared_row_ops,
                index_store_guards,
                || {
                    emit_index_delta_metrics::<E>(
                        0,
                        delta.index_removes,
                        0,
                        delta.reverse_index_removes,
                    );
                },
                || {},
            )?;

            // Response identifiers are validated before begin_commit. The apply
            // phase remains mechanical after the commit boundary.
            let res = response_ids
                .into_iter()
                .zip(rows)
                .map(|(id, row)| (id, row.entity))
                .collect::<Vec<_>>();
            set_rows_from_len(&mut span, res.len());

            Ok(Response(res))
        })()
    }
}
