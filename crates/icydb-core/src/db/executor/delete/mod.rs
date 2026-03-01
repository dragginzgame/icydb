//! Module: executor::delete
//! Responsibility: delete-plan execution and commit-window handoff.
//! Does not own: logical planning, relation semantics, or cursor protocol details.
//! Boundary: delete-specific preflight/decode/apply flow over executable plans.

use crate::{
    db::{
        Db,
        commit::{CommitRowOp, commit_schema_fingerprint_for_entity},
        data::{DataKey, DataRow, RawRow, decode_and_validate_entity_key},
        executor::{
            ExecutablePlan, ExecutionKernel, ExecutionPreparation, ExecutorError, PlanRow,
            mutation::{
                commit_delete_row_ops_with_window, mutation_write_context, preflight_mutation_plan,
            },
            plan_metrics::{record_plan_metrics, record_rows_scanned, set_rows_from_len},
        },
        query::{plan::LogicalPlan, policy},
        response::Response,
    },
    error::InternalError,
    obs::sink::{ExecKind, Span},
    traits::{EntityKind, EntityValue},
    types::Id,
};
use std::{collections::BTreeSet, marker::PhantomData};

///
/// DeleteRow
/// Row wrapper used during delete planning and execution.
///

pub(super) struct DeleteRow<E>
where
    E: EntityKind,
{
    pub(super) key: DataKey,
    pub(super) raw: Option<RawRow>,
    pub(super) entity: E,
}

impl<E: EntityKind> PlanRow<E> for DeleteRow<E> {
    fn entity(&self) -> &E {
        &self.entity
    }
}

/// Decode raw access rows into typed delete rows with key/entity checks.
pub(super) fn decode_rows<E: EntityKind + EntityValue>(
    rows: Vec<DataRow>,
) -> Result<Vec<DeleteRow<E>>, InternalError> {
    rows.into_iter()
        .map(|(dk, raw)| {
            let expected = dk.try_key::<E>()?;
            let entity = decode_and_validate_entity_key::<E, _, _, _, _>(
                expected,
                || raw.try_decode::<E>(),
                |err| {
                    ExecutorError::serialize_corruption(format!(
                        "failed to deserialize row: {dk} ({err})"
                    ))
                    .into()
                },
                |expected, actual| {
                    ExecutorError::store_corruption(format!(
                        "row key mismatch: expected {expected:?}, found {actual:?}"
                    ))
                    .into()
                },
            )?;

            Ok(DeleteRow {
                key: dk,
                raw: Some(raw),
                entity,
            })
        })
        .collect()
}

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
    /// Construct one delete executor bound to a database handle.
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

    /// Execute one delete plan and return deleted entities in response order.
    pub(crate) fn execute(self, plan: ExecutablePlan<E>) -> Result<Response<E>, InternalError> {
        match &plan.as_inner().logical {
            LogicalPlan::Scalar(_) => {}
            LogicalPlan::Grouped(_) => {
                return Err(InternalError::executor_unsupported(
                    "grouped query execution is not yet enabled in this release",
                ));
            }
        }

        if !plan.mode().is_delete() {
            return Err(InternalError::query_executor_invariant(
                "delete executor requires delete plans",
            ));
        }
        debug_assert!(
            policy::validate_plan_shape(&plan.as_inner().logical).is_ok(),
            "delete executor received a plan shape that bypassed planning validation",
        );
        (|| {
            // Phase 1: preflight plan + context setup before any commit-window work.
            let index_prefix_specs = plan.index_prefix_specs()?.to_vec();
            let index_range_specs = plan.index_range_specs()?.to_vec();
            let plan = plan.into_inner();
            let execution_preparation = ExecutionPreparation::for_plan::<E>(&plan);
            preflight_mutation_plan::<E>(&plan)?;
            let ctx = mutation_write_context::<E>(&self.db)?;

            let mut span = Span::<E>::new(ExecKind::Delete);
            record_plan_metrics(&plan.access);

            // Access phase: resolve candidate rows before delete filtering.
            let data_rows = ctx.rows_from_access_plan(
                &plan.access,
                index_prefix_specs.as_slice(),
                index_range_specs.as_slice(),
                plan.scalar_plan().consistency,
            )?;
            record_rows_scanned::<E>(data_rows.len());

            // Decode rows into entities before post-access filtering.
            let mut rows = decode_rows::<E>(data_rows)?;

            // Post-access phase: filter, order, and apply delete limits.
            let stats = ExecutionKernel::apply_post_access_with_compiled_predicate::<E, _, _>(
                &plan,
                &mut rows,
                execution_preparation.compiled_predicate(),
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
            let schema_fingerprint = commit_schema_fingerprint_for_entity::<E>();
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
                        schema_fingerprint,
                    ))
                })
                .collect::<Result<Vec<_>, InternalError>>()?;
            commit_delete_row_ops_with_window::<E>(&self.db, row_ops, "delete_row_apply")?;

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
