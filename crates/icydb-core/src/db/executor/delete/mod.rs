mod helpers;

use crate::{
    db::{
        Db,
        commit::{CommitRowOp, ensure_recovered_for_write},
        executor::{
            debug::{access_summary, yes_no},
            mutation::{
                OpenCommitWindow, apply_prepared_row_ops, emit_index_delta_metrics,
                open_commit_window,
            },
            plan::{record_plan_metrics, record_rows_scanned, set_rows_from_len},
            trace::{
                QueryTraceSink, TraceExecutorKind, emit_access_post_access_phases,
                finish_trace_from_result, start_plan_trace,
            },
        },
        query::plan::{ExecutablePlan, validate::validate_executor_plan},
        response::Response,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
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
    debug: bool,
    trace: Option<&'static dyn QueryTraceSink>,
    _marker: PhantomData<E>,
}

impl<E> DeleteExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Debug is session-scoped via DbSession and propagated into executors;
    // executors do not expose independent debug control.
    #[must_use]
    pub(crate) const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            trace: None,
            _marker: PhantomData,
        }
    }

    fn debug_log(&self, s: impl Into<String>) {
        if self.debug {
            println!("[debug] {}", s.into());
        }
    }

    // ─────────────────────────────────────────────
    // Plan-based delete
    // ─────────────────────────────────────────────

    #[expect(clippy::too_many_lines)]
    pub(crate) fn execute(self, plan: ExecutablePlan<E>) -> Result<Response<E>, InternalError> {
        if !plan.mode().is_delete() {
            return Err(InternalError::new(
                ErrorClass::Unsupported,
                ErrorOrigin::Query,
                "delete executor requires delete plans".to_string(),
            ));
        }
        let mut commit_started = false;
        let trace = start_plan_trace(self.trace, TraceExecutorKind::Delete, &plan);
        let result = (|| {
            // Recovery is mandatory before mutations; read paths recover separately.
            ensure_recovered_for_write(&self.db)?;
            let plan = plan.into_inner();
            validate_executor_plan::<E>(&plan)?;
            let ctx = self.db.recovered_context::<E>()?;

            if self.debug {
                let access = access_summary(&plan.access);
                let ordered = plan
                    .order
                    .as_ref()
                    .is_some_and(|order| !order.fields.is_empty());
                let delete_limit = match plan.delete_limit {
                    Some(limit) => limit.max_rows.to_string(),
                    None => "none".to_string(),
                };

                self.debug_log(format!(
                    "Delete plan on {} (consistency={:?})",
                    E::PATH,
                    plan.consistency
                ));
                self.debug_log(format!("Access: {access}"));
                self.debug_log(format!(
                    "Intent: predicate={}, order={}, delete_limit={}",
                    yes_no(plan.predicate.is_some()),
                    yes_no(ordered),
                    delete_limit
                ));
            }

            let mut span = Span::<E>::new(ExecKind::Delete);
            record_plan_metrics(&plan.access);

            // Access phase: resolve candidate rows before delete filtering.
            let data_rows = ctx.rows_from_access_plan(&plan.access, plan.consistency)?;
            record_rows_scanned::<E>(data_rows.len());

            // Decode rows into entities before post-access filtering.
            let mut rows = helpers::decode_rows::<E>(data_rows)?;
            let access_rows = rows.len();

            // Post-access phase: filter, order, and apply delete limits.
            let stats = plan.apply_post_access::<E, _>(&mut rows)?;
            let post_access_rows = rows.len();
            if stats.delete_was_limited {
                self.debug_log(format!(
                    "applied delete limit -> {} entities selected",
                    rows.len()
                ));
            }

            if rows.is_empty() {
                emit_access_post_access_phases(trace.as_ref(), access_rows, post_access_rows);
                set_rows_from_len(&mut span, 0);
                self.debug_log("Delete complete -> 0 rows (nothing to commit)");
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
                        InternalError::new(
                            ErrorClass::Internal,
                            ErrorOrigin::Store,
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
                delta,
            } = open_commit_window::<E>(&self.db, row_ops)?;
            commit_started = true;
            self.debug_log("Delete commit window opened");

            apply_prepared_row_ops(
                commit,
                "delete_row_apply",
                prepared_row_ops,
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

            // Emit per-phase counts after the delete succeeds.
            emit_access_post_access_phases(trace.as_ref(), access_rows, post_access_rows);

            // Response identifiers are validated before begin_commit. The apply
            // phase remains mechanical after the commit boundary.
            let res = response_ids
                .into_iter()
                .zip(rows)
                .map(|(id, row)| (id, row.entity))
                .collect::<Vec<_>>();
            set_rows_from_len(&mut span, res.len());
            self.debug_log(format!("Delete committed -> {} rows", res.len()));

            Ok(Response(res))
        })();

        if commit_started && result.is_err() {
            self.debug_log("Delete failed during marker apply; best-effort cleanup attempted");
        }

        finish_trace_from_result(trace, &result, |resp| resp.0.len());

        result
    }
}
