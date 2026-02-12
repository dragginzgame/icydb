mod helpers;

use crate::{
    db::{
        CommitDataOp, CommitKind, CommitMarker, Db, begin_commit, ensure_recovered_for_write,
        executor::{
            debug::{access_summary, yes_no},
            mutation::{
                MarkerDataOpMode, PreparedMarkerApply, apply_prepared_marker_ops,
                validate_index_apply_stores_len,
            },
            plan::{record_plan_metrics, set_rows_from_len},
            trace::{QueryTraceSink, TraceExecutorKind, TracePhase, start_plan_trace},
        },
        query::plan::{ExecutablePlan, validate::validate_executor_plan},
        response::Response,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    traits::{EntityKind, EntityValue, Path},
    types::Id,
};
use std::{collections::BTreeMap, marker::PhantomData};

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
pub struct DeleteExecutor<E>
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
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
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

    #[allow(clippy::too_many_lines)]
    pub fn execute(self, plan: ExecutablePlan<E>) -> Result<Response<E>, InternalError> {
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
            sink::record(MetricsEvent::RowsScanned {
                entity_path: E::PATH,
                rows_scanned: data_rows.len() as u64,
            });

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
                if let Some(trace) = trace.as_ref() {
                    // NOTE: Trace metrics saturate on overflow; diagnostics only.
                    let to_u64 = |len| u64::try_from(len).unwrap_or(u64::MAX);
                    trace.phase(TracePhase::Access, to_u64(access_rows));
                    trace.phase(TracePhase::PostAccess, to_u64(post_access_rows));
                }
                set_rows_from_len(&mut span, 0);
                self.debug_log("Delete complete -> 0 rows (nothing to commit)");
                return Ok(Response(Vec::new()));
            }

            let index_plans = self.build_index_plans()?;
            let (index_ops, index_remove_count) = {
                let entities: Vec<&E> = rows.iter().map(|row| &row.entity).collect();
                Self::build_index_removal_ops(&index_plans, &entities)?
            };

            // Preflight store access to ensure no fallible work remains post-commit.
            ctx.with_store(|_| ())?;
            let data_store = self
                .db
                .with_data(|reg| reg.try_get_store(E::DataStore::PATH))?;

            let mut rollback_rows = BTreeMap::new();
            let data_ops = rows
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
                    rollback_rows.insert(raw_key, raw_row);
                    Ok(CommitDataOp {
                        store: E::DataStore::PATH.to_string(),
                        key: raw_key.as_bytes().to_vec(),
                        value: None,
                    })
                })
                .collect::<Result<Vec<_>, InternalError>>()?;

            let marker = CommitMarker::new(CommitKind::Delete, index_ops, data_ops)?;
            let (index_apply_stores, index_rollback_ops) =
                Self::prepare_index_delete_ops(&index_plans, &marker.index_ops)?;
            let data_rollback_ops =
                Self::prepare_data_delete_ops(&marker.data_ops, &rollback_rows)?;
            validate_index_apply_stores_len(&marker, index_apply_stores.len(), E::PATH)?;
            let prepared_apply = PreparedMarkerApply {
                index_apply_stores,
                index_rollback_ops,
                data_store,
                data_rollback_ops,
                data_mode: MarkerDataOpMode::DeleteRemove,
                entity_path: E::PATH,
            };
            let commit = begin_commit(marker)?;
            commit_started = true;
            self.debug_log("Delete commit window opened");

            apply_prepared_marker_ops(
                commit,
                "delete_marker_apply",
                prepared_apply,
                || {
                    for _ in 0..index_remove_count {
                        sink::record(MetricsEvent::IndexRemove {
                            entity_path: E::PATH,
                        });
                    }
                },
                || {},
            )?;

            // Emit per-phase counts after the delete succeeds.
            if let Some(trace) = trace.as_ref() {
                // NOTE: Trace metrics saturate on overflow; diagnostics only.
                let to_u64 = |len| u64::try_from(len).unwrap_or(u64::MAX);
                trace.phase(TracePhase::Access, to_u64(access_rows));
                trace.phase(TracePhase::PostAccess, to_u64(post_access_rows));
            }

            let res = rows
                .into_iter()
                .map(|row| Ok((Id::from_key(row.key.try_key::<E>()?), row.entity)))
                .collect::<Result<Vec<_>, InternalError>>()?;
            set_rows_from_len(&mut span, res.len());
            self.debug_log(format!("Delete committed -> {} rows", res.len()));

            Ok(Response(res))
        })();

        if commit_started && result.is_err() {
            self.debug_log("Delete failed during marker apply; best-effort cleanup attempted");
        }

        if let Some(trace) = trace {
            // NOTE: Trace metrics saturate on overflow; diagnostics only.
            match &result {
                Ok(resp) => trace.finish(u64::try_from(resp.0.len()).unwrap_or(u64::MAX)),
                Err(err) => trace.error(err),
            }
        }

        result
    }
}
