use crate::{
    db::{
        CommitApplyGuard, CommitDataOp, CommitIndexOp, CommitKind, CommitMarker, Db, begin_commit,
        ensure_recovered_for_write,
        executor::{
            ExecutorError,
            commit_ops::{apply_marker_index_ops, resolve_index_key},
            debug::{access_summary, yes_no},
            plan::{record_plan_metrics, set_rows_from_len},
            trace::{QueryTraceSink, TraceExecutorKind, TracePhase, start_plan_trace},
        },
        finish_commit,
        index::{
            IndexEntry, IndexEntryCorruption, IndexKey, IndexStore, RawIndexEntry, RawIndexKey,
        },
        query::plan::{ExecutablePlan, validate::validate_executor_plan},
        response::Response,
        store::{DataKey, DataRow, DataStore, RawDataKey, RawRow},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    prelude::*,
    traits::{EntityKind, EntityValue, Path, Storable},
    types::Id,
};
use std::{
    borrow::Cow, cell::RefCell, collections::BTreeMap, marker::PhantomData, thread::LocalKey,
};

///
/// IndexPlan
/// Prevalidated handle to an index store used during commit planning.
///

struct IndexPlan {
    index: &'static IndexModel,
    store: &'static LocalKey<RefCell<IndexStore>>,
}

///
/// PreparedIndexRollback
/// Prevalidated rollback mutation for index entries.
///

struct PreparedIndexRollback {
    store: &'static LocalKey<RefCell<IndexStore>>,
    key: RawIndexKey,
    value: Option<RawIndexEntry>,
}

///
/// PreparedDataRollback
/// Prevalidated rollback mutation for data rows.
///

struct PreparedDataRollback {
    key: RawDataKey,
    value: RawRow,
}

///
/// DeleteRow
/// Row wrapper used during delete planning and execution.
///

struct DeleteRow<E>
where
    E: EntityKind,
{
    key: DataKey,
    raw: Option<RawRow>,
    entity: E,
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
            let mut rows = decode_rows::<E>(data_rows)?;
            let access_rows = rows.len();

            // Post-access phase: filter, order, and apply delete limits.
            // removed plan.apply_post_access::<E, _>(&mut rows)?;

            if rows.is_empty() {
                if let Some(trace) = trace.as_ref() {
                    // NOTE: Trace metrics saturate on overflow; diagnostics only.
                    let to_u64 = |len| u64::try_from(len).unwrap_or(u64::MAX);
                    trace.phase(TracePhase::Access, to_u64(access_rows));
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
            if index_apply_stores.len() != marker.index_ops.len() {
                return Err(InternalError::new(
                    ErrorClass::InvariantViolation,
                    ErrorOrigin::Index,
                    format!(
                        "commit marker index ops length mismatch: {} ops vs {} stores ({})",
                        marker.index_ops.len(),
                        index_apply_stores.len(),
                        E::PATH
                    ),
                ));
            }
            let commit = begin_commit(marker)?;
            commit_started = true;
            self.debug_log("Delete commit window opened");

            finish_commit(commit, |guard| {
                // Commit boundary: apply the marker's raw mutations mechanically.
                // Durable correctness is marker + recovery owned; guard rollback
                // here is best-effort, in-process cleanup only.
                let mut apply_guard = CommitApplyGuard::new("delete_marker_apply");
                let index_rollback_ops = index_rollback_ops;
                apply_guard
                    .record_rollback(move || Self::apply_index_rollbacks(index_rollback_ops));
                apply_marker_index_ops(&guard.marker.index_ops, index_apply_stores);
                for _ in 0..index_remove_count {
                    sink::record(MetricsEvent::IndexRemove {
                        entity_path: E::PATH,
                    });
                }

                // Apply data mutations recorded in the marker.
                let data_rollback_ops = data_rollback_ops;
                apply_guard.record_rollback(move || {
                    Self::apply_data_rollbacks(data_store, data_rollback_ops);
                });
                Self::apply_marker_data_ops(&guard.marker.data_ops, data_store);

                apply_guard.finish()?;

                Ok(())
            })?;

            // Emit per-phase counts after the delete succeeds.
            if let Some(trace) = trace.as_ref() {
                // NOTE: Trace metrics saturate on overflow; diagnostics only.
                let to_u64 = |len| u64::try_from(len).unwrap_or(u64::MAX);
                trace.phase(TracePhase::Access, to_u64(access_rows));
            }

            let res = rows
                .into_iter()
                .map(|row| Ok((Id::from_storage_key(row.key.try_key::<E>()?), row.entity)))
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

    // ─────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────

    // Resolve commit marker index ops into stores and rollback bytes before committing.
    #[expect(clippy::type_complexity)]
    fn prepare_index_delete_ops(
        plans: &[IndexPlan],
        ops: &[CommitIndexOp],
    ) -> Result<
        (
            Vec<&'static LocalKey<RefCell<IndexStore>>>,
            Vec<PreparedIndexRollback>,
        ),
        InternalError,
    > {
        // Resolve store handles once so commit-time apply is mechanical.
        let mut stores = BTreeMap::new();
        for plan in plans {
            stores.insert(plan.index.store, plan.store);
        }

        let mut apply_stores = Vec::with_capacity(ops.len());
        let mut rollbacks = Vec::with_capacity(ops.len());

        // Prevalidate commit ops and capture rollback bytes from current state.
        for op in ops {
            let (store, raw_key) = resolve_index_key(&stores, op, E::PATH, || {
                Some(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Index,
                    format!(
                        "commit marker index op missing entry before delete: {} ({})",
                        op.store,
                        E::PATH
                    ),
                ))
            })?;
            let rollback_value = store.with_borrow(|s| s.get(&raw_key));
            let rollback_value = rollback_value.ok_or_else(|| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Index,
                    format!(
                        "commit marker index op missing entry before delete: {} ({})",
                        op.store,
                        E::PATH
                    ),
                )
            })?;

            apply_stores.push(store);
            rollbacks.push(PreparedIndexRollback {
                store,
                key: raw_key,
                value: Some(rollback_value),
            });
        }

        Ok((apply_stores, rollbacks))
    }

    // Resolve commit marker data ops and capture rollback rows before committing.
    fn prepare_data_delete_ops(
        ops: &[CommitDataOp],
        rollback_rows: &BTreeMap<RawDataKey, RawRow>,
    ) -> Result<Vec<PreparedDataRollback>, InternalError> {
        let mut rollbacks = Vec::with_capacity(ops.len());

        // Validate marker ops and map them to rollback rows.
        for op in ops {
            if op.store != E::DataStore::PATH {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!(
                        "commit marker references unexpected data store '{}' ({})",
                        op.store,
                        E::PATH
                    ),
                ));
            }
            if op.key.len() != DataKey::STORED_SIZE_USIZE {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!(
                        "commit marker data key length {} does not match {} ({})",
                        op.key.len(),
                        DataKey::STORED_SIZE_USIZE,
                        E::PATH
                    ),
                ));
            }
            if op.value.is_some() {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit marker delete includes data payload ({})", E::PATH),
                ));
            }

            let raw_key = RawDataKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
            let raw_row = rollback_rows.get(&raw_key).ok_or_else(|| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit marker data op missing rollback row ({})", E::PATH),
                )
            })?;
            rollbacks.push(PreparedDataRollback {
                key: raw_key,
                value: raw_row.clone(),
            });
        }

        Ok(rollbacks)
    }

    // Apply rollback mutations for index entries using raw bytes.
    fn apply_index_rollbacks(ops: Vec<PreparedIndexRollback>) {
        for op in ops {
            op.store.with_borrow_mut(|s| {
                if let Some(value) = op.value {
                    s.insert(op.key, value);
                } else {
                    s.remove(&op.key);
                }
            });
        }
    }

    // Apply commit marker data deletes using raw keys only.
    fn apply_marker_data_ops(ops: &[CommitDataOp], store: &'static LocalKey<RefCell<DataStore>>) {
        // SAFETY / INVARIANT:
        // All structural and semantic invariants for these marker ops are fully
        // validated during planning *before* the commit marker is persisted.
        // After marker creation, apply is required to be infallible or trap.
        for op in ops {
            assert!(
                op.value.is_none(),
                "commit marker delete includes data payload ({})",
                E::PATH
            );
            let raw_key = RawDataKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
            store.with_borrow_mut(|s| s.remove(&raw_key));
        }
    }

    // Apply rollback mutations for data rows.
    fn apply_data_rollbacks(
        store: &'static LocalKey<RefCell<DataStore>>,
        ops: Vec<PreparedDataRollback>,
    ) {
        for op in ops {
            store.with_borrow_mut(|s| s.insert(op.key, op.value));
        }
    }

    fn build_index_plans(&self) -> Result<Vec<IndexPlan>, InternalError> {
        E::INDEXES
            .iter()
            .map(|index| {
                let store = self.db.with_index(|reg| reg.try_get_store(index.store))?;
                Ok(IndexPlan { index, store })
            })
            .collect()
    }

    // Build commit-time index ops and count entity-level removals for metrics.
    #[expect(clippy::too_many_lines)]
    fn build_index_removal_ops(
        plans: &[IndexPlan],
        entities: &[&E],
    ) -> Result<(Vec<CommitIndexOp>, usize), InternalError> {
        let mut ops = Vec::new();
        let mut removed = 0usize;

        // Process each index independently to compute its resulting mutations.
        for plan in plans {
            let fields = plan.index.fields.join(", ");

            // Map raw index keys → updated entry (or None if fully removed).
            let mut entries: BTreeMap<RawIndexKey, Option<IndexEntry<E>>> = BTreeMap::new();

            // Fold entity deletions into per-key index entry updates.
            for entity in entities {
                let Some(key) = IndexKey::new(*entity, plan.index)? else {
                    continue;
                };
                let raw_key = key.to_raw();
                let entity_id = entity.id().key();

                // Lazily load and decode the existing index entry once per key.
                let entry = match entries.entry(raw_key) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        let decoded = plan.store.with_borrow(|s| {
                            s.get(&raw_key)
                                .map(|raw| {
                                    raw.try_decode::<E>().map_err(|err| {
                                        ExecutorError::corruption(
                                            ErrorOrigin::Index,
                                            format!(
                                                "index corrupted: {} ({}) -> {}",
                                                E::PATH,
                                                fields,
                                                err
                                            ),
                                        )
                                    })
                                })
                                .transpose()
                        })?;
                        slot.insert(decoded)
                    }

                    std::collections::btree_map::Entry::Occupied(slot) => slot.into_mut(),
                };

                // Prevalidate membership to keep commit-phase mutations infallible.
                let Some(e) = entry.as_ref() else {
                    return Err(ExecutorError::corruption(
                        ErrorOrigin::Index,
                        format!(
                            "index corrupted: {} ({}) -> {}",
                            E::PATH,
                            fields,
                            IndexEntryCorruption::missing_key(raw_key, entity_id),
                        ),
                    )
                    .into());
                };

                if plan.index.unique && e.len() > 1 {
                    return Err(ExecutorError::corruption(
                        ErrorOrigin::Index,
                        format!(
                            "index corrupted: {} ({}) -> {}",
                            E::PATH,
                            fields,
                            IndexEntryCorruption::NonUniqueEntry { keys: e.len() },
                        ),
                    )
                    .into());
                }

                if !e.contains(entity_id) {
                    return Err(ExecutorError::corruption(
                        ErrorOrigin::Index,
                        format!(
                            "index corrupted: {} ({}) -> {}",
                            E::PATH,
                            fields,
                            IndexEntryCorruption::missing_key(raw_key, entity_id),
                        ),
                    )
                    .into());
                }
                removed = removed.saturating_add(1);

                // Remove this entity’s key from the index entry.
                if let Some(e) = entry.as_mut() {
                    e.remove(entity_id);
                    if e.is_empty() {
                        *entry = None;
                    }
                }
            }

            // Emit commit ops for each touched index key.
            for (raw_key, entry) in entries {
                let value = if let Some(entry) = entry {
                    let raw = RawIndexEntry::try_from(&entry).map_err(|err| match err {
                        crate::db::index::entry::IndexEntryEncodeError::TooManyKeys { keys } => {
                            InternalError::new(
                                ErrorClass::Corruption,
                                ErrorOrigin::Index,
                                format!(
                                    "index corrupted: {} ({}) -> {}",
                                    E::PATH,
                                    fields,
                                    IndexEntryCorruption::TooManyKeys { count: keys }
                                ),
                            )
                        }
                        crate::db::index::entry::IndexEntryEncodeError::KeyEncoding(err) => {
                            InternalError::new(
                                ErrorClass::Unsupported,
                                ErrorOrigin::Index,
                                format!(
                                    "index key encoding failed: {} ({fields}) -> {err}",
                                    E::PATH
                                ),
                            )
                        }
                    })?;

                    Some(raw.as_bytes().to_vec())
                } else {
                    // None means the index entry is fully removed.
                    None
                };

                ops.push(CommitIndexOp {
                    store: plan.index.store.to_string(),
                    key: raw_key.as_bytes().to_vec(),
                    value,
                });
            }
        }

        Ok((ops, removed))
    }
}

fn decode_rows<E: EntityKind + EntityValue>(
    rows: Vec<DataRow>,
) -> Result<Vec<DeleteRow<E>>, InternalError> {
    rows.into_iter()
        .map(|(dk, raw)| {
            let dk_for_err = dk.clone();
            let entity = raw.try_decode::<E>().map_err(|err| {
                ExecutorError::corruption(
                    ErrorOrigin::Serialize,
                    format!("failed to deserialize row: {dk_for_err} ({err})"),
                )
            })?;

            let expected = dk.try_key::<E>()?;
            let actual = entity.id().key();
            if expected != actual {
                return Err(ExecutorError::corruption(
                    ErrorOrigin::Store,
                    format!("row key mismatch: expected {expected:?}, found {actual:?}"),
                )
                .into());
            }

            Ok(DeleteRow {
                key: dk,
                raw: Some(raw),
                entity,
            })
        })
        .collect()
}
