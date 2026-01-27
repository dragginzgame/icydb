use crate::{
    db::{
        CommitDataOp, CommitIndexOp, CommitKind, CommitMarker, Db, WriteUnit, begin_commit,
        ensure_recovered,
        executor::{
            Context, ExecutorError, UniqueIndexHandle,
            plan::{record_plan_metrics, set_rows_from_len},
            resolve_unique_pk,
            trace::{
                QueryTraceSink, TraceAccess, TraceExecutorKind, TracePhase, start_exec_trace,
                start_plan_trace,
            },
        },
        finish_commit,
        index::{
            IndexEntry, IndexEntryCorruption, IndexKey, IndexStore, MAX_INDEX_ENTRY_BYTES,
            RawIndexEntry, RawIndexKey,
        },
        query::plan::ExecutablePlan,
        response::Response,
        store::{DataKey, DataRow, RawDataKey, RawRow},
        traits::FromKey,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    prelude::*,
    sanitize::sanitize,
    traits::{EntityKind, Path, Storable},
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

// Prevalidated rollback mutation for index entries.
struct PreparedIndexRollback {
    store: &'static LocalKey<RefCell<IndexStore>>,
    key: RawIndexKey,
    value: Option<RawIndexEntry>,
}

// Prevalidated rollback mutation for data rows.
struct PreparedDataRollback {
    key: RawDataKey,
    value: RawRow,
}

// Row wrapper used during delete planning and execution.
struct DeleteRow<E> {
    key: DataKey,
    raw: Option<RawRow>,
    entity: E,
}

impl<E: EntityKind> crate::db::query::plan::logical::PlanRow<E> for DeleteRow<E> {
    fn entity(&self) -> &E {
        &self.entity
    }
}

///
/// DeleteExecutor
///
/// Stage-1 atomicity invariant:
/// All fallible validation completes before the first stable write.
/// Mutations run inside a WriteUnit so mid-flight failures roll back
/// before the commit marker is cleared.
///
#[derive(Clone, Copy)]
pub struct DeleteExecutor<E: EntityKind> {
    db: Db<E::Canister>,
    debug: bool,
    trace: Option<&'static dyn QueryTraceSink>,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> DeleteExecutor<E> {
    #[must_use]
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            trace: None,
            _marker: PhantomData,
        }
    }

    #[must_use]
    #[allow(dead_code)]
    pub(crate) const fn with_trace_sink(
        mut self,
        sink: Option<&'static dyn QueryTraceSink>,
    ) -> Self {
        self.trace = sink;
        self
    }

    #[must_use]
    pub const fn debug(mut self) -> Self {
        self.debug = true;
        self
    }

    fn debug_log(&self, s: impl Into<String>) {
        if self.debug {
            println!("{}", s.into());
        }
    }

    // ─────────────────────────────────────────────
    // Unique-index delete
    // ─────────────────────────────────────────────

    pub fn by_unique_index(
        self,
        index: UniqueIndexHandle,
        entity: E,
    ) -> Result<Response<E>, InternalError>
    where
        E::PrimaryKey: FromKey,
    {
        let trace = start_exec_trace(
            self.trace,
            TraceExecutorKind::Delete,
            E::PATH,
            Some(TraceAccess::UniqueIndex {
                name: index.index().name,
            }),
            Some(index.index().name),
        );
        let result = (|| {
            self.debug_log(format!(
                "[debug] delete by unique index on {} ({})",
                E::PATH,
                index.index().fields.join(", ")
            ));
            let mut span = Span::<E>::new(ExecKind::Delete);
            ensure_recovered(&self.db)?;

            let index = index.index();
            let mut lookup = entity;
            sanitize(&mut lookup)?;

            // Resolve PK via unique index; absence is a no-op.
            let Some(pk) = resolve_unique_pk::<E>(&self.db, index, &lookup)? else {
                set_rows_from_len(&mut span, 0);
                return Ok(Response(Vec::new()));
            };

            // Intentional re-decode for defense-in-depth; resolve_unique_pk only returns the key.
            let (dk, stored_row, stored) = self.load_existing(pk)?;
            let ctx = self.db.context::<E>();
            let index_plans = self.build_index_plans()?;
            let (index_ops, index_remove_count) =
                Self::build_index_removal_ops(&index_plans, &[&stored])?;

            // Preflight: ensure stores are accessible before committing.
            ctx.with_store(|_| ())?;

            let raw_key = dk.to_raw()?;
            let marker = CommitMarker::new(
                CommitKind::Delete,
                index_ops,
                vec![CommitDataOp {
                    store: E::Store::PATH.to_string(),
                    key: raw_key.as_bytes().to_vec(),
                    value: None,
                }],
            )?;
            let (index_apply_stores, index_rollback_ops) =
                Self::prepare_index_delete_ops(&index_plans, &marker.index_ops)?;
            let mut rollback_rows = BTreeMap::new();
            rollback_rows.insert(raw_key, stored_row);
            let data_rollback_ops =
                Self::prepare_data_delete_ops(&marker.data_ops, &rollback_rows)?;
            let commit = begin_commit(marker)?;

            finish_commit(commit, |guard| {
                let mut unit = WriteUnit::new("delete_unique_row_atomic");

                // Commit boundary: apply the marker's raw mutations mechanically.
                let index_rollback_ops = index_rollback_ops;
                unit.record_rollback(move || Self::apply_index_rollbacks(index_rollback_ops));
                Self::apply_marker_index_ops(&guard.marker.index_ops, index_apply_stores);
                for _ in 0..index_remove_count {
                    sink::record(MetricsEvent::IndexRemove {
                        entity_path: E::PATH,
                    });
                }

                unit.checkpoint("delete_unique_after_indexes")?;

                // Apply data mutations recorded in the marker.
                let data_rollback_ops = data_rollback_ops;
                let db = self.db;
                unit.record_rollback(move || Self::apply_data_rollbacks(db, data_rollback_ops));
                unit.run(|| Self::apply_marker_data_ops(&guard.marker.data_ops, &ctx))?;

                unit.checkpoint("delete_unique_after_data")?;
                unit.commit();
                Ok(())
            })?;

            set_rows_from_len(&mut span, 1);
            Ok(Response(vec![(dk.key(), stored)]))
        })();

        if let Some(trace) = trace {
            match &result {
                Ok(resp) => trace.finish(u64::try_from(resp.0.len()).unwrap_or(u64::MAX)),
                Err(err) => trace.error(err),
            }
        }

        result
    }

    // ─────────────────────────────────────────────
    // Plan-based delete
    // ─────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    pub fn execute(self, plan: ExecutablePlan<E>) -> Result<Response<E>, InternalError> {
        let trace = start_plan_trace(self.trace, TraceExecutorKind::Delete, &plan);
        let result = (|| {
            let plan = plan.into_inner();
            ensure_recovered(&self.db)?;

            self.debug_log(format!("[debug] delete plan on {}", E::PATH));

            let mut span = Span::<E>::new(ExecKind::Delete);
            record_plan_metrics(&plan.access);

            let ctx = self.db.context::<E>();
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
            let stats = plan.apply_post_access::<E, _>(&mut rows)?;
            if stats.delete_limited {
                self.debug_log(format!(
                    "applied delete limit -> {} entities selected",
                    rows.len()
                ));
            }

            if rows.is_empty() {
                if let Some(trace) = trace.as_ref() {
                    let to_u64 = |len| u64::try_from(len).unwrap_or(u64::MAX);
                    trace.phase(TracePhase::Access, to_u64(access_rows));
                    trace.phase(TracePhase::Filter, to_u64(stats.rows_after_filter));
                    trace.phase(TracePhase::Order, to_u64(stats.rows_after_order));
                    trace.phase(
                        TracePhase::DeleteLimit,
                        to_u64(stats.rows_after_delete_limit),
                    );
                }
                set_rows_from_len(&mut span, 0);
                return Ok(Response(Vec::new()));
            }

            let index_plans = self.build_index_plans()?;
            let (index_ops, index_remove_count) = {
                let entities: Vec<&E> = rows.iter().map(|row| &row.entity).collect();
                Self::build_index_removal_ops(&index_plans, &entities)?
            };

            // Preflight store access to ensure no fallible work remains post-commit.
            ctx.with_store(|_| ())?;

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
                        store: E::Store::PATH.to_string(),
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
            let commit = begin_commit(marker)?;

            finish_commit(commit, |guard| {
                let mut unit = WriteUnit::new("delete_rows_atomic");

                // Commit boundary: apply the marker's raw mutations mechanically.
                let index_rollback_ops = index_rollback_ops;
                unit.record_rollback(move || Self::apply_index_rollbacks(index_rollback_ops));
                Self::apply_marker_index_ops(&guard.marker.index_ops, index_apply_stores);
                for _ in 0..index_remove_count {
                    sink::record(MetricsEvent::IndexRemove {
                        entity_path: E::PATH,
                    });
                }

                unit.checkpoint("delete_after_indexes")?;

                // Apply data mutations recorded in the marker.
                let data_rollback_ops = data_rollback_ops;
                let db = self.db;
                unit.record_rollback(move || Self::apply_data_rollbacks(db, data_rollback_ops));
                unit.run(|| Self::apply_marker_data_ops(&guard.marker.data_ops, &ctx))?;

                unit.checkpoint("delete_after_data")?;
                unit.commit();

                Ok(())
            })?;

            // Emit per-phase counts after the delete succeeds.
            if let Some(trace) = trace.as_ref() {
                let to_u64 = |len| u64::try_from(len).unwrap_or(u64::MAX);
                trace.phase(TracePhase::Access, to_u64(access_rows));
                trace.phase(TracePhase::Filter, to_u64(stats.rows_after_filter));
                trace.phase(TracePhase::Order, to_u64(stats.rows_after_order));
                trace.phase(
                    TracePhase::DeleteLimit,
                    to_u64(stats.rows_after_delete_limit),
                );
            }

            let res = rows
                .into_iter()
                .map(|row| (row.key.key(), row.entity))
                .collect::<Vec<_>>();
            set_rows_from_len(&mut span, res.len());

            Ok(Response(res))
        })();

        if let Some(trace) = trace {
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
            let store = stores.get(op.store.as_str()).ok_or_else(|| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Index,
                    format!(
                        "commit marker references unknown index store '{}' ({})",
                        op.store,
                        E::PATH
                    ),
                )
            })?;
            if op.key.len() != IndexKey::STORED_SIZE as usize {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Index,
                    format!(
                        "commit marker index key length {} does not match {} ({})",
                        op.key.len(),
                        IndexKey::STORED_SIZE,
                        E::PATH
                    ),
                ));
            }
            if let Some(value) = &op.value
                && value.len() > MAX_INDEX_ENTRY_BYTES as usize
            {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Index,
                    format!(
                        "commit marker index entry exceeds max size: {} bytes ({})",
                        value.len(),
                        E::PATH
                    ),
                ));
            }

            let raw_key = RawIndexKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
            let rollback_value = store.with_borrow(|s| s.get(&raw_key)).ok_or_else(|| {
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

            apply_stores.push(*store);
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
            if op.store != E::Store::PATH {
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
            if op.key.len() != DataKey::STORED_SIZE as usize {
                return Err(InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!(
                        "commit marker data key length {} does not match {} ({})",
                        op.key.len(),
                        DataKey::STORED_SIZE,
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

    // Apply commit marker index ops using pre-resolved stores.
    fn apply_marker_index_ops(
        ops: &[CommitIndexOp],
        stores: Vec<&'static LocalKey<RefCell<IndexStore>>>,
    ) {
        debug_assert_eq!(
            ops.len(),
            stores.len(),
            "commit marker index ops length mismatch"
        );

        for (op, store) in ops.iter().zip(stores.into_iter()) {
            debug_assert_eq!(op.key.len(), IndexKey::STORED_SIZE as usize);
            let raw_key = RawIndexKey::from_bytes(Cow::Borrowed(op.key.as_slice()));

            store.with_borrow_mut(|s| {
                if let Some(value) = &op.value {
                    debug_assert!(value.len() <= MAX_INDEX_ENTRY_BYTES as usize);
                    let raw_entry = RawIndexEntry::from_bytes(Cow::Borrowed(value.as_slice()));
                    s.insert(raw_key, raw_entry);
                } else {
                    s.remove(&raw_key);
                }
            });
        }
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
    fn apply_marker_data_ops(
        ops: &[CommitDataOp],
        ctx: &Context<'_, E>,
    ) -> Result<(), InternalError> {
        for op in ops {
            debug_assert!(op.value.is_none());
            let raw_key = RawDataKey::from_bytes(Cow::Borrowed(op.key.as_slice()));
            ctx.with_store_mut(|s| s.remove(&raw_key))?;
        }
        Ok(())
    }

    // Apply rollback mutations for data rows.
    fn apply_data_rollbacks(db: Db<E::Canister>, ops: Vec<PreparedDataRollback>) {
        let ctx = db.context::<E>();
        for op in ops {
            let _ = ctx.with_store_mut(|s| s.insert(op.key, op.value));
        }
    }

    fn load_existing(&self, pk: E::PrimaryKey) -> Result<(DataKey, RawRow, E), InternalError> {
        let dk = DataKey::new::<E>(pk.into());
        let row = self.db.context::<E>().read_strict(&dk)?;
        let entity = row.try_decode::<E>().map_err(|err| {
            ExecutorError::corruption(
                ErrorOrigin::Serialize,
                format!("failed to deserialize row: {dk} ({err})"),
            )
        })?;
        Ok((dk, row, entity))
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
            let mut entries: BTreeMap<RawIndexKey, Option<IndexEntry>> = BTreeMap::new();

            // Fold entity deletions into per-key index entry updates.
            for entity in entities {
                let Some(key) = IndexKey::new(*entity, plan.index)? else {
                    continue;
                };
                let raw_key = key.to_raw();
                let entity_key = entity.key();

                // Lazily load and decode the existing index entry once per key.
                let entry = match entries.entry(raw_key) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        let decoded = plan
                            .store
                            .with_borrow(|s| s.get(&raw_key))
                            .map(|raw| {
                                raw.try_decode().map_err(|err| {
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
                            .transpose()?;
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
                            IndexEntryCorruption::missing_key(raw_key, entity_key),
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

                if !e.contains(&entity_key) {
                    return Err(ExecutorError::corruption(
                        ErrorOrigin::Index,
                        format!(
                            "index corrupted: {} ({}) -> {}",
                            E::PATH,
                            fields,
                            IndexEntryCorruption::missing_key(raw_key, entity_key),
                        ),
                    )
                    .into());
                }
                removed = removed.saturating_add(1);

                // Remove this entity’s key from the index entry.
                if let Some(e) = entry.as_mut() {
                    e.remove_key(&entity_key);
                    if e.is_empty() {
                        *entry = None;
                    }
                }
            }

            // Emit commit ops for each touched index key.
            for (raw_key, entry) in entries {
                let value = if let Some(entry) = entry {
                    let raw = RawIndexEntry::try_from_entry(&entry).map_err(|err| match err {
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

fn decode_rows<E: EntityKind>(rows: Vec<DataRow>) -> Result<Vec<DeleteRow<E>>, InternalError> {
    rows.into_iter()
        .map(|(dk, raw)| {
            let dk_for_err = dk.clone();
            let entity = raw.try_decode::<E>().map_err(|err| {
                ExecutorError::corruption(
                    ErrorOrigin::Serialize,
                    format!("failed to deserialize row: {dk_for_err} ({err})"),
                )
            })?;

            let expected = dk.key();
            let actual = entity.key();
            if expected != actual {
                return Err(ExecutorError::corruption(
                    ErrorOrigin::Store,
                    format!("row key mismatch: expected {expected}, found {actual}"),
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
