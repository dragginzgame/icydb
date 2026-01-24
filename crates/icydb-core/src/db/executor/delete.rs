use crate::{
    db::{
        CommitDataOp, CommitIndexOp, CommitKind, CommitMarker, Db, WriteUnit, begin_commit,
        ensure_recovered,
        executor::{
            ExecutorError, UniqueIndexHandle,
            plan::{record_plan_metrics, set_rows_from_len},
            resolve_unique_pk,
            trace::{
                QueryTraceSink, TraceAccess, TraceExecutorKind, start_exec_trace, start_plan_trace,
            },
        },
        finish_commit,
        index::{
            IndexEntry, IndexEntryCorruption, IndexKey, IndexRemoveOutcome, IndexStore,
            RawIndexEntry, RawIndexKey,
        },
        query::{
            plan::{LogicalPlan, OrderDirection, OrderSpec, validate_plan_with_model},
            predicate::{eval as eval_predicate, normalize as normalize_predicate},
        },
        response::Response,
        store::{DataKey, DataRow, RawRow},
        traits::FromKey,
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    prelude::*,
    sanitize::sanitize,
    traits::{EntityKind, Path},
};
use std::{
    cell::RefCell, cmp::Ordering, collections::BTreeMap, marker::PhantomData, thread::LocalKey,
};

///
/// IndexPlan
/// Prevalidated handle to an index store used during commit planning.
///

struct IndexPlan {
    index: &'static IndexModel,
    store: &'static LocalKey<RefCell<IndexStore>>,
}

struct DeleteRow<E> {
    key: DataKey,
    raw: Option<RawRow>,
    entity: E,
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

            let (dk, stored_row, stored) = self.load_existing(pk)?;
            let ctx = self.db.context::<E>();
            let index_plans = self.build_index_plans()?;
            let index_ops = Self::build_index_removal_ops(&index_plans, &[&stored])?;

            // Preflight: ensure stores are accessible before committing.
            ctx.with_store(|_| ())?;

            let marker = CommitMarker::new(
                CommitKind::Delete,
                index_ops,
                vec![CommitDataOp {
                    store: E::Store::PATH.to_string(),
                    key: dk.to_raw().as_bytes().to_vec(),
                    value: None,
                }],
            )?;
            let commit = begin_commit(marker)?;

            finish_commit(commit, |guard| {
                let mut unit = WriteUnit::new("delete_unique_row_atomic");
                for plan in &index_plans {
                    let fields = plan.index.fields.join(", ");
                    let outcome = unit.run(|| {
                        plan.store.with_borrow_mut(|s| {
                            s.remove_index_entry(&stored, plan.index).map_err(|err| {
                                ExecutorError::corruption(
                                    ErrorOrigin::Index,
                                    format!("index corrupted: {} ({fields}) -> {err}", E::PATH),
                                )
                                .into()
                            })
                        })
                    })?;

                    if outcome == IndexRemoveOutcome::Removed {
                        let store = plan.store;
                        let index = plan.index;
                        let stored = stored.clone();
                        unit.record_rollback(move || {
                            let _ = store.with_borrow_mut(|s| s.insert_index_entry(&stored, index));
                        });

                        sink::record(MetricsEvent::IndexRemove {
                            entity_path: E::PATH,
                        });
                    }
                }

                unit.checkpoint("delete_unique_after_indexes")?;
                guard.mark_index_written();

                let raw_key = dk.to_raw();
                let removed = unit.run(|| ctx.with_store_mut(|s| s.remove(&raw_key)))?;
                if removed.is_some() {
                    let db = self.db;
                    let stored_row = stored_row;
                    unit.record_rollback(move || {
                        let ctx = db.context::<E>();
                        let _ = ctx.with_store_mut(|s| {
                            s.insert(raw_key, stored_row);
                        });
                    });
                }

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
    pub fn execute(self, plan: LogicalPlan) -> Result<Response<E>, InternalError> {
        let trace = start_plan_trace(self.trace, TraceExecutorKind::Delete, &plan);
        let result = (|| {
            validate_plan_with_model(&plan, E::MODEL).map_err(|err| {
                InternalError::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
            })?;
            plan.debug_validate_with_model(E::MODEL);
            ensure_recovered(&self.db)?;

            self.debug_log(format!("[debug] delete plan on {}", E::PATH));

            let mut span = Span::<E>::new(ExecKind::Delete);
            record_plan_metrics(&plan.access);

            let ctx = self.db.context::<E>();
            let data_rows = ctx.rows_from_access(&plan.access)?;
            sink::record(MetricsEvent::RowsScanned {
                entity_path: E::PATH,
                rows_scanned: data_rows.len() as u64,
            });

            let mut rows = decode_rows::<E>(data_rows)?;

            let normalized = plan.predicate.as_ref().map(normalize_predicate);
            let filtered = if let Some(predicate) = normalized.as_ref() {
                rows.retain(|row| eval_predicate(&row.entity, predicate));
                true
            } else {
                false
            };

            let ordered = if let Some(order) = &plan.order
                && rows.len() > 1
                && !order.fields.is_empty()
            {
                debug_assert!(
                    plan.predicate.is_none() || filtered,
                    "executor invariant violated: ordering must run after filtering"
                );
                apply_order_spec(&mut rows, order);
                true
            } else {
                false
            };

            if let Some(page) = &plan.page {
                debug_assert!(
                    plan.order.is_none() || ordered,
                    "executor invariant violated: pagination must run after ordering"
                );
                apply_pagination(&mut rows, page.offset, page.limit);
            }

            if rows.is_empty() {
                set_rows_from_len(&mut span, 0);
                return Ok(Response(Vec::new()));
            }

            let index_plans = self.build_index_plans()?;
            let index_ops = {
                let entities: Vec<&E> = rows.iter().map(|row| &row.entity).collect();
                Self::build_index_removal_ops(&index_plans, &entities)?
            };

            // Preflight store access to ensure no fallible work remains post-commit.
            ctx.with_store(|_| ())?;

            let data_ops = rows
                .iter()
                .map(|row| CommitDataOp {
                    store: E::Store::PATH.to_string(),
                    key: row.key.to_raw().as_bytes().to_vec(),
                    value: None,
                })
                .collect();

            let marker = CommitMarker::new(CommitKind::Delete, index_ops, data_ops)?;
            let commit = begin_commit(marker)?;

            finish_commit(commit, |guard| {
                let mut unit = WriteUnit::new("delete_rows_atomic");
                for row in &rows {
                    for plan in &index_plans {
                        let fields = plan.index.fields.join(", ");
                        let outcome = unit.run(|| {
                            plan.store.with_borrow_mut(|s| {
                                s.remove_index_entry(&row.entity, plan.index)
                                    .map_err(|err| {
                                        ExecutorError::corruption(
                                            ErrorOrigin::Index,
                                            format!(
                                                "index corrupted: {} ({fields}) -> {err}",
                                                E::PATH
                                            ),
                                        )
                                        .into()
                                    })
                            })
                        })?;

                        if outcome == IndexRemoveOutcome::Removed {
                            let store = plan.store;
                            let index = plan.index;
                            let entity = row.entity.clone();
                            unit.record_rollback(move || {
                                let _ =
                                    store.with_borrow_mut(|s| s.insert_index_entry(&entity, index));
                            });

                            sink::record(MetricsEvent::IndexRemove {
                                entity_path: E::PATH,
                            });
                        }
                    }
                }

                unit.checkpoint("delete_after_indexes")?;
                guard.mark_index_written();

                for row in &mut rows {
                    let raw_key = row.key.to_raw();
                    let raw_row = row.raw.take();
                    let removed = unit.run(|| ctx.with_store_mut(|s| s.remove(&raw_key)))?;
                    if removed.is_some()
                        && let Some(raw_row) = raw_row
                    {
                        let db = self.db;
                        unit.record_rollback(move || {
                            let ctx = db.context::<E>();
                            let _ = ctx.with_store_mut(|s| {
                                s.insert(raw_key, raw_row);
                            });
                        });
                    }
                    unit.checkpoint("delete_after_data")?;
                }

                unit.commit();
                Ok(())
            })?;

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

    fn build_index_removal_ops(
        plans: &[IndexPlan],
        entities: &[&E],
    ) -> Result<Vec<CommitIndexOp>, InternalError> {
        let mut ops = Vec::new();

        // Process each index independently to compute its resulting mutations.
        for plan in plans {
            let fields = plan.index.fields.join(", ");

            // Map raw index keys → updated entry (or None if fully removed).
            let mut entries: BTreeMap<RawIndexKey, Option<IndexEntry>> = BTreeMap::new();

            // Fold entity deletions into per-key index entry updates.
            for entity in entities {
                let Some(key) = IndexKey::new(*entity, plan.index) else {
                    continue;
                };
                let raw_key = key.to_raw();

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

                // Remove this entity’s key from the index entry.
                if let Some(e) = entry.as_mut() {
                    e.remove_key(&entity.key());
                    if e.is_empty() {
                        *entry = None;
                    }
                }
            }

            // Emit commit ops for each touched index key.
            for (raw_key, entry) in entries {
                let value = if let Some(entry) = entry {
                    let raw = RawIndexEntry::try_from_entry(&entry).map_err(|err| {
                        ExecutorError::corruption(
                            ErrorOrigin::Index,
                            format!(
                                "index corrupted: {} ({}) -> {}",
                                E::PATH,
                                fields,
                                IndexEntryCorruption::TooManyKeys { count: err.keys() }
                            ),
                        )
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

        Ok(ops)
    }
}

fn decode_rows<E: EntityKind>(rows: Vec<DataRow>) -> Result<Vec<DeleteRow<E>>, InternalError> {
    rows.into_iter()
        .map(|(dk, raw)| {
            let dk_for_err = dk.clone();
            raw.try_decode::<E>()
                .map(|entity| DeleteRow {
                    key: dk,
                    raw: Some(raw),
                    entity,
                })
                .map_err(|err| {
                    ExecutorError::corruption(
                        ErrorOrigin::Serialize,
                        format!("failed to deserialize row: {dk_for_err} ({err})"),
                    )
                    .into()
                })
        })
        .collect()
}

fn apply_order_spec<E: EntityKind>(rows: &mut [DeleteRow<E>], order: &OrderSpec) {
    rows.sort_by(|ra, rb| {
        let ea = &ra.entity;
        let eb = &rb.entity;
        for (field, direction) in &order.fields {
            let va = ea.get_value(field);
            let vb = eb.get_value(field);

            let ordering = match (va, vb) {
                (None, None) => continue,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(va), Some(vb)) => match va.partial_cmp(&vb) {
                    Some(ord) => ord,
                    None => continue,
                },
            };

            let ordering = match direction {
                OrderDirection::Asc => ordering,
                OrderDirection::Desc => ordering.reverse(),
            };

            if ordering != Ordering::Equal {
                return ordering;
            }
        }

        Ordering::Equal
    });
}

/// Apply offset/limit pagination to an in-memory vector, in-place.
fn apply_pagination<T>(rows: &mut Vec<T>, offset: u32, limit: Option<u32>) {
    let total = rows.len();
    let start = usize::min(offset as usize, total);
    let end = limit.map_or(total, |l| usize::min(start + l as usize, total));

    if start >= end {
        rows.clear();
    } else {
        rows.drain(..start);
        rows.truncate(end - start);
    }
}
