use crate::{
    db::{
        CommitDataOp, CommitIndexOp, CommitKind, CommitMarker, Db, begin_commit, ensure_recovered,
        executor::{
            ExecutorError, UniqueIndexHandle, WriteUnit,
            plan::{record_plan_metrics, set_rows_from_len},
            resolve_unique_pk,
        },
        finish_commit,
        index::{
            IndexEntry, IndexEntryCorruption, IndexKey, IndexRemoveOutcome, IndexStore,
            RawIndexEntry, RawIndexKey,
        },
        query::v2::{
            plan::{LogicalPlan, OrderDirection, OrderSpec, validate_plan},
            predicate::{eval as eval_v2, normalize as normalize_v2},
        },
        response::Response,
        store::{DataKey, DataRow},
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

///
/// DeleteExecutor
///
/// Stage-1 atomicity invariant:
/// All fallible validation completes before the first stable write.
/// After mutation begins, only infallible operations or traps remain.
/// IC rollback semantics guarantee atomicity within this update call.
///
#[derive(Clone, Copy)]
pub struct DeleteExecutor<E: EntityKind> {
    db: Db<E::Canister>,
    debug: bool,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> DeleteExecutor<E> {
    #[must_use]
    pub const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self {
            db,
            debug,
            _marker: PhantomData,
        }
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

        let (dk, stored) = self.load_existing(pk)?;
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

        finish_commit(
            commit,
            || {
                let _unit = WriteUnit::new("delete_unique_row_stage1_atomic");
                for plan in &index_plans {
                    let outcome = plan
                        .store
                        .with_borrow_mut(|s| s.remove_index_entry(&stored, plan.index))
                        .expect("index remove failed after prevalidation");
                    if outcome == IndexRemoveOutcome::Removed {
                        sink::record(MetricsEvent::IndexRemove {
                            entity_path: E::PATH,
                        });
                    }
                }
            },
            || {
                ctx.with_store_mut(|s| s.remove(&dk.to_raw()))
                    .expect("data store missing after preflight");
            },
        );

        set_rows_from_len(&mut span, 1);
        Ok(Response(vec![(dk.key(), stored)]))
    }

    // ─────────────────────────────────────────────
    // Plan-based delete
    // ─────────────────────────────────────────────

    pub fn execute(self, plan: LogicalPlan) -> Result<Response<E>, InternalError> {
        validate_plan::<E>(&plan).map_err(|err| {
            InternalError::new(ErrorClass::Unsupported, ErrorOrigin::Query, err.to_string())
        })?;
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

        let normalized = plan.predicate.as_ref().map(normalize_v2);
        if let Some(predicate) = normalized.as_ref() {
            rows.retain(|(_, entity)| eval_v2(entity, predicate));
        }

        if let Some(order) = &plan.order
            && rows.len() > 1
            && !order.fields.is_empty()
        {
            apply_order_spec(&mut rows, order);
        }

        if let Some(page) = &plan.page {
            apply_pagination(&mut rows, page.offset, page.limit);
        }

        if rows.is_empty() {
            set_rows_from_len(&mut span, 0);
            return Ok(Response(Vec::new()));
        }

        let index_plans = self.build_index_plans()?;
        let entities: Vec<&E> = rows.iter().map(|(_, e)| e).collect();
        let index_ops = Self::build_index_removal_ops(&index_plans, &entities)?;

        // Preflight store access to ensure no fallible work remains post-commit.
        ctx.with_store(|_| ())?;

        let data_ops = rows
            .iter()
            .map(|(dk, _)| CommitDataOp {
                store: E::Store::PATH.to_string(),
                key: dk.to_raw().as_bytes().to_vec(),
                value: None,
            })
            .collect();

        let marker = CommitMarker::new(CommitKind::Delete, index_ops, data_ops)?;
        let commit = begin_commit(marker)?;

        finish_commit(
            commit,
            || {
                for (_, entity) in &rows {
                    let _unit = WriteUnit::new("delete_row_stage1_atomic");
                    for plan in &index_plans {
                        let outcome = plan
                            .store
                            .with_borrow_mut(|s| s.remove_index_entry(entity, plan.index))
                            .expect("index remove failed after prevalidation");
                        if outcome == IndexRemoveOutcome::Removed {
                            sink::record(MetricsEvent::IndexRemove {
                                entity_path: E::PATH,
                            });
                        }
                    }
                }
            },
            || {
                ctx.with_store_mut(|s| {
                    for (dk, _) in &rows {
                        s.remove(&dk.to_raw());
                    }
                })
                .expect("data store missing after preflight");
            },
        );

        let res = rows
            .into_iter()
            .map(|(dk, e)| (dk.key(), e))
            .collect::<Vec<_>>();
        set_rows_from_len(&mut span, res.len());

        Ok(Response(res))
    }

    // ─────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────

    fn load_existing(&self, pk: E::PrimaryKey) -> Result<(DataKey, E), InternalError> {
        let dk = DataKey::new::<E>(pk.into());
        let row = self.db.context::<E>().read_strict(&dk)?;
        let entity = row.try_decode::<E>().map_err(|err| {
            ExecutorError::corruption(
                ErrorOrigin::Serialize,
                format!("failed to deserialize row: {dk} ({err})"),
            )
        })?;
        Ok((dk, entity))
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

fn decode_rows<E: EntityKind>(rows: Vec<DataRow>) -> Result<Vec<(DataKey, E)>, InternalError> {
    rows.into_iter()
        .map(|(dk, raw)| {
            let dk_for_err = dk.clone();
            raw.try_decode::<E>()
                .map(|entity| (dk, entity))
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

fn apply_order_spec<E: EntityKind>(rows: &mut [(DataKey, E)], order: &OrderSpec) {
    rows.sort_by(|(_, ea), (_, eb)| {
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
