use crate::{
    db::{
        CommitDataOp, CommitIndexOp, CommitKind, CommitMarker, Db, begin_commit, ensure_recovered,
        executor::{
            ExecutorError, FilterEvaluator, UniqueIndexHandle, WriteUnit,
            plan::{plan_for, record_plan_metrics, scan_strict, set_rows_from_len},
            resolve_unique_pk,
        },
        finish_commit,
        index::{
            IndexEntry, IndexEntryCorruption, IndexKey, IndexRemoveOutcome, IndexStore,
            RawIndexEntry, RawIndexKey,
        },
        primitives::FilterExpr,
        query::{DeleteQuery, QueryPlan, QueryValidate},
        response::Response,
        store::DataKey,
        traits::FromKey,
    },
    error::{ErrorOrigin, InternalError},
    obs::sink::{self, ExecKind, MetricsEvent, Span},
    prelude::*,
    sanitize::sanitize,
    traits::{EntityKind, FieldValue, Path},
};
use canic_cdk::structures::Storable;
use std::{
    cell::RefCell, collections::BTreeMap, marker::PhantomData, ops::ControlFlow, thread::LocalKey,
};

///
/// DeleteAccumulator
/// Collects rows to delete during planner execution.
///

struct DeleteAccumulator<'f, E> {
    filter: Option<&'f FilterExpr>,
    offset: usize,
    skipped: usize,
    limit: Option<usize>,
    matches: Vec<(DataKey, E)>,
}

impl<'f, E: EntityKind> DeleteAccumulator<'f, E> {
    fn new(filter: Option<&'f FilterExpr>, offset: usize, limit: Option<usize>) -> Self {
        Self {
            filter,
            offset,
            skipped: 0,
            limit,
            matches: Vec::with_capacity(limit.unwrap_or(0)),
        }
    }

    fn limit_reached(&self) -> bool {
        self.limit.is_some_and(|lim| self.matches.len() >= lim)
    }

    fn should_stop(&mut self, dk: DataKey, entity: E) -> bool {
        if let Some(f) = self.filter
            && !FilterEvaluator::new(&entity).eval(f)
        {
            return false;
        }

        if self.skipped < self.offset {
            self.skipped += 1;
            return false;
        }

        if self.limit_reached() {
            return true;
        }

        self.matches.push((dk, entity));
        false
    }
}

///
/// IndexPlan
/// Prevalidated index store handle.
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

    // ─────────────────────────────────────────────
    // PK helpers
    // ─────────────────────────────────────────────

    pub fn one(self, pk: impl FieldValue) -> Result<Response<E>, InternalError> {
        self.execute(DeleteQuery::new().one::<E>(pk))
    }

    pub fn only(self) -> Result<Response<E>, InternalError> {
        self.execute(DeleteQuery::new().one::<E>(()))
    }

    pub fn many<I, V>(self, values: I) -> Result<Response<E>, InternalError>
    where
        I: IntoIterator<Item = V>,
        V: FieldValue,
    {
        self.execute(DeleteQuery::new().many_by_field(E::PRIMARY_KEY, values))
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
        let mut span = Span::<E>::new(ExecKind::Delete);
        ensure_recovered(&self.db)?;

        let index = index.index();
        let mut lookup = entity;
        sanitize(&mut lookup)?;

        let Some(pk) = resolve_unique_pk::<E>(&self.db, index, &lookup)? else {
            set_rows_from_len(&mut span, 0);
            return Ok(Response(Vec::new()));
        };

        let (dk, stored) = self.load_existing(pk)?;
        let ctx = self.db.context::<E>();
        let index_plans = self.build_index_plans()?;
        let index_ops = Self::build_index_removal_ops(&index_plans, &[&stored])?;

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
    // Planner-based delete
    // ─────────────────────────────────────────────

    pub fn explain(self, query: DeleteQuery) -> Result<QueryPlan, InternalError> {
        QueryValidate::<E>::validate(&query)?;
        Ok(plan_for::<E>(query.filter.as_ref()))
    }

    pub fn execute(self, query: DeleteQuery) -> Result<Response<E>, InternalError> {
        QueryValidate::<E>::validate(&query)?;
        ensure_recovered(&self.db)?;

        let mut span = Span::<E>::new(ExecKind::Delete);
        let plan = plan_for::<E>(query.filter.as_ref());
        record_plan_metrics(&plan);

        let (limit, offset) = match query.limit.as_ref() {
            Some(l) => (l.limit.map(|v| v as usize), l.offset as usize),
            None => (None, 0),
        };

        let filter = query.filter.as_ref().map(|f| f.clone().simplify());
        let mut acc = DeleteAccumulator::new(filter.as_ref(), offset, limit);
        let mut scanned = 0u64;

        scan_strict::<E, _>(&self.db, plan, |dk, entity| {
            scanned += 1;
            if acc.should_stop(dk, entity) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })?;

        sink::record(MetricsEvent::RowsScanned {
            entity_path: E::PATH,
            rows_scanned: scanned,
        });

        if acc.matches.is_empty() {
            set_rows_from_len(&mut span, 0);
            return Ok(Response(Vec::new()));
        }

        let index_plans = self.build_index_plans()?;
        let entities: Vec<&E> = acc.matches.iter().map(|(_, e)| e).collect();
        let index_ops = Self::build_index_removal_ops(&index_plans, &entities)?;

        let ctx = self.db.context::<E>();
        ctx.with_store(|_| ())?;

        let data_ops = acc
            .matches
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
                for (_, entity) in &acc.matches {
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
                    for (dk, _) in &acc.matches {
                        s.remove(&dk.to_raw());
                    }
                })
                .expect("data store missing after preflight");
            },
        );

        let res = acc
            .matches
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

        for plan in plans {
            let fields = plan.index.fields.join(", ");
            let mut entries: BTreeMap<RawIndexKey, Option<IndexEntry>> = BTreeMap::new();

            for entity in entities {
                let Some(key) = IndexKey::new(*entity, plan.index) else {
                    continue;
                };
                let raw_key = key.to_raw();

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

                if let Some(e) = entry.as_mut() {
                    e.remove_key(&entity.key());
                    if e.is_empty() {
                        *entry = None;
                    }
                }
            }

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
                    Some(raw.into_bytes())
                } else {
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
