use crate::{
    db::{
        Db,
        executor::ExecutorError,
        query::QueryPlan,
        store::{DataKey, DataRow, DataStore, RawDataKey, RawRow},
    },
    error::{ErrorOrigin, InternalError},
    key::Key,
    traits::{EntityKind, Path},
};
use std::{marker::PhantomData, ops::Bound};

///
/// Context
///

pub struct Context<'a, E: EntityKind> {
    pub db: &'a Db<E::Canister>,
    _marker: PhantomData<E>,
}

impl<'a, E> Context<'a, E>
where
    E: EntityKind,
{
    #[must_use]
    pub const fn new(db: &'a Db<E::Canister>) -> Self {
        Self {
            db,
            _marker: PhantomData,
        }
    }

    /// Access the entity's data store in read-only mode.
    pub fn with_store<R>(&self, f: impl FnOnce(&DataStore) -> R) -> Result<R, InternalError> {
        self.db.with_data(|reg| reg.with_store(E::Store::PATH, f))
    }

    /// Access the entity's data store mutably.
    pub fn with_store_mut<R>(
        &self,
        f: impl FnOnce(&mut DataStore) -> R,
    ) -> Result<R, InternalError> {
        self.db
            .with_data(|reg| reg.with_store_mut(E::Store::PATH, f))
    }

    /// Read a row strictly; missing rows surface as corruption.
    pub fn read_strict(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw();
            s.get(&raw).ok_or_else(|| {
                ExecutorError::corruption(ErrorOrigin::Store, format!("missing row: {key}")).into()
            })
        })?
    }

    ///
    /// Analyze Plan
    ///

    /// Compute candidate data keys for the given query plan.
    ///
    /// Note: index candidates are returned in deterministic key order.
    /// This ordering is for stability only and does not imply semantic ordering.
    pub fn candidates_from_plan(&self, plan: QueryPlan) -> Result<Vec<DataKey>, InternalError> {
        let is_index_plan = matches!(&plan, QueryPlan::Index(_));

        let mut candidates = match plan {
            QueryPlan::Keys(keys) => Self::to_data_keys(keys),

            QueryPlan::Range(start, end) => self.with_store(|s| {
                let start = Self::to_data_key(start);
                let end = Self::to_data_key(end);
                let start_raw = start.to_raw();
                let end_raw = end.to_raw();

                s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .map(|e| Self::decode_data_key(e.key()))
                    .collect::<Result<Vec<_>, _>>()
            })??,

            QueryPlan::FullScan => self.with_store(|s| {
                let start = DataKey::lower_bound::<E>();
                let end = DataKey::upper_bound::<E>();
                let start_raw = start.to_raw();
                let end_raw = end.to_raw();

                s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .map(|entry| Self::decode_data_key(entry.key()))
                    .collect::<Result<Vec<_>, _>>()
            })??,

            QueryPlan::Index(index_plan) => {
                let index_store = self
                    .db
                    .with_index(|reg| reg.try_get_store(index_plan.index.store))?;

                index_store.with_borrow(|istore| {
                    istore
                        .resolve_data_values::<E>(index_plan.index, &index_plan.values)
                        .map_err(|msg| ExecutorError::corruption(ErrorOrigin::Index, msg))
                })?
            }
        };

        if is_index_plan {
            candidates.sort_unstable();
        }

        Ok(candidates)
    }

    /// Load data rows for the given query plan.
    pub fn rows_from_plan(&self, plan: QueryPlan) -> Result<Vec<DataRow>, InternalError> {
        match plan {
            QueryPlan::Keys(keys) => {
                let data_keys = Self::to_data_keys(keys);
                self.load_many(&data_keys)
            }
            QueryPlan::Range(start, end) => {
                let start = Self::to_data_key(start);
                let end = Self::to_data_key(end);
                self.load_range(start, end)
            }
            QueryPlan::FullScan => self.with_store(|s| {
                let start = DataKey::lower_bound::<E>();
                let end = DataKey::upper_bound::<E>();
                let start_raw = start.to_raw();
                let end_raw = end.to_raw();

                s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .map(|entry| {
                        let dk = Self::decode_data_key(entry.key())?;
                        Ok((dk, entry.value()))
                    })
                    .collect::<Result<Vec<_>, InternalError>>()
            })?,
            QueryPlan::Index(_) => {
                let data_keys = self.candidates_from_plan(plan)?;
                self.load_many(&data_keys)
            }
        }
    }

    /// Fetch rows with pagination applied as early as possible (pre-deserialization),
    /// only when no additional filtering or sorting is required by the executor.
    pub fn rows_from_plan_with_pagination(
        &self,
        plan: QueryPlan,
        offset: u32,
        limit: Option<u32>,
    ) -> Result<Vec<DataRow>, InternalError> {
        let skip = offset as usize;
        let take = limit.map(|l| l as usize);

        match plan {
            QueryPlan::Keys(keys) => {
                // Apply pagination to keys before loading
                let mut keys = keys;
                let total = keys.len();
                let (start, end) = Self::slice_bounds(total, offset, limit);

                if start >= end {
                    return Ok(Vec::new());
                }

                let paged = keys.drain(start..end).collect::<Vec<_>>();
                let data_keys = Self::to_data_keys(paged);

                self.load_many(&data_keys)
            }

            QueryPlan::Range(start, end) => {
                let start = Self::to_data_key(start);
                let end = Self::to_data_key(end);
                let start_raw = start.to_raw();
                let end_raw = end.to_raw();

                self.with_store(|s| {
                    let base = s.range((Bound::Included(start_raw), Bound::Included(end_raw)));
                    let cap = take.unwrap_or(0);
                    let mut out = Vec::with_capacity(cap);
                    match take {
                        Some(t) => {
                            for entry in base.skip(skip).take(t) {
                                let dk = Self::decode_data_key(entry.key())?;
                                out.push((dk, entry.value()));
                            }
                        }
                        None => {
                            for entry in base.skip(skip) {
                                let dk = Self::decode_data_key(entry.key())?;
                                out.push((dk, entry.value()));
                            }
                        }
                    }
                    Ok::<_, InternalError>(out)
                })?
            }

            QueryPlan::FullScan => self.with_store(|s| {
                let start = DataKey::lower_bound::<E>();
                let end = DataKey::upper_bound::<E>();
                let start_raw = start.to_raw();
                let end_raw = end.to_raw();

                let base = s.range((Bound::Included(start_raw), Bound::Included(end_raw)));
                let cap = take.unwrap_or(0);
                let mut out = Vec::with_capacity(cap);
                match take {
                    Some(t) => {
                        for entry in base.skip(skip).take(t) {
                            let dk = Self::decode_data_key(entry.key())?;
                            out.push((dk, entry.value()));
                        }
                    }
                    None => {
                        for entry in base.skip(skip) {
                            let dk = Self::decode_data_key(entry.key())?;
                            out.push((dk, entry.value()));
                        }
                    }
                }
                Ok::<_, InternalError>(out)
            })?,

            QueryPlan::Index(_) => {
                // Resolve candidate keys from index, then paginate before loading
                let mut data_keys = self.candidates_from_plan(plan)?;
                let total = data_keys.len();
                let (start, end) = Self::slice_bounds(total, offset, limit);

                if start >= end {
                    return Ok(Vec::new());
                }

                let paged = data_keys.drain(start..end).collect::<Vec<_>>();

                self.load_many(&paged)
            }
        }
    }

    ///
    /// Load Helpers
    ///

    fn to_data_key(key: Key) -> DataKey {
        DataKey::new::<E>(key)
    }

    fn to_data_keys(keys: Vec<Key>) -> Vec<DataKey> {
        keys.into_iter().map(Self::to_data_key).collect()
    }

    fn slice_bounds(total: usize, offset: u32, limit: Option<u32>) -> (usize, usize) {
        let start = (offset as usize).min(total);
        let end = match limit {
            Some(l) => start.saturating_add(l as usize).min(total),
            None => total,
        };

        (start, end)
    }

    fn load_many(&self, keys: &[DataKey]) -> Result<Vec<DataRow>, InternalError> {
        self.with_store(|s| {
            keys.iter()
                .filter_map(|k| s.get(&k.to_raw()).map(|entry| (k.clone(), entry)))
                .collect()
        })
    }

    fn load_range(&self, start: DataKey, end: DataKey) -> Result<Vec<DataRow>, InternalError> {
        self.with_store(|s| {
            let start_raw = start.to_raw();
            let end_raw = end.to_raw();
            s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                .map(|e| {
                    let dk = Self::decode_data_key(e.key())?;
                    Ok((dk, e.value()))
                })
                .collect::<Result<Vec<_>, InternalError>>()
        })?
    }

    /// Deserialize raw data rows into typed entity rows, mapping `DataKey` → `(Key, E)`.
    #[allow(clippy::unused_self)]
    pub fn deserialize_rows(&self, rows: Vec<DataRow>) -> Result<Vec<(Key, E)>, InternalError> {
        rows.into_iter()
            .map(|(k, v)| {
                v.try_decode::<E>()
                    .map(|entry| (k.key(), entry))
                    .map_err(|err| {
                        ExecutorError::corruption(
                            ErrorOrigin::Serialize,
                            format!("failed to deserialize row: {k} ({err})"),
                        )
                        .into()
                    })
            })
            .collect()
    }

    fn decode_data_key(raw: &RawDataKey) -> Result<DataKey, InternalError> {
        DataKey::try_from_raw(raw)
            .map_err(|msg| ExecutorError::corruption(ErrorOrigin::Store, msg).into())
    }
}
