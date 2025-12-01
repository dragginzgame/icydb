use crate::{
    Error, Key,
    db::{
        Db,
        query::QueryPlan,
        store::{DataKey, DataRow, DataStore},
    },
    deserialize,
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

    pub fn with_store<R>(&self, f: impl FnOnce(&DataStore) -> R) -> Result<R, Error> {
        self.db.with_data(|reg| reg.with_store(E::Store::PATH, f))
    }

    pub fn with_store_mut<R>(&self, f: impl FnOnce(&mut DataStore) -> R) -> Result<R, Error> {
        self.db
            .with_data(|reg| reg.with_store_mut(E::Store::PATH, f))
    }

    ///
    /// Analyze Plan
    ///

    pub fn candidates_from_plan(&self, plan: QueryPlan) -> Result<Vec<DataKey>, Error> {
        let candidates = match plan {
            QueryPlan::Keys(keys) => Self::to_data_keys(keys),

            QueryPlan::Range(start, end) => self.with_store(|s| {
                let start = Self::to_data_key(start);
                let end = Self::to_data_key(end);

                s.range((Bound::Included(start), Bound::Included(end)))
                    .map(|e| e.key().clone())
                    .collect()
            })?,

            QueryPlan::FullScan => self.with_store(|s| {
                let start = DataKey::lower_bound::<E>();
                let end = DataKey::upper_bound::<E>();

                s.range((Bound::Included(start), Bound::Included(end)))
                    .map(|entry| entry.key().clone())
                    .collect()
            })?,

            QueryPlan::Index(index_plan) => {
                let index_store = self
                    .db
                    .with_index(|reg| reg.try_get_store(index_plan.index.store))?;

                index_store.with_borrow(|istore| {
                    istore.resolve_data_values::<E>(index_plan.index, &index_plan.values)
                })
            }
        };

        Ok(candidates)
    }

    pub fn rows_from_plan(&self, plan: QueryPlan) -> Result<Vec<DataRow>, Error> {
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

                s.range((Bound::Included(start), Bound::Included(end)))
                    .map(|entry| (entry.key().clone(), entry.value()))
                    .collect()
            }),
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
    ) -> Result<Vec<DataRow>, Error> {
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

                self.with_store(|s| {
                    let base = s.range((Bound::Included(start), Bound::Included(end)));
                    let cap = take.unwrap_or(0);
                    let mut out = Vec::with_capacity(cap);
                    match take {
                        Some(t) => {
                            for entry in base.skip(skip).take(t) {
                                out.push((entry.key().clone(), entry.value()));
                            }
                        }
                        None => {
                            for entry in base.skip(skip) {
                                out.push((entry.key().clone(), entry.value()));
                            }
                        }
                    }
                    out
                })
            }

            QueryPlan::FullScan => self.with_store(|s| {
                let start = DataKey::lower_bound::<E>();
                let end = DataKey::upper_bound::<E>();

                let base = s.range((Bound::Included(start), Bound::Included(end)));
                let cap = take.unwrap_or(0);
                let mut out = Vec::with_capacity(cap);
                match take {
                    Some(t) => {
                        for entry in base.skip(skip).take(t) {
                            out.push((entry.key().clone(), entry.value()));
                        }
                    }
                    None => {
                        for entry in base.skip(skip) {
                            out.push((entry.key().clone(), entry.value()));
                        }
                    }
                }
                out
            }),

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

    #[inline]
    fn slice_bounds(total: usize, offset: u32, limit: Option<u32>) -> (usize, usize) {
        let start = (offset as usize).min(total);
        let end = match limit {
            Some(l) => start.saturating_add(l as usize).min(total),
            None => total,
        };

        (start, end)
    }

    fn load_many(&self, keys: &[DataKey]) -> Result<Vec<DataRow>, Error> {
        self.with_store(|s| {
            keys.iter()
                .filter_map(|k| s.get(k).map(|entry| (k.clone(), entry)))
                .collect()
        })
    }

    fn load_range(&self, start: DataKey, end: DataKey) -> Result<Vec<DataRow>, Error> {
        self.with_store(|s| {
            s.range((Bound::Included(start), Bound::Included(end)))
                .map(|e| (e.key().clone(), e.value()))
                .collect()
        })
    }

    /// Deserialize raw data rows into typed entity rows, mapping `DataKey` → `(Key, E)`.
    pub fn deserialize_rows(&self, rows: Vec<DataRow>) -> Result<Vec<(Key, E)>, Error> {
        rows.into_iter()
            .map(|(k, v)| deserialize::<E>(&v).map(|entry| (k.key(), entry)))
            .collect()
    }
}
