use crate::{
    db::{
        Db,
        executor::ExecutorError,
        query::{
            ReadConsistency,
            plan::{AccessPath, AccessPlan},
        },
        store::{DataKey, DataRow, DataStore, RawDataKey, RawRow},
    },
    error::{ErrorOrigin, InternalError},
    key::Key,
    traits::{EntityKind, Path},
};
use std::{collections::HashSet, marker::PhantomData, ops::Bound};

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

    /// Read a row; missing rows return `NotFound`.
    pub fn read(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw();
            s.get(&raw)
                .ok_or_else(|| InternalError::store_not_found(key.to_string()))
        })?
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
    /// Analyze Access Path
    ///

    /// Compute candidate data keys for the given access path.
    ///
    /// Note: index candidates are returned in deterministic key order.
    /// This ordering is for stability only and does not imply semantic ordering.
    pub(crate) fn candidates_from_access(
        &self,
        access: &AccessPath,
    ) -> Result<Vec<DataKey>, InternalError> {
        let is_index_path = matches!(access, AccessPath::IndexPrefix { .. });

        let mut candidates = match access {
            AccessPath::ByKey(key) => Self::to_data_keys(vec![*key]),
            AccessPath::ByKeys(keys) => Self::to_data_keys(keys.clone()),
            AccessPath::KeyRange { start, end } => self.with_store(|s| {
                let start = Self::to_data_key(*start);
                let end = Self::to_data_key(*end);
                let start_raw = start.to_raw();
                let end_raw = end.to_raw();

                s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .map(|e| Self::decode_data_key(e.key()))
                    .collect::<Result<Vec<_>, _>>()
            })??,
            AccessPath::FullScan => self.with_store(|s| {
                let start = DataKey::lower_bound::<E>();
                let end = DataKey::upper_bound::<E>();
                let start_raw = start.to_raw();
                let end_raw = end.to_raw();

                s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .map(|entry| Self::decode_data_key(entry.key()))
                    .collect::<Result<Vec<_>, _>>()
            })??,
            AccessPath::IndexPrefix { index, values } => {
                let index_store = self.db.with_index(|reg| reg.try_get_store(index.store))?;

                index_store.with_borrow(|istore| {
                    istore
                        .resolve_data_values::<E>(index, values)
                        .map_err(|msg| ExecutorError::corruption(ErrorOrigin::Index, msg))
                })?
            }
        };

        if is_index_path {
            candidates.sort_unstable();
        }

        Ok(candidates)
    }

    /// Load data rows for the given access path.
    pub(crate) fn rows_from_access(
        &self,
        access: &AccessPath,
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError> {
        match access {
            AccessPath::ByKey(key) => {
                let data_keys = Self::to_data_keys(vec![*key]);
                self.load_many_with_consistency(&data_keys, consistency)
            }
            AccessPath::ByKeys(keys) => {
                let keys = Self::dedup_keys(keys.clone());
                let data_keys = Self::to_data_keys(keys);
                self.load_many_with_consistency(&data_keys, consistency)
            }
            AccessPath::KeyRange { start, end } => {
                let start = Self::to_data_key(*start);
                let end = Self::to_data_key(*end);
                self.load_range(start, end)
            }
            AccessPath::FullScan => self.with_store(|s| {
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
            AccessPath::IndexPrefix { .. } => {
                let data_keys = self.candidates_from_access(access)?;
                self.load_many_with_consistency(&data_keys, consistency)
            }
        }
    }

    /// Load data rows for a composite access plan.
    pub(crate) fn rows_from_access_plan(
        &self,
        access: &AccessPlan,
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError> {
        match access {
            AccessPlan::Path(path) => self.rows_from_access(path, consistency),
            AccessPlan::Union(_) | AccessPlan::Intersection(_) => {
                let keys = self.candidate_keys_for_plan(access)?;
                let keys = keys.into_iter().collect::<Vec<_>>();
                self.load_many_with_consistency(&keys, consistency)
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

    fn dedup_keys(keys: Vec<Key>) -> Vec<Key> {
        let mut seen = HashSet::with_capacity(keys.len());
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            if seen.insert(key) {
                out.push(key);
            }
        }
        out
    }

    fn load_many_with_consistency(
        &self,
        keys: &[DataKey],
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError> {
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            let row = match consistency {
                ReadConsistency::Strict => self.read_strict(k),
                ReadConsistency::MissingOk => self.read(k),
            };

            match row {
                Ok(row) => out.push((k.clone(), row)),
                Err(err) => {
                    if err.is_not_found() {
                        continue;
                    }
                    return Err(err);
                }
            }
        }

        Ok(out)
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

    fn candidate_keys_for_plan(
        &self,
        plan: &AccessPlan,
    ) -> Result<std::collections::BTreeSet<DataKey>, InternalError> {
        match plan {
            AccessPlan::Path(path) => {
                let keys = self.candidates_from_access(path)?;
                Ok(keys.into_iter().collect())
            }
            AccessPlan::Union(children) => {
                let mut keys = std::collections::BTreeSet::new();
                for child in children {
                    keys.extend(self.candidate_keys_for_plan(child)?);
                }
                Ok(keys)
            }
            AccessPlan::Intersection(children) => {
                let mut iter = children.iter();
                let Some(first) = iter.next() else {
                    return Ok(std::collections::BTreeSet::new());
                };
                let mut keys = self.candidate_keys_for_plan(first)?;
                for child in iter {
                    let child_keys = self.candidate_keys_for_plan(child)?;
                    keys.retain(|key| child_keys.contains(key));
                    if keys.is_empty() {
                        break;
                    }
                }
                Ok(keys)
            }
        }
    }

    /// Deserialize raw data rows into typed entity rows, mapping `DataKey` → `(Key, E)`.
    #[allow(clippy::unused_self)]
    pub fn deserialize_rows(&self, rows: Vec<DataRow>) -> Result<Vec<(Key, E)>, InternalError> {
        rows.into_iter()
            .map(|(k, v)| {
                let entry = v.try_decode::<E>().map_err(|err| {
                    ExecutorError::corruption(
                        ErrorOrigin::Serialize,
                        format!("failed to deserialize row: {k} ({err})"),
                    )
                })?;

                let key = k.key();
                let entity_key = entry.key();
                if key != entity_key {
                    return Err(ExecutorError::corruption(
                        ErrorOrigin::Store,
                        format!("row key mismatch: expected {key}, found {entity_key}"),
                    )
                    .into());
                }

                Ok((key, entry))
            })
            .collect::<_>()
    }

    fn decode_data_key(raw: &RawDataKey) -> Result<DataKey, InternalError> {
        DataKey::try_from_raw(raw)
            .map_err(|msg| ExecutorError::corruption(ErrorOrigin::Store, msg).into())
    }
}
