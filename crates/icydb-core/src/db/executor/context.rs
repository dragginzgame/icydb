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
    traits::{EntityKind, EntityValue, Path},
    types::Id,
};
use std::{collections::BTreeSet, marker::PhantomData, ops::Bound};

///
/// Context
///

pub struct Context<'a, E: EntityKind + EntityValue> {
    pub db: &'a Db<E::Canister>,
    _marker: PhantomData<E>,
}

impl<'a, E> Context<'a, E>
where
    E: EntityKind + EntityValue,
{
    #[must_use]
    pub const fn new(db: &'a Db<E::Canister>) -> Self {
        Self {
            db,
            _marker: PhantomData,
        }
    }

    // ------------------------------------------------------------------
    // Store access
    // ------------------------------------------------------------------

    pub fn with_store<R>(&self, f: impl FnOnce(&DataStore) -> R) -> Result<R, InternalError> {
        self.db
            .with_data(|reg| reg.with_store(E::DataStore::PATH, f))
    }

    // ------------------------------------------------------------------
    // Row reads
    // ------------------------------------------------------------------

    pub fn read(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw()?;
            s.get(&raw)
                .ok_or_else(|| InternalError::store_not_found(key.to_string()))
        })?
    }

    pub fn read_strict(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw()?;
            s.get(&raw).ok_or_else(|| {
                ExecutorError::corruption(ErrorOrigin::Store, format!("missing row: {key}")).into()
            })
        })?
    }

    // ------------------------------------------------------------------
    // Access path analysis
    // ------------------------------------------------------------------

    pub(crate) fn candidates_from_access(
        &self,
        access: &AccessPath<E::Key>,
    ) -> Result<Vec<DataKey>, InternalError>
    where
        E: EntityKind,
    {
        let is_index_path = matches!(access, AccessPath::IndexPrefix { .. });

        let mut candidates = match access {
            AccessPath::ByKey(key) => vec![Self::data_key_from_key(*key)?],

            AccessPath::ByKeys(keys) => keys
                .iter()
                .copied()
                .map(Self::data_key_from_key)
                .collect::<Result<Vec<_>, _>>()?,

            AccessPath::KeyRange { start, end } => self.with_store(|s| {
                let start = Self::data_key_from_key(*start)?;
                let end = Self::data_key_from_key(*end)?;
                let start_raw = start.to_raw()?;
                let end_raw = end.to_raw()?;

                s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .map(|e| Self::decode_data_key(e.key()))
                    .collect::<Result<Vec<_>, _>>()
            })??,

            AccessPath::FullScan => self.with_store(|s| {
                let start = DataKey::lower_bound::<E>();
                let end = DataKey::upper_bound::<E>();
                let start_raw = start.to_raw()?;
                let end_raw = end.to_raw()?;

                s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .map(|e| Self::decode_data_key(e.key()))
                    .collect::<Result<Vec<_>, _>>()
            })??,

            AccessPath::IndexPrefix { index, values } => {
                let index_store = self.db.with_index(|reg| reg.try_get_store(index.store))?;
                index_store.with_borrow(|s| s.resolve_data_values::<E>(index, values))?
            }
        };

        if is_index_path {
            candidates.sort_unstable();
        }

        Ok(candidates)
    }

    pub(crate) fn rows_from_access(
        &self,
        access: &AccessPath<E::Key>,
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError>
    where
        E: EntityKind,
    {
        match access {
            AccessPath::ByKey(key) => {
                let keys = vec![Self::data_key_from_key(*key)?];
                self.load_many_with_consistency(&keys, consistency)
            }

            AccessPath::ByKeys(keys) => {
                let keys = Self::dedup_keys(keys.clone())
                    .into_iter()
                    .map(Self::data_key_from_key)
                    .collect::<Result<Vec<_>, _>>()?;
                self.load_many_with_consistency(&keys, consistency)
            }

            AccessPath::KeyRange { start, end } => {
                let start = Self::data_key_from_key(*start)?;
                let end = Self::data_key_from_key(*end)?;
                self.load_range(start, end)
            }

            AccessPath::FullScan => self.with_store(|s| {
                let start = DataKey::lower_bound::<E>();
                let end = DataKey::upper_bound::<E>();
                let start_raw = start.to_raw()?;
                let end_raw = end.to_raw()?;

                s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .map(|e| {
                        let dk = Self::decode_data_key(e.key())?;
                        Ok((dk, e.value()))
                    })
                    .collect::<Result<Vec<_>, InternalError>>()
            })?,

            AccessPath::IndexPrefix { .. } => {
                let keys = self.candidates_from_access(access)?;
                self.load_many_with_consistency(&keys, consistency)
            }
        }
    }

    pub(crate) fn rows_from_access_plan(
        &self,
        access: &AccessPlan<E::Key>,
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError>
    where
        E: EntityKind,
    {
        match access {
            AccessPlan::Path(path) => self.rows_from_access(path, consistency),

            AccessPlan::Union(_) | AccessPlan::Intersection(_) => {
                let keys = self.candidate_keys_for_plan(access)?;
                self.load_many_with_consistency(&keys.into_iter().collect::<Vec<_>>(), consistency)
            }
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    fn data_key_from_key(key: E::Key) -> Result<DataKey, InternalError>
    where
        E: EntityKind,
    {
        DataKey::try_new::<E>(key)
    }

    fn dedup_keys(keys: Vec<E::Key>) -> Vec<E::Key> {
        let mut set = BTreeSet::new();
        set.extend(keys);
        set.into_iter().collect()
    }

    fn load_many_with_consistency(
        &self,
        keys: &[DataKey],
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let row = match consistency {
                ReadConsistency::Strict => self.read_strict(key),
                ReadConsistency::MissingOk => self.read(key),
            };

            match row {
                Ok(row) => out.push((key.clone(), row)),
                Err(err) if err.is_not_found() => {}
                Err(err) => return Err(err),
            }
        }
        Ok(out)
    }

    fn load_range(&self, start: DataKey, end: DataKey) -> Result<Vec<DataRow>, InternalError> {
        self.with_store(|s| {
            let start_raw = start.to_raw()?;
            let end_raw = end.to_raw()?;
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
        plan: &AccessPlan<E::Key>,
    ) -> Result<BTreeSet<DataKey>, InternalError>
    where
        E: EntityKind,
    {
        match plan {
            AccessPlan::Path(path) => {
                let keys = self.candidates_from_access(path)?;
                Ok(keys.into_iter().collect())
            }
            AccessPlan::Union(children) => {
                let mut keys = BTreeSet::new();
                for child in children {
                    keys.extend(self.candidate_keys_for_plan(child)?);
                }
                Ok(keys)
            }
            AccessPlan::Intersection(children) => {
                let mut iter = children.iter();
                let Some(first) = iter.next() else {
                    return Ok(BTreeSet::new());
                };

                let mut keys = self.candidate_keys_for_plan(first)?;
                for child in iter {
                    let child_keys = self.candidate_keys_for_plan(child)?;
                    keys.retain(|k| child_keys.contains(k));
                    if keys.is_empty() {
                        break;
                    }
                }

                Ok(keys)
            }
        }
    }

    fn decode_data_key(raw: &RawDataKey) -> Result<DataKey, InternalError> {
        DataKey::try_from_raw(raw)
            .map_err(|err| ExecutorError::corruption(ErrorOrigin::Store, err.to_string()).into())
    }

    pub fn deserialize_rows(rows: Vec<DataRow>) -> Result<Vec<(Id<E>, E)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        rows.into_iter()
            .map(|(key, row)| {
                let entity = row.try_decode::<E>().map_err(|err| {
                    ExecutorError::corruption(
                        ErrorOrigin::Serialize,
                        format!("failed to deserialize row: {key} ({err})"),
                    )
                })?;

                let key = key.try_key::<E>()?;
                let identity = entity.id();
                let identity_key = identity.into_storage_key();
                if key != identity_key {
                    let expected = DataKey::try_new::<E>(key)?;
                    let found = DataKey::try_new::<E>(identity_key)?;

                    return Err(ExecutorError::corruption(
                        ErrorOrigin::Store,
                        format!("row key mismatch: expected {expected}, found {found}"),
                    )
                    .into());
                }

                Ok((Id::from_storage_key(key), entity))
            })
            .collect()
    }
}
