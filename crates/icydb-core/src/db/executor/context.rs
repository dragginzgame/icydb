use crate::{
    db::{
        Db,
        data::{DataKey, DataRow, DataStore, RawDataKey, RawRow},
        entity_decode::{decode_and_validate_entity_key, format_entity_key_for_mismatch},
        executor::{ExecutorError, KeyOrderComparator, OrderedKeyStream, OrderedKeyStreamBox},
        index::RawIndexKey,
        query::{
            ReadConsistency,
            plan::{AccessPath, AccessPlan, Direction},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue, Path},
    types::Id,
};
use std::{collections::BTreeSet, marker::PhantomData};

///
/// Context
///

pub(crate) struct Context<'a, E: EntityKind + EntityValue> {
    pub db: &'a Db<E::Canister>,
    _marker: PhantomData<E>,
}

impl<'a, E> Context<'a, E>
where
    E: EntityKind + EntityValue,
{
    #[must_use]
    pub(crate) const fn new(db: &'a Db<E::Canister>) -> Self {
        Self {
            db,
            _marker: PhantomData,
        }
    }

    // ------------------------------------------------------------------
    // Store access
    // ------------------------------------------------------------------

    pub(crate) fn with_store<R>(
        &self,
        f: impl FnOnce(&DataStore) -> R,
    ) -> Result<R, InternalError> {
        self.db.with_store_registry(|reg| {
            reg.try_get_store(E::Store::PATH)
                .map(|store| store.with_data(f))
        })
    }

    // ------------------------------------------------------------------
    // Row reads
    // ------------------------------------------------------------------

    pub(crate) fn read(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw()?;
            s.get(&raw)
                .ok_or_else(|| InternalError::store_not_found(key.to_string()))
        })?
    }

    pub(crate) fn read_strict(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw()?;
            s.get(&raw).ok_or_else(|| {
                ExecutorError::store_corruption(format!("missing row: {key}")).into()
            })
        })?
    }

    // ------------------------------------------------------------------
    // Access path analysis
    // ------------------------------------------------------------------

    pub(crate) fn ordered_key_stream_from_access_with_index_range_anchor(
        &self,
        access: &AccessPath<E::Key>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind,
    {
        access.resolve_physical_key_stream(self, index_range_anchor, direction, physical_fetch_hint)
    }

    pub(crate) fn rows_from_access_plan(
        &self,
        access: &AccessPlan<E::Key>,
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError>
    where
        E: EntityKind,
    {
        self.rows_from_access_plan_with_index_range_anchor(
            access,
            consistency,
            None,
            Direction::Asc,
        )
    }

    pub(crate) fn ordered_key_stream_from_access_plan_with_index_range_anchor(
        &self,
        access: &AccessPlan<E::Key>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
        key_comparator: KeyOrderComparator,
        physical_fetch_hint: Option<usize>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind,
    {
        access.produce_key_stream(
            self,
            index_range_anchor,
            direction,
            key_comparator,
            physical_fetch_hint,
        )
    }

    pub(crate) fn rows_from_access_plan_with_index_range_anchor(
        &self,
        access: &AccessPlan<E::Key>,
        consistency: ReadConsistency,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
    ) -> Result<Vec<DataRow>, InternalError>
    where
        E: EntityKind,
    {
        let mut key_stream = self.ordered_key_stream_from_access_plan_with_index_range_anchor(
            access,
            index_range_anchor,
            direction,
            KeyOrderComparator::from_direction(direction),
            None,
        )?;

        self.rows_from_ordered_key_stream(key_stream.as_mut(), consistency)
    }

    // Load rows for an ordered key stream by preserving the stream order.
    pub(crate) fn rows_from_ordered_key_stream(
        &self,
        key_stream: &mut dyn OrderedKeyStream,
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError> {
        let keys = Self::collect_ordered_keys(key_stream)?;

        self.load_many_with_consistency(&keys, consistency)
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    pub(super) fn data_key_from_key(key: E::Key) -> Result<DataKey, InternalError>
    where
        E: EntityKind,
    {
        DataKey::try_new::<E>(key)
    }

    pub(super) fn dedup_keys(keys: Vec<E::Key>) -> Vec<E::Key> {
        let mut set = BTreeSet::new();
        set.extend(keys);
        set.into_iter().collect()
    }

    fn collect_ordered_keys(
        key_stream: &mut dyn OrderedKeyStream,
    ) -> Result<Vec<DataKey>, InternalError> {
        let mut keys = Vec::new();
        while let Some(key) = key_stream.next_key()? {
            keys.push(key);
        }

        Ok(keys)
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

    pub(super) fn decode_data_key(raw: &RawDataKey) -> Result<DataKey, InternalError> {
        DataKey::try_from_raw(raw).map_err(|err| ExecutorError::store_corruption_from(err).into())
    }

    pub(crate) fn deserialize_rows(rows: Vec<DataRow>) -> Result<Vec<(Id<E>, E)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        rows.into_iter()
            .map(|(key, row)| {
                let expected_key = key.try_key::<E>()?;
                let entity = decode_and_validate_entity_key::<E, _, _, _, _>(
                    expected_key,
                    || row.try_decode::<E>(),
                    |err| {
                        ExecutorError::serialize_corruption(format!(
                            "failed to deserialize row: {key} ({err})"
                        ))
                        .into()
                    },
                    |expected_key, actual_key| {
                        let expected = format_entity_key_for_mismatch::<E>(expected_key);
                        let found = format_entity_key_for_mismatch::<E>(actual_key);

                        ExecutorError::store_corruption(format!(
                            "row key mismatch: expected {expected}, found {found}"
                        ))
                        .into()
                    },
                )?;

                Ok((Id::from_key(expected_key), entity))
            })
            .collect()
    }
}
