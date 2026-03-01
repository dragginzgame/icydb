//! Module: executor::context
//! Responsibility: executor-scoped store/index read context and row decoding helpers.
//! Does not own: routing policy, plan lowering, or mutation commit semantics.
//! Boundary: read-only data/index access surface consumed by executor submodules.

use crate::{
    db::{
        Db,
        data::{
            DataKey, DataRow, DataStore, RawDataKey, RawRow, decode_and_validate_entity_key,
            format_entity_key_for_mismatch,
        },
        direction::Direction,
        executor::{ExecutorError, OrderedKeyStream},
        index::{IndexEntryReader, IndexStore, PrimaryRowReader, RawIndexEntry, RawIndexKey},
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{EntityKind, EntityValue, Path},
    types::Id,
};
use std::{collections::BTreeSet, marker::PhantomData, ops::Bound};

// -----------------------------------------------------------------------------
// Context Subdomains
// -----------------------------------------------------------------------------
// 1) Context handle and store access.
// 2) Row reads and consistency-aware materialization.
// 3) Key/spec helper utilities and decoding invariants.

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
    // ------------------------------------------------------------------
    // Context setup
    // ------------------------------------------------------------------

    /// Construct one executor context bound to a database handle.
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

    /// Execute one closure against the entity's data store handle.
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

    /// Read one raw row by key, returning not-found as an error.
    pub(crate) fn read(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw()?;
            s.get(&raw)
                .ok_or_else(|| InternalError::store_not_found(key.to_string()))
        })?
    }

    /// Read one raw row by key, classifying missing rows as store corruption.
    pub(crate) fn read_strict(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw()?;
            s.get(&raw).ok_or_else(|| {
                ExecutorError::store_corruption(format!("missing row: {key}")).into()
            })
        })?
    }

    // Load rows for an ordered key stream by preserving the stream order.
    /// Materialize rows for an ordered key stream while preserving stream order.
    pub(crate) fn rows_from_ordered_key_stream(
        &self,
        key_stream: &mut dyn OrderedKeyStream,
        consistency: MissingRowPolicy,
    ) -> Result<Vec<DataRow>, InternalError> {
        let keys = Self::collect_ordered_keys(key_stream)?;

        self.load_many_with_consistency(keys, consistency)
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /// Build one `DataKey` from entity key type.
    pub(super) fn data_key_from_key(key: E::Key) -> Result<DataKey, InternalError>
    where
        E: EntityKind,
    {
        DataKey::try_new::<E>(key)
    }

    /// Deduplicate entity keys using canonical key ordering.
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
        keys: Vec<DataKey>,
        consistency: MissingRowPolicy,
    ) -> Result<Vec<DataRow>, InternalError> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            // Row storage is authoritative. Index-backed access paths only supply
            // candidate keys and must always be validated by a data-store read.
            let row = match consistency {
                MissingRowPolicy::Error => self.read_strict(&key),
                MissingRowPolicy::Ignore => self.read(&key),
            };

            match row {
                Ok(row) => out.push((key, row)),
                Err(err) if err.is_not_found() => {}
                Err(err) => return Err(err),
            }
        }

        Ok(out)
    }

    /// Decode one raw data key and map decode failures to executor corruption errors.
    pub(super) fn decode_data_key(raw: &RawDataKey) -> Result<DataKey, InternalError> {
        DataKey::try_from_raw(raw).map_err(|err| ExecutorError::store_corruption_from(err).into())
    }

    /// Deserialize data rows into `(Id, Entity)` tuples with key/entity consistency checks.
    pub(crate) fn deserialize_rows(rows: Vec<DataRow>) -> Result<Vec<(Id<E>, E)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        // Phase 1: decode each row payload and enforce key/entity alignment invariants.
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

impl<E> PrimaryRowReader<E> for Context<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_primary_row(&self, key: &DataKey) -> Result<Option<RawRow>, InternalError> {
        match self.read(key) {
            Ok(row) => Ok(Some(row)),
            Err(err) if err.is_not_found() => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl<E> IndexEntryReader<E> for Context<'_, E>
where
    E: EntityKind + EntityValue,
{
    fn read_index_entry(
        &self,
        store: &'static std::thread::LocalKey<std::cell::RefCell<IndexStore>>,
        key: &RawIndexKey,
    ) -> Result<Option<RawIndexEntry>, InternalError> {
        Ok(store.with_borrow(|index_store| index_store.get(key)))
    }

    fn read_index_keys_in_raw_range(
        &self,
        store: &'static std::thread::LocalKey<std::cell::RefCell<IndexStore>>,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        limit: usize,
    ) -> Result<Vec<E::Key>, InternalError> {
        let data_keys = store.with_borrow(|index_store| {
            index_store.resolve_data_values_in_raw_range_limited::<E>(
                index,
                bounds,
                None,
                Direction::Asc,
                limit,
                None,
            )
        })?;

        let mut out = Vec::with_capacity(data_keys.len());
        for data_key in data_keys {
            out.push(data_key.try_key::<E>()?);
        }

        Ok(out)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
