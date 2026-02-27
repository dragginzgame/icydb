use crate::{
    db::{
        Db,
        contracts::ReadConsistency,
        data::{
            DataKey, DataRow, DataStore, RawDataKey, RawRow, decode_and_validate_entity_key,
            format_entity_key_for_mismatch,
        },
        executor::{ExecutorError, OrderedKeyStream},
        index::PrimaryRowReader,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue, Path},
    types::Id,
};
use std::{collections::BTreeSet, marker::PhantomData};

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

    // Load rows for an ordered key stream by preserving the stream order.
    pub(crate) fn rows_from_ordered_key_stream(
        &self,
        key_stream: &mut dyn OrderedKeyStream,
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError> {
        let keys = Self::collect_ordered_keys(key_stream)?;

        self.load_many_with_consistency(keys, consistency)
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
        keys: Vec<DataKey>,
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            // Row storage is authoritative. Index-backed access paths only supply
            // candidate keys and must always be validated by a data-store read.
            let row = match consistency {
                ReadConsistency::Strict => self.read_strict(&key),
                ReadConsistency::MissingOk => self.read(&key),
            };

            match row {
                Ok(row) => out.push((key, row)),
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

///
/// TESTS
///

#[cfg(test)]
mod tests;
