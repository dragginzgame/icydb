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
        executor::{ExecutorError, OrderedKeyStream, saturating_row_len},
        index::{
            IndexEntryReader, IndexScanContinuationInput, IndexStore, PrimaryRowReader,
            RawIndexEntry, RawIndexKey, SealedIndexEntryReader, SealedPrimaryRowReader,
        },
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{EntityKind, EntityValue, Path},
    types::Id,
};
use std::{collections::BTreeSet, ops::Bound};

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
    pub(in crate::db::executor) db: &'a Db<E::Canister>,
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
        Self { db }
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

    /// Fold persisted row payload bytes over one full-scan page window.
    pub(crate) fn sum_row_payload_bytes_full_scan_window(
        &self,
        direction: Direction,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<u64, InternalError> {
        self.with_store(|store| -> Result<u64, InternalError> {
            let mut total = 0u64;
            let mut offset_remaining = offset;
            let mut limit_remaining = limit;

            match direction {
                Direction::Asc => {
                    for entry in store.iter() {
                        if payload_window_limit_exhausted(limit_remaining) {
                            break;
                        }
                        if !payload_window_accept_row(&mut offset_remaining, &mut limit_remaining) {
                            continue;
                        }

                        total = total.saturating_add(saturating_row_len(entry.value().len()));
                    }
                }
                Direction::Desc => {
                    for entry in store.iter().rev() {
                        if payload_window_limit_exhausted(limit_remaining) {
                            break;
                        }
                        if !payload_window_accept_row(&mut offset_remaining, &mut limit_remaining) {
                            continue;
                        }

                        total = total.saturating_add(saturating_row_len(entry.value().len()));
                    }
                }
            }

            Ok(total)
        })?
    }

    /// Fold persisted row payload bytes over one key-range page window.
    pub(crate) fn sum_row_payload_bytes_key_range_window(
        &self,
        start: &DataKey,
        end: &DataKey,
        direction: Direction,
        offset: usize,
        limit: Option<usize>,
    ) -> Result<u64, InternalError> {
        let start_raw = start.to_raw()?;
        let end_raw = end.to_raw()?;

        self.with_store(|store| -> Result<u64, InternalError> {
            let mut total = 0u64;
            let mut offset_remaining = offset;
            let mut limit_remaining = limit;

            match direction {
                Direction::Asc => {
                    for entry in store.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    {
                        if payload_window_limit_exhausted(limit_remaining) {
                            break;
                        }
                        if !payload_window_accept_row(&mut offset_remaining, &mut limit_remaining) {
                            continue;
                        }

                        total = total.saturating_add(saturating_row_len(entry.value().len()));
                    }
                }
                Direction::Desc => {
                    for entry in store
                        .range((Bound::Included(start_raw), Bound::Included(end_raw)))
                        .rev()
                    {
                        if payload_window_limit_exhausted(limit_remaining) {
                            break;
                        }
                        if !payload_window_accept_row(&mut offset_remaining, &mut limit_remaining) {
                            continue;
                        }

                        total = total.saturating_add(saturating_row_len(entry.value().len()));
                    }
                }
            }

            Ok(total)
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
        DataKey::try_from_raw(raw).map_err(|err| {
            InternalError::identity_corruption(format!("failed to decode data key: {err}"))
        })
    }

    /// Deserialize data rows into `(Id, Entity)` tuples with key/entity consistency checks.
    pub(crate) fn deserialize_row(row: DataRow) -> Result<(Id<E>, E), InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let (key, row) = row;
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
    }

    /// Deserialize data rows into `(Id, Entity)` tuples with key/entity consistency checks.
    pub(crate) fn deserialize_rows(rows: Vec<DataRow>) -> Result<Vec<(Id<E>, E)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        // Phase 1: decode each row payload and enforce key/entity alignment invariants.
        rows.into_iter().map(Self::deserialize_row).collect()
    }
}

const fn payload_window_limit_exhausted(limit_remaining: Option<usize>) -> bool {
    matches!(limit_remaining, Some(0))
}

const fn payload_window_accept_row(
    offset_remaining: &mut usize,
    limit_remaining: &mut Option<usize>,
) -> bool {
    if *offset_remaining > 0 {
        *offset_remaining = offset_remaining.saturating_sub(1);
        return false;
    }

    if let Some(remaining) = limit_remaining.as_mut() {
        if *remaining == 0 {
            return false;
        }
        *remaining = remaining.saturating_sub(1);
    }

    true
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

impl<E> SealedPrimaryRowReader<E> for Context<'_, E> where E: EntityKind + EntityValue {}

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
                IndexScanContinuationInput::new(None, Direction::Asc),
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

impl<E> SealedIndexEntryReader<E> for Context<'_, E> where E: EntityKind + EntityValue {}

///
/// TESTS
///

#[cfg(test)]
mod tests;
