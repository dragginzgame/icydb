//! Module: executor::runtime_context
//! Responsibility: executor-scoped store/index read context and row decoding helpers.
//! Does not own: routing policy, plan lowering, or mutation commit semantics.
//! Boundary: read-only data/index access surface consumed by executor submodules.

mod load;

use crate::{
    db::{
        Db,
        cursor::IndexScanContinuationInput,
        data::{DataKey, DataRow, DataStore, RawRow},
        direction::Direction,
        executor::{ExecutorError, OrderedKeyStream, saturating_row_len},
        index::{
            IndexEntryReader, IndexStore, PrimaryRowReader, RawIndexEntry, RawIndexKey,
            SealedIndexEntryReader, SealedPrimaryRowReader,
        },
        predicate::MissingRowPolicy,
        registry::StoreHandle,
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
};
#[cfg(test)]
use std::collections::BTreeSet;
use std::ops::Bound;

// -----------------------------------------------------------------------------
// Context Subdomains
// -----------------------------------------------------------------------------
// 1) Context handle and store access.
// 2) Row reads and consistency-aware materialization.
// 3) Key/spec helper utilities and decoding invariants.

///
/// Context
///

#[derive(Clone, Copy)]
pub(in crate::db) struct Context<'a, E: EntityKind + EntityValue> {
    pub(in crate::db::executor) db: &'a Db<E::Canister>,
}

///
/// StoreLookup
///
/// StoreLookup is the object-safe store-registry lookup boundary used when
/// executor helpers need to resolve an arbitrary named store without carrying
/// a typed `Context<E>` through the call chain.
///

pub(in crate::db) trait StoreLookup {
    fn try_get_store(&self, path: &str) -> Result<StoreHandle, InternalError>;
}

impl<C> StoreLookup for Db<C>
where
    C: CanisterKind,
{
    fn try_get_store(&self, path: &str) -> Result<StoreHandle, InternalError> {
        self.with_store_registry(|registry| registry.try_get_store(path))
    }
}

///
/// StructuralStoreResolver
///
/// StructuralStoreResolver is the non-generic named-store lookup bundle used by
/// executor helpers that must resolve index-owned stores after the typed
/// boundary has already chosen the entity model/runtime shell.
///

#[derive(Clone, Copy)]
pub(in crate::db) struct StructuralStoreResolver<'a> {
    lookup: &'a dyn StoreLookup,
}

impl<'a> StructuralStoreResolver<'a> {
    /// Build one structural named-store resolver from one object-safe lookup boundary.
    #[must_use]
    pub(in crate::db) const fn new(lookup: &'a dyn StoreLookup) -> Self {
        Self { lookup }
    }

    /// Resolve one named store through the captured store-registry boundary.
    pub(in crate::db) fn try_get_store(self, path: &str) -> Result<StoreHandle, InternalError> {
        self.lookup.try_get_store(path)
    }
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
    pub(in crate::db) const fn new(db: &'a Db<E::Canister>) -> Self {
        Self { db }
    }

    // ------------------------------------------------------------------
    // Store access
    // ------------------------------------------------------------------

    /// Execute one closure against the entity's data store handle.
    pub(in crate::db) fn with_store<R>(
        &self,
        f: impl FnOnce(&DataStore) -> R,
    ) -> Result<R, InternalError> {
        self.db.with_store_registry(|reg| {
            reg.try_get_store(E::Store::PATH)
                .map(|store| store.with_data(f))
        })
    }

    /// Recover the structural store handle once for generic-free executor runtime helpers.
    pub(in crate::db::executor) fn structural_store(&self) -> Result<StoreHandle, InternalError> {
        self.db
            .with_store_registry(|reg| reg.try_get_store(E::Store::PATH))
    }

    // ------------------------------------------------------------------
    // Row reads
    // ------------------------------------------------------------------

    /// Read one raw row by key, returning not-found as an error.
    pub(in crate::db) fn read(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw()?;
            s.get(&raw)
                .ok_or_else(|| InternalError::store_not_found(key.to_string()))
        })?
    }

    /// Read one raw row by key, classifying missing rows as store corruption.
    pub(in crate::db) fn read_strict(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw()?;
            s.get(&raw).ok_or_else(|| {
                ExecutorError::store_corruption(format!("missing row: {key}")).into()
            })
        })?
    }

    // Load rows for an ordered key stream by preserving the stream order.
    /// Materialize rows for an ordered key stream while preserving stream order.
    pub(in crate::db::executor) fn rows_from_ordered_key_stream(
        &self,
        key_stream: &mut dyn OrderedKeyStream,
        consistency: MissingRowPolicy,
    ) -> Result<Vec<DataRow>, InternalError> {
        // Shared scan loop runs once in a non-generic helper; this wrapper only
        // supplies the entity-owned consistency read contract.
        collect_rows_from_ordered_key_stream_shared(key_stream, &mut |key| {
            self.read_row_with_consistency_skip_not_found(key, consistency)
        })
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /// Deduplicate entity keys using canonical key ordering.
    #[cfg(test)]
    pub(super) fn dedup_keys(keys: Vec<E::Key>) -> Vec<E::Key> {
        let mut set = BTreeSet::new();
        set.extend(keys);
        set.into_iter().collect()
    }

    // Read one row under one consistency contract, treating not-found as skip.
    fn read_row_with_consistency_skip_not_found(
        &self,
        key: &DataKey,
        consistency: MissingRowPolicy,
    ) -> Result<Option<RawRow>, InternalError> {
        let row = match consistency {
            MissingRowPolicy::Error => self.read_strict(key),
            MissingRowPolicy::Ignore => self.read(key),
        };

        match row {
            Ok(row) => Ok(Some(row)),
            Err(err) if err.is_not_found() => Ok(None),
            Err(err) => Err(err),
        }
    }
}

// Read one raw row under one consistency contract from structural store authority.
pub(in crate::db::executor) fn read_row_with_consistency_from_store(
    store: StoreHandle,
    key: &DataKey,
    consistency: MissingRowPolicy,
) -> Result<Option<RawRow>, InternalError> {
    let read_row = |key: &DataKey| -> Result<Option<RawRow>, InternalError> {
        let raw = key.to_raw()?;

        Ok(store.with_data(|data| data.get(&raw)))
    };

    match consistency {
        MissingRowPolicy::Error => match read_row(key)? {
            Some(row) => Ok(Some(row)),
            None => Err(ExecutorError::store_corruption(format!("missing row: {key}")).into()),
        },
        MissingRowPolicy::Ignore => read_row(key),
    }
}

// Read one persisted row under one consistency contract and preserve the source data key.
pub(in crate::db::executor) fn read_data_row_with_consistency_from_store(
    store: StoreHandle,
    key: &DataKey,
    consistency: MissingRowPolicy,
) -> Result<Option<DataRow>, InternalError> {
    let Some(row) = read_row_with_consistency_from_store(store, key, consistency)? else {
        return Ok(None);
    };

    Ok(Some((key.clone(), row)))
}

/// Fold persisted row payload bytes over one full-scan page window through structural store authority.
pub(in crate::db::executor) fn sum_row_payload_bytes_full_scan_window_with_store(
    store: StoreHandle,
    direction: Direction,
    offset: usize,
    limit: Option<usize>,
) -> u64 {
    store.with_data(|store| match direction {
        Direction::Asc => sum_payload_bytes_from_row_lengths(
            store.iter().map(|entry| entry.value().len()),
            offset,
            limit,
        ),
        Direction::Desc => sum_payload_bytes_from_row_lengths(
            store.iter().rev().map(|entry| entry.value().len()),
            offset,
            limit,
        ),
    })
}

/// Fold persisted row payload bytes over one key-range page window through structural store authority.
pub(in crate::db::executor) fn sum_row_payload_bytes_key_range_window_with_store(
    store: StoreHandle,
    start: &DataKey,
    end: &DataKey,
    direction: Direction,
    offset: usize,
    limit: Option<usize>,
) -> Result<u64, InternalError> {
    let start_raw = start.to_raw()?;
    let end_raw = end.to_raw()?;
    let total = store.with_data(|store| match direction {
        Direction::Asc => sum_payload_bytes_from_row_lengths(
            store
                .range((Bound::Included(start_raw), Bound::Included(end_raw)))
                .map(|entry| entry.value().len()),
            offset,
            limit,
        ),
        Direction::Desc => sum_payload_bytes_from_row_lengths(
            store
                .range((Bound::Included(start_raw), Bound::Included(end_raw)))
                .rev()
                .map(|entry| entry.value().len()),
            offset,
            limit,
        ),
    });

    Ok(total)
}

/// Fold persisted row payload bytes over one ordered key stream page window through structural store authority.
pub(in crate::db::executor) fn sum_row_payload_bytes_from_ordered_key_stream_with_store(
    store: StoreHandle,
    key_stream: &mut dyn OrderedKeyStream,
    consistency: MissingRowPolicy,
    offset: usize,
    limit: Option<usize>,
) -> Result<u64, InternalError> {
    sum_row_payload_bytes_from_ordered_key_stream_shared(
        key_stream,
        &mut |key| read_row_with_consistency_from_store(store, key, consistency),
        offset,
        limit,
    )
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

fn sum_payload_bytes_from_row_lengths(
    row_lengths: impl Iterator<Item = usize>,
    offset: usize,
    limit: Option<usize>,
) -> u64 {
    let mut total = 0u64;
    let mut offset_remaining = offset;
    let mut limit_remaining = limit;

    for row_len in row_lengths {
        if payload_window_limit_exhausted(limit_remaining) {
            break;
        }
        if !payload_window_accept_row(&mut offset_remaining, &mut limit_remaining) {
            continue;
        }

        total = total.saturating_add(saturating_row_len(row_len));
    }

    total
}

// Shared ordered key-stream scan loop used by payload-byte aggregation.
// Entity wrappers provide consistency-aware row reads via callback injection.
fn sum_row_payload_bytes_from_ordered_key_stream_shared(
    key_stream: &mut dyn OrderedKeyStream,
    read_row: &mut dyn FnMut(&DataKey) -> Result<Option<RawRow>, InternalError>,
    offset: usize,
    limit: Option<usize>,
) -> Result<u64, InternalError> {
    let mut total = 0u64;
    let mut offset_remaining = offset;
    let mut limit_remaining = limit;

    while let Some(key) = key_stream.next_key()? {
        if payload_window_limit_exhausted(limit_remaining) {
            break;
        }

        // Index-backed and composite stream rows remain row-authoritative:
        // missing-row ignore skips stale keys, strict mode fails closed.
        let Some(row) = read_row(&key)? else {
            continue;
        };
        if !payload_window_accept_row(&mut offset_remaining, &mut limit_remaining) {
            continue;
        }

        total = total.saturating_add(saturating_row_len(row.len()));
    }

    Ok(total)
}

// Shared ordered key-stream scan loop used by row materialization.
// Entity wrappers provide consistency-aware row reads via callback injection.
fn collect_rows_from_ordered_key_stream_shared(
    key_stream: &mut dyn OrderedKeyStream,
    read_row: &mut dyn FnMut(&DataKey) -> Result<Option<RawRow>, InternalError>,
) -> Result<Vec<DataRow>, InternalError> {
    let mut rows = Vec::new();

    while let Some(key) = key_stream.next_key()? {
        // Row storage is authoritative. Index-backed access paths only supply
        // candidate keys and must always be validated by a data-store read.
        if let Some(row) = read_row(&key)? {
            rows.push((key, row));
        }
    }

    Ok(rows)
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
            index_store.resolve_data_values_in_raw_range_limited(
                E::ENTITY_TAG,
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
