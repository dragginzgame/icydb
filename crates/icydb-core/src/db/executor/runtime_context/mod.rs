//! Module: executor::runtime_context
//! Responsibility: executor-scoped store/index read context and row decoding helpers.
//! Does not own: routing policy, plan lowering, or mutation commit semantics.
//! Boundary: read-only data/index access surface consumed by executor submodules.

use crate::{
    db::{
        Db,
        data::{DataKey, DataRow, DataStore, RawRow},
        direction::Direction,
        executor::{ExecutorError, OrderedKeyStream, saturating_row_len},
        predicate::MissingRowPolicy,
        registry::StoreHandle,
    },
    error::InternalError,
    traits::{CanisterKind, EntityKind, EntityValue, Path},
};
#[cfg(test)]
use crate::{types::EntityTag, value::StorageKey};
#[cfg(any(test, feature = "structural-read-metrics"))]
use std::cell::RefCell;
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

impl<E> crate::db::executor::pipeline::contracts::LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    /// Construct one load executor bound to a database handle and debug mode.
    #[must_use]
    pub(in crate::db) const fn new(db: Db<E::Canister>, debug: bool) -> Self {
        Self { db, debug }
    }
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
/// StoreResolver
///
/// StoreResolver is the non-generic named-store lookup bundle used by
/// executor helpers that must resolve index-owned stores after the typed
/// boundary has already chosen the entity model/runtime shell.
///

#[derive(Clone, Copy)]
pub(in crate::db) struct StoreResolver<'a> {
    lookup: &'a dyn StoreLookup,
}

impl<'a> StoreResolver<'a> {
    /// Build one named-store resolver from one object-safe lookup boundary.
    #[must_use]
    pub(in crate::db) const fn new(lookup: &'a dyn StoreLookup) -> Self {
        Self { lookup }
    }

    /// Resolve one named store through the captured store-registry boundary.
    pub(in crate::db) fn try_get_store(self, path: &str) -> Result<StoreHandle, InternalError> {
        self.lookup.try_get_store(path)
    }
}

///
/// RowCheckMetrics
///
/// RowCheckMetrics aggregates one test-scoped view of the executor-owned
/// `row_check_required` boundary for secondary covering reads.
/// It lets perf probes separate secondary scan traversal, membership decode,
/// and authoritative row-presence probes without changing runtime policy.
///

#[cfg(any(test, feature = "structural-read-metrics"))]
#[cfg_attr(
    all(test, not(feature = "structural-read-metrics")),
    allow(unreachable_pub)
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowCheckMetrics {
    pub index_entries_scanned: u64,
    pub index_membership_single_key_entries: u64,
    pub index_membership_multi_key_entries: u64,
    pub index_membership_keys_decoded: u64,
    pub row_check_covering_candidates_seen: u64,
    pub row_check_rows_emitted: u64,
    pub row_presence_probe_count: u64,
    pub row_presence_probe_hits: u64,
    pub row_presence_probe_misses: u64,
    pub row_presence_probe_borrowed_data_store_count: u64,
    pub row_presence_probe_store_handle_count: u64,
    pub row_presence_key_to_raw_encodes: u64,
}

#[cfg(any(test, feature = "structural-read-metrics"))]
std::thread_local! {
    static ROW_CHECK_METRICS: RefCell<Option<RowCheckMetrics>> = const {
        RefCell::new(None)
    };
}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn update_row_check_metrics(update: impl FnOnce(&mut RowCheckMetrics)) {
    ROW_CHECK_METRICS.with(|metrics| {
        let mut metrics = metrics.borrow_mut();
        let Some(metrics) = metrics.as_mut() else {
            return;
        };

        update(metrics);
    });
}

///
/// RowPresenceProbeSource
///
/// Internal source tag for authoritative row-presence probes.
/// This keeps metrics able to distinguish already-borrowed data-store probes
/// from probes that still re-enter through one store handle boundary.
///

enum RowPresenceProbeSource {
    BorrowedDataStore,
    StoreHandle,
}

///
/// FusedSecondaryCoveringAuthority
///
/// FusedSecondaryCoveringAuthority
///
/// Executor-owned borrowed data-store authority for stale-fallback secondary
/// covering reads. It keeps row-visibility policy in the executor while
/// collapsing candidate admission, authoritative existence probing, and
/// consistency handling into one explicit boundary.
///

#[cfg(test)]
#[derive(Clone, Copy)]
pub(in crate::db::executor) struct FusedSecondaryCoveringAuthority<'a> {
    data: &'a DataStore,
    entity_tag: EntityTag,
    consistency: MissingRowPolicy,
}

#[cfg(test)]
impl<'a> FusedSecondaryCoveringAuthority<'a> {
    /// Construct one fused stale-row authority over one borrowed data-store
    /// boundary and one fixed entity identity.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        data: &'a DataStore,
        entity_tag: EntityTag,
        consistency: MissingRowPolicy,
    ) -> Self {
        Self {
            data,
            entity_tag,
            consistency,
        }
    }

    /// Admit or reject one secondary covering candidate under the existing
    /// fail-closed stale-row contract.
    pub(in crate::db::executor) fn admits_storage_key(
        self,
        storage_key: StorageKey,
    ) -> Result<bool, InternalError> {
        // Phase 1: account for the candidate and encode one authoritative
        // row-store key directly from the entity tag plus storage key.
        record_row_check_covering_candidate_seen();
        record_row_presence_probe_source(RowPresenceProbeSource::BorrowedDataStore);
        record_row_presence_key_to_raw_encode();
        let raw_key = DataKey::raw_from_parts(self.entity_tag, storage_key)?;

        // Phase 2: probe the borrowed data-store authority and preserve the
        // current missing-row policy exactly.
        let row_exists = self.data.contains(&raw_key);
        record_row_presence_probe_result(row_exists);

        match self.consistency {
            MissingRowPolicy::Error => {
                if row_exists {
                    Ok(true)
                } else {
                    Err(
                        ExecutorError::missing_row(&DataKey::new(self.entity_tag, storage_key))
                            .into(),
                    )
                }
            }
            MissingRowPolicy::Ignore => Ok(row_exists),
        }
    }
}

#[cfg(any(test, feature = "structural-read-metrics"))]
pub(in crate::db) fn record_row_check_index_entry_scanned() {
    update_row_check_metrics(|metrics| {
        metrics.index_entries_scanned = metrics.index_entries_scanned.saturating_add(1);
    });
}

#[cfg(not(any(test, feature = "structural-read-metrics")))]
pub(in crate::db) const fn record_row_check_index_entry_scanned() {}

#[cfg(any(test, feature = "structural-read-metrics"))]
pub(in crate::db) fn record_row_check_index_membership_single_key_entry() {
    update_row_check_metrics(|metrics| {
        metrics.index_membership_single_key_entries = metrics
            .index_membership_single_key_entries
            .saturating_add(1);
    });
}

#[cfg(not(any(test, feature = "structural-read-metrics")))]
pub(in crate::db) const fn record_row_check_index_membership_single_key_entry() {}

#[cfg(any(test, feature = "structural-read-metrics"))]
pub(in crate::db) fn record_row_check_index_membership_multi_key_entry() {
    update_row_check_metrics(|metrics| {
        metrics.index_membership_multi_key_entries =
            metrics.index_membership_multi_key_entries.saturating_add(1);
    });
}

#[cfg(not(any(test, feature = "structural-read-metrics")))]
pub(in crate::db) const fn record_row_check_index_membership_multi_key_entry() {}

#[cfg(any(test, feature = "structural-read-metrics"))]
pub(in crate::db) fn record_row_check_index_membership_key_decoded() {
    update_row_check_metrics(|metrics| {
        metrics.index_membership_keys_decoded =
            metrics.index_membership_keys_decoded.saturating_add(1);
    });
}

#[cfg(not(any(test, feature = "structural-read-metrics")))]
pub(in crate::db) const fn record_row_check_index_membership_key_decoded() {}

#[cfg(any(test, feature = "structural-read-metrics"))]
pub(in crate::db) fn record_row_check_covering_candidate_seen() {
    update_row_check_metrics(|metrics| {
        metrics.row_check_covering_candidates_seen =
            metrics.row_check_covering_candidates_seen.saturating_add(1);
    });
}

#[cfg(not(any(test, feature = "structural-read-metrics")))]
pub(in crate::db) const fn record_row_check_covering_candidate_seen() {}

#[cfg(any(test, feature = "structural-read-metrics"))]
pub(in crate::db) fn record_row_check_row_emitted() {
    update_row_check_metrics(|metrics| {
        metrics.row_check_rows_emitted = metrics.row_check_rows_emitted.saturating_add(1);
    });
}

#[cfg(not(any(test, feature = "structural-read-metrics")))]
pub(in crate::db) const fn record_row_check_row_emitted() {}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_row_presence_probe_source(source: RowPresenceProbeSource) {
    update_row_check_metrics(|metrics| match source {
        RowPresenceProbeSource::BorrowedDataStore => {
            metrics.row_presence_probe_borrowed_data_store_count = metrics
                .row_presence_probe_borrowed_data_store_count
                .saturating_add(1);
        }
        RowPresenceProbeSource::StoreHandle => {
            metrics.row_presence_probe_store_handle_count = metrics
                .row_presence_probe_store_handle_count
                .saturating_add(1);
        }
    });
}

#[cfg(not(any(test, feature = "structural-read-metrics")))]
const fn record_row_presence_probe_source(_source: RowPresenceProbeSource) {}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_row_presence_key_to_raw_encode() {
    update_row_check_metrics(|metrics| {
        metrics.row_presence_key_to_raw_encodes =
            metrics.row_presence_key_to_raw_encodes.saturating_add(1);
    });
}

#[cfg(not(any(test, feature = "structural-read-metrics")))]
const fn record_row_presence_key_to_raw_encode() {}

#[cfg(any(test, feature = "structural-read-metrics"))]
fn record_row_presence_probe_result(row_exists: bool) {
    update_row_check_metrics(|metrics| {
        metrics.row_presence_probe_count = metrics.row_presence_probe_count.saturating_add(1);
        if row_exists {
            metrics.row_presence_probe_hits = metrics.row_presence_probe_hits.saturating_add(1);
        } else {
            metrics.row_presence_probe_misses = metrics.row_presence_probe_misses.saturating_add(1);
        }
    });
}

#[cfg(not(any(test, feature = "structural-read-metrics")))]
const fn record_row_presence_probe_result(_row_exists: bool) {}

///
/// with_row_check_metrics
///
/// Run one closure while collecting executor-owned `row_check_required`
/// metrics on the current thread, then return the closure result plus the
/// aggregated snapshot.
///

#[cfg(any(test, feature = "structural-read-metrics"))]
#[cfg_attr(
    all(test, not(feature = "structural-read-metrics")),
    allow(unreachable_pub)
)]
pub fn with_row_check_metrics<T>(f: impl FnOnce() -> T) -> (T, RowCheckMetrics) {
    ROW_CHECK_METRICS.with(|metrics| {
        debug_assert!(
            metrics.borrow().is_none(),
            "row_check metrics captures should not nest"
        );
        *metrics.borrow_mut() = Some(RowCheckMetrics::default());
    });

    let result = f();
    let metrics = ROW_CHECK_METRICS.with(|metrics| metrics.borrow_mut().take().unwrap_or_default());

    (result, metrics)
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
            None => Err(ExecutorError::missing_row(key).into()),
        },
        MissingRowPolicy::Ignore => read_row(key),
    }
}

// Read only row presence under one consistency contract from structural store
// authority. Tests keep this generic selector so they can verify the metrics
// split between store-handle and borrowed-data-store probing.
#[cfg(test)]
pub(in crate::db::executor) fn read_row_presence_with_consistency_from_store(
    store: StoreHandle,
    key: &DataKey,
    consistency: MissingRowPolicy,
) -> Result<bool, InternalError> {
    store.with_data(|data| {
        read_row_presence_with_consistency(
            data,
            key,
            consistency,
            RowPresenceProbeSource::StoreHandle,
        )
    })
}

// Read only row presence from structural store authority while treating
// missing rows as a normal filtered-out outcome.
pub(in crate::db::executor) fn read_row_presence_ignoring_missing_from_store(
    store: StoreHandle,
    key: &DataKey,
) -> Result<bool, InternalError> {
    store.with_data(|data| {
        read_row_presence_ignoring_missing(data, key, RowPresenceProbeSource::StoreHandle)
    })
}

// Read only row presence from structural store authority while preserving the
// fail-closed missing-row contract.
pub(in crate::db::executor) fn read_row_presence_requiring_existing_from_store(
    store: StoreHandle,
    key: &DataKey,
) -> Result<bool, InternalError> {
    store.with_data(|data| {
        read_row_presence_requiring_existing(data, key, RowPresenceProbeSource::StoreHandle)
    })
}

// Read only row presence under one consistency contract from one already
// borrowed data-store boundary. Covering-read decode paths use this helper to
// batch stale-row filtering under one store borrow instead of re-entering the
// registry per decoded secondary key.
pub(in crate::db::executor) fn read_row_presence_with_consistency_from_data_store(
    data: &DataStore,
    key: &DataKey,
    consistency: MissingRowPolicy,
) -> Result<bool, InternalError> {
    read_row_presence_with_consistency(
        data,
        key,
        consistency,
        RowPresenceProbeSource::BorrowedDataStore,
    )
}

fn read_row_presence_with_consistency(
    data: &DataStore,
    key: &DataKey,
    consistency: MissingRowPolicy,
    source: RowPresenceProbeSource,
) -> Result<bool, InternalError> {
    let row_exists = probe_row_presence(data, key, source)?;

    match consistency {
        MissingRowPolicy::Error => {
            if row_exists {
                Ok(true)
            } else {
                Err(ExecutorError::missing_row(key).into())
            }
        }
        MissingRowPolicy::Ignore => Ok(row_exists),
    }
}

fn read_row_presence_ignoring_missing(
    data: &DataStore,
    key: &DataKey,
    source: RowPresenceProbeSource,
) -> Result<bool, InternalError> {
    probe_row_presence(data, key, source)
}

fn read_row_presence_requiring_existing(
    data: &DataStore,
    key: &DataKey,
    source: RowPresenceProbeSource,
) -> Result<bool, InternalError> {
    let row_exists = probe_row_presence(data, key, source)?;

    if row_exists {
        Ok(true)
    } else {
        Err(ExecutorError::missing_row(key).into())
    }
}

fn probe_row_presence(
    data: &DataStore,
    key: &DataKey,
    source: RowPresenceProbeSource,
) -> Result<bool, InternalError> {
    record_row_presence_probe_source(source);
    record_row_presence_key_to_raw_encode();
    let raw = key.to_raw()?;
    let row_exists = data.contains(&raw);
    record_row_presence_probe_result(row_exists);

    Ok(row_exists)
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
pub(in crate::db::executor) fn sum_row_payload_bytes_from_ordered_key_stream_with_store<S>(
    store: StoreHandle,
    key_stream: &mut S,
    consistency: MissingRowPolicy,
    offset: usize,
    limit: Option<usize>,
) -> Result<u64, InternalError>
where
    S: OrderedKeyStream + ?Sized,
{
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
fn sum_row_payload_bytes_from_ordered_key_stream_shared<S, F>(
    key_stream: &mut S,
    read_row: &mut F,
    offset: usize,
    limit: Option<usize>,
) -> Result<u64, InternalError>
where
    S: OrderedKeyStream + ?Sized,
    F: FnMut(&DataKey) -> Result<Option<RawRow>, InternalError>,
{
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        FusedSecondaryCoveringAuthority, read_row_presence_with_consistency_from_data_store,
        read_row_presence_with_consistency_from_store, with_row_check_metrics,
    };
    use crate::{
        db::{
            data::{DataKey, DataStore, RawRow},
            index::IndexStore,
            predicate::MissingRowPolicy,
            registry::StoreHandle,
        },
        testing::test_memory,
        types::EntityTag,
        value::StorageKey,
    };
    use std::cell::RefCell;

    thread_local! {
        static TEST_RUNTIME_CONTEXT_DATA_STORE: RefCell<DataStore> =
            RefCell::new(DataStore::init(test_memory(171)));
        static TEST_RUNTIME_CONTEXT_INDEX_STORE: RefCell<IndexStore> =
            RefCell::new(IndexStore::init(test_memory(172)));
    }

    fn test_key() -> DataKey {
        DataKey::new(EntityTag::new(17), StorageKey::Uint(41))
    }

    fn reset_test_store() {
        let raw_key = test_key().to_raw().expect("test key should encode");
        let raw_row = RawRow::try_new(vec![0xAA]).expect("test raw row should encode");

        TEST_RUNTIME_CONTEXT_DATA_STORE.with_borrow_mut(|store| {
            store.clear();
            let _ = store.insert_raw_for_test(raw_key, raw_row);
        });
    }

    fn test_store_handle() -> StoreHandle {
        StoreHandle::new(
            &TEST_RUNTIME_CONTEXT_DATA_STORE,
            &TEST_RUNTIME_CONTEXT_INDEX_STORE,
        )
    }

    #[test]
    fn row_check_metrics_distinguish_borrowed_data_store_probes() {
        reset_test_store();
        let key = test_key();

        let (row_exists, metrics) = with_row_check_metrics(|| {
            TEST_RUNTIME_CONTEXT_DATA_STORE.with_borrow(|store| {
                read_row_presence_with_consistency_from_data_store(
                    store,
                    &key,
                    MissingRowPolicy::Error,
                )
                .expect("borrowed row-presence probe should succeed")
            })
        });

        assert!(row_exists, "borrowed probe should find the inserted row");
        assert_eq!(metrics.row_presence_probe_count, 1);
        assert_eq!(metrics.row_presence_probe_hits, 1);
        assert_eq!(metrics.row_presence_probe_misses, 0);
        assert_eq!(metrics.row_presence_probe_borrowed_data_store_count, 1);
        assert_eq!(metrics.row_presence_probe_store_handle_count, 0);
        assert_eq!(metrics.row_presence_key_to_raw_encodes, 1);
    }

    #[test]
    fn row_check_metrics_distinguish_store_handle_probes() {
        reset_test_store();
        let key = test_key();

        let (row_exists, metrics) = with_row_check_metrics(|| {
            read_row_presence_with_consistency_from_store(
                test_store_handle(),
                &key,
                MissingRowPolicy::Error,
            )
            .expect("store-handle row-presence probe should succeed")
        });

        assert!(
            row_exists,
            "store-handle probe should find the inserted row"
        );
        assert_eq!(metrics.row_presence_probe_count, 1);
        assert_eq!(metrics.row_presence_probe_hits, 1);
        assert_eq!(metrics.row_presence_probe_misses, 0);
        assert_eq!(metrics.row_presence_probe_borrowed_data_store_count, 0);
        assert_eq!(metrics.row_presence_probe_store_handle_count, 1);
        assert_eq!(metrics.row_presence_key_to_raw_encodes, 1);
    }

    #[test]
    fn fused_secondary_covering_authority_tracks_candidate_and_probe_metrics() {
        reset_test_store();

        let (row_exists, metrics) = with_row_check_metrics(|| {
            TEST_RUNTIME_CONTEXT_DATA_STORE.with_borrow(|store| {
                FusedSecondaryCoveringAuthority::new(
                    store,
                    EntityTag::new(17),
                    MissingRowPolicy::Error,
                )
                .admits_storage_key(StorageKey::Uint(41))
                .expect("fused secondary covering probe should succeed")
            })
        });

        assert!(
            row_exists,
            "fused secondary covering probe should find the inserted row"
        );
        assert_eq!(metrics.row_check_covering_candidates_seen, 1);
        assert_eq!(metrics.row_presence_probe_count, 1);
        assert_eq!(metrics.row_presence_probe_hits, 1);
        assert_eq!(metrics.row_presence_probe_misses, 0);
        assert_eq!(metrics.row_presence_probe_borrowed_data_store_count, 1);
        assert_eq!(metrics.row_presence_probe_store_handle_count, 0);
        assert_eq!(metrics.row_presence_key_to_raw_encodes, 1);
    }
}
