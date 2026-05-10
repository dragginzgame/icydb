//! Module: executor::stream::access::scan
//! Responsibility: low-level primary/index scan adapters over store/index handles.
//! Does not own: access routing decisions or planner spec construction.
//! Boundary: direct scan primitives used by access-stream resolver.

use crate::{
    db::{
        cursor::{
            ContinuationKeyRef, ContinuationRuntime, IndexScanContinuationInput, LoopAction,
            WindowCursorContract,
        },
        data::{DataKey, RawDataKey},
        direction::Direction,
        executor::{
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredIndexScanContract, LoweredKey,
            record_row_check_index_entry_scanned, record_row_check_index_membership_key_decoded,
            record_row_check_index_membership_multi_key_entry,
            record_row_check_index_membership_single_key_entry,
        },
        index::{
            IndexEntryExistenceWitness, IndexEntryMembership, IndexKey, RawIndexEntry, RawIndexKey,
            predicate::{IndexPredicateExecution, eval_index_execution_on_decoded_key},
        },
        registry::StoreHandle,
    },
    error::InternalError,
    types::EntityTag,
};
use std::{ops::Bound, sync::Arc};

type IndexComponentValues = Arc<[Vec<u8>]>;

pub(in crate::db::executor) type IndexComponentRows =
    Vec<(DataKey, IndexEntryExistenceWitness, IndexComponentValues)>;

///
/// PrimaryScan
///
/// Executor-owned adapter for primary data-store iteration.
/// The physical stream resolver must request scans through this boundary instead of
/// traversing store handles directly.
///

pub(in crate::db::executor) struct PrimaryScan;

impl PrimaryScan {
    // Decode one raw data key through the canonical corruption mapping.
    pub(in crate::db::executor) fn decode_data_key(
        raw: &RawDataKey,
    ) -> Result<DataKey, InternalError> {
        DataKey::try_from_raw(raw).map_err(|err| {
            InternalError::identity_corruption(format!("failed to decode data key: {err}"))
        })
    }
}

///
/// IndexScan
///
/// Executor-owned adapter for secondary-index iteration.
/// The physical stream resolver must request index traversal via this adapter so routing
/// stays decoupled from store-registry/index-handle internals.
///

pub(in crate::db::executor) struct IndexScan;

///
/// IndexDataKeyScanChunk
///
/// Executor-owned result of one bounded raw-index chunk.
/// It carries decoded data keys plus the last raw index key visited so callers
/// can resume later chunks without holding an index-store iterator borrow.
///

pub(in crate::db::executor) struct IndexDataKeyScanChunk {
    keys: Vec<DataKey>,
    last_raw_key: Option<RawIndexKey>,
}

impl IndexDataKeyScanChunk {
    /// Construct one chunk from decoded keys and the last scanned raw index key.
    #[must_use]
    const fn new(keys: Vec<DataKey>, last_raw_key: Option<RawIndexKey>) -> Self {
        Self { keys, last_raw_key }
    }

    /// Consume this chunk into decoded keys and resume anchor.
    #[must_use]
    pub(in crate::db::executor) fn into_parts(self) -> (Vec<DataKey>, Option<RawIndexKey>) {
        (self.keys, self.last_raw_key)
    }
}

impl IndexScan {
    // Keep bounded scan preallocation modest so common page-limited reads avoid
    // the first growth step without reserving pathologically large vectors from
    // caller-supplied limits.
    const LIMITED_SCAN_PREALLOC_CAP: usize = 32;

    /// Resolve one lowered index-prefix envelope through structural store authority.
    pub(in crate::db::executor) fn prefix_structural(
        store: StoreHandle,
        entity_tag: EntityTag,
        spec: &LoweredIndexPrefixSpec,
        direction: Direction,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        Self::resolve_data_values_in_raw_range_limited(
            store,
            entity_tag,
            spec.scan_contract(),
            spec.lower(),
            spec.upper(),
            IndexScanContinuationInput::new(None, direction),
            limit,
            predicate_execution,
        )
    }

    /// Resolve one bounded component stream through structural store authority.
    #[expect(clippy::too_many_arguments)]
    pub(in crate::db::executor) fn components_structural(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: LoweredIndexScanContract,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        component_indices: &[usize],
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<IndexComponentRows, InternalError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let continuation =
            ContinuationRuntime::new(continuation, WindowCursorContract::unbounded());
        let bounds = continuation.scan_bounds((lower, upper))?;
        let mut out = Vec::with_capacity(limit.min(Self::LIMITED_SCAN_PREALLOC_CAP));

        store.with_index(|index_store| {
            index_store.visit_raw_entries_in_range(
                (&bounds.0, &bounds.1),
                continuation.direction(),
                |raw_key, value| {
                    match Self::accept_scan_key(&continuation, raw_key)? {
                        LoopAction::Skip => return Ok(false),
                        LoopAction::Emit => {}
                        LoopAction::Stop => return Ok(true),
                    }

                    Self::decode_index_entry_and_push_with_components(
                        entity_tag,
                        &index,
                        raw_key,
                        value,
                        &mut out,
                        Some(limit),
                        component_indices,
                        "range resolve",
                        predicate_execution,
                    )
                },
            )
        })?;

        Ok(out)
    }

    /// Resolve one lowered index-range envelope through structural store authority.
    pub(in crate::db::executor) fn range_structural(
        store: StoreHandle,
        entity_tag: EntityTag,
        spec: &LoweredIndexRangeSpec,
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        Self::resolve_data_values_in_raw_range_limited(
            store,
            entity_tag,
            spec.scan_contract(),
            spec.lower(),
            spec.upper(),
            continuation,
            limit,
            predicate_execution,
        )
    }

    /// Resolve one bounded lowered-index chunk through structural store authority.
    #[expect(clippy::too_many_arguments)]
    pub(in crate::db::executor) fn chunk_structural(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: LoweredIndexScanContract,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        max_entries: usize,
        output_limit: Option<usize>,
    ) -> Result<IndexDataKeyScanChunk, InternalError> {
        Self::resolve_chunk(
            store,
            entity_tag,
            index,
            lower,
            upper,
            continuation,
            max_entries,
            output_limit,
        )
    }

    // Resolve one index range via store registry and index-store iterator boundary.
    #[expect(clippy::too_many_arguments)]
    fn resolve_data_values_in_raw_range_limited(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: LoweredIndexScanContract,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let continuation =
            ContinuationRuntime::new(continuation, WindowCursorContract::unbounded());
        let bounds = continuation.scan_bounds((lower, upper))?;
        let mut keys = Vec::with_capacity(limit.min(Self::LIMITED_SCAN_PREALLOC_CAP));

        store.with_index(|index_store| {
            index_store.visit_raw_entries_in_range(
                (&bounds.0, &bounds.1),
                continuation.direction(),
                |raw_key, value| {
                    match Self::accept_scan_key(&continuation, raw_key)? {
                        LoopAction::Skip => return Ok(false),
                        LoopAction::Emit => {}
                        LoopAction::Stop => return Ok(true),
                    }

                    Self::decode_index_entry_and_push(
                        entity_tag,
                        &index,
                        raw_key,
                        value,
                        &mut keys,
                        Some(limit),
                        "range resolve",
                        predicate_execution,
                    )
                },
            )
        })?;

        Ok(keys)
    }

    // Resolve one index range chunk via store registry and index-store iterator boundary.
    #[expect(clippy::too_many_arguments)]
    fn resolve_chunk(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: LoweredIndexScanContract,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        max_entries: usize,
        output_limit: Option<usize>,
    ) -> Result<IndexDataKeyScanChunk, InternalError> {
        if max_entries == 0 || matches!(output_limit, Some(0)) {
            return Ok(IndexDataKeyScanChunk::new(Vec::new(), None));
        }

        let continuation =
            ContinuationRuntime::new(continuation, WindowCursorContract::unbounded());
        let bounds = continuation.scan_bounds((lower, upper))?;
        let mut keys = Vec::with_capacity(max_entries.min(Self::LIMITED_SCAN_PREALLOC_CAP));
        let mut last_raw_key = None;
        let mut scanned_entries = 0usize;

        store.with_index(|index_store| {
            index_store.visit_raw_entries_in_range(
                (&bounds.0, &bounds.1),
                continuation.direction(),
                |raw_key, value| {
                    match Self::accept_scan_key(&continuation, raw_key)? {
                        LoopAction::Skip => return Ok(false),
                        LoopAction::Emit => {}
                        LoopAction::Stop => return Ok(true),
                    }
                    last_raw_key = Some(raw_key.clone());
                    scanned_entries = scanned_entries.saturating_add(1);

                    if Self::decode_index_entry_and_push(
                        entity_tag,
                        &index,
                        raw_key,
                        value,
                        &mut keys,
                        output_limit,
                        "range stream",
                        None,
                    )? {
                        return Ok(true);
                    }

                    Ok(scanned_entries == max_entries)
                },
            )
        })?;

        let chunk = IndexDataKeyScanChunk::new(keys, last_raw_key);

        Ok(chunk)
    }

    // Apply executor-owned continuation advancement checks for one raw index key.
    fn accept_scan_key(
        continuation: &ContinuationRuntime<'_>,
        raw_key: &RawIndexKey,
    ) -> Result<LoopAction, InternalError> {
        continuation.accept_key(ContinuationKeyRef::scan(raw_key))
    }

    #[expect(clippy::too_many_arguments)]
    fn decode_index_entry_and_push(
        entity: EntityTag,
        index: &LoweredIndexScanContract,
        raw_key: &RawIndexKey,
        value: &RawIndexEntry,
        out: &mut Vec<DataKey>,
        limit: Option<usize>,
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        record_row_check_index_entry_scanned();

        // Phase 1: only decode raw key components when an index-only predicate
        // needs them. Plain membership scans only need the entry payload.
        if let Some(execution) = index_predicate_execution {
            let decoded_key = IndexKey::try_from_raw(raw_key)
                .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
            if !eval_index_execution_on_decoded_key(&decoded_key, execution)? {
                return Ok(false);
            }
        }

        // Phase 2: fast-path one-key entries without allocating the full
        // membership vector.
        if let Some(membership) = value
            .decode_single_membership()
            .map_err(InternalError::index_entry_decode_failed)?
        {
            record_row_check_index_membership_single_key_entry();
            record_row_check_index_membership_key_decoded();
            out.push(Self::data_key_from_membership(entity, &membership));

            if let Some(limit) = limit
                && out.len() == limit
            {
                return Ok(true);
            }

            return Ok(false);
        }

        // Phase 3: stream multi-key entry payloads without first allocating a
        // membership vector, but still validate the full entry before returning.
        let mut halted = false;
        let mut decoded_keys = 0usize;
        record_row_check_index_membership_multi_key_entry();
        let mut storage_keys = value
            .iter_memberships()
            .map_err(InternalError::index_entry_decode_failed)?;

        for storage_key in &mut storage_keys {
            let membership = storage_key.map_err(InternalError::index_entry_decode_failed)?;
            decoded_keys = decoded_keys.saturating_add(1);
            record_row_check_index_membership_key_decoded();

            if halted {
                continue;
            }

            out.push(Self::data_key_from_membership(entity, &membership));

            if let Some(limit) = limit
                && out.len() == limit
            {
                halted = true;
            }
        }

        if index.unique() && decoded_keys != 1 {
            return Err(InternalError::unique_index_entry_single_key_required());
        }

        Ok(halted)
    }

    #[expect(clippy::too_many_arguments)]
    fn decode_index_entry_and_push_with_components(
        entity: EntityTag,
        index: &LoweredIndexScanContract,
        raw_key: &RawIndexKey,
        value: &RawIndexEntry,
        out: &mut IndexComponentRows,
        limit: Option<usize>,
        component_indices: &[usize],
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        record_row_check_index_entry_scanned();

        // Phase 1: decode the raw key once, extract requested components, and
        // evaluate any optional index-only predicate against that decoded view.
        let decoded_key = IndexKey::try_from_raw(raw_key)
            .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
        let mut components = Vec::with_capacity(component_indices.len());
        for component_index in component_indices {
            let Some(component) = decoded_key.component(*component_index) else {
                return Err(InternalError::index_projection_component_required(
                    index.name(),
                    *component_index,
                ));
            };
            components.push(component.to_vec());
        }
        let components: Arc<[Vec<u8>]> = Arc::from(components);

        if let Some(execution) = index_predicate_execution
            && !eval_index_execution_on_decoded_key(&decoded_key, execution)?
        {
            return Ok(false);
        }

        // Phase 2: fast-path one-key entries without allocating the full
        // membership vector.
        if let Some(membership) = value
            .decode_single_membership()
            .map_err(InternalError::index_entry_decode_failed)?
        {
            record_row_check_index_membership_single_key_entry();
            record_row_check_index_membership_key_decoded();
            out.push((
                Self::data_key_from_membership(entity, &membership),
                membership.existence_witness(),
                components,
            ));

            if let Some(limit) = limit
                && out.len() == limit
            {
                return Ok(true);
            }

            return Ok(false);
        }

        // Phase 3: stream multi-key entry payloads without first allocating a
        // membership vector, but still validate the full entry before returning.
        let mut halted = false;
        let mut decoded_keys = 0usize;
        record_row_check_index_membership_multi_key_entry();
        let mut memberships = value
            .iter_memberships()
            .map_err(InternalError::index_entry_decode_failed)?;

        for membership in &mut memberships {
            let membership = membership.map_err(InternalError::index_entry_decode_failed)?;
            decoded_keys = decoded_keys.saturating_add(1);
            record_row_check_index_membership_key_decoded();

            if halted {
                continue;
            }

            out.push((
                Self::data_key_from_membership(entity, &membership),
                membership.existence_witness(),
                Arc::clone(&components),
            ));

            if let Some(limit) = limit
                && out.len() == limit
            {
                halted = true;
            }
        }

        if index.unique() && decoded_keys != 1 {
            return Err(InternalError::unique_index_entry_single_key_required());
        }

        Ok(halted)
    }

    // Rebuild one data key from the raw membership payload without re-encoding
    // the primary key through the value layer.
    fn data_key_from_membership(entity: EntityTag, membership: &IndexEntryMembership) -> DataKey {
        DataKey::new_with_raw(
            entity,
            membership.storage_key(),
            RawDataKey::from_entity_and_stored_storage_key_bytes(
                entity,
                &membership.raw_storage_key_bytes(),
            ),
        )
    }
}
