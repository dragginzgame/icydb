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
        data::{DataStore, DecodedDataStoreKey, RawDataStoreKey},
        direction::Direction,
        executor::{
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredIndexScanContract, LoweredKey,
            lowered_index_prefix_is_proven_empty_at_generation,
            record_row_check_index_entry_scanned, record_row_check_index_key_owned_entry,
            record_row_check_index_row_identity_decoded,
        },
        index::{
            IndexEntryExistenceWitness, IndexEntryRowWitness, IndexEntryValue, IndexKey,
            RawIndexStoreKey,
            predicate::{
                IndexPredicateExecution, eval_index_execution_on_decoded_key,
                eval_index_program_on_prefix_components,
            },
        },
        registry::StoreHandle,
    },
    error::InternalError,
    types::EntityTag,
};
use std::{borrow::Cow, ops::Bound, sync::Arc};

type IndexComponentValues = Arc<[Vec<u8>]>;

pub(in crate::db::executor) type IndexComponentRows = Vec<(
    DecodedDataStoreKey,
    IndexEntryExistenceWitness,
    IndexComponentValues,
)>;

pub(in crate::db::executor) const ACCESS_SCAN_CHUNK_ENTRIES: usize = 64;
const PREFIX_STREAM_SMALL_CHUNK_ENTRIES: usize = 2;
const PREFIX_STREAM_MAX_CHUNK_ENTRIES: usize = 64;

const fn prefix_stream_chunk_entries(fetch_hint: Option<usize>, prefix_count: usize) -> usize {
    let Some(fetch_hint) = fetch_hint else {
        return ACCESS_SCAN_CHUNK_ENTRIES;
    };
    if fetch_hint <= PREFIX_STREAM_SMALL_CHUNK_ENTRIES.saturating_mul(2) {
        return PREFIX_STREAM_SMALL_CHUNK_ENTRIES;
    }

    let prefix_count = if prefix_count == 0 { 1 } else { prefix_count };
    let fair_prefix_window = fetch_hint.div_ceil(prefix_count);
    if fair_prefix_window < PREFIX_STREAM_SMALL_CHUNK_ENTRIES {
        PREFIX_STREAM_SMALL_CHUNK_ENTRIES
    } else if fair_prefix_window > PREFIX_STREAM_MAX_CHUNK_ENTRIES {
        PREFIX_STREAM_MAX_CHUNK_ENTRIES
    } else {
        fair_prefix_window
    }
}

pub(in crate::db::executor) const fn branch_stream_chunk_entries(
    index_fetch_hint: Option<usize>,
    active_branch_count: usize,
) -> usize {
    prefix_stream_chunk_entries(index_fetch_hint, active_branch_count)
}

pub(in crate::db::executor) const fn index_stream_chunk_entries_for_remaining(
    chunk_entries: usize,
    remaining: Option<usize>,
) -> usize {
    let chunk_entries = if chunk_entries == 0 {
        ACCESS_SCAN_CHUNK_ENTRIES
    } else {
        chunk_entries
    };
    match remaining {
        Some(remaining) if remaining < chunk_entries => remaining,
        Some(_) | None => chunk_entries,
    }
}

pub(in crate::db::executor) const fn index_stream_output_limit_for_chunk(
    remaining: Option<usize>,
    chunk_entries: usize,
) -> Option<usize> {
    match remaining {
        Some(remaining) if remaining < chunk_entries => Some(remaining),
        Some(_) => Some(chunk_entries),
        None => None,
    }
}

pub(in crate::db::executor) fn apply_index_scan_chunk_progress(
    anchor: &mut Option<RawIndexStoreKey>,
    remaining: &mut Option<usize>,
    exhausted: &mut bool,
    emitted: usize,
    last_raw_key: Option<RawIndexStoreKey>,
) {
    if let Some(raw_key) = last_raw_key {
        *anchor = Some(raw_key);
    } else {
        *exhausted = true;
    }

    if let Some(remaining) = remaining.as_mut() {
        *remaining = remaining.saturating_sub(emitted);
        if *remaining == 0 {
            *exhausted = true;
        }
    }
}

pub(in crate::db::executor) fn index_predicate_rejects_prefix_components(
    prefix_components: &[Vec<u8>],
    predicate_execution: Option<IndexPredicateExecution<'_>>,
) -> bool {
    predicate_execution
        .and_then(|execution| {
            eval_index_program_on_prefix_components(prefix_components, execution.program)
        })
        .is_some_and(|passed| !passed)
}

pub(in crate::db::executor) fn active_lowered_index_prefix_specs<'a>(
    empty_proof_store: Option<StoreHandle>,
    index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    predicate_execution: Option<IndexPredicateExecution<'_>>,
) -> Vec<&'a LoweredIndexPrefixSpec> {
    let mut active_specs = Vec::with_capacity(index_prefix_specs.len());

    if let Some(store) = empty_proof_store {
        let data_generation = store.with_data(DataStore::generation);
        store.with_index(|index_store| {
            for spec in index_prefix_specs {
                if lowered_index_prefix_is_proven_empty_at_generation(
                    index_store,
                    data_generation,
                    spec,
                ) {
                    continue;
                }
                if index_predicate_rejects_prefix_components(
                    spec.prefix_components(),
                    predicate_execution,
                ) {
                    continue;
                }

                active_specs.push(spec);
            }
        });
    } else {
        for spec in index_prefix_specs {
            if index_predicate_rejects_prefix_components(
                spec.prefix_components(),
                predicate_execution,
            ) {
                continue;
            }

            active_specs.push(spec);
        }
    }

    active_specs
}

///
/// PrimaryScan
///
/// Executor-owned adapter for primary data-store iteration.
/// The physical stream resolver must request scans through this boundary instead of
/// traversing store handles directly.
///

pub(in crate::db::executor) struct PrimaryScan;

impl PrimaryScan {
    // Decode one raw data-store key through the canonical corruption mapping.
    pub(in crate::db::executor) fn decode_data_key(
        raw: &RawDataStoreKey,
    ) -> Result<DecodedDataStoreKey, InternalError> {
        DecodedDataStoreKey::try_from_raw(raw).map_err(|_err| InternalError::identity_corruption())
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
/// IndexDecodedKeyScanChunk
///
/// Executor-owned result of one bounded raw-index chunk.
/// It carries decoded data-store keys plus the last raw index key visited so
/// callers can resume later chunks without holding an index-store iterator
/// borrow.
///

pub(in crate::db::executor) struct IndexDecodedKeyScanChunk {
    keys: Vec<DecodedDataStoreKey>,
    last_raw_key: Option<RawIndexStoreKey>,
}

impl IndexDecodedKeyScanChunk {
    /// Construct one chunk from decoded keys and the last scanned raw index key.
    #[must_use]
    const fn new(keys: Vec<DecodedDataStoreKey>, last_raw_key: Option<RawIndexStoreKey>) -> Self {
        Self { keys, last_raw_key }
    }

    /// Consume this chunk into decoded keys and resume anchor.
    #[must_use]
    pub(in crate::db::executor) fn into_decoded_keys_and_resume_anchor(
        self,
    ) -> (Vec<DecodedDataStoreKey>, Option<RawIndexStoreKey>) {
        (self.keys, self.last_raw_key)
    }
}

///
/// IndexComponentScanChunk
///
/// Executor-owned result of one bounded raw-index component chunk.
/// It carries decoded covering component rows plus the last raw index key
/// visited so callers can resume without keeping an index-store iterator
/// borrow live across pulls.
///

pub(in crate::db::executor) struct IndexComponentScanChunk {
    rows: IndexComponentRows,
    last_raw_key: Option<RawIndexStoreKey>,
}

impl IndexComponentScanChunk {
    /// Construct one chunk from decoded rows and the last scanned raw index key.
    #[must_use]
    const fn new(rows: IndexComponentRows, last_raw_key: Option<RawIndexStoreKey>) -> Self {
        Self { rows, last_raw_key }
    }

    /// Consume this chunk into decoded component rows and resume anchor.
    #[must_use]
    pub(in crate::db::executor) fn into_component_rows_and_resume_anchor(
        self,
    ) -> (IndexComponentRows, Option<RawIndexStoreKey>) {
        (self.rows, self.last_raw_key)
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
    ) -> Result<Vec<DecodedDataStoreKey>, InternalError> {
        Self::resolve_data_values_in_raw_range_limited(
            store,
            entity_tag,
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
    ) -> Result<Vec<DecodedDataStoreKey>, InternalError> {
        if index_predicate_rejects_prefix_components(spec.prefix_components(), predicate_execution)
        {
            return Ok(Vec::new());
        }

        Self::resolve_data_values_in_raw_range_limited(
            store,
            entity_tag,
            spec.lower(),
            spec.upper(),
            continuation,
            limit,
            predicate_execution,
        )
    }

    /// Resolve one bounded lowered-index chunk through structural store authority.
    pub(in crate::db::executor) fn chunk_structural(
        store: StoreHandle,
        entity_tag: EntityTag,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        max_entries: usize,
        output_limit: Option<usize>,
    ) -> Result<IndexDecodedKeyScanChunk, InternalError> {
        Self::resolve_chunk(
            store,
            entity_tag,
            lower,
            upper,
            continuation,
            max_entries,
            output_limit,
        )
    }

    /// Resolve one bounded lowered-index component chunk through structural store authority.
    #[expect(clippy::too_many_arguments)]
    pub(in crate::db::executor) fn components_chunk_structural(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: &LoweredIndexScanContract,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        max_entries: usize,
        output_limit: Option<usize>,
        component_indices: &[usize],
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<IndexComponentScanChunk, InternalError> {
        Self::resolve_component_chunk(
            store,
            entity_tag,
            index,
            lower,
            upper,
            continuation,
            max_entries,
            output_limit,
            component_indices,
            predicate_execution,
        )
    }

    // Resolve one index range via store registry and index-store iterator boundary.
    fn resolve_data_values_in_raw_range_limited(
        store: StoreHandle,
        entity_tag: EntityTag,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DecodedDataStoreKey>, InternalError> {
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
    fn resolve_chunk(
        store: StoreHandle,
        entity_tag: EntityTag,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        max_entries: usize,
        output_limit: Option<usize>,
    ) -> Result<IndexDecodedKeyScanChunk, InternalError> {
        if max_entries == 0 || matches!(output_limit, Some(0)) {
            return Ok(IndexDecodedKeyScanChunk::new(Vec::new(), None));
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

        let chunk = IndexDecodedKeyScanChunk::new(keys, last_raw_key);

        Ok(chunk)
    }

    // Resolve one index range component chunk via store registry and index-store iterator boundary.
    #[expect(clippy::too_many_arguments)]
    fn resolve_component_chunk(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: &LoweredIndexScanContract,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        max_entries: usize,
        output_limit: Option<usize>,
        component_indices: &[usize],
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<IndexComponentScanChunk, InternalError> {
        if max_entries == 0 || matches!(output_limit, Some(0)) {
            return Ok(IndexComponentScanChunk::new(Vec::new(), None));
        }

        let continuation =
            ContinuationRuntime::new(continuation, WindowCursorContract::unbounded());
        let bounds = continuation.scan_bounds((lower, upper))?;
        let mut rows = Vec::with_capacity(max_entries.min(Self::LIMITED_SCAN_PREALLOC_CAP));
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

                    if Self::decode_index_entry_and_push_with_components(
                        entity_tag,
                        index,
                        raw_key,
                        value,
                        &mut rows,
                        output_limit,
                        component_indices,
                        "component stream",
                        predicate_execution,
                    )? {
                        return Ok(true);
                    }

                    Ok(scanned_entries == max_entries)
                },
            )
        })?;

        Ok(IndexComponentScanChunk::new(rows, last_raw_key))
    }

    // Apply executor-owned continuation advancement checks for one raw index key.
    fn accept_scan_key(
        continuation: &ContinuationRuntime<'_>,
        raw_key: &RawIndexStoreKey,
    ) -> Result<LoopAction, InternalError> {
        continuation.accept_key(ContinuationKeyRef::scan(raw_key))
    }

    fn decode_index_entry_and_push(
        entity: EntityTag,
        raw_key: &RawIndexStoreKey,
        value: &IndexEntryValue,
        out: &mut Vec<DecodedDataStoreKey>,
        limit: Option<usize>,
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        record_row_check_index_entry_scanned();

        // Phase 1: decode only the primary-key suffix for ordinary row-identity
        // scans. Predicate scans still need the fully decoded index key.
        let (primary_key_value, primary_key_bytes) = if let Some(execution) =
            index_predicate_execution
        {
            let decoded_key = IndexKey::try_from_raw(raw_key)
                .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
            if !eval_index_execution_on_decoded_key(&decoded_key, execution)? {
                return Ok(false);
            }

            (
                decoded_key
                    .primary_key_value()
                    .map_err(|_| InternalError::index_entry_decode_failed())?,
                Cow::Owned(decoded_key.primary_key_bytes().to_vec()),
            )
        } else {
            let (primary_key_value, primary_key_bytes) =
                IndexKey::primary_key_value_and_bytes_from_raw(raw_key)
                    .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;

            (primary_key_value, Cow::Borrowed(primary_key_bytes))
        };

        // Phase 2: decode the entry-owned existence witness and pair it with
        // the row identity recovered from the raw index-key suffix.
        let row_witness = value
            .decode_row_witness_from_primary_key_value(&primary_key_value)
            .map_err(|_| InternalError::index_entry_decode_failed())?;
        record_row_check_index_key_owned_entry();
        record_row_check_index_row_identity_decoded();
        out.push(Self::data_key_from_row_witness_with_primary_key_bytes(
            entity,
            &row_witness,
            primary_key_bytes.as_ref(),
        ));

        if let Some(limit) = limit
            && out.len() == limit
        {
            return Ok(true);
        }

        Ok(false)
    }

    #[expect(clippy::too_many_arguments)]
    fn decode_index_entry_and_push_with_components(
        entity: EntityTag,
        index: &LoweredIndexScanContract,
        raw_key: &RawIndexStoreKey,
        value: &IndexEntryValue,
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

        // Phase 2: decode the key-owned row witness. The raw index key now owns
        // row identity; the raw entry value carries only the existence witness.
        let row_witness = value
            .decode_row_witness_from_index_key(&decoded_key)
            .map_err(|_| InternalError::index_entry_decode_failed())?;
        record_row_check_index_key_owned_entry();
        record_row_check_index_row_identity_decoded();
        out.push((
            Self::data_key_from_row_witness(entity, &row_witness, &decoded_key),
            row_witness.existence_witness(),
            components,
        ));

        if let Some(limit) = limit
            && out.len() == limit
        {
            return Ok(true);
        }

        Ok(false)
    }

    // Rebuild one data key from the raw row-witness payload without re-encoding
    // the primary key through the value layer.
    fn data_key_from_row_witness(
        entity: EntityTag,
        row_witness: &IndexEntryRowWitness,
        index_key: &IndexKey,
    ) -> DecodedDataStoreKey {
        Self::data_key_from_row_witness_with_primary_key_bytes(
            entity,
            row_witness,
            index_key.primary_key_bytes(),
        )
    }

    fn data_key_from_row_witness_with_primary_key_bytes(
        entity: EntityTag,
        row_witness: &IndexEntryRowWitness,
        primary_key_bytes: &[u8],
    ) -> DecodedDataStoreKey {
        DecodedDataStoreKey::new_with_raw_primary_key_value(
            entity,
            row_witness.primary_key_value(),
            RawDataStoreKey::from_entity_and_primary_key_bytes(entity, primary_key_bytes),
        )
    }
}
