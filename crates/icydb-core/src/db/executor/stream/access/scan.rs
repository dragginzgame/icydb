//! Module: executor::stream::access::scan
//! Responsibility: low-level primary/index scan adapters over store/index handles.
//! Does not own: access routing decisions or planner spec construction.
//! Boundary: direct scan primitives used by access-stream resolver.

use crate::{
    db::{
        PrimaryKeyValue,
        cursor::{ContinuationKeyRef, ContinuationRuntime, IndexScanContinuationInput},
        data::{DataStore, DecodedDataStoreKey, RawDataStoreKey},
        direction::Direction,
        executor::{
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredIndexScanContract, LoweredKey,
            budget::charge_current_execution_budget, lowered_index_prefix_liveness_at_generation,
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
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::{borrow::Cow, cmp::Ordering, mem::size_of, ops::Bound, sync::Arc};

pub(in crate::db::executor) type IndexComponentValues = Arc<[Vec<u8>]>;

pub(in crate::db::executor) type IndexComponentRow = (
    DecodedDataStoreKey,
    IndexEntryExistenceWitness,
    IndexComponentValues,
);

pub(in crate::db::executor) type IndexComponentRows = Vec<IndexComponentRow>;

struct ExactIntersectionPrimaryKey {
    value: PrimaryKeyValue,
}

struct MergedPrimaryKeyOrder {
    value: PrimaryKeyValue,
    bytes_len: usize,
}

impl Eq for MergedPrimaryKeyOrder {}

impl PartialEq for MergedPrimaryKeyOrder {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Ord for MergedPrimaryKeyOrder {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for MergedPrimaryKeyOrder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn charge_merged_range_structural_bytes(bytes: usize) -> Result<(), InternalError> {
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::TemporaryBytes,
        u64::try_from(bytes).unwrap_or(u64::MAX),
    )
}

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
                if !lowered_index_prefix_liveness_at_generation(index_store, data_generation, spec)
                    .should_scan()
                {
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

    fn collect_exact_intersection_child(
        store: StoreHandle,
        spec: &LoweredIndexPrefixSpec,
        expected_cardinality: u64,
        direction: Direction,
    ) -> Result<Vec<ExactIntersectionPrimaryKey>, InternalError> {
        let expected = usize::try_from(expected_cardinality)
            .map_err(|_| InternalError::executor_invariant())?;
        let retained_capacity_bytes = expected
            .checked_mul(size_of::<ExactIntersectionPrimaryKey>())
            .ok_or_else(InternalError::executor_invariant)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            u64::try_from(retained_capacity_bytes).unwrap_or(u64::MAX),
        )?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
            expected_cardinality,
        )?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::CursorSteps,
            expected_cardinality,
        )?;
        let mut keys = Vec::with_capacity(expected);
        let (lower, upper) = spec.raw_bounds()?;
        let mut raw_bytes_read = 0u64;
        let scan_result = store.with_index(|index_store| {
            index_store.visit_raw_entries_in_range((lower, upper), direction, |raw_key, entry| {
                let raw_bytes = u64::try_from(raw_key.as_bytes().len()).unwrap_or(u64::MAX);
                raw_bytes_read = raw_bytes_read
                    .checked_add(raw_bytes)
                    .ok_or_else(InternalError::executor_invariant)?;
                let (primary_key, _primary_key_bytes) =
                    IndexKey::primary_key_value_and_bytes_from_raw(raw_key).map_err(|error| {
                        InternalError::index_scan_key_corrupted_during(
                            "exact intersection probe",
                            error,
                        )
                    })?;
                entry
                    .decode_row_witness_from_primary_key_value(&primary_key)
                    .map_err(|_| InternalError::index_entry_decode_failed())?;
                keys.push(ExactIntersectionPrimaryKey { value: primary_key });

                Ok(keys.len() == expected)
            })
        });
        // The preflight bounds this atomic route to 256 entries. Charge all
        // completed physical work even when validation reports corruption.
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::StoredBytesRead,
            raw_bytes_read,
        )?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::DecodedBytes,
            raw_bytes_read,
        )?;
        scan_result?;
        if keys.len() != expected {
            return Err(InternalError::executor_invariant());
        }

        Ok(keys)
    }

    fn intersect_exact_primary_keys(
        overlap: Vec<ExactIntersectionPrimaryKey>,
        keys: &[ExactIntersectionPrimaryKey],
        direction: Direction,
    ) -> Result<Vec<ExactIntersectionPrimaryKey>, InternalError> {
        let capacity = overlap.len().min(keys.len());
        let capacity_bytes = capacity
            .checked_mul(size_of::<ExactIntersectionPrimaryKey>())
            .ok_or_else(InternalError::executor_invariant)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            u64::try_from(capacity_bytes).unwrap_or(u64::MAX),
        )?;
        let mut next = Vec::with_capacity(capacity);
        let mut left = overlap.into_iter().peekable();
        let mut right = keys.iter().peekable();
        while let (Some(left_key), Some(right_key)) = (left.peek(), right.peek()) {
            let order = match direction {
                Direction::Asc => left_key.value.cmp(&right_key.value),
                Direction::Desc => right_key.value.cmp(&left_key.value),
            };
            match order {
                std::cmp::Ordering::Less => {
                    let _ = left.next();
                }
                std::cmp::Ordering::Greater => {
                    let _ = right.next();
                }
                std::cmp::Ordering::Equal => {
                    let key = left.next().ok_or_else(InternalError::executor_invariant)?;
                    let _ = right.next();
                    next.push(key);
                }
            }
        }

        Ok(next)
    }

    /// Resolve one cursorless, cardinality-bounded exact-prefix intersection
    /// while retaining compact primary-key candidates rather than complete
    /// data keys for entries that cannot survive the intersection.
    pub(in crate::db::executor) fn exact_prefix_intersection_structural(
        store: StoreHandle,
        entity: EntityTag,
        specs: &[&LoweredIndexPrefixSpec],
        child_cardinalities: &[u64],
        direction: Direction,
    ) -> Result<Vec<DecodedDataStoreKey>, InternalError> {
        if specs.len() != child_cardinalities.len() || specs.is_empty() {
            return Err(InternalError::executor_invariant());
        }

        let mut children = Vec::with_capacity(specs.len());
        for (spec, expected_cardinality) in specs.iter().zip(child_cardinalities.iter().copied()) {
            children.push(Self::collect_exact_intersection_child(
                store,
                spec,
                expected_cardinality,
                direction,
            )?);
        }

        let mut children = children.into_iter();
        let Some(mut overlap) = children.next() else {
            return Err(InternalError::executor_invariant());
        };
        for keys in children {
            overlap = Self::intersect_exact_primary_keys(overlap, &keys, direction)?;
            if overlap.is_empty() {
                break;
            }
        }

        let mut result = Vec::with_capacity(overlap.len());
        for key in overlap {
            result.push(DecodedDataStoreKey::new_primary_key_value(
                entity, &key.value,
            ));
        }

        Ok(result)
    }

    /// Resolve disjoint exact-prefix ranges through one physical merge when
    /// the index store can expose one non-overlay backing.
    pub(in crate::db::executor) fn merged_components_without_index_values(
        store: StoreHandle,
        entity: EntityTag,
        bounds: &[(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>)],
        direction: Direction,
        limit: usize,
    ) -> Result<Option<IndexComponentRows>, InternalError> {
        let mut rows = Vec::with_capacity(limit.min(Self::LIMITED_SCAN_PREALLOC_CAP));
        let mut entries_visited = 0u64;
        let mut raw_bytes_read = 0u64;
        let scan_result = store.with_index(|index_store| {
            index_store.visit_raw_entries_in_merged_ranges(
                bounds,
                direction,
                charge_merged_range_structural_bytes,
                |raw_key| {
                    entries_visited = entries_visited
                        .checked_add(1)
                        .ok_or_else(InternalError::executor_invariant)?;
                    let raw_key_bytes = u64::try_from(raw_key.as_bytes().len()).unwrap_or(u64::MAX);
                    raw_bytes_read = raw_bytes_read
                        .checked_add(raw_key_bytes)
                        .ok_or_else(InternalError::executor_invariant)?;
                    let (primary_key_value, primary_key_bytes) =
                        IndexKey::primary_key_value_and_bytes_from_raw(raw_key).map_err(
                            |error| {
                                InternalError::index_scan_key_corrupted_during(
                                    "merged component stream",
                                    error,
                                )
                            },
                        )?;

                    Ok(MergedPrimaryKeyOrder {
                        value: primary_key_value,
                        bytes_len: primary_key_bytes.len(),
                    })
                },
                |order_key, raw_key, value| {
                    let primary_key_value = order_key.value;
                    let row_witness = value
                        .decode_row_witness_from_primary_key_value(&primary_key_value)
                        .map_err(|_| InternalError::index_entry_decode_failed())?;
                    let bytes_start = raw_key
                        .as_bytes()
                        .len()
                        .checked_sub(order_key.bytes_len)
                        .ok_or_else(InternalError::executor_invariant)?;
                    let primary_key_bytes = raw_key
                        .as_bytes()
                        .get(bytes_start..)
                        .ok_or_else(InternalError::executor_invariant)?;
                    let data_key = DecodedDataStoreKey::new_with_raw_primary_key_value(
                        entity,
                        &primary_key_value,
                        RawDataStoreKey::from_entity_and_primary_key_bytes(
                            entity,
                            primary_key_bytes,
                        ),
                    );
                    rows.push((data_key, row_witness.existence_witness(), Arc::default()));

                    Ok(rows.len() == limit)
                },
            )
        });
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
            entries_visited,
        )?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::StoredBytesRead,
            raw_bytes_read,
        )?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::CursorSteps,
            entries_visited,
        )?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::DecodedBytes,
            raw_bytes_read,
        )?;
        let supported = scan_result?;

        Ok(supported.then_some(rows))
    }

    /// Resolve one lowered index-prefix envelope through structural store authority.
    pub(in crate::db::executor) fn prefix_structural(
        store: StoreHandle,
        entity_tag: EntityTag,
        spec: &LoweredIndexPrefixSpec,
        direction: Direction,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DecodedDataStoreKey>, InternalError> {
        let (lower, upper) = spec.raw_bounds()?;
        Self::resolve_data_values_in_raw_range_limited(
            store,
            entity_tag,
            lower,
            upper,
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

        let continuation = ContinuationRuntime::new(continuation);
        let bounds = continuation.scan_bounds((lower, upper))?;
        let mut out = Vec::with_capacity(limit.min(Self::LIMITED_SCAN_PREALLOC_CAP));

        store.with_index(|index_store| {
            index_store.visit_raw_entries_in_range(
                (&bounds.0, &bounds.1),
                continuation.direction(),
                |raw_key, value| {
                    Self::accept_scan_key(&continuation, raw_key)?;

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

        let continuation = ContinuationRuntime::new(continuation);
        let bounds = continuation.scan_bounds((lower, upper))?;
        let mut keys = Vec::with_capacity(limit.min(Self::LIMITED_SCAN_PREALLOC_CAP));

        store.with_index(|index_store| {
            index_store.visit_raw_entries_in_range(
                (&bounds.0, &bounds.1),
                continuation.direction(),
                |raw_key, value| {
                    Self::accept_scan_key(&continuation, raw_key)?;

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

        let continuation = ContinuationRuntime::new(continuation);
        let bounds = continuation.scan_bounds((lower, upper))?;
        let mut keys = Vec::with_capacity(max_entries.min(Self::LIMITED_SCAN_PREALLOC_CAP));
        let mut last_raw_key = None;
        let mut scanned_entries = 0usize;

        store.with_index(|index_store| {
            index_store.visit_raw_entries_in_range(
                (&bounds.0, &bounds.1),
                continuation.direction(),
                |raw_key, value| {
                    Self::accept_scan_key(&continuation, raw_key)?;
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

        let continuation = ContinuationRuntime::new(continuation);
        let bounds = continuation.scan_bounds((lower, upper))?;
        let mut rows = Vec::with_capacity(max_entries.min(Self::LIMITED_SCAN_PREALLOC_CAP));
        let mut last_raw_key = None;
        let mut scanned_entries = 0usize;

        store.with_index(|index_store| {
            index_store.visit_raw_entries_in_range(
                (&bounds.0, &bounds.1),
                continuation.direction(),
                |raw_key, value| {
                    Self::accept_scan_key(&continuation, raw_key)?;
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
    ) -> Result<(), InternalError> {
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
            1,
        )?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::StoredBytesRead,
            u64::try_from(raw_key.as_bytes().len()).unwrap_or(u64::MAX),
        )?;
        charge_current_execution_budget(DiagnosticExecutionBudgetResource::CursorSteps, 1)?;
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
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::DecodedBytes,
            u64::try_from(raw_key.as_bytes().len()).unwrap_or(u64::MAX),
        )?;
        // Phase 1: decode only the primary-key suffix for ordinary row-identity
        // scans. Predicate scans still need the fully decoded index key.
        let (primary_key_value, primary_key_bytes) = if let Some(execution) =
            index_predicate_execution
        {
            charge_current_execution_budget(
                DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
                1,
            )?;
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
        if component_indices.is_empty() && index_predicate_execution.is_none() {
            return Self::decode_index_entry_and_push_without_components(
                entity, raw_key, value, out, limit, context,
            );
        }

        // Phase 1: decode the raw key once, extract requested components, and
        // evaluate any optional index-only predicate against that decoded view.
        let decoded_key = IndexKey::try_from_raw(raw_key)
            .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::DecodedBytes,
            u64::try_from(raw_key.as_bytes().len()).unwrap_or(u64::MAX),
        )?;
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

        if let Some(execution) = index_predicate_execution {
            charge_current_execution_budget(
                DiagnosticExecutionBudgetResource::PredicateExpressionSteps,
                1,
            )?;
            if !eval_index_execution_on_decoded_key(&decoded_key, execution)? {
                return Ok(false);
            }
        }

        // Phase 2: decode the key-owned row witness. The raw index key now owns
        // row identity; the raw entry value carries only the existence witness.
        let row_witness = value
            .decode_row_witness_from_index_key(&decoded_key)
            .map_err(|_| InternalError::index_entry_decode_failed())?;
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

    fn decode_index_entry_and_push_without_components(
        entity: EntityTag,
        raw_key: &RawIndexStoreKey,
        value: &IndexEntryValue,
        out: &mut IndexComponentRows,
        limit: Option<usize>,
        context: &'static str,
    ) -> Result<bool, InternalError> {
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::DecodedBytes,
            u64::try_from(raw_key.as_bytes().len()).unwrap_or(u64::MAX),
        )?;
        let (primary_key_value, primary_key_bytes) =
            IndexKey::primary_key_value_and_bytes_from_raw(raw_key)
                .map_err(|err| InternalError::index_scan_key_corrupted_during(context, err))?;
        let row_witness = value
            .decode_row_witness_from_primary_key_value(&primary_key_value)
            .map_err(|_| InternalError::index_entry_decode_failed())?;
        out.push((
            Self::data_key_from_row_witness_with_primary_key_bytes(
                entity,
                &row_witness,
                primary_key_bytes,
            ),
            row_witness.existence_witness(),
            Arc::default(),
        ));

        Ok(limit.is_some_and(|limit| out.len() == limit))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{
        QueryError,
        executor::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            with_query_execution_budget_for_tests,
        },
    };
    use icydb_diagnostic_code::{
        DiagnosticDetail, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
        DiagnosticFactTag, RuntimeBoundaryCode,
    };

    #[test]
    fn merged_range_structure_returns_typed_temporary_byte_exhaustion() {
        let budget = HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            HardExecutionFailureHeadroom::new(500, 256),
        )
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::TemporaryBytes, 0);
        let context = HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::TrustedRead,
            0x6d65_7267_6564_7261,
        );
        let error = with_query_execution_budget_for_tests(budget, context, || {
            charge_merged_range_structural_bytes(1).map_err(QueryError::execute)
        })
        .expect_err("merged-range structure must consume the temporary-byte budget");

        assert!(matches!(
            error.diagnostic().detail(),
            Some(DiagnosticDetail::RuntimeBoundary {
                boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
            })
        ));
        assert_eq!(
            error.diagnostic_facts()[0],
            (
                DiagnosticFactTag::BudgetResource,
                DiagnosticExecutionBudgetResource::TemporaryBytes.raw(),
            ),
        );
    }
}
