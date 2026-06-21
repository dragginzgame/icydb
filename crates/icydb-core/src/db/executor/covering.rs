//! Module: db::executor::covering
//! Responsibility: shared covering-index decode helpers for executor fast paths.
//! Does not own: index scan selection, terminal semantics, or aggregate orchestration.
//! Boundary: executor lanes import covering component decode from this root instead of duplicating payload logic.

use crate::{
    db::{
        access::{
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredIndexScanContract, LoweredKey,
        },
        cursor::IndexScanContinuationInput,
        data::DecodedDataStoreKey,
        direction::Direction,
        executor::{
            IndexScan, KeyOrderComparator, lowered_index_prefix_empty_bitmap,
            read_row_presence_with_consistency_from_data_store,
            record_row_check_covering_candidate_seen, record_row_check_row_emitted,
        },
        index::{
            IndexEntryExistenceWitness, RawIndexStoreKey,
            predicate::{IndexPredicateExecution, eval_index_program_on_prefix_components},
        },
        predicate::MissingRowPolicy,
        query::plan::{CoveringExistingRowMode, CoveringProjectionOrder},
        registry::StoreHandle,
    },
    error::InternalError,
    types::EntityTag,
    types::Ulid,
    value::{Value, ValueTag},
};
use std::{ops::Bound, sync::Arc};

const COVERING_BOOL_PAYLOAD_LEN: usize = 1;
const COVERING_U64_PAYLOAD_LEN: usize = 8;
const COVERING_ULID_PAYLOAD_LEN: usize = 16;
const COVERING_TEXT_ESCAPE_PREFIX: u8 = 0x00;
const COVERING_TEXT_TERMINATOR: u8 = 0x00;
const COVERING_TEXT_ESCAPED_ZERO: u8 = 0xFF;
const COVERING_I64_SIGN_BIT_BIAS: u64 = 1u64 << 63;
const COVERING_BRANCH_SMALL_PAGE_CHUNK_ENTRIES: usize = 2;
const COVERING_BRANCH_MAX_CHUNK_ENTRIES: usize = 64;

pub(in crate::db::executor) type CoveringComponentValues = Arc<[Vec<u8>]>;

pub(in crate::db::executor) type CoveringProjectionComponentRows = Vec<(
    DecodedDataStoreKey,
    IndexEntryExistenceWitness,
    CoveringComponentValues,
)>;

type CoveringProjectionComponentRow = (
    DecodedDataStoreKey,
    IndexEntryExistenceWitness,
    CoveringComponentValues,
);

// Build the canonical executor-owned covering mode for fast paths that still
// must verify row presence before trusting secondary/index-backed payloads.
pub(in crate::db::executor) const fn covering_requires_row_presence_check()
-> CoveringExistingRowMode {
    CoveringExistingRowMode::RequiresRowPresenceCheck
}

// Resolve one canonical scan direction for covering projections. Any contract
// that still owes primary-key reordering must consume the underlying index in
// ascending storage order before post-access reordering.
pub(in crate::db::executor) const fn covering_projection_scan_direction(
    order_contract: CoveringProjectionOrder,
) -> Direction {
    match order_contract {
        CoveringProjectionOrder::IndexOrder(direction) => direction,
        CoveringProjectionOrder::PrimaryKeyOrder(_) => Direction::Asc,
    }
}

// Reapply the logical covering projection order after component decoding.
pub(in crate::db::executor) fn reorder_covering_projection_pairs<T>(
    order_contract: CoveringProjectionOrder,
    projected_pairs: &mut [(DecodedDataStoreKey, T)],
) {
    match order_contract {
        CoveringProjectionOrder::PrimaryKeyOrder(Direction::Asc) => {
            projected_pairs.sort_by(|left, right| left.0.cmp(&right.0));
        }
        CoveringProjectionOrder::PrimaryKeyOrder(Direction::Desc) => {
            projected_pairs.sort_by(|left, right| right.0.cmp(&left.0));
        }
        CoveringProjectionOrder::IndexOrder(Direction::Asc | Direction::Desc) => {}
    }
}

// Resolve one covering projection component stream from one lowered
// index-prefix or index-range contract.
#[expect(clippy::too_many_arguments)]
pub(in crate::db::executor) fn resolve_covering_projection_components_from_lowered_specs<F>(
    entity_tag: EntityTag,
    index_prefix_specs: &[LoweredIndexPrefixSpec],
    index_range_specs: &[LoweredIndexRangeSpec],
    direction: Direction,
    limit: usize,
    component_indices: &[usize],
    predicate_execution: Option<IndexPredicateExecution<'_>>,
    mut resolve_store_for_index: F,
) -> Result<CoveringProjectionComponentRows, InternalError>
where
    F: FnMut(&str) -> Result<StoreHandle, InternalError>,
{
    let continuation = IndexScanContinuationInput::new(None, direction);

    if let [spec] = index_prefix_specs {
        let scan_contract = spec.scan_contract();
        return resolve_covering_projection_components_for_index_bounds(
            resolve_store_for_index(scan_contract.store_path())?,
            entity_tag,
            scan_contract,
            (spec.lower(), spec.upper()),
            continuation,
            limit,
            component_indices,
            predicate_execution,
        );
    }
    if !index_prefix_specs.is_empty() {
        return resolve_covering_projection_components_for_prefix_set(
            entity_tag,
            index_prefix_specs,
            direction,
            limit,
            component_indices,
            predicate_execution,
            resolve_store_for_index,
        );
    }

    if let [spec] = index_range_specs {
        let scan_contract = spec.scan_contract();
        return resolve_covering_projection_components_for_index_bounds(
            resolve_store_for_index(scan_contract.store_path())?,
            entity_tag,
            scan_contract,
            (spec.lower(), spec.upper()),
            continuation,
            limit,
            component_indices,
            predicate_execution,
        );
    }
    if !index_range_specs.is_empty() {
        return Err(InternalError::query_executor_invariant());
    }

    Err(InternalError::query_executor_invariant())
}

// Resolve a branch/multi-prefix covering projection as merged component
// streams. This keeps the covering lane aligned with scalar branch execution:
// each prefix owns its range cursor, merge order is primary-key comparator
// driven, duplicate keys are suppressed, and collection stops at the requested
// output limit.
fn resolve_covering_projection_components_for_prefix_set<F>(
    entity_tag: EntityTag,
    index_prefix_specs: &[LoweredIndexPrefixSpec],
    direction: Direction,
    limit: usize,
    component_indices: &[usize],
    predicate_execution: Option<IndexPredicateExecution<'_>>,
    mut resolve_store_for_index: F,
) -> Result<CoveringProjectionComponentRows, InternalError>
where
    F: FnMut(&str) -> Result<StoreHandle, InternalError>,
{
    if limit == 0 || index_prefix_specs.is_empty() {
        return Ok(Vec::new());
    }

    let component_indices: Arc<[usize]> = Arc::from(component_indices.to_vec());
    let first_scan_contract = index_prefix_specs[0].scan_contract();
    let first_store_path = first_scan_contract.store_path().to_string();
    let prefix_store = resolve_store_for_index(first_store_path.as_str())?;
    let same_store = index_prefix_specs
        .iter()
        .all(|spec| spec.scan_contract().store_path() == first_store_path.as_str());
    let empty_prefixes = if same_store {
        lowered_index_prefix_empty_bitmap(prefix_store, index_prefix_specs)
    } else {
        vec![false; index_prefix_specs.len()]
    };
    let mut active_specs = Vec::with_capacity(index_prefix_specs.len());
    for (spec, proven_empty) in index_prefix_specs.iter().zip(empty_prefixes) {
        if prefix_components_rejected_by_predicate(spec.prefix_components(), predicate_execution) {
            continue;
        }
        if proven_empty {
            continue;
        }
        let scan_contract = spec.scan_contract();
        let store = if same_store {
            prefix_store
        } else {
            resolve_store_for_index(scan_contract.store_path())?
        };
        active_specs.push((spec, scan_contract, store));
    }
    if active_specs.is_empty() {
        return Ok(Vec::new());
    }
    if active_specs.len() == 1 {
        let Some((spec, scan_contract, store)) = active_specs.pop() else {
            return Err(InternalError::query_executor_invariant());
        };

        return resolve_covering_projection_components_for_index_bounds(
            store,
            entity_tag,
            scan_contract,
            (spec.lower(), spec.upper()),
            IndexScanContinuationInput::new(None, direction),
            limit,
            component_indices.as_ref(),
            predicate_execution,
        );
    }

    let chunk_entries = covering_branch_component_chunk_entries(limit, active_specs.len());
    let mut streams = Vec::with_capacity(active_specs.len());
    for (spec, scan_contract, store) in active_specs {
        streams.push(CoveringComponentStreamBox::prefix(
            store,
            entity_tag,
            scan_contract,
            spec.lower().clone(),
            spec.upper().clone(),
            direction,
            Some(limit),
            chunk_entries,
            Arc::clone(&component_indices),
            predicate_execution,
        ));
    }
    if streams.is_empty() {
        return Ok(Vec::new());
    }

    let Some(mut stream) = CoveringComponentStreamBox::merge_all(
        streams,
        KeyOrderComparator::from_direction(direction),
    ) else {
        return Ok(Vec::new());
    };

    stream.collect_limit(limit)
}

fn prefix_components_rejected_by_predicate(
    prefix_components: &[Vec<u8>],
    predicate_execution: Option<IndexPredicateExecution<'_>>,
) -> bool {
    predicate_execution
        .and_then(|execution| {
            eval_index_program_on_prefix_components(prefix_components, execution.program)
        })
        .is_some_and(|passed| !passed)
}

const fn covering_branch_component_chunk_entries(limit: usize, branch_count: usize) -> usize {
    if limit <= COVERING_BRANCH_SMALL_PAGE_CHUNK_ENTRIES.saturating_mul(2) {
        return COVERING_BRANCH_SMALL_PAGE_CHUNK_ENTRIES;
    }

    let branch_count = if branch_count == 0 { 1 } else { branch_count };
    if branch_count <= 2 {
        return if limit > COVERING_BRANCH_MAX_CHUNK_ENTRIES {
            COVERING_BRANCH_MAX_CHUNK_ENTRIES
        } else {
            limit
        };
    }

    let fair_branch_window = limit.div_ceil(branch_count);
    if fair_branch_window < COVERING_BRANCH_SMALL_PAGE_CHUNK_ENTRIES {
        COVERING_BRANCH_SMALL_PAGE_CHUNK_ENTRIES
    } else if fair_branch_window > COVERING_BRANCH_MAX_CHUNK_ENTRIES {
        COVERING_BRANCH_MAX_CHUNK_ENTRIES
    } else {
        fair_branch_window
    }
}

// Resolve one bounded component stream from one lowered index-bounds contract.
#[expect(clippy::too_many_arguments)]
fn resolve_covering_projection_components_for_index_bounds(
    store: StoreHandle,
    entity_tag: EntityTag,
    index: crate::db::access::LoweredIndexScanContract,
    bounds: (
        &std::ops::Bound<crate::db::index::RawIndexStoreKey>,
        &std::ops::Bound<crate::db::index::RawIndexStoreKey>,
    ),
    continuation: IndexScanContinuationInput<'_>,
    limit: usize,
    component_indices: &[usize],
    predicate_execution: Option<IndexPredicateExecution<'_>>,
) -> Result<CoveringProjectionComponentRows, InternalError> {
    IndexScan::components_structural(
        store,
        entity_tag,
        index,
        bounds.0,
        bounds.1,
        continuation,
        limit,
        component_indices,
        predicate_execution,
    )
}

enum CoveringComponentStreamBox<'a> {
    Prefix(Box<CoveringPrefixComponentStream<'a>>),
    Merge(Box<MergeCoveringComponentStream<'a>>),
}

impl<'a> CoveringComponentStreamBox<'a> {
    #[expect(clippy::too_many_arguments)]
    fn prefix(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: LoweredIndexScanContract,
        lower: Bound<LoweredKey>,
        upper: Bound<LoweredKey>,
        direction: Direction,
        limit: Option<usize>,
        chunk_entries: usize,
        component_indices: Arc<[usize]>,
        predicate_execution: Option<IndexPredicateExecution<'a>>,
    ) -> Self {
        Self::Prefix(Box::new(CoveringPrefixComponentStream::new(
            store,
            entity_tag,
            index,
            lower,
            upper,
            direction,
            limit,
            chunk_entries,
            component_indices,
            predicate_execution,
        )))
    }

    fn merge(left: Self, right: Self, comparator: KeyOrderComparator) -> Self {
        Self::Merge(Box::new(MergeCoveringComponentStream::new(
            left, right, comparator,
        )))
    }

    fn merge_all(mut streams: Vec<Self>, comparator: KeyOrderComparator) -> Option<Self> {
        if streams.is_empty() {
            return None;
        }
        if streams.len() == 1 {
            return streams.pop();
        }

        while streams.len() > 1 {
            let mut next_round = Vec::with_capacity(streams.len().div_ceil(2));
            let mut iter = streams.into_iter();
            while let Some(left) = iter.next() {
                if let Some(right) = iter.next() {
                    next_round.push(Self::merge(left, right, comparator));
                } else {
                    next_round.push(left);
                }
            }
            streams = next_round;
        }

        streams.pop()
    }

    fn next_row(&mut self) -> Result<Option<CoveringProjectionComponentRow>, InternalError> {
        match self {
            Self::Prefix(stream) => stream.next_row(),
            Self::Merge(stream) => stream.next_row(),
        }
    }

    fn collect_limit(
        &mut self,
        limit: usize,
    ) -> Result<CoveringProjectionComponentRows, InternalError> {
        let mut rows = Vec::with_capacity(limit.min(32));
        while rows.len() < limit {
            let Some(row) = self.next_row()? else {
                break;
            };
            rows.push(row);
        }

        Ok(rows)
    }
}

struct CoveringPrefixComponentStream<'a> {
    store: StoreHandle,
    entity_tag: EntityTag,
    index: LoweredIndexScanContract,
    lower: Bound<LoweredKey>,
    upper: Bound<LoweredKey>,
    direction: Direction,
    anchor: Option<RawIndexStoreKey>,
    remaining: Option<usize>,
    chunk_entries: usize,
    component_indices: Arc<[usize]>,
    predicate_execution: Option<IndexPredicateExecution<'a>>,
    buffer: CoveringProjectionComponentRows,
    buffer_pos: usize,
    exhausted: bool,
}

impl<'a> CoveringPrefixComponentStream<'a> {
    #[expect(clippy::too_many_arguments)]
    const fn new(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: LoweredIndexScanContract,
        lower: Bound<LoweredKey>,
        upper: Bound<LoweredKey>,
        direction: Direction,
        limit: Option<usize>,
        chunk_entries: usize,
        component_indices: Arc<[usize]>,
        predicate_execution: Option<IndexPredicateExecution<'a>>,
    ) -> Self {
        Self {
            store,
            entity_tag,
            index,
            lower,
            upper,
            direction,
            anchor: None,
            remaining: limit,
            chunk_entries,
            component_indices,
            predicate_execution,
            buffer: Vec::new(),
            buffer_pos: 0,
            exhausted: false,
        }
    }

    const fn next_output_limit(&self) -> Option<usize> {
        self.remaining
    }

    fn load_next_chunk(&mut self) -> Result<(), InternalError> {
        if self.exhausted || matches!(self.remaining, Some(0)) {
            self.exhausted = true;
            return Ok(());
        }

        let continuation = IndexScanContinuationInput::new(self.anchor.as_ref(), self.direction);
        let chunk = IndexScan::components_chunk_structural(
            self.store,
            self.entity_tag,
            &self.index,
            &self.lower,
            &self.upper,
            continuation,
            self.chunk_entries,
            self.next_output_limit()
                .map(|limit| limit.min(self.chunk_entries)),
            &self.component_indices,
            self.predicate_execution,
        )?;
        let (rows, last_raw_key) = chunk.into_component_rows_and_resume_anchor();
        let emitted = rows.len();
        self.buffer = rows;
        self.buffer_pos = 0;

        if let Some(raw_key) = last_raw_key {
            self.anchor = Some(raw_key);
        } else {
            self.exhausted = true;
        }

        if let Some(remaining) = self.remaining.as_mut() {
            *remaining = remaining.saturating_sub(emitted);
            if *remaining == 0 {
                self.exhausted = true;
            }
        }

        Ok(())
    }

    fn next_row(&mut self) -> Result<Option<CoveringProjectionComponentRow>, InternalError> {
        while self.buffer_pos == self.buffer.len() && !self.exhausted {
            self.load_next_chunk()?;
        }
        if self.buffer_pos == self.buffer.len() {
            return Ok(None);
        }

        let row = self.buffer[self.buffer_pos].clone();
        self.buffer_pos += 1;

        Ok(Some(row))
    }
}

struct CoveringComponentStreamSideState {
    row: Option<CoveringProjectionComponentRow>,
    done: bool,
    last_key: Option<DecodedDataStoreKey>,
    comparator: KeyOrderComparator,
}

impl CoveringComponentStreamSideState {
    const fn new(comparator: KeyOrderComparator) -> Self {
        Self {
            row: None,
            done: false,
            last_key: None,
            comparator,
        }
    }

    fn ensure_row(
        &mut self,
        stream: &mut CoveringComponentStreamBox<'_>,
    ) -> Result<(), InternalError> {
        if self.done || self.row.is_some() {
            return Ok(());
        }

        match stream.next_row()? {
            Some(row) => self.push_row(row)?,
            None => self.done = true,
        }

        Ok(())
    }

    fn push_row(&mut self, row: CoveringProjectionComponentRow) -> Result<(), InternalError> {
        self.validate_monotonicity(&row.0)?;
        self.row = Some(row);

        Ok(())
    }

    fn validate_monotonicity(&self, current: &DecodedDataStoreKey) -> Result<(), InternalError> {
        let Some(previous) = self.last_key.as_ref() else {
            return Ok(());
        };
        if previous.entity_tag() != current.entity_tag() {
            return Err(InternalError::query_executor_invariant());
        }
        if self.comparator.compare_data_keys(previous, current).is_gt() {
            return Err(InternalError::query_executor_invariant());
        }

        Ok(())
    }

    fn take_row(&mut self) -> Option<CoveringProjectionComponentRow> {
        let row = self.row.take()?;
        self.last_key = Some(row.0.clone());

        Some(row)
    }

    fn clear_row(&mut self) {
        if let Some(row) = self.row.take() {
            self.last_key = Some(row.0);
        }
    }
}

struct MergeCoveringComponentStream<'a> {
    left: CoveringComponentStreamBox<'a>,
    right: CoveringComponentStreamBox<'a>,
    left_state: CoveringComponentStreamSideState,
    right_state: CoveringComponentStreamSideState,
    comparator: KeyOrderComparator,
    last_emitted: Option<DecodedDataStoreKey>,
}

impl<'a> MergeCoveringComponentStream<'a> {
    const fn new(
        left: CoveringComponentStreamBox<'a>,
        right: CoveringComponentStreamBox<'a>,
        comparator: KeyOrderComparator,
    ) -> Self {
        Self {
            left,
            right,
            left_state: CoveringComponentStreamSideState::new(comparator),
            right_state: CoveringComponentStreamSideState::new(comparator),
            comparator,
            last_emitted: None,
        }
    }

    fn next_row(&mut self) -> Result<Option<CoveringProjectionComponentRow>, InternalError> {
        loop {
            self.left_state.ensure_row(&mut self.left)?;
            self.right_state.ensure_row(&mut self.right)?;

            if self.left_state.row.is_none() && self.right_state.row.is_none() {
                return Ok(None);
            }

            let next = match (self.left_state.row.as_ref(), self.right_state.row.as_ref()) {
                (Some(left), Some(right)) => {
                    if left.0 == right.0 {
                        self.right_state.clear_row();
                        self.left_state.take_row()
                    } else if self.comparator.compare_data_keys(&left.0, &right.0).is_lt() {
                        self.left_state.take_row()
                    } else {
                        self.right_state.take_row()
                    }
                }
                (Some(_), None) => self.left_state.take_row(),
                (None, Some(_)) => self.right_state.take_row(),
                (None, None) => None,
            };

            let Some(next) = next else {
                return Ok(None);
            };
            if self
                .last_emitted
                .as_ref()
                .is_some_and(|last| last == &next.0)
            {
                continue;
            }

            self.last_emitted = Some(next.0.clone());
            return Ok(Some(next));
        }
    }
}

// Map one raw covering projection stream under the existing-row contract and
// let the caller decide how the admitted component bytes become terminal
// payloads.
pub(in crate::db::executor) fn map_covering_projection_pairs<T, F>(
    raw_pairs: CoveringProjectionComponentRows,
    store: StoreHandle,
    consistency: MissingRowPolicy,
    existing_row_mode: CoveringExistingRowMode,
    mut map_components: F,
) -> Result<Option<Vec<(DecodedDataStoreKey, T)>>, InternalError>
where
    F: FnMut(CoveringComponentValues) -> Result<Option<T>, InternalError>,
{
    let mut projected_pairs = Vec::with_capacity(raw_pairs.len());

    for (data_key, _existence_witness, components) in raw_pairs {
        // Keep the physical-access bucket scoped to the actual row-presence
        // probe only. Planner-proven covering rows should not charge covering
        // decode or terminal row mapping to `s`.
        if existing_row_mode.requires_row_presence_check() {
            record_row_check_covering_candidate_seen();

            let row_present = store.with_data(|data| {
                read_row_presence_with_consistency_from_data_store(data, &data_key, consistency)
            })?;
            if !row_present {
                continue;
            }
        }

        let Some(projected) = map_components(components)? else {
            return Ok(None);
        };
        projected_pairs.push((data_key, projected));
        if existing_row_mode.requires_row_presence_check() {
            record_row_check_row_emitted();
        }
    }

    Ok(Some(projected_pairs))
}

// Decode one canonical covering-index component payload into one runtime
// `Value`. Returning `Ok(None)` keeps unsupported component kinds fail-closed
// at the caller boundary instead of guessing a lossy decode here.
pub(in crate::db::executor) fn decode_covering_projection_component(
    component: &[u8],
) -> Result<Option<Value>, InternalError> {
    let Some((&tag, payload)) = component.split_first() else {
        return Err(InternalError::bytes_covering_component_payload_empty());
    };

    if tag == ValueTag::Bool.to_u8() {
        return decode_covering_bool(payload);
    }
    if tag == ValueTag::Int64.to_u8() {
        return decode_covering_i64(payload);
    }
    if tag == ValueTag::Nat64.to_u8() {
        return decode_covering_u64(payload);
    }
    if tag == ValueTag::Text.to_u8() {
        return decode_covering_text(payload);
    }
    if tag == ValueTag::Ulid.to_u8() {
        return decode_covering_ulid(payload);
    }
    if tag == ValueTag::Unit.to_u8() {
        return Ok(Some(Value::Unit));
    }

    Ok(None)
}

// Decode one ordered component vector into runtime values while keeping
// unsupported component kinds fail-closed at the caller boundary.
#[cfg(feature = "sql")]
fn decode_covering_projection_components(
    components: CoveringComponentValues,
) -> Result<Option<Vec<Value>>, InternalError> {
    let mut decoded = Vec::with_capacity(components.len());
    for component in components.iter() {
        let Some(value) = decode_covering_projection_component(component.as_slice())? else {
            return Ok(None);
        };
        decoded.push(value);
    }

    Ok(Some(decoded))
}

// Decode one single-component vector under the executor invariant that the
// covering route promised exactly one projection payload per row.
pub(in crate::db::executor) fn decode_single_covering_projection_value(
    components: CoveringComponentValues,
) -> Result<Option<Value>, InternalError> {
    let mut components = components.iter();
    let Some(component) = components.next() else {
        return Err(InternalError::query_executor_invariant());
    };
    if components.next().is_some() {
        return Err(InternalError::query_executor_invariant());
    }

    decode_covering_projection_component(component.as_slice())
}

// Share one executor-owned decode-and-map contract across the generic
// multi-component and single-component covering projection lanes.
fn decode_covering_projection_pairs_with<T, D, Decode, Map>(
    raw_pairs: CoveringProjectionComponentRows,
    store: StoreHandle,
    consistency: MissingRowPolicy,
    existing_row_mode: CoveringExistingRowMode,
    mut decode_components: Decode,
    mut map_decoded: Map,
) -> Result<Option<Vec<(DecodedDataStoreKey, T)>>, InternalError>
where
    Decode: FnMut(CoveringComponentValues) -> Result<Option<D>, InternalError>,
    Map: FnMut(D) -> Result<T, InternalError>,
{
    map_covering_projection_pairs(
        raw_pairs,
        store,
        consistency,
        existing_row_mode,
        |components| {
            let Some(decoded) = decode_components(components)? else {
                return Ok(None);
            };

            Ok(Some(map_decoded(decoded)?))
        },
    )
}

// Decode one covering projection stream under the existing-row contract and
// let the caller map the decoded value vector into its terminal payload.
#[cfg(feature = "sql")]
pub(in crate::db::executor) fn decode_covering_projection_pairs<T, F>(
    raw_pairs: CoveringProjectionComponentRows,
    store: StoreHandle,
    consistency: MissingRowPolicy,
    existing_row_mode: CoveringExistingRowMode,
    map_decoded: F,
) -> Result<Option<Vec<(DecodedDataStoreKey, T)>>, InternalError>
where
    F: FnMut(Vec<Value>) -> Result<T, InternalError>,
{
    decode_covering_projection_pairs_with(
        raw_pairs,
        store,
        consistency,
        existing_row_mode,
        decode_covering_projection_components,
        map_decoded,
    )
}

// Decode one single-component covering projection stream under the existing-row
// contract and let the caller map the decoded runtime value.
pub(in crate::db::executor) fn decode_single_covering_projection_pairs<T, F>(
    raw_pairs: CoveringProjectionComponentRows,
    store: StoreHandle,
    consistency: MissingRowPolicy,
    existing_row_mode: CoveringExistingRowMode,
    map_decoded: F,
) -> Result<Option<Vec<(DecodedDataStoreKey, T)>>, InternalError>
where
    F: FnMut(Value) -> Result<T, InternalError>,
{
    decode_covering_projection_pairs_with(
        raw_pairs,
        store,
        consistency,
        existing_row_mode,
        decode_single_covering_projection_value,
        map_decoded,
    )
}

fn decode_covering_bool(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    let Some(value) = payload.first() else {
        return Err(InternalError::bytes_covering_bool_payload_truncated());
    };
    if payload.len() != COVERING_BOOL_PAYLOAD_LEN {
        return Err(InternalError::bytes_covering_component_payload_invalid_length());
    }

    match *value {
        0 => Ok(Some(Value::Bool(false))),
        1 => Ok(Some(Value::Bool(true))),
        _ => Err(InternalError::bytes_covering_bool_payload_invalid_value()),
    }
}

fn decode_covering_i64(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != COVERING_U64_PAYLOAD_LEN {
        return Err(InternalError::bytes_covering_component_payload_invalid_length());
    }

    let mut bytes = [0u8; COVERING_U64_PAYLOAD_LEN];
    bytes.copy_from_slice(payload);
    let biased = u64::from_be_bytes(bytes);
    let unsigned = biased ^ COVERING_I64_SIGN_BIT_BIAS;
    let value = i64::from_be_bytes(unsigned.to_be_bytes());

    Ok(Some(Value::Int64(value)))
}

fn decode_covering_u64(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != COVERING_U64_PAYLOAD_LEN {
        return Err(InternalError::bytes_covering_component_payload_invalid_length());
    }

    let mut bytes = [0u8; COVERING_U64_PAYLOAD_LEN];
    bytes.copy_from_slice(payload);

    Ok(Some(Value::Nat64(u64::from_be_bytes(bytes))))
}

fn decode_covering_text(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    // Fast-path the common ordered-text encoding shape: raw UTF-8 bytes with
    // no embedded zeroes followed by the canonical `[0, 0]` terminator.
    if payload.len() >= 2
        && payload.ends_with(&[COVERING_TEXT_TERMINATOR, COVERING_TEXT_TERMINATOR])
        && !payload[..payload.len().saturating_sub(2)].contains(&COVERING_TEXT_ESCAPE_PREFIX)
    {
        let text = String::from_utf8(payload[..payload.len().saturating_sub(2)].to_vec())
            .map_err(|_| InternalError::bytes_covering_text_payload_invalid_utf8())?;

        return Ok(Some(Value::Text(text)));
    }

    let mut bytes = Vec::new();
    let mut i = 0usize;

    while i < payload.len() {
        let byte = payload[i];
        if byte != COVERING_TEXT_ESCAPE_PREFIX {
            bytes.push(byte);
            i = i.saturating_add(1);
            continue;
        }

        let Some(next) = payload.get(i.saturating_add(1)).copied() else {
            return Err(InternalError::bytes_covering_text_payload_invalid_terminator());
        };
        match next {
            COVERING_TEXT_TERMINATOR => {
                i = i.saturating_add(2);
                if i != payload.len() {
                    return Err(InternalError::bytes_covering_text_payload_trailing_bytes());
                }

                let text = String::from_utf8(bytes)
                    .map_err(|_| InternalError::bytes_covering_text_payload_invalid_utf8())?;

                return Ok(Some(Value::Text(text)));
            }
            COVERING_TEXT_ESCAPED_ZERO => {
                bytes.push(0);
                i = i.saturating_add(2);
            }
            _ => {
                return Err(InternalError::bytes_covering_text_payload_invalid_escape_byte());
            }
        }
    }

    Err(InternalError::bytes_covering_text_payload_missing_terminator())
}

fn decode_covering_ulid(payload: &[u8]) -> Result<Option<Value>, InternalError> {
    if payload.len() != COVERING_ULID_PAYLOAD_LEN {
        return Err(InternalError::bytes_covering_component_payload_invalid_length());
    }

    let mut bytes = [0u8; COVERING_ULID_PAYLOAD_LEN];
    bytes.copy_from_slice(payload);

    Ok(Some(Value::Ulid(Ulid::from_bytes(bytes))))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorClass, ErrorOrigin};

    #[test]
    fn decode_single_covering_projection_value_rejects_multiple_components() {
        let components: CoveringComponentValues = Arc::from(vec![
            vec![ValueTag::Bool.to_u8(), 1],
            vec![ValueTag::Bool.to_u8(), 0],
        ]);

        let error = decode_single_covering_projection_value(components)
            .expect_err("multi-component vectors must violate the single-component invariant");

        assert_eq!(error.class(), ErrorClass::InvariantViolation);
        assert_eq!(error.origin(), ErrorOrigin::Query);
    }

    #[test]
    fn decode_covering_projection_component_decodes_fast_path_text_payload() {
        let component = [
            ValueTag::Text.to_u8(),
            b't',
            b'e',
            b'x',
            b't',
            COVERING_TEXT_TERMINATOR,
            COVERING_TEXT_TERMINATOR,
        ];

        let decoded = decode_covering_projection_component(component.as_slice())
            .expect("fast-path text payload should decode")
            .expect("text payload should remain supported");

        assert_eq!(decoded, Value::Text(String::from("text")));
    }
}
