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
            FlatMergeOrderedChild, FlatMergeSiblingSet, FlatMergeStream, IndexScan,
            KeyOrderComparator, PrefixSetExecutionShape, PrefixSetMergeSafety,
            active_lowered_index_prefix_specs, apply_data_key_ordered_dedup_window,
            apply_index_scan_chunk_progress, branch_stream_chunk_entries,
            index_predicate_rejects_prefix_components, index_stream_chunk_entries_for_remaining,
            index_stream_output_limit_for_chunk,
            read_row_presence_with_consistency_from_data_store,
            record_row_check_covering_candidate_seen, record_row_check_row_emitted,
        },
        index::{IndexEntryExistenceWitness, RawIndexStoreKey, predicate::IndexPredicateExecution},
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
    prefix_set_merge_safety: PrefixSetMergeSafety,
    mut resolve_store_for_index: F,
) -> Result<CoveringProjectionComponentRows, InternalError>
where
    F: FnMut(&str) -> Result<StoreHandle, InternalError>,
{
    let continuation = IndexScanContinuationInput::new(None, direction);

    if !index_prefix_specs.is_empty() {
        return resolve_covering_projection_components_for_prefix_set(
            entity_tag,
            index_prefix_specs,
            CoveringPrefixSetScan {
                direction,
                limit,
                component_indices,
                predicate_execution,
                merge_safety: prefix_set_merge_safety,
            },
            resolve_store_for_index,
        );
    }

    if let [spec] = index_range_specs {
        if index_predicate_rejects_prefix_components(spec.prefix_components(), predicate_execution)
        {
            return Ok(Vec::new());
        }

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

pub(in crate::db::executor) fn resolve_single_covering_projection_component_from_lowered_specs<F>(
    entity_tag: EntityTag,
    index_prefix_specs: &[LoweredIndexPrefixSpec],
    index_range_specs: &[LoweredIndexRangeSpec],
    direction: Direction,
    component_index: usize,
    resolve_store_for_index: F,
) -> Result<CoveringProjectionComponentRows, InternalError>
where
    F: FnMut(&str) -> Result<StoreHandle, InternalError>,
{
    resolve_covering_projection_components_from_lowered_specs(
        entity_tag,
        index_prefix_specs,
        index_range_specs,
        direction,
        usize::MAX,
        &[component_index],
        None,
        PrefixSetMergeSafety::RequiresMaterialization,
        resolve_store_for_index,
    )
}

struct CoveringPrefixSetScan<'a> {
    direction: Direction,
    limit: usize,
    component_indices: &'a [usize],
    predicate_execution: Option<IndexPredicateExecution<'a>>,
    merge_safety: PrefixSetMergeSafety,
}

struct ActiveCoveringPrefixSpec<'a> {
    prefix: &'a LoweredIndexPrefixSpec,
    scan_contract: LoweredIndexScanContract,
    store: StoreHandle,
}

fn active_covering_prefix_specs<'a, F>(
    index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    predicate_execution: Option<IndexPredicateExecution<'_>>,
    resolve_store_for_index: &mut F,
) -> Result<Vec<ActiveCoveringPrefixSpec<'a>>, InternalError>
where
    F: FnMut(&str) -> Result<StoreHandle, InternalError>,
{
    if index_prefix_specs.is_empty() {
        return Ok(Vec::new());
    }

    let first_scan_contract = index_prefix_specs[0].scan_contract();
    let first_store_path = first_scan_contract.store_path().to_string();
    let prefix_store = resolve_store_for_index(first_store_path.as_str())?;
    let same_store = index_prefix_specs
        .iter()
        .all(|spec| spec.scan_contract().store_path() == first_store_path.as_str());
    let empty_proof_store = if same_store { Some(prefix_store) } else { None };
    let mut active_specs = Vec::with_capacity(index_prefix_specs.len());
    for spec in active_lowered_index_prefix_specs(
        empty_proof_store,
        index_prefix_specs,
        predicate_execution,
    ) {
        let scan_contract = spec.scan_contract();
        let store = if same_store {
            prefix_store
        } else {
            resolve_store_for_index(scan_contract.store_path())?
        };
        active_specs.push(ActiveCoveringPrefixSpec {
            prefix: spec,
            scan_contract,
            store,
        });
    }

    Ok(active_specs)
}

// Resolve a branch/multi-prefix covering projection. Proven ordered prefix
// sets use the same lazy merge model as scalar branch execution; unsafe sets
// materialize completely before dedup/sort/windowing.
fn resolve_covering_projection_components_for_prefix_set<F>(
    entity_tag: EntityTag,
    index_prefix_specs: &[LoweredIndexPrefixSpec],
    scan: CoveringPrefixSetScan<'_>,
    mut resolve_store_for_index: F,
) -> Result<CoveringProjectionComponentRows, InternalError>
where
    F: FnMut(&str) -> Result<StoreHandle, InternalError>,
{
    if scan.limit == 0 || index_prefix_specs.is_empty() {
        return Ok(Vec::new());
    }

    let component_indices: Arc<[usize]> = Arc::from(scan.component_indices.to_vec());
    let active_specs = active_covering_prefix_specs(
        index_prefix_specs,
        scan.predicate_execution,
        &mut resolve_store_for_index,
    )?;
    match PrefixSetExecutionShape::from_active_prefixes(active_specs, scan.merge_safety) {
        PrefixSetExecutionShape::Empty => Ok(Vec::new()),
        PrefixSetExecutionShape::Single(active) => {
            resolve_covering_projection_components_for_index_bounds(
                active.store,
                entity_tag,
                active.scan_contract,
                (active.prefix.lower(), active.prefix.upper()),
                IndexScanContinuationInput::new(None, scan.direction),
                scan.limit,
                component_indices.as_ref(),
                scan.predicate_execution,
            )
        }
        PrefixSetExecutionShape::Materialized(active_specs) => {
            resolve_materialized_covering_projection_components_for_prefix_set(
                entity_tag,
                active_specs,
                &scan,
                component_indices.as_ref(),
            )
        }
        PrefixSetExecutionShape::OrderedMerge(active_specs) => {
            let index_fetch_hint = Some(scan.limit);
            let chunk_entries = branch_stream_chunk_entries(index_fetch_hint, active_specs.len());
            let mut streams = Vec::with_capacity(active_specs.len());
            for active in active_specs {
                streams.push(CoveringComponentStreamBox::prefix(
                    active.store,
                    entity_tag,
                    active.scan_contract,
                    active.prefix.lower().clone(),
                    active.prefix.upper().clone(),
                    scan.direction,
                    Some(scan.limit),
                    chunk_entries,
                    Arc::clone(&component_indices),
                    scan.predicate_execution,
                ));
            }

            let Some(mut stream) = CoveringComponentStreamBox::merge_all(
                streams,
                KeyOrderComparator::from_direction(scan.direction),
            ) else {
                return Ok(Vec::new());
            };

            stream.collect_limit(scan.limit)
        }
    }
}

fn resolve_materialized_covering_projection_components_for_prefix_set(
    entity_tag: EntityTag,
    active_specs: Vec<ActiveCoveringPrefixSpec<'_>>,
    scan: &CoveringPrefixSetScan<'_>,
    component_indices: &[usize],
) -> Result<CoveringProjectionComponentRows, InternalError> {
    let mut rows = Vec::new();
    for active in active_specs {
        rows.extend(resolve_covering_projection_components_for_index_bounds(
            active.store,
            entity_tag,
            active.scan_contract,
            (active.prefix.lower(), active.prefix.upper()),
            IndexScanContinuationInput::new(None, scan.direction),
            usize::MAX,
            component_indices,
            scan.predicate_execution,
        )?);
    }
    apply_data_key_ordered_dedup_window(&mut rows, scan.direction, scan.limit, |row| &row.0);

    Ok(rows)
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
    FlatMerge(Box<FlatMergeStream<CoveringComponentFlatMergeChild<'a>>>),
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

    fn merge_all(streams: Vec<Self>, comparator: KeyOrderComparator) -> Option<Self> {
        match FlatMergeSiblingSet::from_vec(streams) {
            FlatMergeSiblingSet::Empty => None,
            FlatMergeSiblingSet::Single(stream) => Some(stream),
            FlatMergeSiblingSet::Pair(left, right) => Some(Self::merge(left, right, comparator)),
            FlatMergeSiblingSet::Many(streams) => {
                Some(Self::FlatMerge(Box::new(FlatMergeStream::new(
                    streams
                        .into_iter()
                        .map(|stream| CoveringComponentFlatMergeChild::new(stream, comparator))
                        .collect(),
                    comparator,
                ))))
            }
        }
    }

    fn next_row(&mut self) -> Result<Option<CoveringProjectionComponentRow>, InternalError> {
        match self {
            Self::Prefix(stream) => stream.next_row(),
            Self::Merge(stream) => stream.next_row(),
            Self::FlatMerge(stream) => stream.next_item(),
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

    fn load_next_chunk(&mut self) -> Result<(), InternalError> {
        if self.exhausted || matches!(self.remaining, Some(0)) {
            self.exhausted = true;
            return Ok(());
        }

        let chunk_entries =
            index_stream_chunk_entries_for_remaining(self.chunk_entries, self.remaining);
        let continuation = IndexScanContinuationInput::new(self.anchor.as_ref(), self.direction);
        let chunk = IndexScan::components_chunk_structural(
            self.store,
            self.entity_tag,
            &self.index,
            &self.lower,
            &self.upper,
            continuation,
            chunk_entries,
            index_stream_output_limit_for_chunk(self.remaining, chunk_entries),
            &self.component_indices,
            self.predicate_execution,
        )?;
        let (rows, last_raw_key) = chunk.into_component_rows_and_resume_anchor();
        let emitted = rows.len();
        self.buffer = rows;
        self.buffer_pos = 0;

        apply_index_scan_chunk_progress(
            &mut self.anchor,
            &mut self.remaining,
            &mut self.exhausted,
            emitted,
            last_raw_key,
        );

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

struct CoveringComponentFlatMergeChild<'a> {
    stream: CoveringComponentStreamBox<'a>,
    state: CoveringComponentStreamSideState,
}

impl<'a> CoveringComponentFlatMergeChild<'a> {
    const fn new(stream: CoveringComponentStreamBox<'a>, comparator: KeyOrderComparator) -> Self {
        Self {
            stream,
            state: CoveringComponentStreamSideState::new(comparator),
        }
    }
}

impl FlatMergeOrderedChild for CoveringComponentFlatMergeChild<'_> {
    type Item = CoveringProjectionComponentRow;
    type KeyWitness = DecodedDataStoreKey;

    fn ensure_item(&mut self) -> Result<(), InternalError> {
        self.state.ensure_row(&mut self.stream)
    }

    fn head_key(&self) -> Option<&DecodedDataStoreKey> {
        self.state.row.as_ref().map(|row| &row.0)
    }

    fn take_item(&mut self) -> Option<Self::Item> {
        self.state.take_row()
    }

    fn clear_item(&mut self) {
        self.state.clear_row();
    }

    fn item_key(item: &Self::Item) -> &DecodedDataStoreKey {
        &item.0
    }

    fn key_witness(key: &DecodedDataStoreKey) -> Self::KeyWitness {
        key.clone()
    }

    fn witness_matches_key(witness: &Self::KeyWitness, key: &DecodedDataStoreKey) -> bool {
        witness == key
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
    let capacity = raw_pairs.len();

    fold_covering_projection_component_rows_in_window(
        raw_pairs,
        store,
        consistency,
        existing_row_mode,
        CoveringProjectionComponentWindow::new(0, None),
        Vec::with_capacity(capacity),
        |mut projected_pairs, data_key, components| {
            let Some(projected) = map_components(components)? else {
                return Ok(None);
            };
            projected_pairs.push((data_key, projected));

            Ok(Some(projected_pairs))
        },
    )
}

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct CoveringProjectionComponentWindow {
    offset: usize,
    limit: Option<usize>,
}

impl CoveringProjectionComponentWindow {
    pub(in crate::db::executor) const fn new(offset: usize, limit: Option<usize>) -> Self {
        Self { offset, limit }
    }
}

// Fold one raw covering component stream through the same existing-row and
// effective-window policy used by index-ordered covering terminals. The caller
// owns terminal-specific decode/fold semantics; this helper owns stale-row
// filtering and row-check attribution.
pub(in crate::db::executor) fn fold_covering_projection_component_rows_in_window<T, F>(
    raw_pairs: CoveringProjectionComponentRows,
    store: StoreHandle,
    consistency: MissingRowPolicy,
    existing_row_mode: CoveringExistingRowMode,
    window: CoveringProjectionComponentWindow,
    initial: T,
    mut fold_component_row: F,
) -> Result<Option<T>, InternalError>
where
    F: FnMut(T, DecodedDataStoreKey, CoveringComponentValues) -> Result<Option<T>, InternalError>,
{
    let mut accumulator = initial;
    let mut present_rows = 0usize;
    let mut emitted_rows = 0usize;

    for (data_key, _existence_witness, components) in raw_pairs {
        if existing_row_mode.requires_row_presence_check() {
            record_row_check_covering_candidate_seen();
            let row_present = store.with_data(|data| {
                read_row_presence_with_consistency_from_data_store(data, &data_key, consistency)
            })?;
            if !row_present {
                continue;
            }
        }

        if present_rows < window.offset {
            present_rows = present_rows.saturating_add(1);
            if existing_row_mode.requires_row_presence_check() {
                record_row_check_row_emitted();
            }
            continue;
        }
        if window.limit.is_some_and(|limit| emitted_rows >= limit) {
            present_rows = present_rows.saturating_add(1);
            if existing_row_mode.requires_row_presence_check() {
                record_row_check_row_emitted();
            }
            continue;
        }

        let Some(next_accumulator) = fold_component_row(accumulator, data_key, components)? else {
            return Ok(None);
        };
        accumulator = next_accumulator;
        present_rows = present_rows.saturating_add(1);
        emitted_rows = emitted_rows.saturating_add(1);
        if existing_row_mode.requires_row_presence_check() {
            record_row_check_row_emitted();
        }
    }

    Ok(Some(accumulator))
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
