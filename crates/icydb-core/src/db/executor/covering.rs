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
        data::{DataStore, DecodedDataStoreKey},
        direction::Direction,
        executor::{
            ExecutorError, FlatMergeOrderedChild, FlatMergeSiblingSet, FlatMergeStream,
            IndexComponentRow, IndexComponentRows, IndexComponentValues, IndexScan,
            KeyOrderComparator, PrefixSetExecutionShape, PrefixSetMergeSafety,
            active_lowered_index_prefix_specs, apply_index_scan_chunk_progress,
            branch_stream_chunk_entries, budget::charge_current_execution_budget,
            index_predicate_rejects_prefix_components, index_stream_chunk_entries_for_remaining,
            index_stream_output_limit_for_chunk, route::IndexPrefixChildExpansionBudget,
        },
        index::{RawIndexStoreKey, predicate::IndexPredicateExecution},
        predicate::MissingRowPolicy,
        query::plan::{CoveringExistingRowMode, CoveringProjectionOrder},
        registry::StoreHandle,
    },
    error::InternalError,
    types::EntityTag,
    types::Ulid,
    value::{Value, ValueTag},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::{mem::size_of, ops::Bound, sync::Arc};

const COVERING_BOOL_PAYLOAD_LEN: usize = 1;
const COVERING_U64_PAYLOAD_LEN: usize = 8;
const COVERING_ULID_PAYLOAD_LEN: usize = 16;
const COVERING_TEXT_ESCAPE_PREFIX: u8 = 0x00;
const COVERING_TEXT_TERMINATOR: u8 = 0x00;
const COVERING_TEXT_ESCAPED_ZERO: u8 = 0xFF;
const COVERING_I64_SIGN_BIT_BIAS: u64 = 1u64 << 63;

type RawIndexBounds = (Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>);

fn read_row_presence_with_consistency_from_data_store(
    data: &DataStore,
    key: &DecodedDataStoreKey,
    consistency: MissingRowPolicy,
) -> Result<bool, InternalError> {
    let raw = key.to_raw()?;
    let row_exists = data.contains(&raw);

    match consistency {
        MissingRowPolicy::Error if !row_exists => Err(ExecutorError::missing_row(key).into()),
        MissingRowPolicy::Error | MissingRowPolicy::Ignore => Ok(row_exists),
    }
}

// Build the canonical executor-owned covering mode for fast paths that still
// must verify row presence before trusting secondary/index-backed payloads.

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
    prefixes_have_exact_non_empty_proof: bool,
    mut resolve_store_for_index: F,
) -> Result<Option<IndexComponentRows>, InternalError>
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
                prefixes_have_exact_non_empty_proof,
            },
            resolve_store_for_index,
        );
    }

    if let [spec] = index_range_specs {
        if index_predicate_rejects_prefix_components(spec.prefix_components(), predicate_execution)
        {
            return Ok(Some(Vec::new()));
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
        )
        .map(Some);
    }
    if !index_range_specs.is_empty() {
        return Err(InternalError::query_executor_invariant());
    }

    Err(InternalError::query_executor_invariant())
}

struct CoveringPrefixSetScan<'a> {
    direction: Direction,
    limit: usize,
    component_indices: &'a [usize],
    predicate_execution: Option<IndexPredicateExecution<'a>>,
    merge_safety: PrefixSetMergeSafety,
    prefixes_have_exact_non_empty_proof: bool,
}

struct ActiveCoveringPrefixSpec<'a> {
    prefix: &'a LoweredIndexPrefixSpec,
    scan_contract: LoweredIndexScanContract,
    store: StoreHandle,
}

fn active_covering_prefix_specs<'a, F>(
    index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    predicate_execution: Option<IndexPredicateExecution<'_>>,
    prefixes_have_exact_non_empty_proof: bool,
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
    // The caller has already read synchronized, positive cardinality for
    // every prefix in this store. Re-reading it cannot change within this
    // synchronous request.
    let empty_proof_store = if prefixes_have_exact_non_empty_proof {
        None
    } else if same_store {
        Some(prefix_store)
    } else {
        None
    };
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
// decline covering execution so the maintained scalar executor owns the read.
fn resolve_covering_projection_components_for_prefix_set<F>(
    entity_tag: EntityTag,
    index_prefix_specs: &[LoweredIndexPrefixSpec],
    scan: CoveringPrefixSetScan<'_>,
    mut resolve_store_for_index: F,
) -> Result<Option<IndexComponentRows>, InternalError>
where
    F: FnMut(&str) -> Result<StoreHandle, InternalError>,
{
    if scan.limit == 0 || index_prefix_specs.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let component_indices: Arc<[usize]> = Arc::from(scan.component_indices.to_vec());
    let mut active_specs = active_covering_prefix_specs(
        index_prefix_specs,
        scan.predicate_execution,
        scan.prefixes_have_exact_non_empty_proof,
        &mut resolve_store_for_index,
    )?;
    if matches!(scan.merge_safety, PrefixSetMergeSafety::OrderedConcatSafe) {
        sort_active_covering_prefix_specs_by_raw_lower_key(&mut active_specs)?;
        if matches!(scan.direction, Direction::Desc) {
            active_specs.reverse();
        }
    }
    match PrefixSetExecutionShape::from_active_prefixes(active_specs, scan.merge_safety) {
        PrefixSetExecutionShape::Empty => Ok(Some(Vec::new())),
        PrefixSetExecutionShape::Single(active) => {
            let (lower, upper) = active.prefix.raw_bounds()?;
            resolve_covering_projection_components_for_index_bounds(
                active.store,
                entity_tag,
                active.scan_contract,
                (lower, upper),
                IndexScanContinuationInput::new(None, scan.direction),
                scan.limit,
                component_indices.as_ref(),
                scan.predicate_execution,
            )
            .map(Some)
        }
        PrefixSetExecutionShape::Fallback(_active_specs) => Ok(None),
        PrefixSetExecutionShape::OrderedConcat(active_specs) => {
            resolve_branch_ordered_covering_projection_components_for_prefix_set(
                entity_tag,
                active_specs,
                &scan,
                Arc::clone(&component_indices),
            )
            .map(Some)
        }
        PrefixSetExecutionShape::OrderedMerge(active_specs) => {
            if scan.component_indices.is_empty() && scan.predicate_execution.is_none() {
                let store = active_specs
                    .first()
                    .map(|active| active.store)
                    .ok_or_else(InternalError::query_executor_invariant)?;
                let bounds = direct_covering_prefix_merge_bounds(active_specs.as_slice())?;
                if let Some(rows) = IndexScan::merged_components_without_index_values(
                    store,
                    entity_tag,
                    bounds.as_slice(),
                    scan.direction,
                    scan.limit,
                )? {
                    return Ok(Some(rows));
                }
            }

            let index_fetch_hint = Some(scan.limit);
            let chunk_entries =
                covering_branch_stream_chunk_entries(index_fetch_hint, active_specs.len());
            let mut streams = Vec::with_capacity(active_specs.len());
            for active in active_specs {
                let (lower, upper) = active.prefix.raw_bounds()?;
                streams.push(CoveringComponentStreamBox::prefix(
                    active.store,
                    entity_tag,
                    active.scan_contract,
                    lower.clone(),
                    upper.clone(),
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
                return Ok(Some(Vec::new()));
            };

            stream.collect_limit(scan.limit).map(Some)
        }
    }
}

fn direct_covering_prefix_merge_bounds(
    active_specs: &[ActiveCoveringPrefixSpec<'_>],
) -> Result<Vec<RawIndexBounds>, InternalError> {
    validate_direct_covering_prefix_child_count(active_specs.len())?;

    let slot_bytes = active_specs
        .len()
        .checked_mul(size_of::<RawIndexBounds>())
        .ok_or_else(InternalError::executor_invariant)?;
    let retained_bytes = active_specs.iter().try_fold(slot_bytes, |bytes, active| {
        let (lower, upper) = active.prefix.raw_bounds()?;
        bytes
            .checked_add(bound_raw_index_key_bytes(lower))
            .and_then(|bytes| bytes.checked_add(bound_raw_index_key_bytes(upper)))
            .ok_or_else(InternalError::executor_invariant)
    })?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::CursorSteps,
        u64::try_from(active_specs.len()).unwrap_or(u64::MAX),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::TemporaryBytes,
        u64::try_from(retained_bytes).unwrap_or(u64::MAX),
    )?;

    let mut bounds = Vec::new();
    bounds
        .try_reserve_exact(active_specs.len())
        .map_err(|_| InternalError::executor_internal())?;
    for active in active_specs {
        let (lower, upper) = active.prefix.raw_bounds()?;
        bounds.push((lower.clone(), upper.clone()));
    }

    Ok(bounds)
}

fn validate_direct_covering_prefix_child_count(count: usize) -> Result<(), InternalError> {
    if count > IndexPrefixChildExpansionBudget::MAX_PREFIXES {
        return Err(InternalError::query_executor_invariant());
    }
    Ok(())
}

fn bound_raw_index_key_bytes(bound: &Bound<RawIndexStoreKey>) -> usize {
    match bound {
        Bound::Included(key) | Bound::Excluded(key) => key.as_bytes().len(),
        Bound::Unbounded => 0,
    }
}

fn resolve_branch_ordered_covering_projection_components_for_prefix_set(
    entity_tag: EntityTag,
    active_specs: Vec<ActiveCoveringPrefixSpec<'_>>,
    scan: &CoveringPrefixSetScan<'_>,
    component_indices: Arc<[usize]>,
) -> Result<IndexComponentRows, InternalError> {
    let mut rows = Vec::with_capacity(scan.limit.min(32));
    let branch_count = active_specs.len();
    let chunk_entries = covering_branch_stream_chunk_entries(Some(scan.limit), branch_count);

    for active in active_specs {
        if rows.len() >= scan.limit {
            break;
        }

        let remaining = scan.limit.saturating_sub(rows.len());
        let (lower, upper) = active.prefix.raw_bounds()?;
        let mut stream = CoveringComponentStreamBox::prefix(
            active.store,
            entity_tag,
            active.scan_contract,
            lower.clone(),
            upper.clone(),
            scan.direction,
            Some(remaining),
            chunk_entries,
            Arc::clone(&component_indices),
            scan.predicate_execution,
        );
        while rows.len() < scan.limit {
            let Some(row) = stream.next_row()? else {
                break;
            };
            rows.push(row);
        }
    }

    Ok(rows)
}

fn sort_active_covering_prefix_specs_by_raw_lower_key(
    specs: &mut Vec<ActiveCoveringPrefixSpec<'_>>,
) -> Result<(), InternalError> {
    let mut keyed_specs = Vec::with_capacity(specs.len());
    for spec in specs.drain(..) {
        let (lower, _upper) = spec.prefix.raw_bounds()?;
        let Bound::Included(raw_key) = lower else {
            return Err(InternalError::query_executor_invariant());
        };
        keyed_specs.push((raw_key.clone(), spec));
    }

    keyed_specs.sort_by(|left, right| left.0.cmp(&right.0));
    specs.extend(keyed_specs.into_iter().map(|(_raw_key, spec)| spec));

    Ok(())
}

fn covering_branch_stream_chunk_entries(
    index_fetch_hint: Option<usize>,
    active_branch_count: usize,
) -> usize {
    let fair_chunk_entries = branch_stream_chunk_entries(index_fetch_hint, active_branch_count);
    let Some(fetch_hint) = index_fetch_hint else {
        return fair_chunk_entries;
    };
    if active_branch_count != 2 || fetch_hint <= 4 {
        return fair_chunk_entries;
    }

    fair_chunk_entries.div_ceil(2).max(2)
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
) -> Result<IndexComponentRows, InternalError> {
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

    fn next_row(&mut self) -> Result<Option<IndexComponentRow>, InternalError> {
        match self {
            Self::Prefix(stream) => stream.next_row(),
            Self::Merge(stream) => stream.next_row(),
            Self::FlatMerge(stream) => stream.next_item(),
        }
    }

    fn collect_limit(&mut self, limit: usize) -> Result<IndexComponentRows, InternalError> {
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
    buffer: IndexComponentRows,
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

    fn next_row(&mut self) -> Result<Option<IndexComponentRow>, InternalError> {
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
    row: Option<IndexComponentRow>,
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

    fn push_row(&mut self, row: IndexComponentRow) -> Result<(), InternalError> {
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

    fn take_row(&mut self) -> Option<IndexComponentRow> {
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

    fn next_row(&mut self) -> Result<Option<IndexComponentRow>, InternalError> {
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
    type Item = IndexComponentRow;
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
    raw_pairs: IndexComponentRows,
    store: StoreHandle,
    consistency: MissingRowPolicy,
    existing_row_mode: CoveringExistingRowMode,
    mut map_components: F,
) -> Result<Option<Vec<(DecodedDataStoreKey, T)>>, InternalError>
where
    F: FnMut(IndexComponentValues) -> Result<Option<T>, InternalError>,
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
    raw_pairs: IndexComponentRows,
    store: StoreHandle,
    consistency: MissingRowPolicy,
    existing_row_mode: CoveringExistingRowMode,
    window: CoveringProjectionComponentWindow,
    initial: T,
    mut fold_component_row: F,
) -> Result<Option<T>, InternalError>
where
    F: FnMut(T, DecodedDataStoreKey, IndexComponentValues) -> Result<Option<T>, InternalError>,
{
    let mut accumulator = initial;
    let mut present_rows = 0usize;
    let mut emitted_rows = 0usize;

    for (data_key, _existence_witness, components) in raw_pairs {
        if matches!(consistency, MissingRowPolicy::Ignore)
            && window.limit.is_some_and(|limit| emitted_rows >= limit)
        {
            break;
        }

        if existing_row_mode.requires_row_presence_check() {
            let row_present = store.with_data(|data| {
                read_row_presence_with_consistency_from_data_store(data, &data_key, consistency)
            })?;
            if !row_present {
                continue;
            }
        }

        if present_rows < window.offset {
            present_rows = present_rows.saturating_add(1);
            continue;
        }
        if window.limit.is_some_and(|limit| emitted_rows >= limit) {
            present_rows = present_rows.saturating_add(1);
            continue;
        }

        let Some(next_accumulator) = fold_component_row(accumulator, data_key, components)? else {
            return Ok(None);
        };
        accumulator = next_accumulator;
        present_rows = present_rows.saturating_add(1);
        emitted_rows = emitted_rows.saturating_add(1);
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
fn decode_covering_projection_components(
    components: IndexComponentValues,
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
    components: IndexComponentValues,
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
    raw_pairs: IndexComponentRows,
    store: StoreHandle,
    consistency: MissingRowPolicy,
    existing_row_mode: CoveringExistingRowMode,
    mut decode_components: Decode,
    mut map_decoded: Map,
) -> Result<Option<Vec<(DecodedDataStoreKey, T)>>, InternalError>
where
    Decode: FnMut(IndexComponentValues) -> Result<Option<D>, InternalError>,
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
pub(in crate::db::executor) fn decode_covering_projection_pairs<T, F>(
    raw_pairs: IndexComponentRows,
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
    raw_pairs: IndexComponentRows,
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
        let components: IndexComponentValues = Arc::from(vec![
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

    #[test]
    fn direct_prefix_merge_rejects_max_plus_one_children() {
        assert!(
            validate_direct_covering_prefix_child_count(
                IndexPrefixChildExpansionBudget::MAX_PREFIXES,
            )
            .is_ok()
        );
        assert!(
            validate_direct_covering_prefix_child_count(
                IndexPrefixChildExpansionBudget::MAX_PREFIXES + 1,
            )
            .is_err()
        );
    }
}
