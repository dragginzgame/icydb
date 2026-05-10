//! Module: executor::stream::access::physical
//! Responsibility: lower executable access-path payloads into physical key streams.
//! Does not own: planner eligibility decisions or post-access semantics.
//! Boundary: physical key resolution through primary/index scan adapters.

use crate::{
    db::{
        access::ExecutionPathPayload,
        cursor::IndexScanContinuationInput,
        data::{DataKey, RawDataKey},
        direction::Direction,
        executor::{
            IndexScan, LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredIndexScanContract,
            LoweredKey, OrderedKeyStream, OrderedKeyStreamBox, PrimaryScan,
            ordered_key_stream_from_materialized_keys,
            pipeline::contracts::AccessScanContinuationInput,
            route::primary_scan_fetch_hint_shape_supported, traversal::IndexRangeTraversalContract,
        },
        index::{RawIndexKey, predicate::IndexPredicateExecution},
        registry::StoreHandle,
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};
use std::ops::Bound;

const PHYSICAL_SCAN_CHUNK_ENTRIES: usize = 64;

///
/// KeyOrderState
///
/// Explicit ordering state for key vectors produced by one access-path resolver.
/// This keeps normalization behavior local and avoids implicit path-shape proxies.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyOrderState {
    FinalOrder,
    AscendingSorted,
    Unordered,
}

///
/// StructuralPhysicalStreamRequest
///
/// StructuralPhysicalStreamRequest is the generic-free physical access request
/// used by structural traversal and erased runtime execution.
/// It carries direct store/index authority plus one entity tag so physical scan
/// leaves do not need typed `Context<'_, E>` recovery.
///

pub(super) struct StructuralPhysicalStreamRequest<'a> {
    pub(super) store: StoreHandle,
    pub(super) entity_tag: EntityTag,
    pub(super) index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    pub(super) index_range_spec: Option<&'a LoweredIndexRangeSpec>,
    pub(super) continuation: AccessScanContinuationInput<'a>,
    pub(super) physical_fetch_hint: Option<usize>,
    pub(super) index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    pub(super) preserve_leaf_index_order: bool,
}

///
/// PhysicalStreamBindings
///
/// Structural physical-resolution inputs shared by all entity-specific
/// resolvers.
/// This excludes the typed executor context so the outer dispatch body can
/// collapse to one key-shape-specific implementation.
///

#[derive(Clone, Copy)]
struct PhysicalStreamBindings<'a> {
    index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    index_range_spec: Option<&'a LoweredIndexRangeSpec>,
    continuation: AccessScanContinuationInput<'a>,
    physical_fetch_hint: Option<usize>,
    index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    preserve_leaf_index_order: bool,
}

// Keep the historical physical-path invariant name stable for CI checks while
// routing the actual contract enforcement through the traversal owner.
fn require_index_range_spec(
    index_range_spec: Option<&LoweredIndexRangeSpec>,
) -> Result<&LoweredIndexRangeSpec, InternalError> {
    IndexRangeTraversalContract::require_spec(index_range_spec)
}

///
/// KeyAccessRuntime
///
/// KeyAccessRuntime binds one recovered typed context to the
/// structural planner-key boundary used by structural fast-path traversal.
/// It recovers typed primary-key values only inside physical leaf resolution.
///

struct KeyAccessRuntime {
    store: StoreHandle,
    entity_tag: EntityTag,
}

impl KeyAccessRuntime {
    const fn new(store: StoreHandle, entity_tag: EntityTag) -> Self {
        Self { store, entity_tag }
    }

    // Resolve one direct primary-key lookup into its canonical ordered output.
    fn resolve_by_key(&self, key: Value) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        Ok((
            vec![DataKey::try_from_structural_key(self.entity_tag, &key)?],
            KeyOrderState::FinalOrder,
        ))
    }

    // Resolve one multi-key primary lookup into canonical ascending key order.
    fn resolve_by_keys(
        &self,
        keys: &[Value],
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let mut data_keys = Vec::with_capacity(keys.len());
        for key in keys {
            data_keys.push(DataKey::try_from_structural_key(self.entity_tag, key)?);
        }
        data_keys.sort_unstable();
        data_keys.dedup();

        Ok((data_keys, KeyOrderState::AscendingSorted))
    }

    // Resolve one primary-key range scan as a dynamic ordered stream.
    fn resolve_key_range_stream(
        &self,
        start: Value,
        end: Value,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let start = DataKey::try_from_structural_key(self.entity_tag, &start)?;
        let end = DataKey::try_from_structural_key(self.entity_tag, &end)?;

        Ok(OrderedKeyStreamBox::primary_range(
            PrimaryRangeKeyStream::new(self.store, start, end, direction, primary_scan_fetch_hint)?,
        ))
    }

    // Resolve one full primary-key scan as a dynamic ordered stream.
    fn resolve_full_scan_stream(
        &self,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let start = DataKey::lower_bound_for(self.entity_tag);
        let end = DataKey::upper_bound_for(self.entity_tag);

        Ok(OrderedKeyStreamBox::primary_range(
            PrimaryRangeKeyStream::new(self.store, start, end, direction, primary_scan_fetch_hint)?,
        ))
    }

    // Resolve one single-prefix secondary-index scan.
    fn resolve_index_prefix(
        &self,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        direction: Direction,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let [spec] = index_prefix_specs else {
            return Err(InternalError::query_executor_invariant(
                "index-prefix execution requires pre-lowered index-prefix spec",
            ));
        };

        let keys = IndexScan::prefix_structural(
            self.store,
            self.entity_tag,
            spec,
            direction,
            index_fetch_hint.unwrap_or(usize::MAX),
            index_predicate_execution,
        )?;
        let key_order_state = if index_fetch_hint.is_some() {
            KeyOrderState::FinalOrder
        } else {
            KeyOrderState::Unordered
        };

        Ok((keys, key_order_state))
    }

    // Resolve one single-prefix secondary-index scan as a dynamic ordered stream.
    fn resolve_index_prefix_stream(
        &self,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        direction: Direction,
        index_fetch_hint: Option<usize>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let [spec] = index_prefix_specs else {
            return Err(InternalError::query_executor_invariant(
                "index-prefix execution requires pre-lowered index-prefix spec",
            ));
        };

        Ok(OrderedKeyStreamBox::index_range(
            IndexRangeKeyStream::from_prefix(
                self.store,
                self.entity_tag,
                spec,
                direction,
                index_fetch_hint,
            ),
        ))
    }

    // Resolve one multi-lookup secondary-index scan and normalize duplicates.
    fn resolve_index_multi_lookup(
        &self,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        value_count: usize,
        direction: Direction,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        if index_prefix_specs.len() != value_count {
            return Err(InternalError::query_executor_invariant(
                "index-multi-lookup execution requires one pre-lowered prefix spec per lookup value",
            ));
        }

        let mut keys = Vec::new();
        for spec in index_prefix_specs {
            keys.extend(IndexScan::prefix_structural(
                self.store,
                self.entity_tag,
                spec,
                direction,
                usize::MAX,
                index_predicate_execution,
            )?);
        }
        keys.sort_unstable();
        keys.dedup();

        Ok((keys, KeyOrderState::AscendingSorted))
    }

    // Resolve one secondary-index range scan.
    fn resolve_index_range(
        &self,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        continuation: IndexScanContinuationInput<'_>,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let spec = require_index_range_spec(index_range_spec)?;
        let fetch_limit = index_fetch_hint.unwrap_or(usize::MAX);
        let keys = IndexScan::range_structural(
            self.store,
            self.entity_tag,
            spec,
            continuation,
            fetch_limit,
            index_predicate_execution,
        )?;
        let key_order_state = if index_fetch_hint.is_some() {
            KeyOrderState::FinalOrder
        } else {
            KeyOrderState::Unordered
        };

        Ok((keys, key_order_state))
    }

    // Resolve one secondary-index range scan as a dynamic ordered stream.
    fn resolve_index_range_stream(
        &self,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        continuation: IndexScanContinuationInput<'_>,
        index_fetch_hint: Option<usize>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let spec = require_index_range_spec(index_range_spec)?;

        Ok(OrderedKeyStreamBox::index_range(
            IndexRangeKeyStream::from_range(
                self.store,
                self.entity_tag,
                spec,
                continuation,
                index_fetch_hint,
            ),
        ))
    }
}

///
/// PrimaryRangeKeyStream
///
/// PrimaryRangeKeyStream incrementally resolves one primary-key data-store
/// range.
/// It owns only raw range bounds and a small decoded-key buffer so callers can
/// consume primary scans without materializing every candidate key up front.
///

pub(in crate::db::executor) struct PrimaryRangeKeyStream {
    store: StoreHandle,
    lower_raw: RawDataKey,
    upper_raw: RawDataKey,
    direction: Direction,
    remaining: Option<usize>,
    last_raw_key: Option<RawDataKey>,
    buffer: Vec<DataKey>,
    buffer_pos: usize,
    exhausted: bool,
}

impl PrimaryRangeKeyStream {
    // Build one primary stream from validated structural data keys.
    pub(in crate::db::executor) fn new(
        store: StoreHandle,
        start: DataKey,
        end: DataKey,
        direction: Direction,
        limit: Option<usize>,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            store,
            lower_raw: start.to_raw()?,
            upper_raw: end.to_raw()?,
            direction,
            remaining: limit,
            last_raw_key: None,
            buffer: Vec::new(),
            buffer_pos: 0,
            exhausted: false,
        })
    }

    // Return the maximum number of keys to read during the next store borrow.
    fn next_chunk_limit(&self) -> usize {
        self.remaining
            .unwrap_or(PHYSICAL_SCAN_CHUNK_ENTRIES)
            .min(PHYSICAL_SCAN_CHUNK_ENTRIES)
    }

    // Re-enter the data store for one bounded range chunk.
    fn load_next_chunk(&mut self) -> Result<(), InternalError> {
        let chunk_limit = self.next_chunk_limit();
        if self.exhausted || chunk_limit == 0 {
            self.exhausted = true;
            return Ok(());
        }

        let (keys, last_raw_key) = self.store.with_data(|store| {
            let mut keys = Vec::with_capacity(chunk_limit);
            let mut last_raw_key = None;

            match self.direction {
                Direction::Asc => {
                    let lower = self
                        .last_raw_key
                        .map_or(Bound::Included(self.lower_raw), Bound::Excluded);
                    for entry in store.range((lower, Bound::Included(self.upper_raw))) {
                        let raw_key = *entry.key();
                        keys.push(PrimaryScan::decode_data_key(&raw_key)?);
                        last_raw_key = Some(raw_key);
                        if keys.len() == chunk_limit {
                            break;
                        }
                    }
                }
                Direction::Desc => {
                    let upper = self
                        .last_raw_key
                        .map_or(Bound::Included(self.upper_raw), Bound::Excluded);
                    for entry in store.range((Bound::Included(self.lower_raw), upper)).rev() {
                        let raw_key = *entry.key();
                        keys.push(PrimaryScan::decode_data_key(&raw_key)?);
                        last_raw_key = Some(raw_key);
                        if keys.len() == chunk_limit {
                            break;
                        }
                    }
                }
            }

            Ok::<_, InternalError>((keys, last_raw_key))
        })?;

        let emitted = keys.len();
        self.buffer = keys;
        self.buffer_pos = 0;

        if let Some(raw_key) = last_raw_key {
            self.last_raw_key = Some(raw_key);
        } else {
            self.exhausted = true;
        }

        if let Some(remaining) = self.remaining.as_mut() {
            *remaining = remaining.saturating_sub(emitted);
            if *remaining == 0 {
                self.exhausted = true;
            }
        }

        if emitted < chunk_limit {
            self.exhausted = true;
        }

        Ok(())
    }
}

impl OrderedKeyStream for PrimaryRangeKeyStream {
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        if self.buffer_pos == self.buffer.len() {
            self.load_next_chunk()?;
        }
        if self.buffer_pos == self.buffer.len() {
            return Ok(None);
        }

        let key = self.buffer[self.buffer_pos].clone();
        self.buffer_pos += 1;

        Ok(Some(key))
    }

    fn cheap_access_candidate_count_hint(&self) -> Option<usize> {
        if self.remaining.is_some() {
            return None;
        }

        None
    }

    fn exact_diagnostic_access_candidate_count(&self) -> Option<usize> {
        if self.remaining.is_some() {
            return None;
        }

        Some(self.store.with_data(|store| {
            store
                .range((
                    Bound::Included(self.lower_raw),
                    Bound::Included(self.upper_raw),
                ))
                .count()
        }))
    }
}

///
/// IndexRangeKeyStream
///
/// IndexRangeKeyStream incrementally resolves one lowered secondary-index
/// range when physical index order is already the final caller-visible order.
/// Cases that still require `DataKey` sorting, deduplication, or residual
/// index-predicate filtering intentionally stay on the materialized fallback.
///

pub(in crate::db::executor) struct IndexRangeKeyStream {
    store: StoreHandle,
    entity_tag: EntityTag,
    index: LoweredIndexScanContract,
    lower: Bound<LoweredKey>,
    upper: Bound<LoweredKey>,
    direction: Direction,
    anchor: Option<RawIndexKey>,
    remaining: Option<usize>,
    buffer: Vec<DataKey>,
    buffer_pos: usize,
    exhausted: bool,
}

impl IndexRangeKeyStream {
    // Build one index stream from a lowered prefix envelope.
    fn from_prefix(
        store: StoreHandle,
        entity_tag: EntityTag,
        spec: &LoweredIndexPrefixSpec,
        direction: Direction,
        limit: Option<usize>,
    ) -> Self {
        Self::new(
            store,
            entity_tag,
            spec.scan_contract(),
            spec.lower().clone(),
            spec.upper().clone(),
            direction,
            None,
            limit,
        )
    }

    // Build one index stream from a lowered range envelope and continuation.
    fn from_range(
        store: StoreHandle,
        entity_tag: EntityTag,
        spec: &LoweredIndexRangeSpec,
        continuation: IndexScanContinuationInput<'_>,
        limit: Option<usize>,
    ) -> Self {
        Self::new(
            store,
            entity_tag,
            spec.scan_contract(),
            spec.lower().clone(),
            spec.upper().clone(),
            continuation.direction(),
            continuation.anchor().cloned(),
            limit,
        )
    }

    #[expect(clippy::too_many_arguments)]
    const fn new(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: LoweredIndexScanContract,
        lower: Bound<LoweredKey>,
        upper: Bound<LoweredKey>,
        direction: Direction,
        anchor: Option<RawIndexKey>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            store,
            entity_tag,
            index,
            lower,
            upper,
            direction,
            anchor,
            remaining: limit,
            buffer: Vec::new(),
            buffer_pos: 0,
            exhausted: false,
        }
    }

    // Return the remaining output-key budget for the next raw-index chunk.
    const fn next_output_limit(&self) -> Option<usize> {
        self.remaining
    }

    // Re-enter the index store for one bounded raw-index chunk.
    fn load_next_chunk(&mut self) -> Result<(), InternalError> {
        if self.exhausted || matches!(self.remaining, Some(0)) {
            self.exhausted = true;
            return Ok(());
        }

        let continuation = IndexScanContinuationInput::new(self.anchor.as_ref(), self.direction);
        let chunk = IndexScan::chunk_structural(
            self.store,
            self.entity_tag,
            self.index.clone(),
            &self.lower,
            &self.upper,
            continuation,
            PHYSICAL_SCAN_CHUNK_ENTRIES,
            self.next_output_limit(),
        )?;
        let (keys, last_raw_key) = chunk.into_parts();
        let emitted = keys.len();
        self.buffer = keys;
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
}

impl OrderedKeyStream for IndexRangeKeyStream {
    fn next_key(&mut self) -> Result<Option<DataKey>, InternalError> {
        while self.buffer_pos == self.buffer.len() && !self.exhausted {
            self.load_next_chunk()?;
        }
        if self.buffer_pos == self.buffer.len() {
            return Ok(None);
        }

        let key = self.buffer[self.buffer_pos].clone();
        self.buffer_pos += 1;

        Ok(Some(key))
    }
}

// Normalize key ordering according to explicit resolver output state.
fn normalize_ordered_keys(
    keys: &mut [DataKey],
    direction: Direction,
    key_order_state: KeyOrderState,
) {
    match key_order_state {
        KeyOrderState::FinalOrder => {}
        KeyOrderState::AscendingSorted => {
            if matches!(direction, Direction::Desc) {
                keys.reverse();
            }
        }
        KeyOrderState::Unordered => {
            keys.sort_unstable();
            if matches!(direction, Direction::Desc) {
                keys.reverse();
            }
        }
    }
}

// Return whether one secondary-index path can preserve raw index traversal
// order directly instead of materializing to sort or deduplicate `DataKey`s.
const fn index_path_can_stream_in_final_order(request: PhysicalStreamBindings<'_>) -> bool {
    request.index_predicate_execution.is_none()
        && (request.preserve_leaf_index_order || request.physical_fetch_hint.is_some())
}

// Resolve one physical access path by dispatching only the coarse path shape
// through the runtime leaf boundary.
fn resolve_physical_key_stream(
    path: &ExecutionPathPayload<'_, Value>,
    request: PhysicalStreamBindings<'_>,
    runtime: &KeyAccessRuntime,
) -> Result<OrderedKeyStreamBox, InternalError> {
    let path_capabilities = path.capabilities();
    let primary_scan_fetch_hint = if primary_scan_fetch_hint_shape_supported(&path_capabilities) {
        request.physical_fetch_hint
    } else {
        None
    };

    let (mut candidates, mut key_order_state) = match path {
        ExecutionPathPayload::ByKey(key) => runtime.resolve_by_key((*key).clone())?,
        ExecutionPathPayload::ByKeys(keys) => runtime.resolve_by_keys(keys)?,
        ExecutionPathPayload::KeyRange { start, end } => {
            return runtime.resolve_key_range_stream(
                (*start).clone(),
                (*end).clone(),
                request.continuation.direction(),
                primary_scan_fetch_hint,
            );
        }
        ExecutionPathPayload::FullScan => {
            return runtime.resolve_full_scan_stream(
                request.continuation.direction(),
                primary_scan_fetch_hint,
            );
        }
        ExecutionPathPayload::IndexPrefix { .. } => {
            if index_path_can_stream_in_final_order(request) {
                return runtime.resolve_index_prefix_stream(
                    request.index_prefix_specs,
                    request.continuation.direction(),
                    request.physical_fetch_hint,
                );
            }

            runtime.resolve_index_prefix(
                request.index_prefix_specs,
                request.continuation.direction(),
                request.physical_fetch_hint,
                request.index_predicate_execution,
            )?
        }
        ExecutionPathPayload::IndexMultiLookup { value_count, .. } => runtime
            .resolve_index_multi_lookup(
                request.index_prefix_specs,
                *value_count,
                request.continuation.direction(),
                request.index_predicate_execution,
            )?,
        ExecutionPathPayload::IndexRange { .. } => {
            if index_path_can_stream_in_final_order(request) {
                return runtime.resolve_index_range_stream(
                    request.index_range_spec,
                    request.continuation.index_scan_continuation(),
                    request.physical_fetch_hint,
                );
            }

            runtime.resolve_index_range(
                request.index_range_spec,
                request.continuation.index_scan_continuation(),
                request.physical_fetch_hint,
                request.index_predicate_execution,
            )?
        }
    };

    // Top-level single-path secondary-index scans must preserve physical index
    // traversal order so route-owned secondary ORDER BY contracts can drive
    // paging without an extra materialized reorder. Composite child streams
    // still disable this flag so merge/intersection reducers continue to
    // consume canonical `DataKey` order.
    if request.preserve_leaf_index_order
        && matches!(
            path,
            ExecutionPathPayload::IndexPrefix { .. } | ExecutionPathPayload::IndexRange { .. }
        )
        && matches!(key_order_state, KeyOrderState::Unordered)
    {
        key_order_state = KeyOrderState::FinalOrder;
    }

    normalize_ordered_keys(
        &mut candidates,
        request.continuation.direction(),
        key_order_state,
    );

    Ok(ordered_key_stream_from_materialized_keys(candidates))
}

impl ExecutionPathPayload<'_, Value> {
    // Physical access lowering for one structural executable access path.
    // Typed key recovery is deferred to the concrete path leaves in the
    // structural runtime adapter.
    /// Build an ordered key stream for one structural access path.
    pub(super) fn resolve_structural_physical_key_stream(
        &self,
        request: StructuralPhysicalStreamRequest<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let runtime = KeyAccessRuntime::new(request.store, request.entity_tag);
        let bindings = PhysicalStreamBindings {
            index_prefix_specs: request.index_prefix_specs,
            index_range_spec: request.index_range_spec,
            continuation: request.continuation,
            physical_fetch_hint: request.physical_fetch_hint,
            index_predicate_execution: request.index_predicate_execution,
            preserve_leaf_index_order: request.preserve_leaf_index_order,
        };

        resolve_physical_key_stream(self, bindings, &runtime)
    }
}
