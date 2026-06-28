//! Module: executor::stream::access::physical
//! Responsibility: lower executable access-path payloads into physical key streams.
//! Does not own: planner eligibility decisions or post-access semantics.
//! Boundary: physical key resolution through primary/index scan adapters.

use crate::{
    db::{
        access::{ExecutionPathPayload, IndexShapeDetails},
        cursor::{CursorBoundary, CursorBoundarySlot, IndexScanContinuationInput},
        data::{
            DecodedDataStoreKey, RawDataStoreKey, StoreVisit,
            primary_key_value_from_structural_value,
        },
        direction::Direction,
        executor::{
            ACCESS_SCAN_CHUNK_ENTRIES, AccessStreamExecutionPolicy, IndexLeafOrderPolicy,
            IndexScan, LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredKey, OrderedKeyStream,
            OrderedKeyStreamBox, PrefixSetExecutionShape, PrefixSetMergeSafety, PrimaryScan,
            active_lowered_index_prefix_specs, apply_index_scan_chunk_progress,
            branch_stream_chunk_entries, expand_index_prefix_family_with_exact_child_prefixes,
            index_predicate_rejects_prefix_components, index_stream_chunk_entries_for_remaining,
            index_stream_output_limit_for_chunk, lowered_index_prefix_is_proven_empty,
            ordered_key_stream_from_materialized_keys,
            pipeline::contracts::AccessScanContinuationInput, route::IndexPrefixChildExpansionHint,
            route::primary_scan_fetch_hint_shape_supported, stream::key::KeyOrderComparator,
            traversal::IndexRangeTraversalContract,
        },
        index::{IndexKey, RawIndexStoreKey, predicate::IndexPredicateExecution},
        key_taxonomy::RawDataStoreKeyRange,
        registry::StoreHandle,
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};
use std::ops::Bound;

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

enum PhysicalKeyResolution {
    Stream(Box<OrderedKeyStreamBox>),
    Materialized {
        candidates: Vec<DecodedDataStoreKey>,
        key_order_state: KeyOrderState,
    },
}

#[derive(Clone, Copy)]
enum PrefixMergeResumePolicy {
    None,
    PrimaryKeySuffix,
}

impl PrefixMergeResumePolicy {
    const fn from_index_leaf_order_policy(policy: IndexLeafOrderPolicy) -> Self {
        match policy {
            IndexLeafOrderPolicy::CanonicalKeyOrder => Self::None,
            IndexLeafOrderPolicy::PreservePhysicalLeafOrder => Self::PrimaryKeySuffix,
        }
    }
}

///
/// MergedIndexPrefixStreamSpec
///
/// Runtime-local contract for one family of exact secondary-index prefix
/// streams that may be merged by decoded primary key.
///

#[derive(Clone, Copy)]
struct MergedIndexPrefixStreamSpec<'a> {
    index: &'a IndexShapeDetails,
    index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    continuation: AccessScanContinuationInput<'a>,
    index_fetch_hint: Option<usize>,
    resume_policy: PrefixMergeResumePolicy,
}

impl<'a> MergedIndexPrefixStreamSpec<'a> {
    const fn new(
        index: &'a IndexShapeDetails,
        index_prefix_specs: &'a [LoweredIndexPrefixSpec],
        continuation: AccessScanContinuationInput<'a>,
        index_fetch_hint: Option<usize>,
        resume_policy: PrefixMergeResumePolicy,
    ) -> Self {
        Self {
            index,
            index_prefix_specs,
            continuation,
            index_fetch_hint,
            resume_policy,
        }
    }

    fn resume_anchor_for(
        self,
        spec: &LoweredIndexPrefixSpec,
    ) -> Result<Option<RawIndexStoreKey>, InternalError> {
        match self.resume_policy {
            PrefixMergeResumePolicy::None => Ok(None),
            PrefixMergeResumePolicy::PrimaryKeySuffix => self
                .continuation
                .primary_key_boundary()
                .map(|boundary| {
                    primary_key_suffix_resume_anchor_for_prefix(self.index, spec, boundary)
                })
                .transpose(),
        }
    }
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
    pub(super) execution_policy: AccessStreamExecutionPolicy,
    pub(super) index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    pub(super) index_prefix_child_expansion: Option<IndexPrefixChildExpansionHint>,
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
    execution_policy: AccessStreamExecutionPolicy,
    index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    index_prefix_child_expansion: Option<IndexPrefixChildExpansionHint>,
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
    fn resolve_by_key(
        &self,
        key: Value,
    ) -> Result<(Vec<DecodedDataStoreKey>, KeyOrderState), InternalError> {
        Ok((
            vec![DecodedDataStoreKey::try_from_structural_key(
                self.entity_tag,
                &key,
            )?],
            KeyOrderState::FinalOrder,
        ))
    }

    // Resolve one multi-key primary lookup into canonical ascending key order.
    fn resolve_by_keys(
        &self,
        keys: &[Value],
    ) -> Result<(Vec<DecodedDataStoreKey>, KeyOrderState), InternalError> {
        let mut data_keys = Vec::with_capacity(keys.len());
        for key in keys {
            data_keys.push(DecodedDataStoreKey::try_from_structural_key(
                self.entity_tag,
                key,
            )?);
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
        let start = DecodedDataStoreKey::try_from_structural_key(self.entity_tag, &start)?;
        let end = DecodedDataStoreKey::try_from_structural_key(self.entity_tag, &end)?;

        Ok(OrderedKeyStreamBox::primary_range(
            PrimaryRangeKeyStream::new(self.store, start, end, direction, primary_scan_fetch_hint)?,
        ))
    }

    // Resolve one full primary-key scan as a dynamic ordered stream.
    fn resolve_full_scan_stream(
        &self,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> OrderedKeyStreamBox {
        OrderedKeyStreamBox::primary_range(PrimaryRangeKeyStream::new_full_scan(
            self.store,
            self.entity_tag,
            direction,
            primary_scan_fetch_hint,
        ))
    }

    // Resolve one single-prefix secondary-index scan.
    fn resolve_index_prefix(
        &self,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        direction: Direction,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DecodedDataStoreKey>, KeyOrderState), InternalError> {
        let [spec] = index_prefix_specs else {
            return Err(InternalError::query_executor_invariant());
        };
        let key_order_state = if index_fetch_hint.is_some() {
            KeyOrderState::FinalOrder
        } else {
            KeyOrderState::Unordered
        };
        if index_predicate_rejects_prefix_components(
            spec.prefix_components(),
            index_predicate_execution,
        ) {
            return Ok((Vec::new(), key_order_state));
        }
        if lowered_index_prefix_is_proven_empty(self.store, spec) {
            return Ok((Vec::new(), key_order_state));
        }

        let keys = IndexScan::prefix_structural(
            self.store,
            self.entity_tag,
            spec,
            direction,
            index_fetch_hint.unwrap_or(usize::MAX),
            index_predicate_execution,
        )?;

        Ok((keys, key_order_state))
    }

    // Resolve one single-prefix secondary-index scan as a dynamic ordered stream.
    fn resolve_index_prefix_stream(
        &self,
        index: &IndexShapeDetails,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        continuation: AccessScanContinuationInput<'_>,
        index_fetch_hint: Option<usize>,
        index_leaf_order_policy: IndexLeafOrderPolicy,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        self.resolve_index_prefix_family_stream(
            index,
            index_prefix_specs,
            1,
            continuation,
            index_fetch_hint,
            PrefixMergeResumePolicy::from_index_leaf_order_policy(index_leaf_order_policy),
        )
    }

    // Resolve a branch-aware composite prefix scan as lazily merged dynamic
    // prefix streams. Each branch is internally ordered by the primary-key
    // suffix after the fixed index prefix, and the merge stream suppresses
    // duplicate decoded primary keys defensively.
    fn resolve_index_branch_set_stream(
        &self,
        index: &IndexShapeDetails,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        branch_count: usize,
        continuation: AccessScanContinuationInput<'_>,
        index_fetch_hint: Option<usize>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        self.resolve_index_prefix_family_stream(
            index,
            index_prefix_specs,
            branch_count,
            continuation,
            index_fetch_hint,
            PrefixMergeResumePolicy::PrimaryKeySuffix,
        )
    }

    // Resolve one multi-lookup secondary-index scan and normalize duplicates.
    fn resolve_index_multi_lookup(
        &self,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        value_count: usize,
        direction: Direction,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DecodedDataStoreKey>, KeyOrderState), InternalError> {
        validate_index_prefix_count(index_prefix_specs, value_count)?;

        let per_prefix_limit = index_fetch_hint.unwrap_or(usize::MAX);
        let mut keys = Vec::new();
        for spec in active_lowered_index_prefix_specs(
            Some(self.store),
            index_prefix_specs,
            index_predicate_execution,
        ) {
            keys.extend(IndexScan::prefix_structural(
                self.store,
                self.entity_tag,
                spec,
                direction,
                per_prefix_limit,
                index_predicate_execution,
            )?);
        }
        keys.sort_unstable();
        keys.dedup();

        Ok((keys, KeyOrderState::AscendingSorted))
    }

    // Resolve one multi-lookup secondary-index scan as lazily merged prefix streams.
    fn resolve_index_multi_lookup_stream(
        &self,
        index: &IndexShapeDetails,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        value_count: usize,
        continuation: AccessScanContinuationInput<'_>,
        index_fetch_hint: Option<usize>,
        index_leaf_order_policy: IndexLeafOrderPolicy,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        self.resolve_index_prefix_family_stream(
            index,
            index_prefix_specs,
            value_count,
            continuation,
            index_fetch_hint,
            PrefixMergeResumePolicy::from_index_leaf_order_policy(index_leaf_order_policy),
        )
    }

    fn resolve_index_prefix_family_stream(
        &self,
        index: &IndexShapeDetails,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        expected_prefix_count: usize,
        continuation: AccessScanContinuationInput<'_>,
        index_fetch_hint: Option<usize>,
        resume_policy: PrefixMergeResumePolicy,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        validate_index_prefix_count(index_prefix_specs, expected_prefix_count)?;
        let spec = MergedIndexPrefixStreamSpec::new(
            index,
            index_prefix_specs,
            continuation,
            index_fetch_hint,
            resume_policy,
        );

        self.resolve_merged_index_prefix_streams(spec)
    }

    fn expanded_index_multi_lookup_stream(
        &self,
        index: &IndexShapeDetails,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        value_count: usize,
        continuation: AccessScanContinuationInput<'_>,
        index_fetch_hint: Option<usize>,
        expansion: IndexPrefixChildExpansionHint,
    ) -> Result<Option<OrderedKeyStreamBox>, InternalError> {
        validate_index_prefix_count(index_prefix_specs, value_count)?;

        let Some(expanded_family) = expand_index_prefix_family_with_exact_child_prefixes(
            self.store,
            self.entity_tag,
            index,
            index_prefix_specs,
            expansion,
        )?
        else {
            return Ok(None);
        };
        if expanded_family.specs().is_empty() {
            return Ok(Some(ordered_key_stream_from_materialized_keys(Vec::new())));
        }

        self.resolve_merged_index_prefix_streams(MergedIndexPrefixStreamSpec::new(
            expanded_family.index(),
            expanded_family.specs(),
            continuation,
            index_fetch_hint,
            PrefixMergeResumePolicy::PrimaryKeySuffix,
        ))
        .map(Some)
    }

    fn resolve_merged_index_prefix_streams(
        &self,
        request: MergedIndexPrefixStreamSpec<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        if request.index_prefix_specs.is_empty() {
            return Ok(ordered_key_stream_from_materialized_keys(Vec::new()));
        }

        let active_specs =
            active_lowered_index_prefix_specs(Some(self.store), request.index_prefix_specs, None);
        match PrefixSetExecutionShape::from_active_prefixes(
            active_specs,
            PrefixSetMergeSafety::OrderedMergeSafe,
        ) {
            PrefixSetExecutionShape::Empty => {
                Ok(ordered_key_stream_from_materialized_keys(Vec::new()))
            }
            PrefixSetExecutionShape::Single(spec) => self.index_prefix_stream(request, spec, 1),
            PrefixSetExecutionShape::OrderedMerge(active_specs) => {
                let branch_count = active_specs.len();
                let mut streams = Vec::with_capacity(branch_count);
                for spec in active_specs {
                    streams.push(self.index_prefix_stream(request, spec, branch_count)?);
                }

                Ok(OrderedKeyStreamBox::merge_all(
                    streams,
                    KeyOrderComparator::from_direction(request.continuation.direction()),
                ))
            }
            PrefixSetExecutionShape::Materialized(_) => {
                Err(InternalError::query_executor_invariant())
            }
        }
    }

    fn index_prefix_stream(
        &self,
        request: MergedIndexPrefixStreamSpec<'_>,
        spec: &LoweredIndexPrefixSpec,
        active_branch_count: usize,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let branch_chunk_entries =
            branch_stream_chunk_entries(request.index_fetch_hint, active_branch_count);
        let resume_anchor = request.resume_anchor_for(spec)?;

        Ok(OrderedKeyStreamBox::index_range(
            IndexRangeKeyStream::from_prefix(
                self.store,
                self.entity_tag,
                spec,
                request.continuation.direction(),
                resume_anchor,
                request.index_fetch_hint,
                branch_chunk_entries,
            ),
        ))
    }

    // Resolve one secondary-index range scan.
    fn resolve_index_range(
        &self,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        continuation: IndexScanContinuationInput<'_>,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DecodedDataStoreKey>, KeyOrderState), InternalError> {
        let spec = require_index_range_spec(index_range_spec)?;
        let fetch_limit = index_fetch_hint.unwrap_or(usize::MAX);
        let key_order_state = if index_fetch_hint.is_some() {
            KeyOrderState::FinalOrder
        } else {
            KeyOrderState::Unordered
        };

        let keys = IndexScan::range_structural(
            self.store,
            self.entity_tag,
            spec,
            continuation,
            fetch_limit,
            index_predicate_execution,
        )?;

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

fn primary_key_suffix_resume_anchor_for_prefix(
    index: &IndexShapeDetails,
    spec: &LoweredIndexPrefixSpec,
    primary_key_boundary: &CursorBoundary,
) -> Result<RawIndexStoreKey, InternalError> {
    let prefix_len = index.slot_arity();
    let key_arity = index.key_arity();
    if prefix_len > key_arity {
        return Err(InternalError::query_executor_invariant());
    }

    let prefix_start = lowered_prefix_start_key(spec)?;
    if prefix_start.component_count() != key_arity {
        return Err(InternalError::query_executor_invariant());
    }
    if prefix_len == key_arity {
        let (primary_key, _values) =
            primary_key_suffix_values(primary_key_boundary, primary_key_boundary.slots.len())?;
        return Ok(
            IndexKey::new_from_existing_prefix_and_suffix_values_with_primary_key_value(
                &prefix_start,
                prefix_len,
                &[],
                &primary_key,
            )?
            .to_raw()?,
        );
    }

    let suffix_len = key_arity.saturating_sub(prefix_len);
    let (primary_key, suffix_values) = primary_key_suffix_values(primary_key_boundary, suffix_len)?;

    // Prefix-family continuation is valid only when route planning has proven
    // that the remaining index suffix is exactly the primary key. Fill that
    // suffix from the cursor boundary so each prefix stream resumes at the
    // same global primary-key position.
    Ok(
        IndexKey::new_from_existing_prefix_and_suffix_values_with_primary_key_value(
            &prefix_start,
            prefix_len,
            suffix_values.as_slice(),
            &primary_key,
        )?
        .to_raw()?,
    )
}

fn lowered_prefix_start_key(spec: &LoweredIndexPrefixSpec) -> Result<IndexKey, InternalError> {
    let Bound::Included(raw_key) = spec.lower() else {
        return Err(InternalError::query_executor_invariant());
    };

    IndexKey::try_from_raw(raw_key).map_err(|_err| InternalError::query_executor_invariant())
}

fn primary_key_suffix_values(
    boundary: &CursorBoundary,
    suffix_len: usize,
) -> Result<(crate::db::PrimaryKeyValue, Vec<Value>), InternalError> {
    if boundary.slots.len() != suffix_len {
        return Err(InternalError::query_executor_invariant());
    }

    let values = boundary
        .slots
        .iter()
        .map(|slot| match slot {
            CursorBoundarySlot::Present(value) => Ok(value.clone()),
            CursorBoundarySlot::Missing => Err(InternalError::query_executor_invariant()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let primary_key = if let [value] = values.as_slice() {
        primary_key_value_from_structural_value(value)?
    } else {
        primary_key_value_from_structural_value(&Value::List(values.clone()))?
    };

    Ok((primary_key, values))
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
    lower_raw: RawDataStoreKey,
    upper_bound: Bound<RawDataStoreKey>,
    direction: Direction,
    remaining: Option<usize>,
    last_raw_key: Option<RawDataStoreKey>,
    buffer: Vec<DecodedDataStoreKey>,
    buffer_pos: usize,
    exhausted: bool,
}

impl PrimaryRangeKeyStream {
    // Build one primary stream from validated structural data keys.
    pub(in crate::db::executor) fn new(
        store: StoreHandle,
        start: DecodedDataStoreKey,
        end: DecodedDataStoreKey,
        direction: Direction,
        limit: Option<usize>,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            store,
            lower_raw: start.to_raw()?,
            upper_bound: Bound::Included(end.to_raw()?),
            direction,
            remaining: limit,
            last_raw_key: None,
            buffer: Vec::new(),
            buffer_pos: 0,
            exhausted: false,
        })
    }

    // Build one primary stream over all rows for one entity using compact
    // raw-prefix bounds rather than synthetic primary-key sentinels.
    pub(in crate::db::executor) fn new_full_scan(
        store: StoreHandle,
        entity: EntityTag,
        direction: Direction,
        limit: Option<usize>,
    ) -> Self {
        let range = RawDataStoreKeyRange::entity_prefix(entity);
        let lower_raw = RawDataStoreKey::store_range_lower_key(&range);
        let upper_bound = range
            .upper_exclusive()
            .map(RawDataStoreKey::from_store_range_bound)
            .map_or(Bound::Unbounded, Bound::Excluded);

        Self {
            store,
            lower_raw,
            upper_bound,
            direction,
            remaining: limit,
            last_raw_key: None,
            buffer: Vec::new(),
            buffer_pos: 0,
            exhausted: false,
        }
    }

    // Return the maximum number of keys to read during the next store borrow.
    fn next_chunk_limit(&self) -> usize {
        self.remaining
            .unwrap_or(ACCESS_SCAN_CHUNK_ENTRIES)
            .min(ACCESS_SCAN_CHUNK_ENTRIES)
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
                        .clone()
                        .map_or_else(|| Bound::Included(self.lower_raw.clone()), Bound::Excluded);
                    store.visit_range((lower, self.upper_bound.clone()), |raw_key, _row| {
                        let raw_key = raw_key.clone();
                        keys.push(PrimaryScan::decode_data_key(&raw_key)?);
                        last_raw_key = Some(raw_key);
                        Ok::<StoreVisit, InternalError>(if keys.len() == chunk_limit {
                            StoreVisit::Stop
                        } else {
                            StoreVisit::Continue
                        })
                    })?;
                }
                Direction::Desc => {
                    let upper = self
                        .last_raw_key
                        .clone()
                        .map_or_else(|| self.upper_bound.clone(), Bound::Excluded);
                    store.visit_range_rev(
                        (Bound::Included(self.lower_raw.clone()), upper),
                        |raw_key, _row| {
                            let raw_key = raw_key.clone();
                            keys.push(PrimaryScan::decode_data_key(&raw_key)?);
                            last_raw_key = Some(raw_key);
                            Ok::<StoreVisit, InternalError>(if keys.len() == chunk_limit {
                                StoreVisit::Stop
                            } else {
                                StoreVisit::Continue
                            })
                        },
                    )?;
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
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
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

    #[cfg(test)]
    fn exact_diagnostic_access_candidate_count(&self) -> Option<usize> {
        if self.remaining.is_some() {
            return None;
        }

        Some(self.store.with_data(|store| {
            let mut count = 0usize;
            let _: Result<(), InternalError> = store.visit_range(
                (
                    Bound::Included(self.lower_raw.clone()),
                    self.upper_bound.clone(),
                ),
                |_raw_key, _row| {
                    count = count.saturating_add(1);
                    Ok(StoreVisit::Continue)
                },
            );
            count
        }))
    }
}

///
/// IndexRangeKeyStream
///
/// IndexRangeKeyStream incrementally resolves one lowered secondary-index
/// range when physical index order is already the final caller-visible order.
/// Cases that still require `DecodedDataStoreKey` sorting, deduplication, or residual
/// index-predicate filtering intentionally stay on the materialized fallback.
///

pub(in crate::db::executor) struct IndexRangeKeyStream {
    store: StoreHandle,
    entity_tag: EntityTag,
    lower: Bound<LoweredKey>,
    upper: Bound<LoweredKey>,
    direction: Direction,
    anchor: Option<RawIndexStoreKey>,
    remaining: Option<usize>,
    chunk_entries: usize,
    buffer: Vec<DecodedDataStoreKey>,
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
        anchor: Option<RawIndexStoreKey>,
        limit: Option<usize>,
        chunk_entries: usize,
    ) -> Self {
        Self::new(
            store,
            entity_tag,
            (spec.lower().clone(), spec.upper().clone()),
            direction,
            anchor,
            limit,
            chunk_entries,
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
            (spec.lower().clone(), spec.upper().clone()),
            continuation.direction(),
            continuation.anchor().cloned(),
            limit,
            ACCESS_SCAN_CHUNK_ENTRIES,
        )
    }

    fn new(
        store: StoreHandle,
        entity_tag: EntityTag,
        bounds: (Bound<LoweredKey>, Bound<LoweredKey>),
        direction: Direction,
        anchor: Option<RawIndexStoreKey>,
        limit: Option<usize>,
        chunk_entries: usize,
    ) -> Self {
        let (lower, upper) = bounds;
        Self {
            store,
            entity_tag,
            lower,
            upper,
            direction,
            anchor,
            remaining: limit,
            chunk_entries,
            buffer: Vec::new(),
            buffer_pos: 0,
            exhausted: false,
        }
    }

    // Re-enter the index store for one bounded raw-index chunk.
    fn load_next_chunk(&mut self) -> Result<(), InternalError> {
        if self.exhausted || matches!(self.remaining, Some(0)) {
            self.exhausted = true;
            return Ok(());
        }

        let chunk_entries =
            index_stream_chunk_entries_for_remaining(self.chunk_entries, self.remaining);
        let continuation = IndexScanContinuationInput::new(self.anchor.as_ref(), self.direction);
        let chunk = IndexScan::chunk_structural(
            self.store,
            self.entity_tag,
            &self.lower,
            &self.upper,
            continuation,
            chunk_entries,
            index_stream_output_limit_for_chunk(self.remaining, chunk_entries),
        )?;
        let (keys, last_raw_key) = chunk.into_decoded_keys_and_resume_anchor();
        let emitted = keys.len();
        self.buffer = keys;
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
}

impl OrderedKeyStream for IndexRangeKeyStream {
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
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
    keys: &mut [DecodedDataStoreKey],
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
// order directly instead of materializing to sort or deduplicate `DecodedDataStoreKey`s.
const fn index_path_can_stream_in_final_order(request: PhysicalStreamBindings<'_>) -> bool {
    request.index_predicate_execution.is_none()
        && (request
            .execution_policy
            .index_leaf_order_policy()
            .preserves_leaf_index_order()
            || request.execution_policy.physical_fetch_hint().is_some())
}

fn validate_index_prefix_count(
    index_prefix_specs: &[LoweredIndexPrefixSpec],
    expected_prefix_count: usize,
) -> Result<(), InternalError> {
    if index_prefix_specs.len() != expected_prefix_count {
        return Err(InternalError::query_executor_invariant());
    }

    Ok(())
}

fn resolve_index_multi_lookup_physical_key_stream(
    index: &IndexShapeDetails,
    value_count: usize,
    request: PhysicalStreamBindings<'_>,
    runtime: &KeyAccessRuntime,
) -> Result<PhysicalKeyResolution, InternalError> {
    if let Some(expansion) = request.index_prefix_child_expansion {
        if let Some(stream) = runtime.expanded_index_multi_lookup_stream(
            index,
            request.index_prefix_specs,
            value_count,
            request.continuation,
            request.execution_policy.physical_fetch_hint(),
            expansion,
        )? {
            return Ok(PhysicalKeyResolution::Stream(Box::new(stream)));
        }

        let (candidates, key_order_state) = runtime.resolve_index_multi_lookup(
            request.index_prefix_specs,
            value_count,
            request.continuation.direction(),
            request.execution_policy.physical_fetch_hint(),
            request.index_predicate_execution,
        )?;

        return Ok(PhysicalKeyResolution::Materialized {
            candidates,
            key_order_state,
        });
    }

    if index_path_can_stream_in_final_order(request) {
        return Ok(PhysicalKeyResolution::Stream(Box::new(
            runtime.resolve_index_multi_lookup_stream(
                index,
                request.index_prefix_specs,
                value_count,
                request.continuation,
                request.execution_policy.physical_fetch_hint(),
                request.execution_policy.index_leaf_order_policy(),
            )?,
        )));
    }

    let (candidates, key_order_state) = runtime.resolve_index_multi_lookup(
        request.index_prefix_specs,
        value_count,
        request.continuation.direction(),
        request.execution_policy.physical_fetch_hint(),
        request.index_predicate_execution,
    )?;

    Ok(PhysicalKeyResolution::Materialized {
        candidates,
        key_order_state,
    })
}

fn resolve_index_physical_key_stream(
    path: &ExecutionPathPayload<'_, Value>,
    request: PhysicalStreamBindings<'_>,
    runtime: &KeyAccessRuntime,
) -> Result<PhysicalKeyResolution, InternalError> {
    let (candidates, key_order_state) = match path {
        ExecutionPathPayload::IndexPrefix { index } => {
            if index_path_can_stream_in_final_order(request) {
                return Ok(PhysicalKeyResolution::Stream(Box::new(
                    runtime.resolve_index_prefix_stream(
                        index,
                        request.index_prefix_specs,
                        request.continuation,
                        request.execution_policy.physical_fetch_hint(),
                        request.execution_policy.index_leaf_order_policy(),
                    )?,
                )));
            }

            runtime.resolve_index_prefix(
                request.index_prefix_specs,
                request.continuation.direction(),
                request.execution_policy.physical_fetch_hint(),
                request.index_predicate_execution,
            )?
        }
        ExecutionPathPayload::IndexMultiLookup { index, value_count } => {
            match resolve_index_multi_lookup_physical_key_stream(
                index,
                *value_count,
                request,
                runtime,
            )? {
                PhysicalKeyResolution::Stream(stream) => {
                    return Ok(PhysicalKeyResolution::Stream(stream));
                }
                PhysicalKeyResolution::Materialized {
                    candidates,
                    key_order_state,
                } => (candidates, key_order_state),
            }
        }
        ExecutionPathPayload::IndexBranchSet {
            index,
            branch_count,
        } => {
            if index_path_can_stream_in_final_order(request) {
                return Ok(PhysicalKeyResolution::Stream(Box::new(
                    runtime.resolve_index_branch_set_stream(
                        index,
                        request.index_prefix_specs,
                        *branch_count,
                        request.continuation,
                        request.execution_policy.physical_fetch_hint(),
                    )?,
                )));
            }

            runtime.resolve_index_multi_lookup(
                request.index_prefix_specs,
                *branch_count,
                request.continuation.direction(),
                request.execution_policy.physical_fetch_hint(),
                request.index_predicate_execution,
            )?
        }
        ExecutionPathPayload::IndexRange { .. } => {
            if index_path_can_stream_in_final_order(request) {
                return Ok(PhysicalKeyResolution::Stream(Box::new(
                    runtime.resolve_index_range_stream(
                        request.index_range_spec,
                        request.continuation.index_scan_continuation(),
                        request.execution_policy.physical_fetch_hint(),
                    )?,
                )));
            }

            runtime.resolve_index_range(
                request.index_range_spec,
                request.continuation.index_scan_continuation(),
                request.execution_policy.physical_fetch_hint(),
                request.index_predicate_execution,
            )?
        }
        ExecutionPathPayload::ByKey(_)
        | ExecutionPathPayload::ByKeys(_)
        | ExecutionPathPayload::KeyRange { .. }
        | ExecutionPathPayload::FullScan => return Err(InternalError::query_executor_invariant()),
    };

    Ok(PhysicalKeyResolution::Materialized {
        candidates,
        key_order_state,
    })
}

// Resolve one physical access path by dispatching only the coarse path shape
// through the runtime leaf boundary.
fn resolve_physical_key_stream(
    path: &ExecutionPathPayload<'_, Value>,
    request: PhysicalStreamBindings<'_>,
    runtime: &KeyAccessRuntime,
) -> Result<OrderedKeyStreamBox, InternalError> {
    let path_facts = path.shape_facts();
    let primary_scan_fetch_hint = if primary_scan_fetch_hint_shape_supported(&path_facts) {
        request.execution_policy.physical_fetch_hint()
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
            return Ok(runtime.resolve_full_scan_stream(
                request.continuation.direction(),
                primary_scan_fetch_hint,
            ));
        }
        ExecutionPathPayload::IndexPrefix { .. }
        | ExecutionPathPayload::IndexMultiLookup { .. }
        | ExecutionPathPayload::IndexBranchSet { .. }
        | ExecutionPathPayload::IndexRange { .. } => {
            match resolve_index_physical_key_stream(path, request, runtime)? {
                PhysicalKeyResolution::Stream(stream) => return Ok(*stream),
                PhysicalKeyResolution::Materialized {
                    candidates,
                    key_order_state,
                } => (candidates, key_order_state),
            }
        }
    };

    // Top-level single-path secondary-index scans must preserve physical index
    // traversal order so route-owned secondary ORDER BY contracts can drive
    // paging without an extra materialized reorder. Composite child streams
    // still disable this flag so merge/intersection reducers continue to
    // consume canonical `DecodedDataStoreKey` order.
    if request
        .execution_policy
        .index_leaf_order_policy()
        .preserves_leaf_index_order()
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
            execution_policy: request.execution_policy,
            index_predicate_execution: request.index_predicate_execution,
            index_prefix_child_expansion: request.index_prefix_child_expansion,
        };

        resolve_physical_key_stream(self, bindings, &runtime)
    }
}
