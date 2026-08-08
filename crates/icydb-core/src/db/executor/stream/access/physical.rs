//! Module: executor::stream::access::physical
//! Responsibility: lower executable access-path payloads into physical key streams.
//! Does not own: planner eligibility decisions or post-access semantics.
//! Boundary: physical key resolution through primary/index scan adapters.

use crate::{
    db::{
        access::{ExecutionPathPayload, IndexShapeDetails},
        cursor::{CursorBoundary, CursorBoundarySlot, IndexScanContinuationInput},
        data::{
            DecodedDataStoreKey, RawDataStoreKey, RawRow, StoreVisit,
            primary_key_value_from_structural_value,
        },
        direction::Direction,
        executor::{
            ACCESS_SCAN_CHUNK_ENTRIES, AccessStreamExecutionPolicy, IndexLeafOrderPolicy,
            IndexScan, LoweredIndexPrefixSpec, LoweredIndexRangeSpec, LoweredKey, OrderedKeyStream,
            OrderedKeyStreamBox, PrefixSetExecutionShape, PrefixSetMergeSafety, PrimaryScan,
            active_lowered_index_prefix_specs, apply_index_scan_chunk_progress,
            branch_stream_chunk_entries,
            budget::{charge_current_execution_budget, charge_sort_work},
            expand_index_prefix_family_with_exact_child_prefixes,
            index_predicate_rejects_prefix_components, index_stream_chunk_entries_for_remaining,
            index_stream_output_limit_for_chunk, lowered_index_prefix_liveness,
            ordered_key_stream_from_materialized_keys,
            pipeline::contracts::AccessScanContinuationInput,
            production_scalar_page_access_entry_limit,
            route::primary_scan_fetch_hint_shape_supported,
            route::{IndexPrefixChildExpansionBudget, IndexPrefixChildExpansionHint},
            stream::key::{
                HeldHeadKeyStream, HeldHeadSeekOutcome, HeldHeadSeekWork, KeyOrderComparator,
            },
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
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::{cell::Cell, mem::size_of, ops::Bound};

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
            IndexLeafOrderPolicy::PreservePhysicalLeaf => Self::PrimaryKeySuffix,
            IndexLeafOrderPolicy::CanonicalKey | IndexLeafOrderPolicy::PreservePrefixBranch => {
                Self::None
            }
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
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
            1,
        )?;
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
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
            u64::try_from(data_keys.len()).unwrap_or(u64::MAX),
        )?;

        Ok((data_keys, KeyOrderState::AscendingSorted))
    }

    // Resolve one primary-key range scan as a dynamic ordered stream.
    fn resolve_key_range_stream(
        &self,
        start: Value,
        end: Value,
        continuation: AccessScanContinuationInput<'_>,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let start = DecodedDataStoreKey::try_from_structural_key(self.entity_tag, &start)?;
        let end = DecodedDataStoreKey::try_from_structural_key(self.entity_tag, &end)?;
        let mut stream = PrimaryRangeKeyStream::new(
            self.store,
            start,
            end,
            continuation.direction(),
            primary_scan_fetch_hint,
        )?;
        if let Some(boundary) = continuation.primary_key_boundary() {
            stream
                .resume_strictly_after(primary_key_boundary_data_key(self.entity_tag, boundary)?)?;
        }

        Ok(OrderedKeyStreamBox::primary_range(stream))
    }

    // Resolve one full primary-key scan as a dynamic ordered stream.
    fn resolve_full_scan_stream(
        &self,
        continuation: AccessScanContinuationInput<'_>,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let mut stream = PrimaryRangeKeyStream::new_full_scan(
            self.store,
            self.entity_tag,
            continuation.direction(),
            primary_scan_fetch_hint,
        )?;
        if let Some(boundary) = continuation.primary_key_boundary() {
            stream
                .resume_strictly_after(primary_key_boundary_data_key(self.entity_tag, boundary)?)?;
        }

        Ok(OrderedKeyStreamBox::primary_range(stream))
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
        if !lowered_index_prefix_liveness(self.store, spec).should_scan() {
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
        charge_materialized_secondary_index_keys(keys.as_slice(), keys.capacity())?;

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
        let active_specs = active_lowered_index_prefix_specs(
            Some(self.store),
            index_prefix_specs,
            index_predicate_execution,
        );
        let key_capacity = index_fetch_hint.map_or(0, |hint| {
            hint.saturating_mul(active_specs.len())
                .min(ACCESS_SCAN_CHUNK_ENTRIES)
        });
        let mut keys = new_materialized_secondary_index_key_vector(key_capacity)?;
        for spec in active_specs {
            let child = IndexScan::prefix_structural(
                self.store,
                self.entity_tag,
                spec,
                direction,
                per_prefix_limit,
                index_predicate_execution,
            )?;
            charge_materialized_secondary_index_keys(child.as_slice(), child.capacity())?;
            reserve_materialized_secondary_index_key_capacity(&mut keys, child.len())?;
            keys.extend(child);
        }
        charge_sort_work::<DecodedDataStoreKey>(keys.len())?;
        keys.sort_unstable();
        charge_materialized_key_dedup_comparisons(keys.len())?;
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
        validate_index_prefix_count(index_prefix_specs, value_count)?;
        if index_leaf_order_policy.preserves_prefix_branch_order() {
            return self.resolve_branch_ordered_index_prefix_streams(
                MergedIndexPrefixStreamSpec::new(
                    index,
                    index_prefix_specs,
                    continuation,
                    index_fetch_hint,
                    PrefixMergeResumePolicy::None,
                ),
            );
        }

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

        let request = MergedIndexPrefixStreamSpec::new(
            expanded_family.index(),
            expanded_family.specs(),
            continuation,
            index_fetch_hint,
            PrefixMergeResumePolicy::PrimaryKeySuffix,
        );

        self.resolve_merged_index_prefix_streams(request).map(Some)
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
        charge_prefix_family_construction(active_specs.as_slice())?;
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
            PrefixSetExecutionShape::OrderedConcat(_)
            | PrefixSetExecutionShape::Materialized(_) => {
                Err(InternalError::query_executor_invariant())
            }
        }
    }

    fn resolve_branch_ordered_index_prefix_streams(
        &self,
        request: MergedIndexPrefixStreamSpec<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        if request.index_prefix_specs.is_empty() {
            return Ok(ordered_key_stream_from_materialized_keys(Vec::new()));
        }

        let mut active_specs =
            active_lowered_index_prefix_specs(Some(self.store), request.index_prefix_specs, None);
        sort_lowered_index_prefix_specs_by_raw_lower_key(&mut active_specs)?;
        if matches!(request.continuation.direction(), Direction::Desc) {
            active_specs.reverse();
        }

        match PrefixSetExecutionShape::from_active_prefixes(
            active_specs,
            PrefixSetMergeSafety::OrderedConcatSafe,
        ) {
            PrefixSetExecutionShape::Empty => {
                Ok(ordered_key_stream_from_materialized_keys(Vec::new()))
            }
            PrefixSetExecutionShape::Single(spec) => self.index_prefix_stream(request, spec, 1),
            PrefixSetExecutionShape::OrderedConcat(active_specs) => {
                let branch_count = active_specs.len();
                let mut streams = Vec::with_capacity(branch_count);
                for spec in active_specs {
                    streams.push(self.index_prefix_stream(request, spec, branch_count)?);
                }

                Ok(OrderedKeyStreamBox::concat_all(streams))
            }
            PrefixSetExecutionShape::OrderedMerge(_) | PrefixSetExecutionShape::Materialized(_) => {
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

        let primary_key_ordered = matches!(
            request.resume_policy,
            PrefixMergeResumePolicy::PrimaryKeySuffix
        );
        let stream = IndexRangeKeyStream::from_prefix(
            self.store,
            self.entity_tag,
            request.index,
            spec,
            request.continuation.direction(),
            resume_anchor,
            request.index_fetch_hint,
            branch_chunk_entries,
            primary_key_ordered,
        )?;

        Ok(if primary_key_ordered && active_branch_count > 1 {
            OrderedKeyStreamBox::seekable_index_range(SeekableIndexRangeKeyStream::new(stream))
        } else {
            OrderedKeyStreamBox::index_range(stream)
        })
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
        charge_materialized_secondary_index_keys(keys.as_slice(), keys.capacity())?;

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

fn charge_prefix_family_construction(
    specs: &[&LoweredIndexPrefixSpec],
) -> Result<(), InternalError> {
    validate_prefix_family_child_count(specs.len())?;

    let mut descriptor_bytes = specs
        .len()
        .checked_mul(size_of::<OrderedKeyStreamBox>())
        .ok_or_else(InternalError::executor_invariant)?;
    for spec in specs {
        for component in spec.prefix_components() {
            descriptor_bytes = descriptor_bytes
                .checked_add(component.len())
                .ok_or_else(InternalError::executor_invariant)?;
        }
        let (lower, upper) = spec.raw_bounds()?;
        descriptor_bytes = descriptor_bytes
            .checked_add(bound_raw_index_key_bytes(lower))
            .and_then(|bytes| bytes.checked_add(bound_raw_index_key_bytes(upper)))
            .ok_or_else(InternalError::executor_invariant)?;
    }

    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::CursorSteps,
        u64::try_from(specs.len()).unwrap_or(u64::MAX),
    )?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::TemporaryBytes,
        u64::try_from(descriptor_bytes).unwrap_or(u64::MAX),
    )
}

fn validate_prefix_family_child_count(count: usize) -> Result<(), InternalError> {
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

fn sort_lowered_index_prefix_specs_by_raw_lower_key(
    specs: &mut Vec<&LoweredIndexPrefixSpec>,
) -> Result<(), InternalError> {
    let mut keyed_specs = Vec::with_capacity(specs.len());
    for spec in specs.drain(..) {
        let (lower, _upper) = spec.raw_bounds()?;
        let Bound::Included(raw_key) = lower else {
            return Err(InternalError::query_executor_invariant());
        };
        keyed_specs.push((raw_key.clone(), spec));
    }

    keyed_specs.sort_by(|left, right| left.0.cmp(&right.0));
    specs.extend(keyed_specs.into_iter().map(|(_raw_key, spec)| spec));

    Ok(())
}

fn lowered_prefix_start_key(spec: &LoweredIndexPrefixSpec) -> Result<IndexKey, InternalError> {
    let (lower, _upper) = spec.raw_bounds()?;
    let Bound::Included(raw_key) = lower else {
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

fn primary_key_boundary_data_key(
    entity_tag: EntityTag,
    boundary: &CursorBoundary,
) -> Result<DecodedDataStoreKey, InternalError> {
    let (primary_key, _values) = primary_key_suffix_values(boundary, boundary.slots.len())?;

    Ok(DecodedDataStoreKey::new_primary_key_value(
        entity_tag,
        &primary_key,
    ))
}

fn raw_key_within_bounds<K: Ord>(key: &K, lower: &Bound<K>, upper: &Bound<K>) -> bool {
    let above_lower = match lower {
        Bound::Included(lower) => key >= lower,
        Bound::Excluded(lower) => key > lower,
        Bound::Unbounded => true,
    };
    let below_upper = match upper {
        Bound::Included(upper) => key <= upper,
        Bound::Excluded(upper) => key < upper,
        Bound::Unbounded => true,
    };
    above_lower && below_upper
}

fn held_head_outcome(
    held: Option<&DecodedDataStoreKey>,
) -> Result<HeldHeadSeekOutcome<'_>, InternalError> {
    held.map(HeldHeadSeekOutcome::Held)
        .map_or(Ok(HeldHeadSeekOutcome::Exhausted), Ok)
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
    entity_tag: EntityTag,
    lower_bound: Bound<RawDataStoreKey>,
    upper_bound: Bound<RawDataStoreKey>,
    direction: Direction,
    remaining: Option<usize>,
    chunk_entries: usize,
    buffer: Vec<DecodedDataStoreKey>,
    buffer_pos: usize,
    held: Option<DecodedDataStoreKey>,
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
            entity_tag: start.entity_tag(),
            lower_bound: Bound::Included(start.to_raw()?),
            upper_bound: Bound::Included(end.to_raw()?),
            direction,
            remaining: limit,
            chunk_entries: primary_range_chunk_entries_for_active_page()?,
            buffer: Vec::new(),
            buffer_pos: 0,
            held: None,
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
    ) -> Result<Self, InternalError> {
        let range = RawDataStoreKeyRange::entity_prefix(entity);
        let lower_bound = Bound::Included(RawDataStoreKey::store_range_lower_key(&range));
        let upper_bound = range
            .upper_exclusive()
            .map(RawDataStoreKey::from_store_range_bound)
            .map_or(Bound::Unbounded, Bound::Excluded);

        Ok(Self {
            store,
            entity_tag: entity,
            lower_bound,
            upper_bound,
            direction,
            remaining: limit,
            chunk_entries: primary_range_chunk_entries_for_active_page()?,
            buffer: Vec::new(),
            buffer_pos: 0,
            held: None,
            exhausted: false,
        })
    }

    /// Visit ASC primary rows through one open physical range when the store
    /// has a single visible backing and this leaf has not otherwise advanced.
    pub(in crate::db::executor) fn try_visit_rows_direct(
        &mut self,
        begin_row: &mut dyn FnMut() -> Result<bool, InternalError>,
        visit_row: &mut dyn for<'row> FnMut(
            DecodedDataStoreKey,
            &'row RawRow,
        ) -> Result<StoreVisit, InternalError>,
    ) -> Result<Option<()>, InternalError> {
        if !matches!(self.direction, Direction::Asc)
            || self.buffer_pos != 0
            || !self.buffer.is_empty()
            || self.held.is_some()
        {
            return Ok(None);
        }
        if self.exhausted || matches!(self.remaining, Some(0)) {
            self.exhausted = true;
            return Ok(Some(()));
        }

        let row_limit = self.remaining;
        let visited = Cell::new(0usize);
        let mut last_raw_key = None;
        let outcome = self.store.with_data(|store| {
            store.try_visit_range_with_row_preflight(
                (self.lower_bound.clone(), self.upper_bound.clone()),
                |_raw_key| {
                    if row_limit.is_some_and(|limit| visited.get() >= limit) || !begin_row()? {
                        return Ok::<StoreVisit, InternalError>(StoreVisit::Stop);
                    }

                    Ok::<StoreVisit, InternalError>(StoreVisit::Continue)
                },
                |raw_key, row| {
                    charge_current_execution_budget(
                        DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
                        1,
                    )?;
                    let key = PrimaryScan::decode_data_key(raw_key)?;
                    let visit = visit_row(key, row)?;
                    visited.set(visited.get().saturating_add(1));
                    last_raw_key = Some(raw_key.clone());

                    Ok(visit)
                },
            )
        })?;
        let Some(naturally_exhausted) = outcome else {
            return Ok(None);
        };

        if let Some(raw_key) = last_raw_key {
            self.lower_bound = Bound::Excluded(raw_key);
        }
        if let Some(remaining) = self.remaining.as_mut() {
            *remaining = remaining.saturating_sub(visited.get());
        }
        self.exhausted = naturally_exhausted || matches!(self.remaining, Some(0));

        Ok(Some(()))
    }

    // Bind an authenticated primary-key continuation directly into this
    // stream's raw range before its first pull. The route fetch hint is then
    // page-local rather than being consumed while replaying earlier pages.
    fn resume_strictly_after(
        &mut self,
        boundary: DecodedDataStoreKey,
    ) -> Result<(), InternalError> {
        if boundary.entity_tag() != self.entity_tag
            || self.buffer_pos != 0
            || !self.buffer.is_empty()
            || self.held.is_some()
        {
            return Err(InternalError::query_executor_invariant());
        }
        let raw_boundary = boundary.to_raw()?;
        let bound_bytes = u64::try_from(raw_boundary.as_bytes().len()).unwrap_or(u64::MAX);
        charge_current_execution_budget(DiagnosticExecutionBudgetResource::CursorSteps, 1)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            bound_bytes,
        )?;

        match self.direction {
            Direction::Asc => {
                if lower_bound_precedes_raw(&self.lower_bound, &raw_boundary) {
                    self.lower_bound = Bound::Excluded(raw_boundary);
                }
            }
            Direction::Desc => {
                if upper_bound_follows_raw(&self.upper_bound, &raw_boundary) {
                    self.upper_bound = Bound::Excluded(raw_boundary);
                }
            }
        }
        self.held = None;
        self.exhausted = !raw_bounds_may_contain_key(&self.lower_bound, &self.upper_bound);

        Ok(())
    }

    // Return the maximum number of keys to read during the next store borrow.
    const fn next_chunk_limit(&self) -> usize {
        match self.remaining {
            Some(remaining) if remaining < self.chunk_entries => remaining,
            Some(_) | None => self.chunk_entries,
        }
    }

    // Return the complete physical-entry bound for the next pull. Buffered or
    // held keys require no new storage traversal; an empty buffer may refill
    // one already-bounded chunk. The page owner reserves this amount before
    // polling, so batching does not advance unadmitted storage work.
    const fn next_pull_entry_bound(&self) -> usize {
        if self.held.is_some()
            || self.buffer_pos < self.buffer.len()
            || self.exhausted
            || matches!(self.remaining, Some(0))
        {
            0
        } else {
            self.next_chunk_limit()
        }
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
                    store.visit_key_range(
                        (self.lower_bound.clone(), self.upper_bound.clone()),
                        |raw_key| {
                            charge_current_execution_budget(
                                DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
                                1,
                            )?;
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
                Direction::Desc => {
                    store.visit_key_range_rev(
                        (self.lower_bound.clone(), self.upper_bound.clone()),
                        |raw_key| {
                            charge_current_execution_budget(
                                DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
                                1,
                            )?;
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
            match self.direction {
                Direction::Asc => self.lower_bound = Bound::Excluded(raw_key),
                Direction::Desc => self.upper_bound = Bound::Excluded(raw_key),
            }
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

    fn pull_next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
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

    fn configure_physical_seek(
        &mut self,
        target: &DecodedDataStoreKey,
        work: &mut HeldHeadSeekWork,
    ) -> Result<bool, InternalError> {
        if self.remaining.is_some() {
            return Ok(false);
        }

        let raw_target = target.to_raw()?;
        if !raw_key_within_bounds(&raw_target, &self.lower_bound, &self.upper_bound) {
            self.buffer.clear();
            self.buffer_pos = 0;
            self.exhausted = true;
            return Ok(true);
        }

        let bound_bytes = u64::try_from(raw_target.as_bytes().len()).unwrap_or(u64::MAX);
        charge_current_execution_budget(DiagnosticExecutionBudgetResource::CursorSteps, 1)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            bound_bytes,
        )?;
        work.record_physical_seek(bound_bytes)?;

        match self.direction {
            Direction::Asc => self.lower_bound = Bound::Included(raw_target),
            Direction::Desc => self.upper_bound = Bound::Included(raw_target),
        }
        self.buffer.clear();
        self.buffer_pos = 0;
        self.exhausted = false;
        Ok(true)
    }

    fn ensure_physical_head(
        &mut self,
        work: &mut HeldHeadSeekWork,
    ) -> Result<HeldHeadSeekOutcome<'_>, InternalError> {
        if self.held.is_some() {
            return held_head_outcome(self.held.as_ref());
        }
        if self.exhausted && self.buffer_pos == self.buffer.len() {
            return Ok(HeldHeadSeekOutcome::Exhausted);
        }
        if !work.admits_pull() {
            return Ok(HeldHeadSeekOutcome::PageStop);
        }
        work.record_pull_attempt()?;
        self.held = self.pull_next_key()?;
        held_head_outcome(self.held.as_ref())
    }
}

fn primary_range_chunk_entries_for_active_page() -> Result<usize, InternalError> {
    let Some(page_entry_limit) = production_scalar_page_access_entry_limit()? else {
        return Ok(ACCESS_SCAN_CHUNK_ENTRIES);
    };

    Ok(ACCESS_SCAN_CHUNK_ENTRIES.min(page_entry_limit.max(1)))
}

fn lower_bound_precedes_raw(lower: &Bound<RawDataStoreKey>, raw: &RawDataStoreKey) -> bool {
    match lower {
        Bound::Included(value) => value <= raw,
        Bound::Excluded(value) => value < raw,
        Bound::Unbounded => true,
    }
}

fn upper_bound_follows_raw(upper: &Bound<RawDataStoreKey>, raw: &RawDataStoreKey) -> bool {
    match upper {
        Bound::Included(value) => value >= raw,
        Bound::Excluded(value) => value > raw,
        Bound::Unbounded => true,
    }
}

fn raw_bounds_may_contain_key(
    lower: &Bound<RawDataStoreKey>,
    upper: &Bound<RawDataStoreKey>,
) -> bool {
    match (lower, upper) {
        (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
        (Bound::Included(lower), Bound::Included(upper)) => lower <= upper,
        (Bound::Included(lower) | Bound::Excluded(lower), Bound::Excluded(upper))
        | (Bound::Excluded(lower), Bound::Included(upper)) => lower < upper,
    }
}

impl OrderedKeyStream for PrimaryRangeKeyStream {
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        if self.held.is_some() {
            return Ok(self.held.take());
        }
        self.pull_next_key()
    }

    fn cheap_access_candidate_count_hint(&self) -> Option<usize> {
        if self.remaining.is_some() {
            return None;
        }

        None
    }

    fn page_access_entry_bound(&self) -> Option<usize> {
        Some(self.next_pull_entry_bound())
    }
}

impl HeldHeadKeyStream for PrimaryRangeKeyStream {
    fn ensure_head(
        &mut self,
        work: &mut HeldHeadSeekWork,
    ) -> Result<HeldHeadSeekOutcome<'_>, InternalError> {
        self.ensure_physical_head(work)
    }

    fn seek_head_at_or_after(
        &mut self,
        target: &DecodedDataStoreKey,
        work: &mut HeldHeadSeekWork,
    ) -> Result<HeldHeadSeekOutcome<'_>, InternalError> {
        if target.entity_tag() != self.entity_tag {
            return Err(InternalError::executor_invariant());
        }

        loop {
            let direction = self.direction;
            let held_before_target = match self.ensure_physical_head(work)? {
                HeldHeadSeekOutcome::Held(held) => {
                    work.record_comparison()?;
                    KeyOrderComparator::from_direction(direction)
                        .compare_data_keys(held, target)
                        .is_lt()
                }
                HeldHeadSeekOutcome::Exhausted => return Ok(HeldHeadSeekOutcome::Exhausted),
                HeldHeadSeekOutcome::PageStop => return Ok(HeldHeadSeekOutcome::PageStop),
            };
            if !held_before_target {
                return held_head_outcome(self.held.as_ref());
            }

            work.record_skipped_consumptions(1)?;
            self.held = None;
            self.configure_physical_seek(target, work)?;
        }
    }

    fn consume_head(
        &mut self,
        work: &mut HeldHeadSeekWork,
    ) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        if self.held.is_none() {
            return Ok(None);
        }
        work.record_consumed()?;
        Ok(self.held.take())
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
    held: Option<DecodedDataStoreKey>,
    primary_key_seek: Option<IndexPrimaryKeySeek>,
    exhausted: bool,
}

/// Ordered polling adapter for an index leaf that retains held-head work state.
///
/// Prefix-family merges use this concrete shape so they no longer materialize
/// sibling candidate vectors and so later seek-aware owners can position the
/// same physical leaf without changing its consumption semantics.
pub(in crate::db::executor) struct SeekableIndexRangeKeyStream {
    inner: IndexRangeKeyStream,
    work: HeldHeadSeekWork,
    pending_target: Option<DecodedDataStoreKey>,
}

impl SeekableIndexRangeKeyStream {
    #[must_use]
    const fn new(inner: IndexRangeKeyStream) -> Self {
        Self {
            inner,
            work: HeldHeadSeekWork::unbounded(),
            pending_target: None,
        }
    }
}

impl OrderedKeyStream for SeekableIndexRangeKeyStream {
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        let Some(target) = self.pending_target.take() else {
            return self.inner.next_key();
        };
        let outcome = self.inner.seek_head_at_or_after(&target, &mut self.work)?;
        let next = match outcome {
            HeldHeadSeekOutcome::Held(key) => Some(key.clone()),
            HeldHeadSeekOutcome::Exhausted => None,
            HeldHeadSeekOutcome::PageStop => return Err(InternalError::executor_invariant()),
        };
        if next.is_none() {
            return Ok(None);
        }

        let consumed = self.inner.consume_head(&mut self.work)?;
        if consumed != next {
            return Err(InternalError::executor_invariant());
        }
        Ok(consumed)
    }

    fn page_access_entry_bound(&self) -> Option<usize> {
        self.inner.page_access_entry_bound()
    }
}

struct IndexPrimaryKeySeek {
    prefix_start: IndexKey,
    prefix_len: usize,
    suffix_len: usize,
}

impl IndexPrimaryKeySeek {
    fn new(
        index: &IndexShapeDetails,
        spec: &LoweredIndexPrefixSpec,
    ) -> Result<Self, InternalError> {
        let prefix_len = index.slot_arity();
        let key_arity = index.key_arity();
        if prefix_len > key_arity || spec.prefix_components().len() != prefix_len {
            return Err(InternalError::query_executor_invariant());
        }
        let prefix_start = lowered_prefix_start_key(spec)?;
        if prefix_start.component_count() != key_arity {
            return Err(InternalError::query_executor_invariant());
        }

        Ok(Self {
            prefix_start,
            prefix_len,
            suffix_len: key_arity.saturating_sub(prefix_len),
        })
    }

    fn raw_target(&self, target: &DecodedDataStoreKey) -> Result<RawIndexStoreKey, InternalError> {
        let mut suffix_values = Vec::with_capacity(self.suffix_len);
        for component_index in 0..self.suffix_len {
            suffix_values.push(target.primary_key_component_runtime_value(component_index)?);
        }

        Ok(
            IndexKey::new_from_existing_prefix_and_suffix_values_with_primary_key_value(
                &self.prefix_start,
                self.prefix_len,
                suffix_values.as_slice(),
                &target.primary_key_value(),
            )?
            .to_raw()?,
        )
    }
}

impl IndexRangeKeyStream {
    // Build one index stream from a lowered prefix envelope.
    #[expect(
        clippy::too_many_arguments,
        reason = "one physical leaf freezes store, index, bounds, continuation, window, and seek authority together"
    )]
    fn from_prefix(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: &IndexShapeDetails,
        spec: &LoweredIndexPrefixSpec,
        direction: Direction,
        anchor: Option<RawIndexStoreKey>,
        limit: Option<usize>,
        chunk_entries: usize,
        primary_key_ordered: bool,
    ) -> Result<Self, InternalError> {
        let (lower, upper) = spec.raw_bounds()?;
        let primary_key_seek = primary_key_ordered
            .then(|| IndexPrimaryKeySeek::new(index, spec))
            .transpose()?;
        Ok(Self::new(
            store,
            entity_tag,
            (lower.clone(), upper.clone()),
            direction,
            anchor,
            limit,
            chunk_entries,
            primary_key_seek,
        ))
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
            None,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one physical leaf freezes its complete bounded traversal state"
    )]
    fn new(
        store: StoreHandle,
        entity_tag: EntityTag,
        bounds: (Bound<LoweredKey>, Bound<LoweredKey>),
        direction: Direction,
        anchor: Option<RawIndexStoreKey>,
        limit: Option<usize>,
        chunk_entries: usize,
        primary_key_seek: Option<IndexPrimaryKeySeek>,
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
            held: None,
            primary_key_seek,
            exhausted: false,
        }
    }

    // Return the maximum raw index entries one empty-buffer pull may visit.
    const fn next_chunk_limit(&self) -> usize {
        index_stream_chunk_entries_for_remaining(self.chunk_entries, self.remaining)
    }

    // Return the complete physical-entry bound for the next pull. The bound
    // falls to zero while decoded keys remain buffered, allowing page-local
    // admission to preserve chunked store traversal without over-reserving
    // every candidate.
    const fn next_pull_entry_bound(&self) -> usize {
        if self.held.is_some()
            || self.buffer_pos < self.buffer.len()
            || self.exhausted
            || matches!(self.remaining, Some(0))
        {
            0
        } else {
            self.next_chunk_limit()
        }
    }

    // Re-enter the index store for one bounded raw-index chunk.
    fn load_next_chunk(&mut self) -> Result<(), InternalError> {
        if self.exhausted || matches!(self.remaining, Some(0)) {
            self.exhausted = true;
            return Ok(());
        }

        let chunk_entries = self.next_chunk_limit();
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

    fn pull_next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
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

    fn configure_physical_seek(
        &mut self,
        target: &DecodedDataStoreKey,
        work: &mut HeldHeadSeekWork,
    ) -> Result<bool, InternalError> {
        if self.remaining.is_some() {
            return Ok(false);
        }
        let Some(seek) = self.primary_key_seek.as_ref() else {
            return Ok(false);
        };
        let raw_target = seek.raw_target(target)?;
        if !raw_key_within_bounds(&raw_target, &self.lower, &self.upper) {
            self.buffer.clear();
            self.buffer_pos = 0;
            self.exhausted = true;
            return Ok(true);
        }

        let bound_bytes = u64::try_from(raw_target.as_bytes().len()).unwrap_or(u64::MAX);
        charge_current_execution_budget(DiagnosticExecutionBudgetResource::CursorSteps, 1)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            bound_bytes,
        )?;
        work.record_physical_seek(bound_bytes)?;

        match self.direction {
            Direction::Asc => self.lower = Bound::Included(raw_target),
            Direction::Desc => self.upper = Bound::Included(raw_target),
        }
        self.anchor = None;
        self.buffer.clear();
        self.buffer_pos = 0;
        self.exhausted = false;
        Ok(true)
    }

    fn ensure_physical_head(
        &mut self,
        work: &mut HeldHeadSeekWork,
    ) -> Result<HeldHeadSeekOutcome<'_>, InternalError> {
        if self.held.is_some() {
            return held_head_outcome(self.held.as_ref());
        }
        if self.exhausted && self.buffer_pos == self.buffer.len() {
            return Ok(HeldHeadSeekOutcome::Exhausted);
        }
        if !work.admits_pull() {
            return Ok(HeldHeadSeekOutcome::PageStop);
        }
        work.record_pull_attempt()?;
        self.held = self.pull_next_key()?;
        held_head_outcome(self.held.as_ref())
    }
}

impl OrderedKeyStream for IndexRangeKeyStream {
    fn next_key(&mut self) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        if self.held.is_some() {
            return Ok(self.held.take());
        }
        self.pull_next_key()
    }

    fn page_access_entry_bound(&self) -> Option<usize> {
        Some(self.next_pull_entry_bound())
    }
}

impl HeldHeadKeyStream for IndexRangeKeyStream {
    fn ensure_head(
        &mut self,
        work: &mut HeldHeadSeekWork,
    ) -> Result<HeldHeadSeekOutcome<'_>, InternalError> {
        self.ensure_physical_head(work)
    }

    fn seek_head_at_or_after(
        &mut self,
        target: &DecodedDataStoreKey,
        work: &mut HeldHeadSeekWork,
    ) -> Result<HeldHeadSeekOutcome<'_>, InternalError> {
        if target.entity_tag() != self.entity_tag {
            return Err(InternalError::executor_invariant());
        }

        loop {
            let direction = self.direction;
            let held_before_target = match self.ensure_physical_head(work)? {
                HeldHeadSeekOutcome::Held(held) => {
                    work.record_comparison()?;
                    KeyOrderComparator::from_direction(direction)
                        .compare_data_keys(held, target)
                        .is_lt()
                }
                HeldHeadSeekOutcome::Exhausted => return Ok(HeldHeadSeekOutcome::Exhausted),
                HeldHeadSeekOutcome::PageStop => return Ok(HeldHeadSeekOutcome::PageStop),
            };
            if !held_before_target {
                return held_head_outcome(self.held.as_ref());
            }

            work.record_skipped_consumptions(1)?;
            self.held = None;
            self.configure_physical_seek(target, work)?;
        }
    }

    fn consume_head(
        &mut self,
        work: &mut HeldHeadSeekWork,
    ) -> Result<Option<DecodedDataStoreKey>, InternalError> {
        if self.held.is_none() {
            return Ok(None);
        }
        work.record_consumed()?;
        Ok(self.held.take())
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

// Charge the complete retained owner of one materialized secondary-index key
// vector. `DecodedDataStoreKey` keeps its raw persisted key in an owned cache,
// so both vector capacity and every live raw-key allocation belong to this
// blocking fallback rather than only the logical key count.
pub(in crate::db::executor) fn charge_materialized_secondary_index_keys(
    keys: &[DecodedDataStoreKey],
    capacity: usize,
) -> Result<(), InternalError> {
    charge_materialized_secondary_index_key_capacity(capacity)?;

    let mut raw_key_bytes = 0usize;
    for key in keys {
        raw_key_bytes = raw_key_bytes
            .checked_add(key.raw_key()?.as_bytes().len())
            .ok_or_else(InternalError::executor_invariant)?;
    }

    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::TemporaryBytes,
        u64::try_from(raw_key_bytes).unwrap_or(u64::MAX),
    )
}

fn charge_materialized_secondary_index_key_capacity(capacity: usize) -> Result<(), InternalError> {
    let bytes = capacity
        .checked_mul(size_of::<DecodedDataStoreKey>())
        .ok_or_else(InternalError::executor_invariant)?;
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::TemporaryBytes,
        u64::try_from(bytes).unwrap_or(u64::MAX),
    )
}

fn new_materialized_secondary_index_key_vector(
    capacity: usize,
) -> Result<Vec<DecodedDataStoreKey>, InternalError> {
    charge_materialized_secondary_index_key_capacity(capacity)?;
    let mut keys = Vec::new();
    keys.try_reserve_exact(capacity)
        .map_err(|_| InternalError::executor_internal())?;
    let uncharged_capacity = keys.capacity().saturating_sub(capacity);
    charge_materialized_secondary_index_key_capacity(uncharged_capacity)?;

    Ok(keys)
}

fn reserve_materialized_secondary_index_key_capacity(
    keys: &mut Vec<DecodedDataStoreKey>,
    additional: usize,
) -> Result<(), InternalError> {
    let required = keys
        .len()
        .checked_add(additional)
        .ok_or_else(InternalError::executor_invariant)?;
    if required <= keys.capacity() {
        return Ok(());
    }

    let prior_capacity = keys.capacity();
    let minimum_growth = required.saturating_sub(prior_capacity);
    charge_materialized_secondary_index_key_capacity(minimum_growth)?;
    keys.try_reserve_exact(additional)
        .map_err(|_| InternalError::executor_internal())?;
    let actual_growth = keys.capacity().saturating_sub(prior_capacity);
    charge_materialized_secondary_index_key_capacity(actual_growth.saturating_sub(minimum_growth))
}

fn charge_materialized_key_dedup_comparisons(entries: usize) -> Result<(), InternalError> {
    charge_current_execution_budget(
        DiagnosticExecutionBudgetResource::SortComparisons,
        u64::try_from(entries.saturating_sub(1)).unwrap_or(u64::MAX),
    )
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
                request.continuation,
                primary_scan_fetch_hint,
            );
        }
        ExecutionPathPayload::FullScan => {
            return runtime.resolve_full_scan_stream(request.continuation, primary_scan_fetch_hint);
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

    if matches!(key_order_state, KeyOrderState::Unordered) {
        charge_sort_work::<DecodedDataStoreKey>(candidates.len())?;
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

#[cfg(test)]
mod physical_seek_tests {
    use super::*;
    use crate::{
        db::{
            QueryError,
            data::{DataStore, RawRow},
            executor::budget::{
                HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
                with_query_execution_budget_for_tests,
            },
            index::{IndexEntryValue, IndexId, IndexStore},
            key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
            registry::{StoreAllocationIdentities, StoreRuntimeStorageCapabilities},
            schema::SchemaStore,
            test_support::index::nat64_index_key,
        },
        types::EntityTag,
    };
    use ic_stable_structures::Storable;
    use icydb_diagnostic_code::{
        DiagnosticDetail, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
        DiagnosticFactTag, RuntimeBoundaryCode,
    };
    use std::{borrow::Cow, cell::RefCell};

    const ENTITY: EntityTag = EntityTag::new(0x222);

    thread_local! {
        static DATA: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
        static INDEX: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
        static SCHEMA: RefCell<SchemaStore> = const { RefCell::new(SchemaStore::init_heap()) };
    }

    const STORE: StoreHandle = StoreHandle::new(
        &DATA,
        &INDEX,
        &SCHEMA,
        StoreAllocationIdentities::absent(),
        StoreRuntimeStorageCapabilities::heap(),
    );

    fn data_key(value: u64) -> DecodedDataStoreKey {
        DecodedDataStoreKey::new(
            ENTITY,
            &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(value)),
        )
    }

    fn index_key(index_id: &IndexId, component: &[u8], value: u64) -> IndexKey {
        nat64_index_key(index_id, component, value)
    }

    fn reset_heap_stores() {
        DATA.with_borrow_mut(|store| *store = DataStore::init_heap());
        INDEX.with_borrow_mut(|store| *store = IndexStore::init_heap());
    }

    fn load_primary_keys() {
        DATA.with_borrow_mut(|store| {
            for value in 1..=100 {
                store.insert_raw_for_test(
                    data_key(value)
                        .to_raw()
                        .expect("test data key should encode"),
                    RawRow::try_new(vec![0]).expect("test row should be bounded"),
                );
            }
        });
    }

    fn load_index_keys(index_id: &IndexId, component: &[u8]) {
        INDEX.with_borrow_mut(|store| {
            for value in 1..=100 {
                store.insert(
                    index_key(index_id, component, value)
                        .to_raw()
                        .expect("test index key should encode"),
                    IndexEntryValue::presence(),
                );
            }
        });
    }

    #[test]
    fn primary_leaf_physically_repositions_in_both_directions() {
        reset_heap_stores();
        load_primary_keys();

        for (direction, first, target, next) in
            [(Direction::Asc, 1, 80, 81), (Direction::Desc, 100, 20, 19)]
        {
            let mut stream =
                PrimaryRangeKeyStream::new(STORE, data_key(1), data_key(100), direction, None)
                    .expect("primary stream should build");
            let mut work = HeldHeadSeekWork::unbounded();

            assert_eq!(
                stream
                    .ensure_head(&mut work)
                    .expect("first head should load"),
                HeldHeadSeekOutcome::Held(&data_key(first)),
            );
            assert_eq!(
                stream
                    .seek_head_at_or_after(&data_key(target), &mut work)
                    .expect("physical seek should position"),
                HeldHeadSeekOutcome::Held(&data_key(target)),
            );
            assert_eq!(work.physical_seeks(), 1);
            assert!(work.reposition_bound_bytes() > 0);
            assert_eq!(
                stream
                    .consume_head(&mut work)
                    .expect("target should consume"),
                Some(data_key(target)),
            );
            assert_eq!(
                stream
                    .ensure_head(&mut work)
                    .expect("successor should load"),
                HeldHeadSeekOutcome::Held(&data_key(next)),
            );
        }
    }

    #[test]
    fn limited_primary_leaf_uses_equivalent_repeated_progress() {
        reset_heap_stores();
        load_primary_keys();

        let mut stream =
            PrimaryRangeKeyStream::new(STORE, data_key(1), data_key(100), Direction::Asc, Some(10))
                .expect("limited primary stream should build");
        let mut work = HeldHeadSeekWork::unbounded();

        assert_eq!(
            stream
                .seek_head_at_or_after(&data_key(8), &mut work)
                .expect("limited seek should progress"),
            HeldHeadSeekOutcome::Held(&data_key(8)),
        );
        assert_eq!(work.physical_seeks(), 0);
        assert_eq!(work.skipped_occurrences(), 7);
    }

    #[test]
    fn limited_primary_leaf_applies_page_fetch_hint_after_resume_boundary() {
        reset_heap_stores();
        load_primary_keys();

        for (direction, expected) in [
            (Direction::Asc, [51, 52, 53]),
            (Direction::Desc, [49, 48, 47]),
        ] {
            let mut stream =
                PrimaryRangeKeyStream::new(STORE, data_key(1), data_key(100), direction, Some(3))
                    .expect("limited primary stream should build");
            stream
                .resume_strictly_after(data_key(50))
                .expect("primary continuation boundary should apply");

            for expected_key in expected {
                assert_eq!(
                    stream.next_key().expect("resumed key should load"),
                    Some(data_key(expected_key)),
                );
            }
            assert_eq!(
                stream.next_key().expect("fetch hint should end the page"),
                None,
            );
        }
    }

    #[test]
    fn primary_key_ordered_index_leaf_repositions_without_visiting_the_gap() {
        reset_heap_stores();
        let index_id = IndexId::new(ENTITY, 1);
        let component = b"lane";
        load_index_keys(&index_id, component);

        let lower = index_key(&index_id, component, 1)
            .to_raw()
            .expect("lower index key should encode");
        let upper = index_key(&index_id, component, 100)
            .to_raw()
            .expect("upper index key should encode");
        let seek = IndexPrimaryKeySeek {
            prefix_start: index_key(&index_id, component, 1),
            prefix_len: 1,
            suffix_len: 0,
        };
        let mut stream = IndexRangeKeyStream::new(
            STORE,
            ENTITY,
            (Bound::Included(lower), Bound::Included(upper)),
            Direction::Asc,
            None,
            None,
            ACCESS_SCAN_CHUNK_ENTRIES,
            Some(seek),
        );
        let mut work = HeldHeadSeekWork::unbounded();

        assert_eq!(
            stream
                .ensure_head(&mut work)
                .expect("first index head should load"),
            HeldHeadSeekOutcome::Held(&data_key(1)),
        );
        assert_eq!(
            stream
                .seek_head_at_or_after(&data_key(80), &mut work)
                .expect("index seek should position"),
            HeldHeadSeekOutcome::Held(&data_key(80)),
        );
        assert_eq!(work.physical_seeks(), 1);
        assert_eq!(work.skipped_occurrences(), 1);
        assert_eq!(work.pull_attempts(), 2);
    }

    #[test]
    fn physical_leaf_page_stop_does_not_start_a_store_pull() {
        reset_heap_stores();
        load_primary_keys();
        let mut stream =
            PrimaryRangeKeyStream::new(STORE, data_key(1), data_key(100), Direction::Asc, None)
                .expect("primary stream should build");
        let mut work = HeldHeadSeekWork::with_pull_attempt_limit(0);

        assert_eq!(
            stream.ensure_head(&mut work).expect("page stop is success"),
            HeldHeadSeekOutcome::PageStop,
        );
        assert_eq!(work.pull_attempts(), 0);
        assert_eq!(work.physical_seeks(), 0);
    }

    #[test]
    fn primary_leaf_caps_refills_to_the_active_page_entry_limit() {
        for (page_limit, expected_refill) in [(4, 4), (100, 64), (0, 1)] {
            let envelope = crate::db::executor::PageWorkEnvelope::default_scalar()
                .with_limit_for_tests(
                    DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
                    page_limit,
                );
            let bounded = crate::db::executor::with_production_scalar_page_work(envelope, || {
                PrimaryRangeKeyStream::new(STORE, data_key(1), data_key(100), Direction::Asc, None)
            })
            .expect("primary leaf should inherit the active page envelope");

            assert_eq!(bounded.value.next_pull_entry_bound(), expected_refill);
        }
    }

    #[test]
    fn prefix_family_child_count_rejects_max_plus_one_before_allocation() {
        assert!(
            validate_prefix_family_child_count(IndexPrefixChildExpansionBudget::MAX_PREFIXES)
                .is_ok()
        );
        assert!(
            validate_prefix_family_child_count(IndexPrefixChildExpansionBudget::MAX_PREFIXES + 1,)
                .is_err()
        );
    }

    #[test]
    fn materialized_secondary_index_keys_charge_complete_temporary_backing() {
        let budget = HardExecutionBudget::uniform_for_tests(
            u64::MAX,
            HardExecutionFailureHeadroom::new(500, 256),
        )
        .with_limit_for_tests(DiagnosticExecutionBudgetResource::TemporaryBytes, 0);
        let context = HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::TrustedRead,
            0x6d61_7465_7269_616c,
        );
        let keys = vec![data_key(1)];
        let error = with_query_execution_budget_for_tests(budget, context, || {
            charge_materialized_secondary_index_keys(keys.as_slice(), keys.capacity())
                .map_err(QueryError::execute)
        })
        .expect_err("materialized key-vector capacity must consume the temporary-byte budget");

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

    #[test]
    fn index_leaf_propagates_malformed_entry_corruption() {
        reset_heap_stores();
        let index_id = IndexId::new(ENTITY, 1);
        let component = b"lane";
        let raw_key = index_key(&index_id, component, 50)
            .to_raw()
            .expect("index key should encode");
        INDEX.with_borrow_mut(|store| {
            store.insert(
                raw_key.clone(),
                <IndexEntryValue as Storable>::from_bytes(Cow::Owned(vec![0xff])),
            );
        });
        let mut stream = IndexRangeKeyStream::new(
            STORE,
            ENTITY,
            (Bound::Included(raw_key.clone()), Bound::Included(raw_key)),
            Direction::Asc,
            None,
            None,
            ACCESS_SCAN_CHUNK_ENTRIES,
            None,
        );
        let mut work = HeldHeadSeekWork::unbounded();

        assert!(stream.ensure_head(&mut work).is_err());
        assert_eq!(work.pull_attempts(), 1);
    }
}
