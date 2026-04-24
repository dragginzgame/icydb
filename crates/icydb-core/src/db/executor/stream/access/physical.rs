//! Module: executor::stream::access::physical
//! Responsibility: lower executable access-path payloads into physical key streams.
//! Does not own: planner eligibility decisions or post-access semantics.
//! Boundary: physical key resolution through primary/index scan adapters.

use crate::{
    db::{
        access::{ExecutableAccessPathDispatch, dispatch_executable_access_path},
        cursor::IndexScanContinuationInput,
        data::DataKey,
        direction::Direction,
        executor::stream::access::AccessScanContinuationInput,
        executor::{
            ExecutableAccessPath, IndexScan, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            OrderedKeyStreamBox, PrimaryScan, ordered_key_stream_from_materialized_keys,
            traversal::IndexRangeTraversalContract,
        },
        index::predicate::IndexPredicateExecution,
        registry::StoreHandle,
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};

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

    // Resolve one primary-key range scan.
    fn resolve_key_range(
        &self,
        start: Value,
        end: Value,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let start = DataKey::try_from_structural_key(self.entity_tag, &start)?;
        let end = DataKey::try_from_structural_key(self.entity_tag, &end)?;
        let keys = PrimaryScan::range_structural(
            self.store,
            &start,
            &end,
            direction,
            primary_scan_fetch_hint,
        )?;
        let key_order_state = if primary_scan_fetch_hint.is_some() {
            KeyOrderState::FinalOrder
        } else {
            KeyOrderState::AscendingSorted
        };

        Ok((keys, key_order_state))
    }

    // Resolve one full primary-key scan.
    fn resolve_full_scan(
        &self,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let start = DataKey::lower_bound_for(self.entity_tag);
        let end = DataKey::upper_bound_for(self.entity_tag);
        let keys = PrimaryScan::range_structural(
            self.store,
            &start,
            &end,
            direction,
            primary_scan_fetch_hint,
        )?;
        let key_order_state = if primary_scan_fetch_hint.is_some() {
            KeyOrderState::FinalOrder
        } else {
            KeyOrderState::AscendingSorted
        };

        Ok((keys, key_order_state))
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

// Resolve one physical access path by dispatching only the coarse path shape
// through the runtime leaf boundary.
fn resolve_physical_key_stream(
    path: &ExecutableAccessPath<'_, Value>,
    request: PhysicalStreamBindings<'_>,
    runtime: &KeyAccessRuntime,
) -> Result<OrderedKeyStreamBox, InternalError> {
    let dispatch = dispatch_executable_access_path(path);
    let primary_scan_fetch_hint = if path.capabilities().supports_primary_scan_fetch_hint() {
        request.physical_fetch_hint
    } else {
        None
    };

    let (mut candidates, mut key_order_state) = match dispatch {
        ExecutableAccessPathDispatch::ByKey(key) => runtime.resolve_by_key(key.clone())?,
        ExecutableAccessPathDispatch::ByKeys(keys) => runtime.resolve_by_keys(keys)?,
        ExecutableAccessPathDispatch::KeyRange { start, end } => runtime.resolve_key_range(
            start.clone(),
            end.clone(),
            request.continuation.direction(),
            primary_scan_fetch_hint,
        )?,
        ExecutableAccessPathDispatch::FullScan => {
            runtime.resolve_full_scan(request.continuation.direction(), primary_scan_fetch_hint)?
        }
        ExecutableAccessPathDispatch::IndexPrefix { .. } => runtime.resolve_index_prefix(
            request.index_prefix_specs,
            request.continuation.direction(),
            request.physical_fetch_hint,
            request.index_predicate_execution,
        )?,
        ExecutableAccessPathDispatch::IndexMultiLookup { value_count, .. } => runtime
            .resolve_index_multi_lookup(
                request.index_prefix_specs,
                value_count,
                request.continuation.direction(),
                request.index_predicate_execution,
            )?,
        ExecutableAccessPathDispatch::IndexRange { .. } => runtime.resolve_index_range(
            request.index_range_spec,
            request.continuation.index_scan_continuation(),
            request.physical_fetch_hint,
            request.index_predicate_execution,
        )?,
    };

    // Top-level single-path secondary-index scans must preserve physical index
    // traversal order so route-owned secondary ORDER BY contracts can drive
    // paging without an extra materialized reorder. Composite child streams
    // still disable this flag so merge/intersection reducers continue to
    // consume canonical `DataKey` order.
    if request.preserve_leaf_index_order
        && matches!(
            dispatch,
            ExecutableAccessPathDispatch::IndexPrefix { .. }
                | ExecutableAccessPathDispatch::IndexRange { .. }
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

impl ExecutableAccessPath<'_, Value> {
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
