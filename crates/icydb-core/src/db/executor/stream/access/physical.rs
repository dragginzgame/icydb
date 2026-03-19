//! Module: executor::stream::access::physical
//! Responsibility: lower executable access-path payloads into physical key streams.
//! Does not own: planner eligibility decisions or post-access semantics.
//! Boundary: physical key resolution through primary/index scan adapters.

use crate::{
    db::{
        access::{ExecutableAccessPathDispatch, StructuralKey, dispatch_executable_access_path},
        cursor::IndexScanContinuationInput,
        data::DataKey,
        direction::Direction,
        executor::stream::access::AccessScanContinuationInput,
        executor::{
            Context, ExecutableAccessPath, IndexScan, LoweredIndexPrefixSpec,
            LoweredIndexRangeSpec, OrderedKeyStreamBox, PrimaryScan, VecOrderedKeyStream,
            traversal::require_index_range_spec,
        },
        index::predicate::IndexPredicateExecution,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
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
/// PhysicalStreamRequest
///
/// Canonical request envelope for one physical key-stream resolution attempt.
/// Bundles lowered spec constraints, traversal direction, and execution hints
/// so the physical resolver boundary does not rely on loose optional args.
///

pub(super) struct PhysicalStreamRequest<'ctx, 'a, E>
where
    E: EntityKind + EntityValue,
{
    pub(super) ctx: &'a Context<'ctx, E>,
    pub(super) index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    pub(super) index_range_spec: Option<&'a LoweredIndexRangeSpec>,
    pub(super) continuation: AccessScanContinuationInput<'a>,
    pub(super) physical_fetch_hint: Option<usize>,
    pub(super) index_predicate_execution: Option<IndexPredicateExecution<'a>>,
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
}

///
/// PhysicalAccessRuntime
///
/// Execution-focused leaf runtime for one typed physical access context.
/// The outer physical-path dispatcher uses this only for concrete scan and
/// key-lowering leaves; it must not absorb match orchestration or ordering
/// normalization.
///

trait PhysicalAccessRuntime<K> {
    fn resolve_by_key(&self, key: K) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>;
    fn resolve_by_keys(&self, keys: &[K]) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>;
    fn resolve_key_range(
        &self,
        start: K,
        end: K,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>;
    fn resolve_full_scan(
        &self,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>;
    fn resolve_index_prefix(
        &self,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        direction: Direction,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>;
    fn resolve_index_multi_lookup(
        &self,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        value_count: usize,
        direction: Direction,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>;
    fn resolve_index_range(
        &self,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        continuation: IndexScanContinuationInput<'_>,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>;
}

///
/// ContextPhysicalAccessRuntime
///
/// Typed physical scan/runtime adapter bound to one recovered executor
/// context.
/// This keeps store and index leaves specialized while the parent access-path
/// match and ordering logic stay outside the entity-type implementation body.
///

struct ContextPhysicalAccessRuntime<'ctx, 'a, E>
where
    E: EntityKind + EntityValue,
{
    ctx: &'a Context<'ctx, E>,
}

impl<'ctx, 'a, E> ContextPhysicalAccessRuntime<'ctx, 'a, E>
where
    E: EntityKind + EntityValue,
{
    const fn new(ctx: &'a Context<'ctx, E>) -> Self {
        Self { ctx }
    }
}

impl<E> PhysicalAccessRuntime<E::Key> for ContextPhysicalAccessRuntime<'_, '_, E>
where
    E: EntityKind + EntityValue,
{
    fn resolve_by_key(&self, key: E::Key) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        Ok((
            vec![DataKey::try_from_field_value(E::ENTITY_TAG, &key)?],
            KeyOrderState::FinalOrder,
        ))
    }

    fn resolve_by_keys(
        &self,
        keys: &[E::Key],
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let keys = Context::<E>::dedup_keys(keys.to_vec())
            .into_iter()
            .map(|key| DataKey::try_from_field_value(E::ENTITY_TAG, &key))
            .collect::<Result<Vec<_>, _>>()?;

        Ok((keys, KeyOrderState::AscendingSorted))
    }

    fn resolve_key_range(
        &self,
        start: E::Key,
        end: E::Key,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let start = DataKey::try_from_field_value(E::ENTITY_TAG, &start)?;
        let end = DataKey::try_from_field_value(E::ENTITY_TAG, &end)?;
        let keys =
            PrimaryScan::range::<E>(self.ctx, &start, &end, direction, primary_scan_fetch_hint)?;
        let key_order_state = if primary_scan_fetch_hint.is_some() {
            KeyOrderState::FinalOrder
        } else {
            KeyOrderState::AscendingSorted
        };

        Ok((keys, key_order_state))
    }

    fn resolve_full_scan(
        &self,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let start = DataKey::lower_bound_for(E::ENTITY_TAG);
        let end = DataKey::upper_bound_for(E::ENTITY_TAG);
        let keys =
            PrimaryScan::range::<E>(self.ctx, &start, &end, direction, primary_scan_fetch_hint)?;
        let key_order_state = if primary_scan_fetch_hint.is_some() {
            KeyOrderState::FinalOrder
        } else {
            KeyOrderState::AscendingSorted
        };

        Ok((keys, key_order_state))
    }

    fn resolve_index_prefix(
        &self,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        direction: Direction,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let [spec] = index_prefix_specs else {
            return Err(crate::db::error::query_executor_invariant(
                "index-prefix execution requires pre-lowered index-prefix spec",
            ));
        };

        let fetch_limit = index_fetch_hint.unwrap_or(usize::MAX);
        let keys = IndexScan::prefix::<E>(
            self.ctx,
            spec,
            direction,
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

    fn resolve_index_multi_lookup(
        &self,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        value_count: usize,
        direction: Direction,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        if index_prefix_specs.len() != value_count {
            return Err(crate::db::error::query_executor_invariant(
                "index-multi-lookup execution requires one pre-lowered prefix spec per lookup value",
            ));
        }

        let mut keys = Vec::new();
        for spec in index_prefix_specs {
            keys.extend(IndexScan::prefix::<E>(
                self.ctx,
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

    fn resolve_index_range(
        &self,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        continuation: IndexScanContinuationInput<'_>,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let spec = require_index_range_spec(index_range_spec)?;
        let fetch_limit = index_fetch_hint.unwrap_or(usize::MAX);
        let keys = IndexScan::range::<E>(
            self.ctx,
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

///
/// StructuralKeyAccessRuntime
///
/// StructuralKeyAccessRuntime binds one recovered typed context to the
/// structural planner-key boundary used by structural fast-path traversal.
/// It recovers typed primary-key values only inside physical leaf resolution.
///

struct StructuralKeyAccessRuntime<'ctx, 'a, E>
where
    E: EntityKind + EntityValue,
{
    ctx: &'a Context<'ctx, E>,
}

impl<'ctx, 'a, E> StructuralKeyAccessRuntime<'ctx, 'a, E>
where
    E: EntityKind + EntityValue,
{
    const fn new(ctx: &'a Context<'ctx, E>) -> Self {
        Self { ctx }
    }
}

impl<E> PhysicalAccessRuntime<StructuralKey> for StructuralKeyAccessRuntime<'_, '_, E>
where
    E: EntityKind + EntityValue,
{
    fn resolve_by_key(
        &self,
        key: StructuralKey,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        Ok((
            vec![DataKey::try_from_structural_key(E::ENTITY_TAG, &key)?],
            KeyOrderState::FinalOrder,
        ))
    }

    fn resolve_by_keys(
        &self,
        keys: &[StructuralKey],
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let mut data_keys = Vec::with_capacity(keys.len());
        for key in keys {
            data_keys.push(DataKey::try_from_structural_key(E::ENTITY_TAG, key)?);
        }
        data_keys.sort_unstable();
        data_keys.dedup();

        Ok((data_keys, KeyOrderState::AscendingSorted))
    }

    fn resolve_key_range(
        &self,
        start: StructuralKey,
        end: StructuralKey,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let start = DataKey::try_from_structural_key(E::ENTITY_TAG, &start)?;
        let end = DataKey::try_from_structural_key(E::ENTITY_TAG, &end)?;
        let keys =
            PrimaryScan::range::<E>(self.ctx, &start, &end, direction, primary_scan_fetch_hint)?;
        let key_order_state = if primary_scan_fetch_hint.is_some() {
            KeyOrderState::FinalOrder
        } else {
            KeyOrderState::AscendingSorted
        };

        Ok((keys, key_order_state))
    }

    fn resolve_full_scan(
        &self,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let runtime = ContextPhysicalAccessRuntime::new(self.ctx);

        <ContextPhysicalAccessRuntime<'_, '_, E> as PhysicalAccessRuntime<E::Key>>::resolve_full_scan(
            &runtime,
            direction,
            primary_scan_fetch_hint,
        )
    }

    fn resolve_index_prefix(
        &self,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        direction: Direction,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let runtime = ContextPhysicalAccessRuntime::new(self.ctx);

        <ContextPhysicalAccessRuntime<'_, '_, E> as PhysicalAccessRuntime<E::Key>>::resolve_index_prefix(
            &runtime,
            index_prefix_specs,
            direction,
            index_fetch_hint,
            index_predicate_execution,
        )
    }

    fn resolve_index_multi_lookup(
        &self,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        value_count: usize,
        direction: Direction,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let runtime = ContextPhysicalAccessRuntime::new(self.ctx);

        <ContextPhysicalAccessRuntime<'_, '_, E> as PhysicalAccessRuntime<E::Key>>::resolve_index_multi_lookup(
            &runtime,
            index_prefix_specs,
            value_count,
            direction,
            index_predicate_execution,
        )
    }

    fn resolve_index_range(
        &self,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        continuation: IndexScanContinuationInput<'_>,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError> {
        let runtime = ContextPhysicalAccessRuntime::new(self.ctx);

        <ContextPhysicalAccessRuntime<'_, '_, E> as PhysicalAccessRuntime<E::Key>>::resolve_index_range(
            &runtime,
            index_range_spec,
            continuation,
            index_fetch_hint,
            index_predicate_execution,
        )
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
fn resolve_physical_key_stream<K>(
    path: &ExecutableAccessPath<'_, K>,
    request: PhysicalStreamBindings<'_>,
    runtime: &dyn PhysicalAccessRuntime<K>,
) -> Result<OrderedKeyStreamBox, InternalError>
where
    K: Clone,
{
    let primary_scan_fetch_hint = if path.capabilities().supports_primary_scan_fetch_hint() {
        request.physical_fetch_hint
    } else {
        None
    };

    let (mut candidates, key_order_state) = match dispatch_executable_access_path(path) {
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

    normalize_ordered_keys(
        &mut candidates,
        request.continuation.direction(),
        key_order_state,
    );

    Ok(Box::new(VecOrderedKeyStream::new(candidates)))
}

impl<K> ExecutableAccessPath<'_, K> {
    // Physical access lowering for one executable access path.
    // All store/index traversal must route through `PrimaryScan`/`IndexScan`.
    /// Build an ordered key stream for this access path.
    pub(super) fn resolve_physical_key_stream<E>(
        &self,
        request: PhysicalStreamRequest<'_, '_, E>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Clone,
    {
        let runtime = ContextPhysicalAccessRuntime::new(request.ctx);
        let bindings = PhysicalStreamBindings {
            index_prefix_specs: request.index_prefix_specs,
            index_range_spec: request.index_range_spec,
            continuation: request.continuation,
            physical_fetch_hint: request.physical_fetch_hint,
            index_predicate_execution: request.index_predicate_execution,
        };

        resolve_physical_key_stream(self, bindings, &runtime)
    }
}

impl ExecutableAccessPath<'_, StructuralKey> {
    // Physical access lowering for one structural executable access path.
    // Typed key recovery is deferred to the concrete path leaves in the
    // structural runtime adapter.
    /// Build an ordered key stream for one structural access path.
    pub(super) fn resolve_structural_physical_key_stream<E>(
        &self,
        request: PhysicalStreamRequest<'_, '_, E>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        let runtime = StructuralKeyAccessRuntime::new(request.ctx);
        let bindings = PhysicalStreamBindings {
            index_prefix_specs: request.index_prefix_specs,
            index_range_spec: request.index_range_spec,
            continuation: request.continuation,
            physical_fetch_hint: request.physical_fetch_hint,
            index_predicate_execution: request.index_predicate_execution,
        };

        resolve_physical_key_stream(self, bindings, &runtime)
    }
}
