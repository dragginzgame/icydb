//! Module: executor::stream::access::physical
//! Responsibility: lower executable access-path payloads into physical key streams.
//! Does not own: planner eligibility decisions or post-access semantics.
//! Boundary: physical key resolution through primary/index scan adapters.

use crate::{
    db::{
        access::{ExecutableAccessPathDispatch, dispatch_executable_access_path},
        data::DataKey,
        direction::Direction,
        executor::stream::access::AccessScanContinuationInput,
        executor::{
            Context, ExecutableAccessPath, IndexScan, LoweredIndexPrefixSpec,
            LoweredIndexRangeSpec, OrderedKeyStreamBox, PrimaryScan, VecOrderedKeyStream,
            route::primary_scan_fetch_hint_for_executable_access_path,
        },
        index::{IndexScanContinuationInput, predicate::IndexPredicateExecution},
    },
    error::InternalError,
    model::index::IndexModel,
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
        K: Copy + Ord,
    {
        let PhysicalStreamRequest {
            ctx,
            index_prefix_specs,
            index_range_spec,
            continuation,
            physical_fetch_hint,
            index_predicate_execution,
        } = request;

        // Only apply bounded physical scans where key-stream semantics remain
        // equivalent without requiring full-set normalization.
        let primary_scan_fetch_hint =
            primary_scan_fetch_hint_for_executable_access_path(self, physical_fetch_hint);

        // Resolve candidate keys and track explicit ordering state.
        let (mut candidates, key_order_state) = match dispatch_executable_access_path(self) {
            ExecutableAccessPathDispatch::ByKey(key) => Self::resolve_by_key::<E>(*key)?,
            ExecutableAccessPathDispatch::ByKeys(keys) => Self::resolve_by_keys::<E>(keys)?,
            ExecutableAccessPathDispatch::KeyRange { start, end } => Self::resolve_key_range::<E>(
                ctx,
                *start,
                *end,
                continuation.direction(),
                primary_scan_fetch_hint,
            )?,
            ExecutableAccessPathDispatch::FullScan => Self::resolve_full_scan::<E>(
                ctx,
                continuation.direction(),
                primary_scan_fetch_hint,
            )?,
            ExecutableAccessPathDispatch::IndexPrefix { index } => Self::resolve_index_prefix::<E>(
                ctx,
                index,
                index_prefix_specs,
                continuation.direction(),
                physical_fetch_hint,
                index_predicate_execution,
            )?,
            ExecutableAccessPathDispatch::IndexMultiLookup { index, value_count } => {
                Self::resolve_index_multi_lookup::<E>(
                    ctx,
                    index,
                    index_prefix_specs,
                    value_count,
                    continuation.direction(),
                    index_predicate_execution,
                )?
            }
            ExecutableAccessPathDispatch::IndexRange { index, .. } => {
                Self::resolve_index_range::<E>(
                    ctx,
                    index,
                    index_range_spec,
                    continuation.index_scan_continuation(),
                    physical_fetch_hint,
                    index_predicate_execution,
                )?
            }
        };

        Self::normalize_ordered_keys(&mut candidates, continuation.direction(), key_order_state);

        Ok(Box::new(VecOrderedKeyStream::new(candidates)))
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

    // Resolve one direct primary-key lookup.
    fn resolve_by_key<E>(key: K) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        Ok((
            vec![Context::<E>::data_key_from_key(key)?],
            KeyOrderState::FinalOrder,
        ))
    }

    // Resolve one `ByKeys` shape with canonical deduplication.
    fn resolve_by_keys<E>(keys: &[K]) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        let keys = Context::<E>::dedup_keys(keys.to_vec())
            .into_iter()
            .map(Context::<E>::data_key_from_key)
            .collect::<Result<Vec<_>, _>>()?;

        Ok((keys, KeyOrderState::AscendingSorted))
    }

    // Resolve one primary-key range traversal.
    fn resolve_key_range<E>(
        ctx: &Context<'_, E>,
        start: K,
        end: K,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        let start = Context::<E>::data_key_from_key(start)?;
        let end = Context::<E>::data_key_from_key(end)?;
        let keys = PrimaryScan::range::<E>(ctx, &start, &end, direction, primary_scan_fetch_hint)?;
        let key_order_state = if primary_scan_fetch_hint.is_some() {
            KeyOrderState::FinalOrder
        } else {
            KeyOrderState::AscendingSorted
        };

        Ok((keys, key_order_state))
    }

    // Resolve one full primary-key scan traversal.
    fn resolve_full_scan<E>(
        ctx: &Context<'_, E>,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        let start = DataKey::lower_bound::<E>();
        let end = DataKey::upper_bound::<E>();
        let keys = PrimaryScan::range::<E>(ctx, &start, &end, direction, primary_scan_fetch_hint)?;
        let key_order_state = if primary_scan_fetch_hint.is_some() {
            KeyOrderState::FinalOrder
        } else {
            KeyOrderState::AscendingSorted
        };

        Ok((keys, key_order_state))
    }

    // Resolve one index-prefix traversal using a pre-lowered index-prefix spec.
    fn resolve_index_prefix<E>(
        ctx: &Context<'_, E>,
        _index: Option<IndexModel>,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        direction: Direction,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        let [spec] = index_prefix_specs else {
            return Err(invariant(
                "index-prefix execution requires pre-lowered index-prefix spec",
            ));
        };

        let fetch_limit = index_fetch_hint.unwrap_or(usize::MAX);
        let keys =
            IndexScan::prefix::<E>(ctx, spec, direction, fetch_limit, index_predicate_execution)?;

        let key_order_state = if index_fetch_hint.is_some() {
            KeyOrderState::FinalOrder
        } else {
            KeyOrderState::Unordered
        };

        Ok((keys, key_order_state))
    }

    // Resolve one index multi-lookup traversal by scanning each pre-lowered
    // one-field index-prefix bucket and unioning emitted keys.
    fn resolve_index_multi_lookup<E>(
        ctx: &Context<'_, E>,
        _index: Option<IndexModel>,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        value_count: usize,
        direction: Direction,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        if index_prefix_specs.len() != value_count {
            return Err(invariant(
                "index-multi-lookup execution requires one pre-lowered prefix spec per lookup value",
            ));
        }

        let mut keys = Vec::new();
        for spec in index_prefix_specs {
            keys.extend(IndexScan::prefix::<E>(
                ctx,
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

    // Resolve one index-range traversal using a pre-lowered index-range spec.
    fn resolve_index_range<E>(
        ctx: &Context<'_, E>,
        _index: Option<IndexModel>,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        continuation: IndexScanContinuationInput<'_>,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        let Some(spec) = index_range_spec else {
            return Err(invariant(
                "index-range execution requires pre-lowered index-range spec",
            ));
        };

        let fetch_limit = index_fetch_hint.unwrap_or(usize::MAX);
        let keys = IndexScan::range::<E>(
            ctx,
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

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
