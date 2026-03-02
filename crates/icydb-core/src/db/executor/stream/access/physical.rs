//! Module: executor::stream::access::physical
//! Responsibility: lower executable access-path payloads into physical key streams.
//! Does not own: planner eligibility decisions or post-access semantics.
//! Boundary: physical key resolution through primary/index scan adapters.

use crate::{
    db::{
        data::DataKey,
        direction::Direction,
        executor::LoweredKey,
        executor::{
            Context, ExecutableAccessPath, ExecutionPathPayload, IndexScan, LoweredIndexPrefixSpec,
            LoweredIndexRangeSpec, OrderedKeyStreamBox, PrimaryScan, VecOrderedKeyStream,
            route::primary_scan_fetch_hint_for_executable_access_path,
        },
        index::predicate::IndexPredicateExecution,
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

impl<K> ExecutableAccessPath<'_, K> {
    // Physical access lowering for one executable access path.
    // All store/index traversal must route through `PrimaryScan`/`IndexScan`.
    /// Build an ordered key stream for this access path.
    #[expect(clippy::too_many_arguments)]
    pub(super) fn resolve_physical_key_stream<E>(
        &self,
        ctx: &Context<'_, E>,
        index_prefix_spec: Option<&LoweredIndexPrefixSpec>,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        index_range_anchor: Option<&LoweredKey>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        // Only apply bounded physical scans where key-stream semantics remain
        // equivalent without requiring full-set normalization.
        let primary_scan_fetch_hint =
            primary_scan_fetch_hint_for_executable_access_path(self, physical_fetch_hint);

        // Resolve candidate keys and track explicit ordering state.
        let (mut candidates, key_order_state) = match self.payload() {
            ExecutionPathPayload::ByKey(key) => Self::resolve_by_key::<E>(**key)?,
            ExecutionPathPayload::ByKeys(keys) => Self::resolve_by_keys::<E>(keys)?,
            ExecutionPathPayload::KeyRange { start, end } => Self::resolve_key_range::<E>(
                ctx,
                **start,
                **end,
                direction,
                primary_scan_fetch_hint,
            )?,
            ExecutionPathPayload::FullScan => {
                Self::resolve_full_scan::<E>(ctx, direction, primary_scan_fetch_hint)?
            }
            ExecutionPathPayload::IndexPrefix => Self::resolve_index_prefix::<E>(
                ctx,
                self.index_prefix_details().map(|(index, _)| index),
                index_prefix_spec,
                direction,
                physical_fetch_hint,
                index_predicate_execution,
            )?,
            ExecutionPathPayload::IndexRange { .. } => Self::resolve_index_range::<E>(
                ctx,
                self.index_range_details().map(|(index, _)| index),
                index_range_spec,
                index_range_anchor,
                direction,
                physical_fetch_hint,
                index_predicate_execution,
            )?,
        };

        Self::normalize_ordered_keys(&mut candidates, direction, key_order_state);

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
        index_prefix_spec: Option<&LoweredIndexPrefixSpec>,
        direction: Direction,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, KeyOrderState), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        let Some(spec) = index_prefix_spec else {
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

    // Resolve one index-range traversal using a pre-lowered index-range spec.
    fn resolve_index_range<E>(
        ctx: &Context<'_, E>,
        _index: Option<IndexModel>,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        index_range_anchor: Option<&LoweredKey>,
        direction: Direction,
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
            index_range_anchor,
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
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
