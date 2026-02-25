use crate::{
    db::{
        data::DataKey,
        executor::{
            Context, LoweredIndexPrefixSpec, LoweredIndexRangeSpec, OrderedKeyStreamBox,
            VecOrderedKeyStream, normalize_ordered_keys,
        },
        index::predicate::IndexPredicateExecution,
        lowering::LoweredKey,
        query::plan::{AccessPath, Direction},
    },
    error::InternalError,
    model::index::IndexModel,
    traits::{EntityKind, EntityValue},
};
use std::ops::Bound;

impl<K> AccessPath<K> {
    // Physical access lowering for one access path.
    // Direct store/index traversal here is intentional and resolver-owned.
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
        let primary_scan_fetch_hint = self.primary_scan_fetch_hint(physical_fetch_hint);

        // Resolve candidate keys and track whether the vector is already in
        // final stream order for the requested direction.
        let (mut candidates, already_in_stream_order) = match self {
            Self::ByKey(key) => Self::resolve_by_key::<E>(*key)?,
            Self::ByKeys(keys) => Self::resolve_by_keys::<E>(keys)?,
            Self::KeyRange { start, end } => {
                Self::resolve_key_range::<E>(ctx, *start, *end, direction, primary_scan_fetch_hint)?
            }
            Self::FullScan => {
                Self::resolve_full_scan::<E>(ctx, direction, primary_scan_fetch_hint)?
            }
            Self::IndexPrefix { index, .. } => Self::resolve_index_prefix::<E>(
                ctx,
                index,
                index_prefix_spec,
                direction,
                physical_fetch_hint,
                index_predicate_execution,
            )?,
            Self::IndexRange { spec } => Self::resolve_index_range::<E>(
                ctx,
                spec.index(),
                index_range_spec,
                index_range_anchor,
                direction,
                physical_fetch_hint,
                index_predicate_execution,
            )?,
        };

        if !already_in_stream_order {
            normalize_ordered_keys(&mut candidates, direction, !self.is_index_path());
        }

        Ok(Box::new(VecOrderedKeyStream::new(candidates)))
    }

    // Only primary-data scans support safe bounded physical probing.
    const fn primary_scan_fetch_hint(&self, physical_fetch_hint: Option<usize>) -> Option<usize> {
        match self {
            Self::ByKey(_) | Self::KeyRange { .. } | Self::FullScan => physical_fetch_hint,
            Self::ByKeys(_) | Self::IndexPrefix { .. } | Self::IndexRange { .. } => None,
        }
    }

    // Resolve one direct primary-key lookup.
    fn resolve_by_key<E>(key: K) -> Result<(Vec<DataKey>, bool), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        Ok((vec![Context::<E>::data_key_from_key(key)?], true))
    }

    // Resolve one `ByKeys` shape with canonical deduplication.
    fn resolve_by_keys<E>(keys: &[K]) -> Result<(Vec<DataKey>, bool), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        let keys = Context::<E>::dedup_keys(keys.to_vec())
            .into_iter()
            .map(Context::<E>::data_key_from_key)
            .collect::<Result<Vec<_>, _>>()?;

        Ok((keys, false))
    }

    // Resolve one primary-key range traversal.
    fn resolve_key_range<E>(
        ctx: &Context<'_, E>,
        start: K,
        end: K,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<(Vec<DataKey>, bool), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        // Direction-aware bounded scan can stop after the hint count.
        if let Some(fetch_limit) = primary_scan_fetch_hint {
            let keys = ctx.with_store(|s| -> Result<Vec<DataKey>, InternalError> {
                let start = Context::<E>::data_key_from_key(start)?;
                let end = Context::<E>::data_key_from_key(end)?;
                let start_raw = start.to_raw()?;
                let end_raw = end.to_raw()?;
                let range = (Bound::Included(start_raw), Bound::Included(end_raw));
                let mut out = Vec::new();
                if fetch_limit > 0 {
                    match direction {
                        Direction::Asc => {
                            for entry in s.range(range) {
                                out.push(Context::<E>::decode_data_key(entry.key())?);
                                if out.len() == fetch_limit {
                                    break;
                                }
                            }
                        }
                        Direction::Desc => {
                            for entry in s.range(range).rev() {
                                out.push(Context::<E>::decode_data_key(entry.key())?);
                                if out.len() == fetch_limit {
                                    break;
                                }
                            }
                        }
                    }
                }

                Ok(out)
            })??;

            return Ok((keys, true));
        }

        let keys = ctx.with_store(|s| {
            let start = Context::<E>::data_key_from_key(start)?;
            let end = Context::<E>::data_key_from_key(end)?;
            let start_raw = start.to_raw()?;
            let end_raw = end.to_raw()?;

            s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                .map(|e| Context::<E>::decode_data_key(e.key()))
                .collect::<Result<Vec<_>, _>>()
        })??;

        Ok((keys, false))
    }

    // Resolve one full primary-key scan traversal.
    fn resolve_full_scan<E>(
        ctx: &Context<'_, E>,
        direction: Direction,
        primary_scan_fetch_hint: Option<usize>,
    ) -> Result<(Vec<DataKey>, bool), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        // Direction-aware bounded scan can stop after the hint count.
        if let Some(fetch_limit) = primary_scan_fetch_hint {
            let keys = ctx.with_store(|s| -> Result<Vec<DataKey>, InternalError> {
                let start = DataKey::lower_bound::<E>();
                let end = DataKey::upper_bound::<E>();
                let start_raw = start.to_raw()?;
                let end_raw = end.to_raw()?;
                let range = (Bound::Included(start_raw), Bound::Included(end_raw));
                let mut out = Vec::new();
                if fetch_limit > 0 {
                    match direction {
                        Direction::Asc => {
                            for entry in s.range(range) {
                                out.push(Context::<E>::decode_data_key(entry.key())?);
                                if out.len() == fetch_limit {
                                    break;
                                }
                            }
                        }
                        Direction::Desc => {
                            for entry in s.range(range).rev() {
                                out.push(Context::<E>::decode_data_key(entry.key())?);
                                if out.len() == fetch_limit {
                                    break;
                                }
                            }
                        }
                    }
                }

                Ok(out)
            })??;

            return Ok((keys, true));
        }

        let keys = ctx.with_store(|s| {
            let start = DataKey::lower_bound::<E>();
            let end = DataKey::upper_bound::<E>();
            let start_raw = start.to_raw()?;
            let end_raw = end.to_raw()?;

            s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                .map(|e| Context::<E>::decode_data_key(e.key()))
                .collect::<Result<Vec<_>, _>>()
        })??;

        Ok((keys, false))
    }

    // Resolve one index-prefix traversal using a pre-lowered index-prefix spec.
    fn resolve_index_prefix<E>(
        ctx: &Context<'_, E>,
        _index: &IndexModel,
        index_prefix_spec: Option<&LoweredIndexPrefixSpec>,
        direction: Direction,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, bool), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        let Some(spec) = index_prefix_spec else {
            return Err(InternalError::query_executor_invariant(
                "index-prefix execution requires pre-lowered index-prefix spec",
            ));
        };

        let store = ctx
            .db
            .with_store_registry(|reg| reg.try_get_store(spec.index().store))?;
        let fetch_limit = index_fetch_hint.unwrap_or(usize::MAX);
        let keys = store.with_index(|s| {
            s.resolve_data_values_in_raw_range_limited::<E>(
                spec.index(),
                (spec.lower(), spec.upper()),
                None,
                direction,
                fetch_limit,
                index_predicate_execution,
            )
        })?;

        Ok((keys, index_fetch_hint.is_some()))
    }

    // Resolve one index-range traversal using a pre-lowered index-range spec.
    fn resolve_index_range<E>(
        ctx: &Context<'_, E>,
        _index: &IndexModel,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
        index_range_anchor: Option<&LoweredKey>,
        direction: Direction,
        index_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<(Vec<DataKey>, bool), InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        let Some(spec) = index_range_spec else {
            return Err(InternalError::query_executor_invariant(
                "index-range execution requires pre-lowered index-range spec",
            ));
        };

        let store = ctx
            .db
            .with_store_registry(|reg| reg.try_get_store(spec.index().store))?;
        let fetch_limit = index_fetch_hint.unwrap_or(usize::MAX);
        let keys = store.with_index(|s| {
            s.resolve_data_values_in_raw_range_limited::<E>(
                spec.index(),
                (spec.lower(), spec.upper()),
                index_range_anchor,
                direction,
                fetch_limit,
                index_predicate_execution,
            )
        })?;

        Ok((keys, index_fetch_hint.is_some()))
    }
}
