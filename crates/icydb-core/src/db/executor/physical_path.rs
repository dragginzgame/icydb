use crate::{
    db::{
        data::DataKey,
        executor::{Context, OrderedKeyStreamBox, VecOrderedKeyStream, normalize_ordered_keys},
        index::RawIndexKey,
        query::plan::{AccessPath, Direction},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::ops::Bound;

impl<K> AccessPath<K> {
    /// Build an ordered key stream for this access path.
    #[expect(clippy::too_many_lines)]
    pub(super) fn resolve_physical_key_stream<E>(
        &self,
        ctx: &Context<'_, E>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
        physical_fetch_hint: Option<usize>,
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
            Self::ByKey(key) => (vec![Context::<E>::data_key_from_key(*key)?], true),

            Self::ByKeys(keys) => (
                Context::<E>::dedup_keys(keys.clone())
                    .into_iter()
                    .map(Context::<E>::data_key_from_key)
                    .collect::<Result<Vec<_>, _>>()?,
                false,
            ),

            Self::KeyRange { start, end } => {
                // Direction-aware bounded scan can stop after the hint count.
                if let Some(fetch_limit) = primary_scan_fetch_hint {
                    let keys = ctx.with_store(|s| -> Result<Vec<DataKey>, InternalError> {
                        let start = Context::<E>::data_key_from_key(*start)?;
                        let end = Context::<E>::data_key_from_key(*end)?;
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

                    (keys, true)
                } else {
                    let keys = ctx.with_store(|s| {
                        let start = Context::<E>::data_key_from_key(*start)?;
                        let end = Context::<E>::data_key_from_key(*end)?;
                        let start_raw = start.to_raw()?;
                        let end_raw = end.to_raw()?;

                        s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                            .map(|e| Context::<E>::decode_data_key(e.key()))
                            .collect::<Result<Vec<_>, _>>()
                    })??;

                    (keys, false)
                }
            }

            Self::FullScan => {
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

                    (keys, true)
                } else {
                    let keys = ctx.with_store(|s| {
                        let start = DataKey::lower_bound::<E>();
                        let end = DataKey::upper_bound::<E>();
                        let start_raw = start.to_raw()?;
                        let end_raw = end.to_raw()?;

                        s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                            .map(|e| Context::<E>::decode_data_key(e.key()))
                            .collect::<Result<Vec<_>, _>>()
                    })??;

                    (keys, false)
                }
            }

            Self::IndexPrefix { index, values } => {
                let store = ctx
                    .db
                    .with_store_registry(|reg| reg.try_get_store(index.store))?;
                let keys = store.with_index(|s| s.resolve_data_values::<E>(index, values))?;

                (keys, false)
            }

            Self::IndexRange {
                index,
                prefix,
                lower,
                upper,
            } => {
                let store = ctx
                    .db
                    .with_store_registry(|reg| reg.try_get_store(index.store))?;
                let keys = store.with_index(|s| {
                    s.resolve_data_values_in_range_from_start_exclusive::<E>(
                        index,
                        prefix,
                        lower,
                        upper,
                        index_range_anchor,
                        direction,
                    )
                })?;

                (keys, false)
            }
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
}
