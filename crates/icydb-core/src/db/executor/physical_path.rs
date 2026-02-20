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
    pub(super) fn resolve_physical_key_stream<E>(
        &self,
        ctx: &Context<'_, E>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy + Ord,
    {
        let mut candidates = match self {
            Self::ByKey(key) => vec![Context::<E>::data_key_from_key(*key)?],

            Self::ByKeys(keys) => Context::<E>::dedup_keys(keys.clone())
                .into_iter()
                .map(Context::<E>::data_key_from_key)
                .collect::<Result<Vec<_>, _>>()?,

            Self::KeyRange { start, end } => ctx.with_store(|s| {
                let start = Context::<E>::data_key_from_key(*start)?;
                let end = Context::<E>::data_key_from_key(*end)?;
                let start_raw = start.to_raw()?;
                let end_raw = end.to_raw()?;

                s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .map(|e| Context::<E>::decode_data_key(e.key()))
                    .collect::<Result<Vec<_>, _>>()
            })??,

            Self::FullScan => ctx.with_store(|s| {
                let start = DataKey::lower_bound::<E>();
                let end = DataKey::upper_bound::<E>();
                let start_raw = start.to_raw()?;
                let end_raw = end.to_raw()?;

                s.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    .map(|e| Context::<E>::decode_data_key(e.key()))
                    .collect::<Result<Vec<_>, _>>()
            })??,

            Self::IndexPrefix { index, values } => {
                let store = ctx
                    .db
                    .with_store_registry(|reg| reg.try_get_store(index.store))?;
                store.with_index(|s| s.resolve_data_values::<E>(index, values))?
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
                store.with_index(|s| {
                    s.resolve_data_values_in_range_from_start_exclusive::<E>(
                        index,
                        prefix,
                        lower,
                        upper,
                        index_range_anchor,
                        direction,
                    )
                })?
            }
        };

        normalize_ordered_keys(&mut candidates, direction, !self.is_index_path());

        Ok(Box::new(VecOrderedKeyStream::new(candidates)))
    }
}
