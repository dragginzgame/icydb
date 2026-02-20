use crate::{
    db::{
        Db,
        data::{DataKey, DataRow, DataStore, RawDataKey, RawRow},
        entity_decode::{decode_and_validate_entity_key, format_entity_key_for_mismatch},
        executor::{
            ExecutorError, IntersectOrderedKeyStream, MergeOrderedKeyStream, OrderedKeyStream,
            OrderedKeyStreamBox, VecOrderedKeyStream, normalize_ordered_keys,
        },
        index::RawIndexKey,
        query::{
            ReadConsistency,
            plan::{AccessPath, AccessPlan, Direction},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue, Path},
    types::Id,
};
use std::{collections::BTreeSet, marker::PhantomData, ops::Bound};

///
/// Context
///

pub(crate) struct Context<'a, E: EntityKind + EntityValue> {
    pub db: &'a Db<E::Canister>,
    _marker: PhantomData<E>,
}

impl<'a, E> Context<'a, E>
where
    E: EntityKind + EntityValue,
{
    #[must_use]
    pub(crate) const fn new(db: &'a Db<E::Canister>) -> Self {
        Self {
            db,
            _marker: PhantomData,
        }
    }

    // ------------------------------------------------------------------
    // Store access
    // ------------------------------------------------------------------

    pub(crate) fn with_store<R>(
        &self,
        f: impl FnOnce(&DataStore) -> R,
    ) -> Result<R, InternalError> {
        self.db.with_store_registry(|reg| {
            reg.try_get_store(E::Store::PATH)
                .map(|store| store.with_data(f))
        })
    }

    // ------------------------------------------------------------------
    // Row reads
    // ------------------------------------------------------------------

    pub(crate) fn read(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw()?;
            s.get(&raw)
                .ok_or_else(|| InternalError::store_not_found(key.to_string()))
        })?
    }

    pub(crate) fn read_strict(&self, key: &DataKey) -> Result<RawRow, InternalError> {
        self.with_store(|s| {
            let raw = key.to_raw()?;
            s.get(&raw).ok_or_else(|| {
                ExecutorError::store_corruption(format!("missing row: {key}")).into()
            })
        })?
    }

    // ------------------------------------------------------------------
    // Access path analysis
    // ------------------------------------------------------------------

    pub(crate) fn ordered_key_stream_from_access_with_index_range_anchor(
        &self,
        access: &AccessPath<E::Key>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind,
    {
        access.produce_key_stream(self, index_range_anchor, direction)
    }

    pub(crate) fn rows_from_access_plan(
        &self,
        access: &AccessPlan<E::Key>,
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError>
    where
        E: EntityKind,
    {
        self.rows_from_access_plan_with_index_range_anchor(
            access,
            consistency,
            None,
            Direction::Asc,
        )
    }

    pub(crate) fn ordered_key_stream_from_access_plan_with_index_range_anchor(
        &self,
        access: &AccessPlan<E::Key>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind,
    {
        access.produce_key_stream(self, index_range_anchor, direction)
    }

    pub(crate) fn rows_from_access_plan_with_index_range_anchor(
        &self,
        access: &AccessPlan<E::Key>,
        consistency: ReadConsistency,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
    ) -> Result<Vec<DataRow>, InternalError>
    where
        E: EntityKind,
    {
        let mut key_stream = self.ordered_key_stream_from_access_plan_with_index_range_anchor(
            access,
            index_range_anchor,
            direction,
        )?;

        self.rows_from_ordered_key_stream(key_stream.as_mut(), consistency)
    }

    // Load rows for an ordered key stream by preserving the stream order.
    pub(crate) fn rows_from_ordered_key_stream(
        &self,
        key_stream: &mut dyn OrderedKeyStream,
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError> {
        let keys = Self::collect_ordered_keys(key_stream)?;

        self.load_many_with_consistency(&keys, consistency)
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    fn data_key_from_key(key: E::Key) -> Result<DataKey, InternalError>
    where
        E: EntityKind,
    {
        DataKey::try_new::<E>(key)
    }

    fn dedup_keys(keys: Vec<E::Key>) -> Vec<E::Key> {
        let mut set = BTreeSet::new();
        set.extend(keys);
        set.into_iter().collect()
    }

    fn collect_ordered_keys(
        key_stream: &mut dyn OrderedKeyStream,
    ) -> Result<Vec<DataKey>, InternalError> {
        let mut keys = Vec::new();
        while let Some(key) = key_stream.next_key()? {
            keys.push(key);
        }

        Ok(keys)
    }

    fn load_many_with_consistency(
        &self,
        keys: &[DataKey],
        consistency: ReadConsistency,
    ) -> Result<Vec<DataRow>, InternalError> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let row = match consistency {
                ReadConsistency::Strict => self.read_strict(key),
                ReadConsistency::MissingOk => self.read(key),
            };

            match row {
                Ok(row) => out.push((key.clone(), row)),
                Err(err) if err.is_not_found() => {}
                Err(err) => return Err(err),
            }
        }
        Ok(out)
    }

    fn decode_data_key(raw: &RawDataKey) -> Result<DataKey, InternalError> {
        DataKey::try_from_raw(raw).map_err(|err| ExecutorError::store_corruption_from(err).into())
    }

    pub(crate) fn deserialize_rows(rows: Vec<DataRow>) -> Result<Vec<(Id<E>, E)>, InternalError>
    where
        E: EntityKind + EntityValue,
    {
        rows.into_iter()
            .map(|(key, row)| {
                let expected_key = key.try_key::<E>()?;
                let entity = decode_and_validate_entity_key::<E, _, _, _, _>(
                    expected_key,
                    || row.try_decode::<E>(),
                    |err| {
                        ExecutorError::serialize_corruption(format!(
                            "failed to deserialize row: {key} ({err})"
                        ))
                        .into()
                    },
                    |expected_key, actual_key| {
                        let expected = format_entity_key_for_mismatch::<E>(expected_key);
                        let found = format_entity_key_for_mismatch::<E>(actual_key);

                        ExecutorError::store_corruption(format!(
                            "row key mismatch: expected {expected}, found {found}"
                        ))
                        .into()
                    },
                )?;

                Ok((Id::from_key(expected_key), entity))
            })
            .collect()
    }
}

impl<K> AccessPlan<K> {
    // Collect one child key stream for each child access plan.
    fn collect_child_key_streams<E>(
        ctx: &Context<'_, E>,
        children: &[Self],
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
    ) -> Result<Vec<OrderedKeyStreamBox>, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        let mut streams = Vec::with_capacity(children.len());
        for child in children {
            streams.push(child.produce_key_stream(ctx, index_range_anchor, direction)?);
        }

        Ok(streams)
    }

    // Reduce child streams pairwise using a stream combiner.
    fn reduce_key_streams<F>(
        mut streams: Vec<OrderedKeyStreamBox>,
        combiner: F,
    ) -> OrderedKeyStreamBox
    where
        F: Fn(OrderedKeyStreamBox, OrderedKeyStreamBox) -> OrderedKeyStreamBox,
    {
        if streams.is_empty() {
            return Box::new(VecOrderedKeyStream::new(Vec::new()));
        }
        if streams.len() == 1 {
            return streams
                .pop()
                .unwrap_or_else(|| Box::new(VecOrderedKeyStream::new(Vec::new())));
        }

        while streams.len() > 1 {
            let mut next_round = Vec::with_capacity((streams.len().saturating_add(1)) / 2);
            let mut iter = streams.into_iter();
            while let Some(left) = iter.next() {
                if let Some(right) = iter.next() {
                    next_round.push(combiner(left, right));
                } else {
                    next_round.push(left);
                }
            }
            streams = next_round;
        }

        streams
            .pop()
            .unwrap_or_else(|| Box::new(VecOrderedKeyStream::new(Vec::new())))
    }

    // Build an ordered key stream for this access plan.
    fn produce_key_stream<E>(
        &self,
        ctx: &Context<'_, E>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        match self {
            Self::Path(path) => ctx.ordered_key_stream_from_access_with_index_range_anchor(
                path,
                index_range_anchor,
                direction,
            ),
            Self::Union(children) => {
                Self::produce_union_key_stream(ctx, children, index_range_anchor, direction)
            }
            Self::Intersection(children) => {
                Self::produce_intersection_key_stream(ctx, children, index_range_anchor, direction)
            }
        }
    }

    // Build one canonical stream for a union by pairwise-merging child streams.
    fn produce_union_key_stream<E>(
        ctx: &Context<'_, E>,
        children: &[Self],
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        let streams =
            Self::collect_child_key_streams(ctx, children, index_range_anchor, direction)?;

        Ok(Self::reduce_key_streams(streams, |left, right| {
            Box::new(MergeOrderedKeyStream::new(left, right, direction))
        }))
    }

    // Build one canonical stream for an intersection by pairwise-intersecting child streams.
    fn produce_intersection_key_stream<E>(
        ctx: &Context<'_, E>,
        children: &[Self],
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        let streams =
            Self::collect_child_key_streams(ctx, children, index_range_anchor, direction)?;

        Ok(Self::reduce_key_streams(streams, |left, right| {
            Box::new(IntersectOrderedKeyStream::new(left, right, direction))
        }))
    }
}

impl<K> AccessPath<K> {
    /// Build an ordered key stream for this access path.
    fn produce_key_stream<E>(
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
