use crate::{
    db::{
        executor::{
            Context, IntersectOrderedKeyStream, KeyOrderComparator, MergeOrderedKeyStream,
            OrderedKeyStreamBox, VecOrderedKeyStream,
        },
        index::RawIndexKey,
        query::plan::{AccessPlan, Direction},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<K> AccessPlan<K> {
    // Collect one child key stream for each child access plan.
    fn collect_child_key_streams<E>(
        ctx: &Context<'_, E>,
        children: &[Self],
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
        key_comparator: KeyOrderComparator,
    ) -> Result<Vec<OrderedKeyStreamBox>, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        let mut streams = Vec::with_capacity(children.len());
        for child in children {
            streams.push(child.produce_key_stream(
                ctx,
                index_range_anchor,
                direction,
                key_comparator,
            )?);
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
    pub(super) fn produce_key_stream<E>(
        &self,
        ctx: &Context<'_, E>,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
        key_comparator: KeyOrderComparator,
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
            Self::Union(children) => Self::produce_union_key_stream(
                ctx,
                children,
                index_range_anchor,
                direction,
                key_comparator,
            ),
            Self::Intersection(children) => Self::produce_intersection_key_stream(
                ctx,
                children,
                index_range_anchor,
                direction,
                key_comparator,
            ),
        }
    }

    // Build one canonical stream for a union by pairwise-merging child streams.
    fn produce_union_key_stream<E>(
        ctx: &Context<'_, E>,
        children: &[Self],
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
        key_comparator: KeyOrderComparator,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        let streams = Self::collect_child_key_streams(
            ctx,
            children,
            index_range_anchor,
            direction,
            key_comparator,
        )?;

        Ok(Self::reduce_key_streams(streams, |left, right| {
            Box::new(MergeOrderedKeyStream::new_with_comparator(
                left,
                right,
                key_comparator,
            ))
        }))
    }

    // Build one canonical stream for an intersection by pairwise-intersecting child streams.
    fn produce_intersection_key_stream<E>(
        ctx: &Context<'_, E>,
        children: &[Self],
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
        key_comparator: KeyOrderComparator,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        let streams = Self::collect_child_key_streams(
            ctx,
            children,
            index_range_anchor,
            direction,
            key_comparator,
        )?;

        Ok(Self::reduce_key_streams(streams, |left, right| {
            Box::new(IntersectOrderedKeyStream::new_with_comparator(
                left,
                right,
                key_comparator,
            ))
        }))
    }
}
