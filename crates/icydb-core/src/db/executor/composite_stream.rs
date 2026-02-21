use crate::{
    db::{
        executor::{
            AccessSpecCursor, AccessStreamInputs, IntersectOrderedKeyStream, MergeOrderedKeyStream,
            OrderedKeyStreamBox, VecOrderedKeyStream,
        },
        query::plan::{AccessPath, AccessPlan, IndexPrefixSpec, IndexRangeSpec},
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<K> AccessPlan<K> {
    // Validate that a consumed prefix spec belongs to the same index path node.
    fn validate_index_prefix_spec_alignment(
        path: &AccessPath<K>,
        index_prefix_spec: Option<&IndexPrefixSpec>,
    ) -> Result<(), InternalError> {
        if let (Some(spec), AccessPath::IndexPrefix { index, .. }) = (index_prefix_spec, path)
            && spec.index() != index
        {
            return Err(InternalError::query_executor_invariant(
                "index-prefix spec does not match access path index",
            ));
        }

        Ok(())
    }

    // Validate that a consumed range spec belongs to the same index path node.
    fn validate_index_range_spec_alignment(
        path: &AccessPath<K>,
        index_range_spec: Option<&IndexRangeSpec>,
    ) -> Result<(), InternalError> {
        if let (Some(spec), AccessPath::IndexRange { index, .. }) = (index_range_spec, path)
            && spec.index() != index
        {
            return Err(InternalError::query_executor_invariant(
                "index-range spec does not match access path index",
            ));
        }

        Ok(())
    }

    // Collect one child key stream for each child access plan.
    fn collect_child_key_streams<'a, E>(
        children: &[Self],
        inputs: &AccessStreamInputs<'_, 'a, E>,
        spec_cursor: &mut AccessSpecCursor<'a>,
    ) -> Result<Vec<OrderedKeyStreamBox>, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        let mut streams = Vec::with_capacity(children.len());
        for child in children {
            // Composite plans never need physical fetch-hint expansion on child lookups.
            let child_inputs = inputs.with_physical_fetch_hint(None);
            streams.push(child.produce_key_stream(&child_inputs, spec_cursor)?);
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
    pub(super) fn produce_key_stream<'a, E>(
        &self,
        inputs: &AccessStreamInputs<'_, 'a, E>,
        spec_cursor: &mut AccessSpecCursor<'a>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        match self {
            Self::Path(path) => {
                let index_prefix_spec = if matches!(path.as_ref(), AccessPath::IndexPrefix { .. }) {
                    spec_cursor.next_index_prefix_spec()
                } else {
                    None
                };
                let index_range_spec = if matches!(path.as_ref(), AccessPath::IndexRange { .. }) {
                    spec_cursor.next_index_range_spec()
                } else {
                    None
                };
                Self::validate_index_prefix_spec_alignment(path.as_ref(), index_prefix_spec)?;
                Self::validate_index_range_spec_alignment(path.as_ref(), index_range_spec)?;

                inputs
                    .ctx
                    .ordered_key_stream_from_access_with_index_range_anchor(
                        path,
                        index_prefix_spec,
                        index_range_spec,
                        inputs.index_range_anchor,
                        inputs.direction,
                        inputs.physical_fetch_hint,
                    )
            }
            Self::Union(children) => Self::produce_union_key_stream(children, inputs, spec_cursor),
            Self::Intersection(children) => {
                Self::produce_intersection_key_stream(children, inputs, spec_cursor)
            }
        }
    }

    // Build one canonical stream for a union by pairwise-merging child streams.
    fn produce_union_key_stream<'a, E>(
        children: &[Self],
        inputs: &AccessStreamInputs<'_, 'a, E>,
        spec_cursor: &mut AccessSpecCursor<'a>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        let streams = Self::collect_child_key_streams(children, inputs, spec_cursor)?;
        let key_comparator = inputs.key_comparator;

        Ok(Self::reduce_key_streams(streams, |left, right| {
            Box::new(MergeOrderedKeyStream::new_with_comparator(
                left,
                right,
                key_comparator,
            ))
        }))
    }

    // Build one canonical stream for an intersection by pairwise-intersecting child streams.
    fn produce_intersection_key_stream<'a, E>(
        children: &[Self],
        inputs: &AccessStreamInputs<'_, 'a, E>,
        spec_cursor: &mut AccessSpecCursor<'a>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        let streams = Self::collect_child_key_streams(children, inputs, spec_cursor)?;
        let key_comparator = inputs.key_comparator;

        Ok(Self::reduce_key_streams(streams, |left, right| {
            Box::new(IntersectOrderedKeyStream::new_with_comparator(
                left,
                right,
                key_comparator,
            ))
        }))
    }
}
