use crate::{
    db::{
        executor::{
            Context, IntersectOrderedKeyStream, KeyOrderComparator, MergeOrderedKeyStream,
            OrderedKeyStreamBox, VecOrderedKeyStream,
        },
        index::RawIndexKey,
        query::{
            ReadConsistency,
            plan::{AccessPath, AccessPlan, Direction, IndexPrefixSpec, IndexRangeSpec},
            predicate::IndexPredicateExecution,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// AccessStreamInputs
///
/// Canonical access-stream construction inputs shared across context/composite boundaries.
/// This bundles spec slices and traversal controls to avoid argument-order drift.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct AccessStreamInputs<'ctx, 'a, E: EntityKind + EntityValue> {
    pub(in crate::db::executor) ctx: &'a Context<'ctx, E>,
    pub(in crate::db::executor) index_prefix_specs: &'a [IndexPrefixSpec],
    pub(in crate::db::executor) index_range_specs: &'a [IndexRangeSpec],
    pub(in crate::db::executor) index_range_anchor: Option<&'a RawIndexKey>,
    pub(in crate::db::executor) direction: Direction,
    pub(in crate::db::executor) key_comparator: KeyOrderComparator,
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) index_predicate_execution: Option<IndexPredicateExecution<'a>>,
}

impl<'a, E> AccessStreamInputs<'_, 'a, E>
where
    E: EntityKind + EntityValue,
{
    #[must_use]
    pub(in crate::db::executor) const fn with_physical_fetch_hint(
        &self,
        physical_fetch_hint: Option<usize>,
    ) -> Self {
        Self {
            ctx: self.ctx,
            index_prefix_specs: self.index_prefix_specs,
            index_range_specs: self.index_range_specs,
            index_range_anchor: self.index_range_anchor,
            direction: self.direction,
            key_comparator: self.key_comparator,
            physical_fetch_hint,
            index_predicate_execution: self.index_predicate_execution,
        }
    }

    #[must_use]
    fn spec_cursor(&self) -> AccessSpecCursor<'a> {
        AccessSpecCursor {
            index_prefix_specs: self.index_prefix_specs.iter(),
            index_range_specs: self.index_range_specs.iter(),
        }
    }
}

///
/// AccessSpecCursor
///
/// Mutable traversal cursor for index prefix/range specs while walking an access plan.
/// Keeps consumption order explicit and exposes one end-of-traversal invariant check.
///

pub(in crate::db::executor) struct AccessSpecCursor<'a> {
    index_prefix_specs: std::slice::Iter<'a, IndexPrefixSpec>,
    index_range_specs: std::slice::Iter<'a, IndexRangeSpec>,
}

impl<'a> AccessSpecCursor<'a> {
    pub(in crate::db::executor) fn next_index_prefix_spec(
        &mut self,
    ) -> Option<&'a IndexPrefixSpec> {
        self.index_prefix_specs.next()
    }

    pub(in crate::db::executor) fn next_index_range_spec(&mut self) -> Option<&'a IndexRangeSpec> {
        self.index_range_specs.next()
    }

    pub(in crate::db::executor) fn validate_consumed(&mut self) -> Result<(), InternalError> {
        if self.index_prefix_specs.next().is_some() {
            return Err(InternalError::query_executor_invariant(
                "unused index-prefix executable specs after access-plan traversal",
            ));
        }
        if self.index_range_specs.next().is_some() {
            return Err(InternalError::query_executor_invariant(
                "unused index-range executable specs after access-plan traversal",
            ));
        }

        Ok(())
    }
}

///
/// AccessStreamBindings
///
/// Shared access-stream traversal bindings reused by execution and key-stream
/// request wrappers so spec/anchor/direction fields stay aligned.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct AccessStreamBindings<'a> {
    pub(in crate::db::executor) index_prefix_specs: &'a [IndexPrefixSpec],
    pub(in crate::db::executor) index_range_specs: &'a [IndexRangeSpec],
    pub(in crate::db::executor) index_range_anchor: Option<&'a RawIndexKey>,
    pub(in crate::db::executor) direction: Direction,
}

///
/// AccessPlanStreamRequest
///
/// Canonical request payload for access-plan key-stream production.
/// Bundles access path, lowered specs, and traversal controls so call sites
/// do not pass ordering and spec parameters as loose arguments.
///

pub(in crate::db::executor) struct AccessPlanStreamRequest<'a, K> {
    pub(in crate::db::executor) access: &'a AccessPlan<K>,
    pub(in crate::db::executor) bindings: AccessStreamBindings<'a>,
    pub(in crate::db::executor) key_comparator: KeyOrderComparator,
    pub(in crate::db::executor) physical_fetch_hint: Option<usize>,
    pub(in crate::db::executor) index_predicate_execution: Option<IndexPredicateExecution<'a>>,
}

///
/// IndexStreamConstraints
///
/// Canonical constraint envelope for index-backed path resolution.
/// Groups prefix/range/anchor controls so call sites pass one structural input
/// rather than multiple loosely related optional arguments.
///

pub(in crate::db) struct IndexStreamConstraints<'a> {
    pub prefix: Option<&'a IndexPrefixSpec>,
    pub range: Option<&'a IndexRangeSpec>,
    pub anchor: Option<&'a RawIndexKey>,
}

///
/// StreamExecutionHints
///
/// Canonical execution-hint envelope for access-path stream production.
/// Keeps bounded fetch and index-predicate pushdown hints grouped and extensible.
///

pub(in crate::db) struct StreamExecutionHints<'a> {
    pub physical_fetch_hint: Option<usize>,
    pub predicate_execution: Option<IndexPredicateExecution<'a>>,
}

impl<E> Context<'_, E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve one access path into an ordered key stream with optional
    /// index-lowered constraints and execution hints.
    pub(in crate::db) fn ordered_key_stream_from_access(
        &self,
        access: &AccessPath<E::Key>,
        constraints: IndexStreamConstraints<'_>,
        direction: Direction,
        hints: StreamExecutionHints<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind,
    {
        access.resolve_physical_key_stream(
            self,
            constraints.prefix,
            constraints.range,
            constraints.anchor,
            direction,
            hints.physical_fetch_hint,
            hints.predicate_execution,
        )
    }

    /// Resolve an access plan to rows using default ascending traversal with no anchor.
    pub(in crate::db) fn rows_from_access_plan(
        &self,
        access: &AccessPlan<E::Key>,
        index_prefix_specs: &[IndexPrefixSpec],
        index_range_specs: &[IndexRangeSpec],
        consistency: ReadConsistency,
    ) -> Result<Vec<crate::db::data::DataRow>, InternalError>
    where
        E: EntityKind,
    {
        self.rows_from_access_plan_with_index_range_anchor(
            access,
            index_prefix_specs,
            index_range_specs,
            consistency,
            None,
            Direction::Asc,
        )
    }

    /// Resolve an access plan to an ordered key stream while consuming lowered specs
    /// in traversal order, including optional index-range pagination anchor.
    pub(in crate::db::executor) fn ordered_key_stream_from_access_plan_with_index_range_anchor(
        &self,
        request: AccessPlanStreamRequest<'_, E::Key>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind,
    {
        let inputs = AccessStreamInputs {
            ctx: self,
            index_prefix_specs: request.bindings.index_prefix_specs,
            index_range_specs: request.bindings.index_range_specs,
            index_range_anchor: request.bindings.index_range_anchor,
            direction: request.bindings.direction,
            key_comparator: request.key_comparator,
            physical_fetch_hint: request.physical_fetch_hint,
            index_predicate_execution: request.index_predicate_execution,
        };
        let mut spec_cursor = inputs.spec_cursor();
        let key_stream = AccessPlanStreamResolver::produce_key_stream(
            request.access,
            &inputs,
            &mut spec_cursor,
        )?;
        spec_cursor.validate_consumed()?;

        Ok(key_stream)
    }

    /// Resolve rows from an access plan with explicit direction and optional range anchor.
    pub(in crate::db) fn rows_from_access_plan_with_index_range_anchor(
        &self,
        access: &AccessPlan<E::Key>,
        index_prefix_specs: &[IndexPrefixSpec],
        index_range_specs: &[IndexRangeSpec],
        consistency: ReadConsistency,
        index_range_anchor: Option<&RawIndexKey>,
        direction: Direction,
    ) -> Result<Vec<crate::db::data::DataRow>, InternalError>
    where
        E: EntityKind,
    {
        let bindings = AccessStreamBindings {
            index_prefix_specs,
            index_range_specs,
            index_range_anchor,
            direction,
        };
        let request = AccessPlanStreamRequest {
            access,
            bindings,
            key_comparator: KeyOrderComparator::from_direction(direction),
            physical_fetch_hint: None,
            index_predicate_execution: None,
        };
        let mut key_stream =
            self.ordered_key_stream_from_access_plan_with_index_range_anchor(request)?;

        self.rows_from_ordered_key_stream(key_stream.as_mut(), consistency)
    }
}

///
/// AccessPlanStreamResolver
///
/// Executor-owned access-plan traversal and key-stream production.
/// This isolates physical stream wiring from `AccessPlan` so plan types remain
/// data-only while executor mechanics stay in executor modules.
///

struct AccessPlanStreamResolver;

impl AccessPlanStreamResolver {
    // Validate that a consumed prefix spec belongs to the same index path node.
    fn validate_index_prefix_spec_alignment<K>(
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
    fn validate_index_range_spec_alignment<K>(
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
    fn collect_child_key_streams<'a, E, K>(
        children: &[AccessPlan<K>],
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
            streams.push(Self::produce_key_stream(child, &child_inputs, spec_cursor)?);
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
    pub(super) fn produce_key_stream<'a, E, K>(
        access: &AccessPlan<K>,
        inputs: &AccessStreamInputs<'_, 'a, E>,
        spec_cursor: &mut AccessSpecCursor<'a>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        match access {
            AccessPlan::Path(path) => {
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

                inputs.ctx.ordered_key_stream_from_access(
                    path,
                    IndexStreamConstraints {
                        prefix: index_prefix_spec,
                        range: index_range_spec,
                        anchor: inputs.index_range_anchor,
                    },
                    inputs.direction,
                    StreamExecutionHints {
                        physical_fetch_hint: inputs.physical_fetch_hint,
                        predicate_execution: inputs.index_predicate_execution,
                    },
                )
            }
            AccessPlan::Union(children) => {
                Self::produce_union_key_stream(children, inputs, spec_cursor)
            }
            AccessPlan::Intersection(children) => {
                Self::produce_intersection_key_stream(children, inputs, spec_cursor)
            }
        }
    }

    // Build one canonical stream for a union by pairwise-merging child streams.
    fn produce_union_key_stream<'a, E, K>(
        children: &[AccessPlan<K>],
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
    fn produce_intersection_key_stream<'a, E, K>(
        children: &[AccessPlan<K>],
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
