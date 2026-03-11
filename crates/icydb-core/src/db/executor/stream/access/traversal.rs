//! Module: executor::stream::access::traversal
//! Responsibility: build and execute access-path traversal streams for runtime loading.
//! Does not own: access-plan construction or planner routing semantics.
//! Boundary: lowers executable access contracts into ordered key/data stream traversal.

#[cfg(test)]
use crate::db::access::{AccessPath, lower_executable_access_path};
use crate::{
    db::{
        access::AccessPlan,
        data::DataRow,
        executor::{
            Context, ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan,
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            stream::{
                access::{
                    bindings::{
                        AccessExecutionDescriptor, AccessScanContinuationInput, AccessSpecCursor,
                        AccessStreamBindings, AccessStreamInputs, IndexStreamConstraints,
                        StreamExecutionHints,
                    },
                    physical,
                },
                key::{
                    IntersectOrderedKeyStream, KeyOrderComparator, MergeOrderedKeyStream,
                    OrderedKeyStreamBox, VecOrderedKeyStream,
                },
            },
        },
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

impl<E> Context<'_, E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve one access path into an ordered key stream with optional
    /// index-lowered constraints and execution hints.
    #[cfg(test)]
    pub(in crate::db::executor) fn ordered_key_stream_from_access(
        &self,
        access: &AccessPath<E::Key>,
        constraints: IndexStreamConstraints<'_>,
        continuation: AccessScanContinuationInput<'_>,
        hints: StreamExecutionHints<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind,
    {
        let executable_access = lower_executable_access_path(access);
        self.ordered_key_stream_from_executable_access(
            &executable_access,
            constraints,
            continuation,
            hints,
        )
    }

    /// Resolve one executable access path into an ordered key stream.
    pub(in crate::db::executor) fn ordered_key_stream_from_executable_access(
        &self,
        access: &ExecutableAccessPath<'_, E::Key>,
        constraints: IndexStreamConstraints<'_>,
        continuation: AccessScanContinuationInput<'_>,
        hints: StreamExecutionHints<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind,
    {
        access.resolve_physical_key_stream(physical::PhysicalStreamRequest {
            ctx: self,
            index_prefix_specs: constraints.prefixes,
            index_range_spec: constraints.range,
            continuation,
            physical_fetch_hint: hints.physical_fetch_hint,
            index_predicate_execution: hints.predicate_execution,
        })
    }

    /// Resolve an access plan to rows using default ascending traversal with no anchor.
    pub(in crate::db) fn rows_from_access_plan(
        &self,
        access: &AccessPlan<E::Key>,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        index_range_specs: &[LoweredIndexRangeSpec],
        consistency: MissingRowPolicy,
    ) -> Result<Vec<DataRow>, InternalError>
    where
        E: EntityKind,
    {
        self.rows_from_access_plan_with_scan_continuation(
            access,
            index_prefix_specs,
            index_range_specs,
            consistency,
            AccessScanContinuationInput::initial_asc(),
        )
    }

    /// Resolve an access plan to an ordered key stream while consuming lowered specs
    /// in traversal order, including optional index-range pagination anchor.
    pub(in crate::db::executor) fn ordered_key_stream_from_access_descriptor(
        &self,
        request: AccessExecutionDescriptor<'_, E::Key>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind,
    {
        let inputs = AccessStreamInputs {
            ctx: self,
            index_prefix_specs: request.bindings.index_prefix_specs,
            index_range_specs: request.bindings.index_range_specs,
            continuation: request.bindings.continuation,
            physical_fetch_hint: request.physical_fetch_hint,
            index_predicate_execution: request.index_predicate_execution,
        };
        let mut spec_cursor = inputs.spec_cursor();
        let key_stream = AccessPlanStreamResolver::produce_key_stream(
            &request.executable_access,
            &inputs,
            &mut spec_cursor,
        )?;
        spec_cursor.validate_consumed()?;

        Ok(key_stream)
    }

    /// Resolve rows from an access plan with explicit continuation scan bindings.
    pub(in crate::db) fn rows_from_access_plan_with_scan_continuation(
        &self,
        access: &AccessPlan<E::Key>,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        index_range_specs: &[LoweredIndexRangeSpec],
        consistency: MissingRowPolicy,
        continuation: AccessScanContinuationInput<'_>,
    ) -> Result<Vec<DataRow>, InternalError>
    where
        E: EntityKind,
    {
        let bindings = AccessStreamBindings {
            index_prefix_specs,
            index_range_specs,
            continuation,
        };
        let descriptor = AccessExecutionDescriptor::from_bindings(access, bindings, None, None);
        let mut key_stream = self.ordered_key_stream_from_access_descriptor(descriptor)?;

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
    // Lower one path through the canonical physical resolver boundary.
    fn lower_path_access<E, K>(
        path: &ExecutableAccessPath<'_, K>,
        inputs: &AccessStreamInputs<'_, '_, E>,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        index_range_spec: Option<&LoweredIndexRangeSpec>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        let constraints = IndexStreamConstraints {
            prefixes: index_prefix_specs,
            range: index_range_spec,
        };
        let hints = StreamExecutionHints {
            physical_fetch_hint: inputs.physical_fetch_hint,
            predicate_execution: inputs.index_predicate_execution,
        };
        inputs.ctx.ordered_key_stream_from_executable_access(
            path,
            constraints,
            inputs.continuation,
            hints,
        )
    }

    // Validate that a consumed prefix spec belongs to the same index path node.
    fn validate_index_prefix_spec_alignment<K>(
        path: &ExecutableAccessPath<'_, K>,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
    ) -> Result<(), InternalError> {
        let path_capabilities = path.capabilities();
        if let Some(index) = path_capabilities.index_prefix_model() {
            for spec in index_prefix_specs {
                if spec.index() != &index {
                    return Err(crate::db::error::query_executor_invariant(
                        "index-prefix spec does not match access path index",
                    ));
                }
            }
        }

        Ok(())
    }

    // Validate that a consumed range spec belongs to the same index path node.
    fn validate_index_range_spec_alignment<K>(
        path: &ExecutableAccessPath<'_, K>,
        index_range_spec: Option<&LoweredIndexRangeSpec>,
    ) -> Result<(), InternalError> {
        let path_capabilities = path.capabilities();
        if let Some(spec) = index_range_spec
            && let Some(index) = path_capabilities.index_range_model()
            && spec.index() != &index
        {
            return Err(crate::db::error::query_executor_invariant(
                "index-range spec does not match access path index",
            ));
        }

        Ok(())
    }

    // Collect one child key stream for each child access plan.
    fn collect_child_key_streams<'a, E, K>(
        children: &[ExecutableAccessPlan<'a, K>],
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
    /// Produce one ordered key stream for an access plan while consuming lowered specs.
    fn produce_key_stream<'a, E, K>(
        access: &ExecutableAccessPlan<'a, K>,
        inputs: &AccessStreamInputs<'_, 'a, E>,
        spec_cursor: &mut AccessSpecCursor<'a>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        match access.node() {
            ExecutableAccessNode::Path(path) => {
                let path_capabilities = path.capabilities();
                let index_prefix_specs = if path_capabilities.index_prefix_spec_count() > 0 {
                    spec_cursor
                        .next_index_prefix_specs(path_capabilities.index_prefix_spec_count())
                        .ok_or_else(|| {
                            crate::db::error::query_executor_invariant(
                                "index-prefix execution requires pre-lowered specs",
                            )
                        })?
                } else {
                    &[]
                };
                let index_range_spec = if path_capabilities.consumes_index_range_spec() {
                    spec_cursor.next_index_range_spec()
                } else {
                    None
                };
                Self::validate_index_prefix_spec_alignment(path, index_prefix_specs)?;
                Self::validate_index_range_spec_alignment(path, index_range_spec)?;

                Self::lower_path_access(path, inputs, index_prefix_specs, index_range_spec)
            }
            ExecutableAccessNode::Union(children) => {
                Self::produce_union_key_stream(children, inputs, spec_cursor)
            }
            ExecutableAccessNode::Intersection(children) => {
                Self::produce_intersection_key_stream(children, inputs, spec_cursor)
            }
        }
    }

    // Build one canonical stream for a union by pairwise-merging child streams.
    fn produce_union_key_stream<'a, E, K>(
        children: &[ExecutableAccessPlan<'a, K>],
        inputs: &AccessStreamInputs<'_, 'a, E>,
        spec_cursor: &mut AccessSpecCursor<'a>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        let streams = Self::collect_child_key_streams(children, inputs, spec_cursor)?;
        let key_comparator = KeyOrderComparator::from_direction(inputs.continuation.direction());

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
        children: &[ExecutableAccessPlan<'a, K>],
        inputs: &AccessStreamInputs<'_, 'a, E>,
        spec_cursor: &mut AccessSpecCursor<'a>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind<Key = K> + EntityValue,
        K: Copy,
    {
        let streams = Self::collect_child_key_streams(children, inputs, spec_cursor)?;
        let key_comparator = KeyOrderComparator::from_direction(inputs.continuation.direction());

        Ok(Self::reduce_key_streams(streams, |left, right| {
            Box::new(IntersectOrderedKeyStream::new_with_comparator(
                left,
                right,
                key_comparator,
            ))
        }))
    }
}
