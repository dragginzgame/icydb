//! Module: executor::stream::access::traversal
//! Responsibility: build and execute access-path traversal streams for runtime loading.
//! Does not own: access-plan construction or planner routing semantics.
//! Boundary: lowers executable access contracts into ordered key/data stream traversal.

use crate::{
    db::{
        access::{AccessPlan, StructuralKey},
        data::DataRow,
        executor::{
            Context, ExecutableAccessNode, ExecutableAccessPath, ExecutableAccessPlan,
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            stream::{
                access::{
                    bindings::{
                        AccessScanContinuationInput, AccessSpecCursor, AccessStreamBindings,
                        ExecutableAccess, IndexStreamConstraints, StreamExecutionHints,
                    },
                    physical,
                },
                key::{
                    IntersectOrderedKeyStream, KeyOrderComparator, MergeOrderedKeyStream,
                    OrderedKeyStreamBox, VecOrderedKeyStream,
                },
            },
            traversal::validate_index_range_spec_alignment,
        },
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// TraversalInputs
///
/// TraversalInputs carries the structural traversal bindings needed by
/// access-plan stream resolution.
/// This deliberately excludes typed context so recursive traversal orchestration
/// can stay monomorphic while physical path resolution remains in typed leaves.
///

#[derive(Clone, Copy)]
struct TraversalInputs<'a> {
    index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    index_range_specs: &'a [LoweredIndexRangeSpec],
    continuation: AccessScanContinuationInput<'a>,
    physical_fetch_hint: Option<usize>,
    index_predicate_execution: Option<crate::db::index::predicate::IndexPredicateExecution<'a>>,
}

impl<'a> TraversalInputs<'a> {
    // Clone this traversal envelope with one overridden physical fetch hint.
    const fn with_physical_fetch_hint(self, physical_fetch_hint: Option<usize>) -> Self {
        Self {
            physical_fetch_hint,
            ..self
        }
    }

    // Build one mutable spec-consumption cursor over prefix/range slices.
    const fn spec_cursor(&self) -> AccessSpecCursor<'a> {
        AccessSpecCursor::new(self.index_prefix_specs, self.index_range_specs)
    }
}

///
/// AccessTraversalRuntime
///
/// AccessTraversalRuntime keeps typed physical path resolution behind one
/// execution-focused runtime boundary.
/// Recursive access-plan traversal uses this only to resolve leaf path nodes;
/// it must not absorb union/intersection orchestration or stream reduction.
///

trait AccessTraversalRuntime<K> {
    /// Resolve one executable path leaf through the typed physical access boundary.
    fn lower_path_access(
        &self,
        path: &ExecutableAccessPath<'_, K>,
        inputs: TraversalInputs<'_>,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        index_range_spec: Option<&LoweredIndexRangeSpec>,
    ) -> Result<OrderedKeyStreamBox, InternalError>;
}

///
/// ContextTraversalRuntime
///
/// ContextTraversalRuntime binds one recovered typed context to the
/// `AccessTraversalRuntime` leaf-resolution contract.
/// This keeps context-owned physical stream resolution typed while the parent
/// traversal recursion stays generic only over key shape.
///

#[cfg(test)]
struct ContextTraversalRuntime<'ctx, 'a, E>
where
    E: EntityKind + EntityValue,
{
    ctx: &'a Context<'ctx, E>,
}

#[cfg(test)]
impl<'ctx, 'a, E> ContextTraversalRuntime<'ctx, 'a, E>
where
    E: EntityKind + EntityValue,
{
    // Build one typed traversal runtime from one recovered executor context.
    const fn new(ctx: &'a Context<'ctx, E>) -> Self {
        Self { ctx }
    }
}

#[cfg(test)]
impl<E> AccessTraversalRuntime<E::Key> for ContextTraversalRuntime<'_, '_, E>
where
    E: EntityKind + EntityValue,
{
    fn lower_path_access(
        &self,
        path: &ExecutableAccessPath<'_, E::Key>,
        inputs: TraversalInputs<'_>,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        index_range_spec: Option<&LoweredIndexRangeSpec>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let constraints = IndexStreamConstraints {
            prefixes: index_prefix_specs,
            range: index_range_spec,
        };
        let hints = StreamExecutionHints {
            physical_fetch_hint: inputs.physical_fetch_hint,
            predicate_execution: inputs.index_predicate_execution,
        };

        self.ctx.ordered_key_stream_from_executable_access(
            path,
            constraints,
            inputs.continuation,
            hints,
        )
    }
}

///
/// StructuralTraversalRuntime
///
/// StructuralTraversalRuntime carries the structural store/index authority
/// needed to resolve planner-key executable access paths without recovering
/// `Context<'_, E>` inside the execution hot path.
/// It is the structural fast-path/runtime leaf used by erased execution
/// adapters and typed context shells alike.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct StructuralTraversalRuntime {
    pub(in crate::db::executor) store: crate::db::registry::StoreHandle,
    pub(in crate::db::executor) entity_tag: crate::types::EntityTag,
}

impl StructuralTraversalRuntime {
    /// Build one structural traversal runtime from canonical store authority.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        store: crate::db::registry::StoreHandle,
        entity_tag: crate::types::EntityTag,
    ) -> Self {
        Self { store, entity_tag }
    }

    /// Resolve one structural executable access binding into an ordered key stream.
    pub(in crate::db::executor) fn ordered_key_stream_from_structural_runtime_access(
        &self,
        request: ExecutableAccess<'_, StructuralKey>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let inputs = TraversalInputs {
            index_prefix_specs: request.bindings.index_prefix_specs,
            index_range_specs: request.bindings.index_range_specs,
            continuation: request.bindings.continuation,
            physical_fetch_hint: request.physical_fetch_hint,
            index_predicate_execution: request.index_predicate_execution,
        };
        let mut spec_cursor = inputs.spec_cursor();
        let key_stream = AccessPlanStreamResolver::produce_key_stream(
            self,
            &request.plan,
            inputs,
            &mut spec_cursor,
        )?;
        spec_cursor.validate_consumed()?;

        Ok(key_stream)
    }
}

impl AccessTraversalRuntime<StructuralKey> for StructuralTraversalRuntime {
    fn lower_path_access(
        &self,
        path: &ExecutableAccessPath<'_, StructuralKey>,
        inputs: TraversalInputs<'_>,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        index_range_spec: Option<&LoweredIndexRangeSpec>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let constraints = IndexStreamConstraints {
            prefixes: index_prefix_specs,
            range: index_range_spec,
        };
        let hints = StreamExecutionHints {
            physical_fetch_hint: inputs.physical_fetch_hint,
            predicate_execution: inputs.index_predicate_execution,
        };

        path.resolve_structural_physical_key_stream(physical::StructuralPhysicalStreamRequest {
            store: self.store,
            entity_tag: self.entity_tag,
            index_prefix_specs: constraints.prefixes,
            index_range_spec: constraints.range,
            continuation: inputs.continuation,
            physical_fetch_hint: hints.physical_fetch_hint,
            index_predicate_execution: hints.predicate_execution,
        })
    }
}

impl<E> Context<'_, E>
where
    E: EntityKind + EntityValue,
{
    /// Resolve one executable access path into an ordered key stream.
    #[cfg(test)]
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

    /// Resolve structural access-plan rows using default ascending traversal with no anchor.
    pub(in crate::db) fn rows_from_structural_access_plan(
        &self,
        access: &AccessPlan<StructuralKey>,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
        index_range_specs: &[LoweredIndexRangeSpec],
        consistency: MissingRowPolicy,
    ) -> Result<Vec<DataRow>, InternalError>
    where
        E: EntityKind,
    {
        self.rows_from_structural_access_plan_with_scan_continuation(
            access,
            index_prefix_specs,
            index_range_specs,
            consistency,
            AccessScanContinuationInput::initial_asc(),
        )
    }

    /// Resolve an access plan to an ordered key stream while consuming lowered specs
    /// in traversal order, including optional index-range pagination anchor.
    #[cfg(test)]
    pub(in crate::db::executor) fn ordered_key_stream_from_runtime_access(
        &self,
        request: ExecutableAccess<'_, E::Key>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind,
    {
        let inputs = TraversalInputs {
            index_prefix_specs: request.bindings.index_prefix_specs,
            index_range_specs: request.bindings.index_range_specs,
            continuation: request.bindings.continuation,
            physical_fetch_hint: request.physical_fetch_hint,
            index_predicate_execution: request.index_predicate_execution,
        };
        let runtime = ContextTraversalRuntime::new(self);
        let mut spec_cursor = inputs.spec_cursor();
        let key_stream = AccessPlanStreamResolver::produce_key_stream(
            &runtime,
            &request.plan,
            inputs,
            &mut spec_cursor,
        )?;
        spec_cursor.validate_consumed()?;

        Ok(key_stream)
    }

    /// Resolve a structural access plan to an ordered key stream while consuming lowered specs.
    pub(in crate::db::executor) fn ordered_key_stream_from_structural_runtime_access(
        &self,
        request: ExecutableAccess<'_, StructuralKey>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        E: EntityKind,
    {
        StructuralTraversalRuntime::new(self.structural_store()?, E::ENTITY_TAG)
            .ordered_key_stream_from_structural_runtime_access(request)
    }

    /// Resolve rows from a structural access plan with explicit continuation scan bindings.
    pub(in crate::db) fn rows_from_structural_access_plan_with_scan_continuation(
        &self,
        access: &AccessPlan<StructuralKey>,
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
        let executable_access = ExecutableAccess::new(access, bindings, None, None);
        let mut key_stream =
            self.ordered_key_stream_from_structural_runtime_access(executable_access)?;

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

    // Collect one child key stream for each child access plan.
    fn collect_child_key_streams<'a, K>(
        runtime: &dyn AccessTraversalRuntime<K>,
        children: &[ExecutableAccessPlan<'a, K>],
        inputs: TraversalInputs<'a>,
        spec_cursor: &mut AccessSpecCursor<'a>,
    ) -> Result<Vec<OrderedKeyStreamBox>, InternalError>
    where
        K: Clone,
    {
        let mut streams = Vec::with_capacity(children.len());
        for child in children {
            // Composite plans never need physical fetch-hint expansion on child lookups.
            let child_inputs = inputs.with_physical_fetch_hint(None);
            streams.push(Self::produce_key_stream(
                runtime,
                child,
                child_inputs,
                spec_cursor,
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
    /// Produce one ordered key stream for an access plan while consuming lowered specs.
    fn produce_key_stream<'a, K>(
        runtime: &dyn AccessTraversalRuntime<K>,
        access: &ExecutableAccessPlan<'a, K>,
        inputs: TraversalInputs<'a>,
        spec_cursor: &mut AccessSpecCursor<'a>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        K: Clone,
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
                validate_index_range_spec_alignment(path, index_range_spec)?;

                runtime.lower_path_access(path, inputs, index_prefix_specs, index_range_spec)
            }
            ExecutableAccessNode::Union(children) => {
                Self::produce_union_key_stream(runtime, children, inputs, spec_cursor)
            }
            ExecutableAccessNode::Intersection(children) => {
                Self::produce_intersection_key_stream(runtime, children, inputs, spec_cursor)
            }
        }
    }

    // Build one canonical stream for a union by pairwise-merging child streams.
    fn produce_union_key_stream<'a, K>(
        runtime: &dyn AccessTraversalRuntime<K>,
        children: &[ExecutableAccessPlan<'a, K>],
        inputs: TraversalInputs<'a>,
        spec_cursor: &mut AccessSpecCursor<'a>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        K: Clone,
    {
        let streams = Self::collect_child_key_streams(runtime, children, inputs, spec_cursor)?;
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
    fn produce_intersection_key_stream<'a, K>(
        runtime: &dyn AccessTraversalRuntime<K>,
        children: &[ExecutableAccessPlan<'a, K>],
        inputs: TraversalInputs<'a>,
        spec_cursor: &mut AccessSpecCursor<'a>,
    ) -> Result<OrderedKeyStreamBox, InternalError>
    where
        K: Clone,
    {
        let streams = Self::collect_child_key_streams(runtime, children, inputs, spec_cursor)?;
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
