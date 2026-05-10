//! Module: executor::stream::access::traversal
//! Responsibility: build and execute access-path traversal streams for runtime loading.
//! Does not own: access-plan construction or planner routing semantics.
//! Boundary: lowers executable access contracts into ordered key/data stream traversal.

use crate::{
    db::{
        executor::{
            ExecutableAccessNode, ExecutableAccessPlan, ExecutionPathPayload,
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            pipeline::contracts::{AccessScanContinuationInput, AccessStreamBindings},
            stream::{
                access::{
                    bindings::{
                        AccessSpecCursor, ExecutableAccess, IndexStreamConstraints,
                        StreamExecutionHints,
                    },
                    physical,
                },
                key::{
                    KeyOrderComparator, OrderedKeyStreamBox,
                    ordered_key_stream_from_materialized_keys,
                },
            },
            traversal::IndexRangeTraversalContract,
        },
        index::predicate::IndexPredicateExecution,
    },
    error::InternalError,
    value::Value,
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
    preserve_leaf_index_order: bool,
}

impl<'a> TraversalInputs<'a> {
    // Clone this traversal envelope with one overridden physical fetch hint.
    const fn with_physical_fetch_hint(self, physical_fetch_hint: Option<usize>) -> Self {
        Self {
            physical_fetch_hint,
            ..self
        }
    }

    // Composite child streams must stay canonicalized by `DataKey` order so
    // merge/intersection reducers can consume them under one shared key comparator.
    const fn without_leaf_index_order_preservation(self) -> Self {
        Self {
            preserve_leaf_index_order: false,
            ..self
        }
    }

    // Build one mutable spec-consumption cursor over prefix/range slices.
    const fn spec_cursor(&self) -> AccessSpecCursor<'a> {
        AccessSpecCursor::new(self.index_prefix_specs, self.index_range_specs)
    }
}

// Keep the historical traversal-layer invariant name stable for CI checks while
// routing the actual contract enforcement through the traversal owner.
fn validate_index_range_spec_alignment(
    path: &ExecutionPathPayload<'_, Value>,
    index_range_spec: Option<&LoweredIndexRangeSpec>,
) -> Result<(), InternalError> {
    IndexRangeTraversalContract::validate_spec_alignment(path, index_range_spec)
}

///
/// TraversalRuntime
///
/// TraversalRuntime carries the store/index authority
/// needed to resolve planner-key executable access paths without recovering
/// `Context<'_, E>` inside the execution hot path.
/// It is the fast-path runtime leaf used by erased execution
/// adapters and typed context shells alike.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct TraversalRuntime {
    pub(in crate::db::executor) store: crate::db::registry::StoreHandle,
    pub(in crate::db::executor) entity_tag: crate::types::EntityTag,
}

impl TraversalRuntime {
    /// Build one traversal runtime from canonical store authority.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        store: crate::db::registry::StoreHandle,
        entity_tag: crate::types::EntityTag,
    ) -> Self {
        Self { store, entity_tag }
    }

    /// Resolve one executable access binding into an ordered key stream.
    pub(in crate::db::executor) fn ordered_key_stream_from_runtime_access(
        &self,
        request: ExecutableAccess<'_, Value>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        self.ordered_key_stream_from_executable_plan(
            &request.plan,
            request.bindings,
            request.physical_fetch_hint,
            request.index_predicate_execution,
            request.preserve_leaf_index_order,
        )
    }

    /// Resolve one borrowed executable access plan plus bindings into an
    /// ordered key stream without cloning the access plan wrapper.
    pub(in crate::db::executor) fn ordered_key_stream_from_executable_plan<'input>(
        &self,
        plan: &ExecutableAccessPlan<'_, Value>,
        bindings: AccessStreamBindings<'input>,
        physical_fetch_hint: Option<usize>,
        index_predicate_execution: Option<IndexPredicateExecution<'input>>,
        preserve_leaf_index_order: bool,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let inputs = TraversalInputs {
            index_prefix_specs: bindings.index_prefix_specs,
            index_range_specs: bindings.index_range_specs,
            continuation: bindings.continuation,
            physical_fetch_hint,
            index_predicate_execution,
            preserve_leaf_index_order,
        };
        let mut spec_cursor = inputs.spec_cursor();
        let key_stream =
            AccessPlanStreamResolver::produce_key_stream(self, plan, inputs, &mut spec_cursor)?;
        spec_cursor.validate_consumed()?;

        Ok(key_stream)
    }

    // Resolve one executable path leaf through the structural physical access
    // boundary without re-erasing the traversal runtime behind a local trait.
    fn lower_path_access(
        &self,
        path: &ExecutionPathPayload<'_, Value>,
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
            preserve_leaf_index_order: inputs.preserve_leaf_index_order,
        })
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
    fn validate_index_prefix_spec_alignment(
        path: &ExecutionPathPayload<'_, Value>,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
    ) -> Result<(), InternalError> {
        let path_capabilities = path.capabilities();
        if let Some(details) = path_capabilities.index_prefix_details() {
            for spec in index_prefix_specs {
                if spec.scan_contract().name() != details.name() {
                    return Err(InternalError::query_executor_invariant(
                        "index-prefix spec does not match access path index",
                    ));
                }
            }
        }

        Ok(())
    }

    // Collect one child key stream for each child access plan.
    fn collect_child_key_streams(
        runtime: &TraversalRuntime,
        children: &[ExecutableAccessPlan<'_, Value>],
        inputs: TraversalInputs<'_>,
        spec_cursor: &mut AccessSpecCursor<'_>,
    ) -> Result<Vec<OrderedKeyStreamBox>, InternalError> {
        let mut streams = Vec::with_capacity(children.len());
        for child in children {
            // Composite plans never need physical fetch-hint expansion on child lookups.
            let child_inputs = inputs
                .with_physical_fetch_hint(None)
                .without_leaf_index_order_preservation();
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
            return ordered_key_stream_from_materialized_keys(Vec::new());
        }
        if streams.len() == 1 {
            return streams
                .pop()
                .unwrap_or_else(|| ordered_key_stream_from_materialized_keys(Vec::new()));
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
            .unwrap_or_else(|| ordered_key_stream_from_materialized_keys(Vec::new()))
    }

    // Build an ordered key stream for this access plan.
    /// Produce one ordered key stream for an access plan while consuming lowered specs.
    fn produce_key_stream(
        runtime: &TraversalRuntime,
        access: &ExecutableAccessPlan<'_, Value>,
        inputs: TraversalInputs<'_>,
        spec_cursor: &mut AccessSpecCursor<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        match access.node() {
            ExecutableAccessNode::Path(path) => {
                let path_capabilities = path.capabilities();
                let index_prefix_specs = if path_capabilities.index_prefix_spec_count() > 0 {
                    spec_cursor.require_next_index_prefix_specs(
                        path_capabilities.index_prefix_spec_count(),
                    )?
                } else {
                    &[]
                };
                let index_range_spec = if path_capabilities.consumes_index_range_spec() {
                    Some(spec_cursor.require_next_index_range_spec()?)
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
    fn produce_union_key_stream(
        runtime: &TraversalRuntime,
        children: &[ExecutableAccessPlan<'_, Value>],
        inputs: TraversalInputs<'_>,
        spec_cursor: &mut AccessSpecCursor<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let streams = Self::collect_child_key_streams(runtime, children, inputs, spec_cursor)?;
        let key_comparator = KeyOrderComparator::from_direction(inputs.continuation.direction());

        Ok(Self::reduce_key_streams(streams, |left, right| {
            OrderedKeyStreamBox::merge(left, right, key_comparator)
        }))
    }

    // Build one canonical stream for an intersection by pairwise-intersecting child streams.
    fn produce_intersection_key_stream(
        runtime: &TraversalRuntime,
        children: &[ExecutableAccessPlan<'_, Value>],
        inputs: TraversalInputs<'_>,
        spec_cursor: &mut AccessSpecCursor<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let streams = Self::collect_child_key_streams(runtime, children, inputs, spec_cursor)?;
        let key_comparator = KeyOrderComparator::from_direction(inputs.continuation.direction());

        Ok(Self::reduce_key_streams(streams, |left, right| {
            OrderedKeyStreamBox::intersect(left, right, key_comparator)
        }))
    }
}
