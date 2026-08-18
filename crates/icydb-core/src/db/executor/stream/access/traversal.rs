//! Module: executor::stream::access::traversal
//! Responsibility: build and execute access-path traversal streams for runtime loading.
//! Does not own: access-plan construction or planner routing semantics.
//! Boundary: lowers executable access contracts into ordered key/data stream traversal.

use crate::{
    db::{
        data::{DecodedDataStoreKey, RawDataStoreKey},
        executor::{
            ExecutableAccessNode, ExecutableAccessPlan, ExecutionPathPayload, IndexScan,
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            budget::charge_current_execution_budget,
            lowered_index_prefix_exact_cardinalities_for_admitted_root,
            pipeline::contracts::{AccessScanContinuationInput, AccessStreamBindings},
            route::IndexPrefixChildExpansionHint,
            stream::{
                access::{
                    bindings::{
                        AccessSpecCursor, AccessStreamExecutionPolicy, ExecutableAccess,
                        IndexLeafOrderPolicy, IndexStreamConstraints,
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
        integrity::DatabaseIncarnationId,
        schema::cardinality_generation::CardinalityAcceptedRootIdentity,
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::mem::size_of;

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
    execution_policy: AccessStreamExecutionPolicy,
    index_predicate_execution: Option<crate::db::index::predicate::IndexPredicateExecution<'a>>,
    index_prefix_child_expansion: Option<IndexPrefixChildExpansionHint>,
}

#[cfg(test)]
mod exact_intersection_tests {
    use super::{
        AccessPlanStreamResolver, ExactIntersectionPreflight,
        MAX_ATOMIC_EXACT_INTERSECTION_CHILDREN,
    };

    fn preflight(child_cardinalities: &[u64]) -> ExactIntersectionPreflight {
        let mut cardinalities = [0; MAX_ATOMIC_EXACT_INTERSECTION_CHILDREN];
        cardinalities[..child_cardinalities.len()].copy_from_slice(child_cardinalities);
        ExactIntersectionPreflight {
            child_cardinalities: cardinalities,
            child_count: child_cardinalities.len(),
            total_cardinality: child_cardinalities.iter().sum(),
        }
    }

    #[test]
    fn worst_case_cost_gate_accepts_sparse_fixtures_and_rejects_dense_ties() {
        assert!(
            AccessPlanStreamResolver::exact_intersection_probe_can_beat_single(&preflight(&[
                21, 20
            ]),)
        );
        assert!(
            AccessPlanStreamResolver::exact_intersection_probe_can_beat_single(&preflight(&[
                120, 21, 20
            ]),)
        );
        assert!(
            !AccessPlanStreamResolver::exact_intersection_probe_can_beat_single(&preflight(&[
                20, 20
            ]),)
        );
    }

    #[test]
    fn overflowed_cost_authority_fails_closed() {
        assert!(
            !AccessPlanStreamResolver::exact_intersection_cost_beats_single(
                &ExactIntersectionPreflight {
                    child_cardinalities: [u64::MAX, 1, 0],
                    child_count: 2,
                    total_cardinality: u64::MAX,
                },
                1,
            )
        );
    }
}

impl<'a> TraversalInputs<'a> {
    // Clone this traversal envelope with one overridden physical fetch hint.
    const fn with_physical_fetch_hint(self, physical_fetch_hint: Option<usize>) -> Self {
        Self {
            execution_policy: self
                .execution_policy
                .with_physical_fetch_hint(physical_fetch_hint),
            ..self
        }
    }

    // Composite child streams must stay canonicalized by `DecodedDataStoreKey` order so
    // merge/intersection reducers can consume them under one shared key comparator.
    const fn without_leaf_index_order_preservation(self) -> Self {
        Self {
            execution_policy: self
                .execution_policy
                .with_index_leaf_order_policy(IndexLeafOrderPolicy::CanonicalKey),
            ..self
        }
    }

    // Exact-prefix intersection leaves retain physical primary-key suffix order
    // so the bounded overlap probe can intersect them without reordering.
    const fn with_physical_leaf_order(self) -> Self {
        Self {
            execution_policy: self
                .execution_policy
                .with_index_leaf_order_policy(IndexLeafOrderPolicy::PreservePhysicalLeaf),
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
    database_incarnation: DatabaseIncarnationId,
    accepted_root: CardinalityAcceptedRootIdentity,
}

impl TraversalRuntime {
    /// Build one traversal runtime from canonical store authority.
    #[must_use]
    pub(in crate::db::executor) const fn new(
        store: crate::db::registry::StoreHandle,
        entity_tag: crate::types::EntityTag,
        database_incarnation: DatabaseIncarnationId,
        accepted_root: CardinalityAcceptedRootIdentity,
    ) -> Self {
        Self {
            store,
            entity_tag,
            database_incarnation,
            accepted_root,
        }
    }

    /// Resolve one executable access binding into an ordered key stream.
    pub(in crate::db::executor) fn ordered_key_stream_from_runtime_access(
        &self,
        request: ExecutableAccess<'_, Value>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        self.ordered_key_stream_from_executable_plan(
            &request.plan,
            request.bindings,
            request.execution_policy,
            request.index_predicate_execution,
        )
    }

    /// Resolve one borrowed executable access plan plus bindings into an
    /// ordered key stream without cloning the access plan wrapper.
    pub(in crate::db::executor) fn ordered_key_stream_from_executable_plan<'input>(
        &self,
        plan: &ExecutableAccessPlan<'_, Value>,
        bindings: AccessStreamBindings<'input>,
        execution_policy: AccessStreamExecutionPolicy,
        index_predicate_execution: Option<IndexPredicateExecution<'input>>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let inputs = TraversalInputs {
            index_prefix_specs: bindings.index_prefix_specs,
            index_range_specs: bindings.index_range_specs,
            continuation: bindings.continuation,
            execution_policy,
            index_predicate_execution,
            index_prefix_child_expansion: bindings.index_prefix_child_expansion,
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
        path.resolve_structural_physical_key_stream(physical::StructuralPhysicalStreamRequest {
            store: self.store,
            entity_tag: self.entity_tag,
            index_prefix_specs: constraints.prefixes,
            index_range_spec: constraints.range,
            continuation: inputs.continuation,
            execution_policy: inputs.execution_policy,
            index_predicate_execution: inputs.index_predicate_execution,
            index_prefix_child_expansion: inputs.index_prefix_child_expansion,
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

const MAX_ATOMIC_EXACT_INTERSECTION_CHILDREN: usize = 3;
const MAX_ATOMIC_EXACT_INTERSECTION_ENTRIES: u64 = 256;
const MAX_ATOMIC_EXACT_INTERSECTION_KEY_BYTES: u64 =
    MAX_ATOMIC_EXACT_INTERSECTION_ENTRIES * RawDataStoreKey::MAX_STORED_SIZE_BYTES;
const INTERSECTION_ROW_READ_COST_WEIGHT: u64 = 32;

struct ExactIntersectionPreflight {
    child_cardinalities: [u64; MAX_ATOMIC_EXACT_INTERSECTION_CHILDREN],
    child_count: usize,
    total_cardinality: u64,
}

impl ExactIntersectionPreflight {
    const fn child_cardinalities(&self) -> &[u64] {
        self.child_cardinalities.split_at(self.child_count).0
    }
}

enum ExactIntersectionAdmission {
    NotApplicable,
    ConservativeFallback,
    ProvenEmpty,
    Probe(ExactIntersectionPreflight),
}

impl AccessPlanStreamResolver {
    // Admit one child-stream vector before allocation. Union and intersection
    // construction share this owner so neither composite route can bypass the
    // temporary-byte budget through an equivalent reservation flow.
    fn reserve_stream_slots(capacity: usize) -> Result<Vec<OrderedKeyStreamBox>, InternalError> {
        let slot_bytes = capacity
            .checked_mul(size_of::<OrderedKeyStreamBox>())
            .ok_or_else(InternalError::executor_invariant)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            u64::try_from(slot_bytes).unwrap_or(u64::MAX),
        )?;
        let mut streams = Vec::new();
        streams
            .try_reserve_exact(capacity)
            .map_err(|_| InternalError::executor_internal())?;
        let extra_capacity = streams.capacity().saturating_sub(capacity);
        if extra_capacity != 0 {
            let extra_bytes = extra_capacity
                .checked_mul(size_of::<OrderedKeyStreamBox>())
                .ok_or_else(InternalError::executor_invariant)?;
            charge_current_execution_budget(
                DiagnosticExecutionBudgetResource::TemporaryBytes,
                u64::try_from(extra_bytes).unwrap_or(u64::MAX),
            )?;
        }

        Ok(streams)
    }

    // Validate that a consumed prefix spec belongs to the same index path node.
    fn validate_index_prefix_spec_alignment(
        path: &ExecutionPathPayload<'_, Value>,
        index_prefix_specs: &[LoweredIndexPrefixSpec],
    ) -> Result<(), InternalError> {
        let path_facts = path.shape_facts();
        if let Some(details) = path_facts.index_prefix_details() {
            for spec in index_prefix_specs {
                if spec.scan_contract().name() != details.name() {
                    return Err(InternalError::query_executor_invariant());
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
        let mut streams = Self::reserve_stream_slots(children.len())?;
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

    // Collect direct exact-prefix children while retaining their accepted
    // primary-key suffix order for one bounded overlap probe.
    fn collect_exact_intersection_child_streams(
        runtime: &TraversalRuntime,
        children: &[ExecutableAccessPlan<'_, Value>],
        inputs: TraversalInputs<'_>,
        spec_cursor: &mut AccessSpecCursor<'_>,
    ) -> Result<Vec<OrderedKeyStreamBox>, InternalError> {
        let mut streams = Self::reserve_stream_slots(children.len())?;
        for child in children {
            let child_inputs = inputs
                .with_physical_fetch_hint(None)
                .with_physical_leaf_order();
            streams.push(Self::produce_key_stream(
                runtime,
                child,
                child_inputs,
                spec_cursor,
            )?);
        }

        Ok(streams)
    }

    fn exact_intersection_admission(
        runtime: &TraversalRuntime,
        children: &[ExecutableAccessPlan<'_, Value>],
        inputs: TraversalInputs<'_>,
        spec_cursor: AccessSpecCursor<'_>,
    ) -> ExactIntersectionAdmission {
        if !(2..=MAX_ATOMIC_EXACT_INTERSECTION_CHILDREN).contains(&children.len())
            || inputs.index_predicate_execution.is_some()
        {
            return ExactIntersectionAdmission::NotApplicable;
        }

        let mut metadata_cursor = spec_cursor;
        let mut cardinality_specs = Vec::with_capacity(children.len());
        for child in children {
            let ExecutableAccessNode::Path(path) = child.node() else {
                return ExactIntersectionAdmission::NotApplicable;
            };
            let ExecutionPathPayload::IndexPrefix { .. } = path else {
                return ExactIntersectionAdmission::NotApplicable;
            };
            let path_facts = path.shape_facts();
            if path_facts.index_prefix_spec_count() != 1 || path_facts.consumes_index_range_spec() {
                return ExactIntersectionAdmission::NotApplicable;
            }
            let Some(spec) = metadata_cursor
                .next_index_prefix_specs(1)
                .and_then(|specs| specs.first())
            else {
                return ExactIntersectionAdmission::ConservativeFallback;
            };
            cardinality_specs.push(spec);
        }
        let Some(cardinalities) = lowered_index_prefix_exact_cardinalities_for_admitted_root(
            runtime.store,
            runtime.database_incarnation,
            runtime.accepted_root,
            cardinality_specs.iter().copied(),
        ) else {
            return ExactIntersectionAdmission::ConservativeFallback;
        };
        let mut child_cardinalities = [0; MAX_ATOMIC_EXACT_INTERSECTION_CHILDREN];
        let mut total_cardinality = 0u64;
        for (child_index, cardinality) in cardinalities.into_iter().enumerate() {
            if cardinality == 0 {
                return ExactIntersectionAdmission::ProvenEmpty;
            }
            let Some(next_total) = total_cardinality.checked_add(cardinality) else {
                return ExactIntersectionAdmission::ConservativeFallback;
            };
            if next_total > MAX_ATOMIC_EXACT_INTERSECTION_ENTRIES {
                return ExactIntersectionAdmission::ConservativeFallback;
            }
            total_cardinality = next_total;
            child_cardinalities[child_index] = cardinality;
        }

        ExactIntersectionAdmission::Probe(ExactIntersectionPreflight {
            child_cardinalities,
            child_count: children.len(),
            total_cardinality,
        })
    }

    fn exact_intersection_cost_beats_single(
        preflight: &ExactIntersectionPreflight,
        overlap_cardinality: u64,
    ) -> bool {
        let Some(single_cardinality) = preflight.child_cardinalities().first().copied() else {
            return false;
        };
        let Some(single_row_cost) = single_cardinality
            .checked_mul(INTERSECTION_ROW_READ_COST_WEIGHT)
            .and_then(|row_cost| row_cost.checked_add(single_cardinality))
        else {
            return false;
        };
        let Some(intersection_cost) = overlap_cardinality
            .checked_mul(INTERSECTION_ROW_READ_COST_WEIGHT)
            .and_then(|row_cost| row_cost.checked_add(preflight.total_cardinality))
        else {
            return false;
        };

        intersection_cost < single_row_cost
    }

    fn exact_intersection_probe_can_beat_single(preflight: &ExactIntersectionPreflight) -> bool {
        let Some(maximum_overlap) = preflight.child_cardinalities().iter().copied().min() else {
            return false;
        };

        Self::exact_intersection_cost_beats_single(preflight, maximum_overlap)
    }

    fn collect_exact_intersection_overlap(
        streams: Vec<OrderedKeyStreamBox>,
        comparator: KeyOrderComparator,
        preflight: &ExactIntersectionPreflight,
    ) -> Result<Vec<DecodedDataStoreKey>, InternalError> {
        let maximum_keys_u64 = preflight
            .child_cardinalities()
            .iter()
            .copied()
            .min()
            .ok_or_else(InternalError::executor_invariant)?;
        let maximum_keys =
            usize::try_from(maximum_keys_u64).map_err(|_| InternalError::executor_invariant())?;
        let slot_bytes = maximum_keys
            .checked_mul(size_of::<DecodedDataStoreKey>())
            .ok_or_else(InternalError::executor_invariant)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            u64::try_from(slot_bytes).unwrap_or(u64::MAX),
        )?;

        let mut intersection = OrderedKeyStreamBox::intersect_all(streams, comparator)?;
        let mut overlap = Vec::with_capacity(maximum_keys);
        let mut retained_key_bytes = 0u64;
        while let Some(key) = intersection.next_key()? {
            if overlap.len() >= maximum_keys {
                return Err(InternalError::executor_invariant());
            }
            let key_bytes = u64::try_from(key.raw_key()?.as_bytes().len()).unwrap_or(u64::MAX);
            retained_key_bytes = retained_key_bytes
                .checked_add(key_bytes)
                .ok_or_else(InternalError::executor_invariant)?;
            if retained_key_bytes > MAX_ATOMIC_EXACT_INTERSECTION_KEY_BYTES {
                return Err(InternalError::executor_invariant());
            }
            charge_current_execution_budget(
                DiagnosticExecutionBudgetResource::TemporaryBytes,
                key_bytes,
            )?;
            overlap.push(key);
        }

        Ok(overlap)
    }

    // Resolve one bounded exact-prefix intersection without allocating the
    // general held-head stream tree. This route is limited to a cursorless
    // atomic probe; resumed traversal retains the ordinary stream contract.
    fn collect_direct_exact_intersection_overlap(
        runtime: &TraversalRuntime,
        children: &[ExecutableAccessPlan<'_, Value>],
        inputs: TraversalInputs<'_>,
        spec_cursor: &mut AccessSpecCursor<'_>,
        preflight: &ExactIntersectionPreflight,
    ) -> Result<Option<Vec<DecodedDataStoreKey>>, InternalError> {
        if inputs.continuation.primary_key_boundary().is_some()
            || inputs
                .continuation
                .index_scan_continuation()
                .anchor()
                .is_some()
        {
            return Ok(None);
        }

        let mut specs = [None; 3];
        for (slot, child) in specs.iter_mut().zip(children) {
            let ExecutableAccessNode::Path(path) = child.node() else {
                return Err(InternalError::executor_invariant());
            };
            let child_specs = spec_cursor.require_next_index_prefix_specs(1)?;
            Self::validate_index_prefix_spec_alignment(path, child_specs)?;
            let spec = child_specs
                .first()
                .ok_or_else(InternalError::executor_invariant)?;
            *slot = Some(spec);
        }
        let overlap = match children.len() {
            2 => IndexScan::exact_prefix_intersection_structural(
                runtime.store,
                runtime.entity_tag,
                &[
                    specs[0].ok_or_else(InternalError::executor_invariant)?,
                    specs[1].ok_or_else(InternalError::executor_invariant)?,
                ],
                preflight.child_cardinalities(),
                inputs.continuation.direction(),
            )?,
            3 => IndexScan::exact_prefix_intersection_structural(
                runtime.store,
                runtime.entity_tag,
                &[
                    specs[0].ok_or_else(InternalError::executor_invariant)?,
                    specs[1].ok_or_else(InternalError::executor_invariant)?,
                    specs[2].ok_or_else(InternalError::executor_invariant)?,
                ],
                preflight.child_cardinalities(),
                inputs.continuation.direction(),
            )?,
            _ => return Err(InternalError::executor_invariant()),
        };
        Ok(Some(overlap))
    }

    fn first_stream_or_empty(streams: Vec<OrderedKeyStreamBox>) -> OrderedKeyStreamBox {
        streams
            .into_iter()
            .next()
            .unwrap_or_else(OrderedKeyStreamBox::empty)
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
                let path_facts = path.shape_facts();
                let index_prefix_specs = if path_facts.index_prefix_spec_count() > 0 {
                    spec_cursor
                        .require_next_index_prefix_specs(path_facts.index_prefix_spec_count())?
                } else {
                    &[]
                };
                let index_range_spec = if path_facts.consumes_index_range_spec() {
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
        let union_uses_accepted_index = children.iter().any(Self::plan_uses_accepted_index);
        let mut streams = Self::reserve_stream_slots(children.len())?;
        for child in children {
            let child_inputs = inputs
                .with_physical_fetch_hint(None)
                .without_leaf_index_order_preservation();
            let stream = Self::produce_key_stream(runtime, child, child_inputs, spec_cursor)?;
            let stream = if union_uses_accepted_index
                && !Self::plan_uses_accepted_index(child)
                && Self::plan_may_emit_unverified_primary_lookup(child)
            {
                Self::filter_existing_primary_union_keys(runtime, child, stream)?
            } else {
                stream
            };
            streams.push(stream);
        }
        let key_comparator = KeyOrderComparator::from_direction(inputs.continuation.direction());

        OrderedKeyStreamBox::merge_all(streams, key_comparator)
    }

    // An accepted-index union uses strict missing-row consistency at the final
    // row boundary. Exact primary-key siblings therefore remove ordinary
    // absent lookup candidates before the merge, so a missing key that reaches
    // the strict boundary still carries accepted-index provenance.
    fn filter_existing_primary_union_keys(
        runtime: &TraversalRuntime,
        plan: &ExecutableAccessPlan<'_, Value>,
        mut stream: OrderedKeyStreamBox,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let capacity = Self::primary_lookup_candidate_upper_bound(plan)
            .ok_or_else(InternalError::executor_invariant)?;
        let slot_bytes = capacity
            .checked_mul(size_of::<DecodedDataStoreKey>())
            .ok_or_else(InternalError::executor_invariant)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            u64::try_from(slot_bytes).unwrap_or(u64::MAX),
        )?;
        let mut existing = Vec::new();
        existing
            .try_reserve_exact(capacity)
            .map_err(|_| InternalError::executor_internal())?;
        let extra_capacity = existing.capacity().saturating_sub(capacity);
        let extra_slot_bytes = extra_capacity
            .checked_mul(size_of::<DecodedDataStoreKey>())
            .ok_or_else(InternalError::executor_invariant)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            u64::try_from(extra_slot_bytes).unwrap_or(u64::MAX),
        )?;
        while let Some(key) = stream.next_key()? {
            let raw = key.to_raw()?;
            charge_current_execution_budget(DiagnosticExecutionBudgetResource::RowsVisited, 1)?;
            charge_current_execution_budget(
                DiagnosticExecutionBudgetResource::TemporaryBytes,
                u64::try_from(raw.as_bytes().len()).unwrap_or(u64::MAX),
            )?;
            if runtime.store.with_data(|store| store.contains(&raw)) {
                existing.push(key);
            }
        }

        Ok(ordered_key_stream_from_materialized_keys(existing))
    }

    fn plan_uses_accepted_index(plan: &ExecutableAccessPlan<'_, Value>) -> bool {
        match plan.node() {
            ExecutableAccessNode::Path(path) => matches!(
                path,
                ExecutionPathPayload::IndexPrefix { .. }
                    | ExecutionPathPayload::IndexMultiLookup { .. }
                    | ExecutionPathPayload::IndexBranchSet { .. }
                    | ExecutionPathPayload::IndexRange { .. }
            ),
            ExecutableAccessNode::Union(children)
            | ExecutableAccessNode::Intersection(children) => {
                children.iter().any(Self::plan_uses_accepted_index)
            }
        }
    }

    fn plan_may_emit_unverified_primary_lookup(plan: &ExecutableAccessPlan<'_, Value>) -> bool {
        match plan.node() {
            ExecutableAccessNode::Path(path) => matches!(
                path,
                ExecutionPathPayload::ByKey(_) | ExecutionPathPayload::ByKeys(_)
            ),
            ExecutableAccessNode::Union(children) => children
                .iter()
                .any(Self::plan_may_emit_unverified_primary_lookup),
            ExecutableAccessNode::Intersection(children) => children
                .iter()
                .all(Self::plan_may_emit_unverified_primary_lookup),
        }
    }

    fn primary_lookup_candidate_upper_bound(
        plan: &ExecutableAccessPlan<'_, Value>,
    ) -> Option<usize> {
        match plan.node() {
            ExecutableAccessNode::Path(ExecutionPathPayload::ByKey(_)) => Some(1),
            ExecutableAccessNode::Path(ExecutionPathPayload::ByKeys(keys)) => Some(keys.len()),
            ExecutableAccessNode::Path(
                ExecutionPathPayload::KeyRange { .. }
                | ExecutionPathPayload::IndexPrefix { .. }
                | ExecutionPathPayload::IndexMultiLookup { .. }
                | ExecutionPathPayload::IndexBranchSet { .. }
                | ExecutionPathPayload::IndexRange { .. }
                | ExecutionPathPayload::FullScan,
            ) => None,
            ExecutableAccessNode::Union(children) => {
                children.iter().try_fold(0usize, |total, child| {
                    total.checked_add(Self::primary_lookup_candidate_upper_bound(child)?)
                })
            }
            ExecutableAccessNode::Intersection(children) => {
                let mut children = children.iter();
                let first = Self::primary_lookup_candidate_upper_bound(children.next()?)?;
                children.try_fold(first, |minimum, child| {
                    Some(minimum.min(Self::primary_lookup_candidate_upper_bound(child)?))
                })
            }
        }
    }

    // Build one canonical stream for an intersection by pairwise-intersecting child streams.
    fn produce_intersection_key_stream(
        runtime: &TraversalRuntime,
        children: &[ExecutableAccessPlan<'_, Value>],
        inputs: TraversalInputs<'_>,
        spec_cursor: &mut AccessSpecCursor<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let key_comparator = KeyOrderComparator::from_direction(inputs.continuation.direction());
        let admission = Self::exact_intersection_admission(runtime, children, inputs, *spec_cursor);
        match admission {
            ExactIntersectionAdmission::NotApplicable => {
                let streams =
                    Self::collect_child_key_streams(runtime, children, inputs, spec_cursor)?;
                OrderedKeyStreamBox::intersect_all(streams, key_comparator)
            }
            ExactIntersectionAdmission::ConservativeFallback => {
                let streams = Self::collect_exact_intersection_child_streams(
                    runtime,
                    children,
                    inputs,
                    spec_cursor,
                )?;
                Ok(Self::first_stream_or_empty(streams))
            }
            ExactIntersectionAdmission::ProvenEmpty => {
                let _consumed_streams = Self::collect_exact_intersection_child_streams(
                    runtime,
                    children,
                    inputs,
                    spec_cursor,
                )?;
                Ok(OrderedKeyStreamBox::empty())
            }
            ExactIntersectionAdmission::Probe(preflight) => {
                if !Self::exact_intersection_probe_can_beat_single(&preflight) {
                    let streams = Self::collect_exact_intersection_child_streams(
                        runtime,
                        children,
                        inputs,
                        spec_cursor,
                    )?;
                    return Ok(Self::first_stream_or_empty(streams));
                }

                let mut direct_cursor = *spec_cursor;
                if let Some(overlap) = Self::collect_direct_exact_intersection_overlap(
                    runtime,
                    children,
                    inputs,
                    &mut direct_cursor,
                    &preflight,
                )? {
                    *spec_cursor = direct_cursor;
                    if !Self::exact_intersection_cost_beats_single(
                        &preflight,
                        u64::try_from(overlap.len()).unwrap_or(u64::MAX),
                    ) {
                        return Err(InternalError::executor_invariant());
                    }

                    return Ok(ordered_key_stream_from_materialized_keys(overlap));
                }

                let mut probe_cursor = *spec_cursor;
                let probe_streams = Self::collect_exact_intersection_child_streams(
                    runtime,
                    children,
                    inputs,
                    &mut probe_cursor,
                )?;
                let overlap = Self::collect_exact_intersection_overlap(
                    probe_streams,
                    key_comparator,
                    &preflight,
                )?;
                *spec_cursor = probe_cursor;
                if !Self::exact_intersection_cost_beats_single(
                    &preflight,
                    u64::try_from(overlap.len()).unwrap_or(u64::MAX),
                ) {
                    return Err(InternalError::executor_invariant());
                }

                Ok(ordered_key_stream_from_materialized_keys(overlap))
            }
        }
    }
}
