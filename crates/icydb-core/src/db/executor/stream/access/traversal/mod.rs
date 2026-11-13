//! Module: executor::stream::access::traversal
//! Responsibility: build and execute access-path traversal streams for runtime loading.
//! Does not own: access-plan construction or planner routing semantics.
//! Boundary: lowers executable access contracts into ordered key/data stream traversal.

use crate::{
    db::{
        data::{DecodedDataStoreKey, RawDataStoreKey},
        executor::{
            ExecutableAccessChildren, ExecutableAccessPlan, ExecutionPathPayload, IndexScan,
            lowered_index_prefix_exact_cardinalities_for_admitted_root,
            pipeline::contracts::{AccessScanContinuationInput, AccessStreamBindings},
            production_scalar_page_work_is_active,
            route::IndexPrefixChildExpansionHint,
            stream::{
                access::{
                    bindings::{
                        AccessStreamExecutionPolicy, ExecutableAccess, IndexLeafOrderPolicy,
                    },
                    physical,
                },
                key::{
                    KeyOrderComparator, OrderedKeyStreamBox,
                    ordered_key_stream_from_materialized_keys,
                },
            },
        },
        index::predicate::IndexPredicateExecution,
        integrity::DatabaseIncarnationId,
        resource::budget::charge_current_execution_budget,
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
    continuation: AccessScanContinuationInput<'a>,
    execution_policy: AccessStreamExecutionPolicy,
    index_predicate_execution: Option<crate::db::index::predicate::IndexPredicateExecution<'a>>,
    index_prefix_child_expansion: Option<IndexPrefixChildExpansionHint>,
}

#[cfg(test)]
mod exact_intersection_tests;

impl TraversalInputs<'_> {
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
        request: &ExecutableAccess<'_, Value>,
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
            continuation: bindings.continuation,
            execution_policy,
            index_predicate_execution,
            index_prefix_child_expansion: bindings.index_prefix_child_expansion,
        };
        let key_stream = AccessPlanStreamResolver::produce_key_stream(self, plan, inputs)?;

        Ok(key_stream)
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

    // Collect one child key stream for each child access plan.
    fn collect_child_key_streams(
        runtime: &TraversalRuntime,
        children: &ExecutableAccessChildren<'_, Value>,
        inputs: TraversalInputs<'_>,
    ) -> Result<Vec<OrderedKeyStreamBox>, InternalError> {
        let mut streams = Self::reserve_stream_slots(children.len())?;
        // Callers select leaf ordering once. Composite children never inherit a
        // physical fetch hint, regardless of their admitted ordering policy.
        let child_inputs = inputs.with_physical_fetch_hint(None);
        for child in children.iter() {
            streams.push(Self::produce_key_stream(runtime, &child, child_inputs)?);
        }
        Ok(streams)
    }

    // The selected fallback is already the planner-preferred first prefix.
    // Require every child to be prepared, but do not construct physical
    // streams for rejected siblings. Overlap probes still visit every child.
    fn produce_exact_intersection_fallback(
        runtime: &TraversalRuntime,
        children: &ExecutableAccessChildren<'_, Value>,
        inputs: TraversalInputs<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let mut children = children.iter();
        let first = children
            .next()
            .ok_or_else(InternalError::executor_invariant)?;
        let stream = Self::produce_key_stream(
            runtime,
            &first,
            inputs
                .with_physical_fetch_hint(None)
                .with_physical_leaf_order(),
        )?;
        for child in children {
            let ExecutableAccessPlan::Path(_, _) = &child else {
                return Err(InternalError::executor_invariant());
            };
            let _specs = child
                .index_specs()?
                .ok_or_else(InternalError::executor_invariant)?
                .prefixes();
        }
        Ok(stream)
    }

    fn exact_intersection_admission(
        runtime: &TraversalRuntime,
        children: &ExecutableAccessChildren<'_, Value>,
        inputs: TraversalInputs<'_>,
    ) -> ExactIntersectionAdmission {
        if !(2..=MAX_ATOMIC_EXACT_INTERSECTION_CHILDREN).contains(&children.len())
            || inputs.index_predicate_execution.is_some()
        {
            return ExactIntersectionAdmission::NotApplicable;
        }

        let mut cardinality_specs = Vec::with_capacity(children.len());
        for child in children.iter() {
            let ExecutableAccessPlan::Path(path, _) = &child else {
                return ExactIntersectionAdmission::NotApplicable;
            };
            let ExecutionPathPayload::IndexPrefix { .. } = path else {
                return ExactIntersectionAdmission::NotApplicable;
            };
            let path_facts = path.shape_facts();
            if path_facts.index_prefix_spec_count() != 1 || path_facts.consumes_index_range_spec() {
                return ExactIntersectionAdmission::NotApplicable;
            }
            let Some(spec) = child
                .index_specs()
                .ok()
                .flatten()
                .and_then(|specs| specs.prefixes().first())
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
    // general ordered stream tree. This route is limited to a cursorless
    // atomic probe; resumed traversal retains the ordinary stream contract.
    fn collect_direct_exact_intersection_overlap(
        runtime: &TraversalRuntime,
        children: &ExecutableAccessChildren<'_, Value>,
        inputs: TraversalInputs<'_>,
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
        for (slot, child) in specs.iter_mut().zip(children.iter()) {
            let ExecutableAccessPlan::Path(_, _) = &child else {
                return Err(InternalError::executor_invariant());
            };
            let child_specs = child
                .index_specs()?
                .ok_or_else(InternalError::executor_invariant)?
                .prefixes();

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

    // Build an ordered key stream for this access plan.
    /// Produce one ordered key stream for an access plan using each leaf's admitted bounds.
    fn produce_key_stream(
        runtime: &TraversalRuntime,
        access: &ExecutableAccessPlan<'_, Value>,
        inputs: TraversalInputs<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        match access {
            ExecutableAccessPlan::Path(_, _) => physical::resolve_structural_physical_key_stream(
                access,
                runtime.store,
                runtime.entity_tag,
                AccessStreamBindings::new(inputs.continuation)
                    .with_index_prefix_child_expansion(inputs.index_prefix_child_expansion),
                inputs.execution_policy,
                inputs.index_predicate_execution,
            ),
            ExecutableAccessPlan::Union(children) => {
                Self::produce_union_key_stream(runtime, children, inputs)
            }
            ExecutableAccessPlan::Intersection(children) => {
                Self::produce_intersection_key_stream(runtime, children, inputs)
            }
        }
    }

    // Build one canonical stream for a union by pairwise-merging child streams.
    fn produce_union_key_stream(
        runtime: &TraversalRuntime,
        children: &ExecutableAccessChildren<'_, Value>,
        inputs: TraversalInputs<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let union_uses_accepted_index = children
            .iter()
            .any(|child| Self::plan_uses_accepted_index(&child));
        let mut streams = Self::reserve_stream_slots(children.len())?;
        for child in children.iter() {
            let child_inputs = inputs
                .with_physical_fetch_hint(None)
                .without_leaf_index_order_preservation();
            let stream = Self::produce_key_stream(runtime, &child, child_inputs)?;
            let stream = if union_uses_accepted_index
                && !Self::plan_uses_accepted_index(&child)
                && Self::plan_may_emit_unverified_primary_lookup(&child)
            {
                Self::filter_existing_primary_union_keys(runtime, &child, stream)?
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
            let raw = key.raw_key()?;
            charge_current_execution_budget(DiagnosticExecutionBudgetResource::RowsVisited, 1)?;
            charge_current_execution_budget(
                DiagnosticExecutionBudgetResource::TemporaryBytes,
                u64::try_from(raw.as_bytes().len()).unwrap_or(u64::MAX),
            )?;
            if runtime.store.with_data(|store| store.contains(raw)) {
                existing.push(key);
            }
        }

        Ok(ordered_key_stream_from_materialized_keys(existing))
    }

    fn plan_uses_accepted_index(plan: &ExecutableAccessPlan<'_, Value>) -> bool {
        match plan {
            ExecutableAccessPlan::Path(path, _) => matches!(
                path,
                ExecutionPathPayload::IndexPrefix { .. }
                    | ExecutionPathPayload::IndexMultiLookup { .. }
                    | ExecutionPathPayload::IndexBranchSet { .. }
                    | ExecutionPathPayload::IndexRange { .. }
            ),
            ExecutableAccessPlan::Union(children)
            | ExecutableAccessPlan::Intersection(children) => children
                .iter()
                .any(|child| Self::plan_uses_accepted_index(&child)),
        }
    }

    fn plan_may_emit_unverified_primary_lookup(plan: &ExecutableAccessPlan<'_, Value>) -> bool {
        match plan {
            ExecutableAccessPlan::Path(path, _) => matches!(
                path,
                ExecutionPathPayload::ByKey(_) | ExecutionPathPayload::ByKeys(_)
            ),
            ExecutableAccessPlan::Union(children) => children
                .iter()
                .any(|child| Self::plan_may_emit_unverified_primary_lookup(&child)),
            ExecutableAccessPlan::Intersection(children) => children
                .iter()
                .all(|child| Self::plan_may_emit_unverified_primary_lookup(&child)),
        }
    }

    fn primary_lookup_candidate_upper_bound(
        plan: &ExecutableAccessPlan<'_, Value>,
    ) -> Option<usize> {
        match plan {
            ExecutableAccessPlan::Path(ExecutionPathPayload::ByKey(_), _) => Some(1),
            ExecutableAccessPlan::Path(ExecutionPathPayload::ByKeys(keys), _) => Some(keys.len()),
            ExecutableAccessPlan::Path(
                ExecutionPathPayload::KeyRange { .. }
                | ExecutionPathPayload::IndexPrefix { .. }
                | ExecutionPathPayload::IndexMultiLookup { .. }
                | ExecutionPathPayload::IndexBranchSet { .. }
                | ExecutionPathPayload::IndexRange { .. }
                | ExecutionPathPayload::FullScan,
                _,
            ) => None,
            ExecutableAccessPlan::Union(children) => {
                children.iter().try_fold(0usize, |total, child| {
                    total.checked_add(Self::primary_lookup_candidate_upper_bound(&child)?)
                })
            }
            ExecutableAccessPlan::Intersection(children) => {
                let mut children = children.iter();
                let first = Self::primary_lookup_candidate_upper_bound(&children.next()?)?;
                children.try_fold(first, |minimum, child| {
                    Some(minimum.min(Self::primary_lookup_candidate_upper_bound(&child)?))
                })
            }
        }
    }

    // Build one canonical stream for an intersection by pairwise-intersecting child streams.
    fn produce_intersection_key_stream(
        runtime: &TraversalRuntime,
        children: &ExecutableAccessChildren<'_, Value>,
        inputs: TraversalInputs<'_>,
    ) -> Result<OrderedKeyStreamBox, InternalError> {
        let key_comparator = KeyOrderComparator::from_direction(inputs.continuation.direction());
        let admission = Self::exact_intersection_admission(runtime, children, inputs);
        match admission {
            ExactIntersectionAdmission::NotApplicable => {
                let streams = Self::collect_child_key_streams(
                    runtime,
                    children,
                    inputs.without_leaf_index_order_preservation(),
                )?;
                let stream = OrderedKeyStreamBox::intersect_all(streams, key_comparator)?;
                // A pull may skip arbitrarily many nonmatching child keys.
                // Finish this blocking fallback under the hard request budget
                // before scalar page units require a finite per-pull bound.
                if production_scalar_page_work_is_active()? {
                    physical::materialize_ordered_key_stream(stream)
                } else {
                    Ok(stream)
                }
            }
            ExactIntersectionAdmission::ConservativeFallback => {
                Self::produce_exact_intersection_fallback(runtime, children, inputs)
            }
            ExactIntersectionAdmission::ProvenEmpty => {
                let _consumed_streams = Self::collect_child_key_streams(
                    runtime,
                    children,
                    inputs.with_physical_leaf_order(),
                )?;
                Ok(OrderedKeyStreamBox::empty())
            }
            ExactIntersectionAdmission::Probe(preflight) => {
                if !Self::exact_intersection_probe_can_beat_single(&preflight) {
                    return Self::produce_exact_intersection_fallback(runtime, children, inputs);
                }

                if let Some(overlap) = Self::collect_direct_exact_intersection_overlap(
                    runtime, children, inputs, &preflight,
                )? {
                    if !Self::exact_intersection_cost_beats_single(
                        &preflight,
                        u64::try_from(overlap.len()).unwrap_or(u64::MAX),
                    ) {
                        return Err(InternalError::executor_invariant());
                    }

                    return Ok(ordered_key_stream_from_materialized_keys(overlap));
                }

                let probe_streams = Self::collect_child_key_streams(
                    runtime,
                    children,
                    inputs.with_physical_leaf_order(),
                )?;
                let overlap = Self::collect_exact_intersection_overlap(
                    probe_streams,
                    key_comparator,
                    &preflight,
                )?;

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
