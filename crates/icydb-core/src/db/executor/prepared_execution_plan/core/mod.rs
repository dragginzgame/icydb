use super::contracts::{
    AcceptedContinuationIdentity, AccessPlannedQuery, CoveringHybridReadExecutionPlan,
    CoveringReadExecutionPlan, ExecutionOrdering, PlannedContinuationContract,
};
use crate::{
    db::{
        access::{
            LoweredAccessError, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            lowering::bind_access_with_schema_info,
        },
        commit::CommitSchemaFingerprint,
        cursor::{ContinuationSignature, CursorPlanError, ValidatedGroupedCursor},
        executor::{
            EntityAuthority, ExecutionPreparation, ExecutionRoutePlan, GroupedPaginationWindow,
            ScalarContinuationContext,
            budget::read_shape_fingerprint_prefix,
            pipeline::{
                contracts::{CursorEmissionMode, ProjectionMaterializationMode},
                runtime::{
                    compile_grouped_row_slot_layout_from_inputs,
                    compile_retained_slot_layout_for_mode,
                },
            },
            planning::{
                preparation::slot_map_for_model_plan,
                route::{RoutePlanRequest, build_execution_route_plan},
            },
            projection::{PreparedProjectionContract, prepare_projection_contract_from_plan},
            terminal::RetainedSlotLayout,
        },
        resource::{budget::ExecutionConstructionBudget, construction::ConstructionBudget},
    },
    error::InternalError,
    retained::{Retained, RetainedBytes},
};
use std::{
    cell::RefCell,
    rc::{Rc, Weak},
    sync::OnceLock,
};

use crate::db::executor::planning::preparation::covering_strict_predicate_compatible_for_plan;

///
/// ExecutionFamily
///
/// Executor-facing execution family summary derived from planner ordering.
/// Session and runtime entrypoints consume this strategy and must not
/// re-derive grouped/scalar routing shape from boolean flags.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionFamily {
    /// Continuation follows primary-key order.
    PrimaryKey,
    /// Continuation follows the admitted explicit ordering.
    Ordered,
    /// Continuation follows grouped result ordering.
    Grouped,
}

///
/// PreparedExecutionPlanResidents
///
/// Prepared-plan residents stored behind `PreparedExecutionPlanCore`.
/// The struct keeps immutable plan state, lowered access metadata, and lazy
/// runtime residents together so cloned prepared plans share expensive
/// preparation products without duplicating logical plans.
///

pub(in crate::db::executor::prepared_execution_plan) struct PreparedExecutionPlanResidents {
    cache_retention: RefCell<Weak<crate::db::resource::bounded_cache::CacheEntryWeight>>,
    pub(in crate::db::executor::prepared_execution_plan) plan: Rc<AccessPlannedQuery>,
    pub(in crate::db::executor::prepared_execution_plan) execution_shape_fingerprint_prefix: u64,
    pub(in crate::db::executor::prepared_execution_plan) continuation_identity:
        AcceptedContinuationIdentity,
    pub(in crate::db::executor::prepared_execution_plan) prepared_projection_contract:
        OnceLock<Option<Rc<PreparedProjectionContract>>>,
    pub(in crate::db::executor::prepared_execution_plan) projection_covering_read_execution_plan:
        OnceLock<Option<Rc<CoveringReadExecutionPlan>>>,
    pub(in crate::db::executor::prepared_execution_plan) hybrid_covering_read_plan:
        OnceLock<Option<Rc<CoveringHybridReadExecutionPlan>>>,
    pub(in crate::db::executor::prepared_execution_plan) prepared_grouped_runtime_residents:
        OnceLock<Rc<PreparedGroupedRuntimeResidents>>,
    pub(in crate::db::executor::prepared_execution_plan) aggregate_execution_preparation:
        OnceLock<Rc<ExecutionPreparation>>,
    pub(in crate::db::executor::prepared_execution_plan) scalar_execution_preparation:
        OnceLock<Rc<ExecutionPreparation>>,
    pub(in crate::db::executor::prepared_execution_plan) initial_scalar_route_plan:
        OnceLock<ExecutionRoutePlan>,
    pub(in crate::db::executor::prepared_execution_plan) cursorless_retained_slot_layout:
        OnceLock<Option<RetainedSlotLayout>>,
    pub(in crate::db::executor::prepared_execution_plan) continuation:
        Option<PlannedContinuationContract>,
}

impl std::fmt::Debug for PreparedExecutionPlanResidents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparedExecutionPlanResidents(..)")
    }
}

///
/// PreparedExecutionPlanCore
///
/// Generic-free prepared execution-plan payload. This keeps cursor, ordering,
/// and lowered structural plan state monomorphic below every frontend
/// boundary.
///

#[derive(Clone, Debug)]
pub(in crate::db::executor::prepared_execution_plan) struct PreparedExecutionPlanCore {
    pub(in crate::db::executor::prepared_execution_plan) residents:
        Rc<PreparedExecutionPlanResidents>,
}

///
/// PreparedGroupedRuntimeResidents
///
/// Lazily cached grouped runtime preparation pair for one prepared plan.
/// Grouped load wrappers share this resident as a unit so execution preparation
/// and retained-slot layout cannot drift across separate cache lookups.
///

pub(in crate::db::executor) struct PreparedGroupedRuntimeResidents {
    execution_preparation: ExecutionPreparation,
    grouped_slot_layout: RetainedSlotLayout,
}

impl std::fmt::Debug for PreparedGroupedRuntimeResidents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparedGroupedRuntimeResidents(..)")
    }
}

impl PreparedGroupedRuntimeResidents {
    /// Build one grouped preparation/layout bundle from the same logical-plan
    /// provenance.
    const fn new(
        execution_preparation: ExecutionPreparation,
        grouped_slot_layout: RetainedSlotLayout,
    ) -> Self {
        Self {
            execution_preparation,
            grouped_slot_layout,
        }
    }

    /// Borrow immutable predicate preparation from the paired resident.
    pub(in crate::db::executor) const fn execution_preparation(&self) -> &ExecutionPreparation {
        &self.execution_preparation
    }

    /// Borrow the row layout compiled for this preparation's runtime filter.
    pub(in crate::db::executor) const fn grouped_slot_layout(&self) -> &RetainedSlotLayout {
        &self.grouped_slot_layout
    }
}

///
/// PreparedScalarPlanCore
///
/// Shared scalar prepared-plan handle carried through route runtime assembly.
/// Scalar execution borrows the logical plan and lowered access specs from this
/// handle so cached prepared plans do not clone the full `AccessPlannedQuery`
/// just to cross the scalar materialization boundary.
///

pub(in crate::db::executor) struct PreparedScalarPlanCore {
    pub(in crate::db::executor::prepared_execution_plan) core: PreparedExecutionPlanCore,
}

impl PreparedScalarPlanCore {
    #[must_use]
    pub(in crate::db::executor) fn plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    pub(in crate::db::executor) fn single_index_prefix_specs(&self) -> &[LoweredIndexPrefixSpec] {
        self.core.residents.plan.access.single_index_prefix_specs()
    }

    pub(in crate::db::executor) fn single_index_range_specs(&self) -> &[LoweredIndexRangeSpec] {
        self.core.residents.plan.access.single_index_range_specs()
    }

    pub(in crate::db::executor) fn get_or_init_initial_scalar_route_plan(
        &self,
        authority: &EntityAuthority,
    ) -> Result<ExecutionRoutePlan, InternalError> {
        self.core.get_or_init_initial_scalar_route_plan(authority)
    }

    pub(in crate::db::executor) fn get_or_init_cursorless_retained_slot_layout(
        &self,
        authority: &EntityAuthority,
    ) -> Result<Option<RetainedSlotLayout>, InternalError> {
        self.core
            .get_or_init_cursorless_retained_slot_layout(authority)
    }
}

impl PreparedExecutionPlanCore {
    pub(in crate::db::executor::prepared_execution_plan) fn cache_retention_available(
        &self,
    ) -> bool {
        self.residents.cache_retention.borrow().upgrade().is_none()
    }

    pub(in crate::db::executor::prepared_execution_plan) fn attach_cache_retention(
        &self,
        entry: &Rc<crate::db::resource::bounded_cache::CacheEntryWeight>,
    ) {
        *self.residents.cache_retention.borrow_mut() = Rc::downgrade(entry);
    }

    // Measure the actual clone to be retained, not its source or wire encoding.
    // Inline OnceLock storage was already charged when the entry was admitted.
    fn remember_lazy<T: Retained + Clone>(&self, cell: &OnceLock<T>, value: &T) {
        retain_lazy(&self.residents.cache_retention.borrow(), cell, value);
    }

    fn initialize_lazy<T: Retained + Clone>(
        &self,
        cell: &OnceLock<T>,
        build: impl FnOnce() -> T,
    ) -> T {
        if let Some(value) = cell.get() {
            return value.clone();
        }
        let value = build();
        self.remember_lazy(cell, &value);
        value
    }

    // A failed builder never becomes a completed resident or changes its weight.
    fn try_initialize_lazy<T: Retained + Clone>(
        &self,
        cell: &OnceLock<T>,
        build: impl FnOnce() -> Result<T, InternalError>,
    ) -> Result<T, InternalError> {
        if let Some(value) = cell.get() {
            return Ok(value.clone());
        }
        let value = build()?;
        self.remember_lazy(cell, &value);

        Ok(value)
    }

    #[must_use]
    fn new(
        plan: Rc<AccessPlannedQuery>,
        execution_shape_fingerprint_prefix: u64,
        continuation_identity: AcceptedContinuationIdentity,
        continuation: Option<PlannedContinuationContract>,
    ) -> Self {
        Self {
            residents: Rc::new(PreparedExecutionPlanResidents {
                cache_retention: RefCell::new(Weak::new()),
                plan,
                execution_shape_fingerprint_prefix,
                continuation_identity,
                prepared_projection_contract: OnceLock::new(),
                projection_covering_read_execution_plan: OnceLock::new(),
                hybrid_covering_read_plan: OnceLock::new(),
                prepared_grouped_runtime_residents: OnceLock::new(),
                aggregate_execution_preparation: OnceLock::new(),
                scalar_execution_preparation: OnceLock::new(),
                initial_scalar_route_plan: OnceLock::new(),
                cursorless_retained_slot_layout: OnceLock::new(),
                continuation,
            }),
        }
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn plan(&self) -> &AccessPlannedQuery {
        &self.residents.plan
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_projection_shape(
        &self,
        authority: &EntityAuthority,
    ) -> Result<Option<Rc<PreparedProjectionContract>>, InternalError> {
        // Projection adapters request and consume this shape before execution.
        self.try_initialize_lazy(&self.residents.prepared_projection_contract, || {
            if self.residents.plan.scalar_projection_plan().is_some() {
                prepare_projection_contract_from_plan(
                    authority.row_layout_ref(),
                    &self.residents.plan,
                )
                .map(Rc::new)
                .map(Some)
            } else {
                Ok(None)
            }
        })
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_projection_covering_read_execution_plan(
        &self,
        authority: &EntityAuthority,
    ) -> Option<Rc<CoveringReadExecutionPlan>> {
        self.initialize_lazy(
            &self.residents.projection_covering_read_execution_plan,
            || {
                let strict_predicate_compatible =
                    covering_strict_predicate_compatible_for_plan(&self.residents.plan).ok()?;

                authority
                    .covering_read_execution_plan(&self.residents.plan, strict_predicate_compatible)
                    .map(Rc::new)
            },
        )
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_hybrid_covering_read_plan(
        &self,
        authority: &EntityAuthority,
    ) -> Option<Rc<CoveringHybridReadExecutionPlan>> {
        self.initialize_lazy(&self.residents.hybrid_covering_read_plan, || {
            let strict_predicate_compatible =
                covering_strict_predicate_compatible_for_plan(&self.residents.plan).ok()?;

            authority
                .covering_hybrid_projection_plan(&self.residents.plan, strict_predicate_compatible)
                .map(Rc::new)
        })
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_grouped_runtime_residents(
        &self,
        authority: &EntityAuthority,
    ) -> Result<Rc<PreparedGroupedRuntimeResidents>, InternalError> {
        // A grouped runtime requires the complete planner contract. Failed
        // construction is never cached; declined retention still returns the
        // completed preparation/layout pair for this execution.
        self.try_initialize_lazy(&self.residents.prepared_grouped_runtime_residents, || {
            let plan = &self.residents.plan;
            let grouped_plan = plan
                .grouped_plan()
                .ok_or_else(InternalError::planner_executor_invariant)?;
            let grouped_distinct_execution_strategy = plan
                .grouped_distinct_execution_strategy()
                .ok_or_else(InternalError::planner_executor_invariant)?;
            let aggregate_specs = plan
                .grouped_aggregate_execution_specs()
                .ok_or_else(InternalError::planner_executor_invariant)?;
            let execution_preparation = ExecutionPreparation::from_runtime_plan(
                plan,
                plan.slot_map().map(<[usize]>::to_vec),
                &ExecutionConstructionBudget,
            )?;
            let grouped_slot_layout = compile_grouped_row_slot_layout_from_inputs(
                &authority.row_layout(),
                &grouped_plan.group.group_fields,
                aggregate_specs,
                grouped_distinct_execution_strategy,
                execution_preparation.effective_runtime_filter_program(),
            );

            Ok(Rc::new(PreparedGroupedRuntimeResidents::new(
                execution_preparation,
                grouped_slot_layout,
            )))
        })
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_scalar_execution_preparation(
        &self,
    ) -> Result<Rc<ExecutionPreparation>, InternalError> {
        // Scalar execution preparation is fully plan-deterministic: it depends
        // on the effective runtime predicate and slot map, but not on store
        // handles, cursor state, route retry policy, diagnostics, or
        // materialization mode. Share the immutable bundle through execution
        // so warm handoffs do not copy its programs, literals or slot metadata.
        self.try_initialize_lazy(&self.residents.scalar_execution_preparation, || {
            ExecutionPreparation::from_runtime_plan(
                &self.residents.plan,
                slot_map_for_model_plan(&self.residents.plan),
                &ExecutionConstructionBudget,
            )
            .map(Rc::new)
        })
    }

    #[cfg(feature = "sql")]
    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_aggregate_execution_preparation(
        &self,
    ) -> Result<Rc<ExecutionPreparation>, InternalError> {
        // Aggregate route planning additionally consumes the predicate
        // capability snapshot and strict index program. Keep that immutable
        // preparation in the existing aggregate resident rather than
        // rebuilding it in SQL or terminal execution.
        self.try_initialize_lazy(&self.residents.aggregate_execution_preparation, || {
            ExecutionPreparation::from_plan(
                &self.residents.plan,
                slot_map_for_model_plan(&self.residents.plan),
                &ExecutionConstructionBudget,
            )
            .map(Rc::new)
        })
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_initial_scalar_route_plan(
        &self,
        authority: &EntityAuthority,
    ) -> Result<ExecutionRoutePlan, InternalError> {
        self.try_initialize_lazy(&self.residents.initial_scalar_route_plan, || {
            build_execution_route_plan(
                &self.residents.plan,
                RoutePlanRequest::Load {
                    continuation: ScalarContinuationContext::initial(),
                    authority: Some(authority),
                    load_terminal_fast_path: None,
                },
            )
        })
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_cursorless_retained_slot_layout(
        &self,
        authority: &EntityAuthority,
    ) -> Result<Option<RetainedSlotLayout>, InternalError> {
        // Only cursorless retained output shares a cached scalar layout.
        self.try_initialize_lazy(&self.residents.cursorless_retained_slot_layout, || {
            compile_retained_slot_layout_for_mode(
                authority,
                &self.residents.plan,
                ProjectionMaterializationMode::RetainSlotRows,
                CursorEmissionMode::Suppress,
            )
        })
    }

    // Classification only inspects the retained variant; do not copy its order
    // expressions. Missing continuation still follows the shared error boundary.
    pub(in crate::db::executor::prepared_execution_plan) fn execution_family(
        &self,
    ) -> Result<ExecutionFamily, InternalError> {
        let ordering = self.continuation_contract()?.order_contract().ordering();

        Ok(match ordering {
            ExecutionOrdering::PrimaryKey => ExecutionFamily::PrimaryKey,
            ExecutionOrdering::Explicit(_) => ExecutionFamily::Ordered,
            ExecutionOrdering::Grouped(_) => ExecutionFamily::Grouped,
        })
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn execution_shape_fingerprint_prefix(
        &self,
    ) -> u64 {
        self.residents.execution_shape_fingerprint_prefix
    }

    pub(in crate::db::executor::prepared_execution_plan) fn continuation_signature_for_runtime(
        &self,
    ) -> Result<ContinuationSignature, InternalError> {
        let contract = self.continuation_contract()?;
        Ok(contract.continuation_signature())
    }

    pub(in crate::db::executor::prepared_execution_plan) fn grouped_cursor_boundary_arity(
        &self,
    ) -> Result<usize, InternalError> {
        let contract = self.continuation_contract()?;
        if !contract.is_grouped() {
            return Err(CursorPlanError::continuation_cursor_invariant().into_internal_error());
        }

        Ok(contract.boundary_arity())
    }

    pub(in crate::db::executor::prepared_execution_plan) fn grouped_pagination_window(
        &self,
        cursor: &ValidatedGroupedCursor,
    ) -> Result<GroupedPaginationWindow, InternalError> {
        let contract = self.continuation_contract()?;
        contract
            .project_grouped_paging_window(cursor)
            .map_err(CursorPlanError::into_internal_error)
    }

    // Borrow immutable continuation contract for load-mode plans.
    pub(in crate::db::executor::prepared_execution_plan) fn continuation_contract(
        &self,
    ) -> Result<&PlannedContinuationContract, InternalError> {
        self.residents
            .continuation
            .as_ref()
            .ok_or_else(|| CursorPlanError::continuation_cursor_invariant().into_internal_error())
    }
}

// Keep the reservation boundary independent from which preparation product is
// being built. A declined attachment leaves the caller's owned result usable.
fn retain_lazy<T: Retained + Clone>(
    accounting: &Weak<crate::db::resource::bounded_cache::CacheEntryWeight>,
    cell: &OnceLock<T>,
    value: &T,
) {
    if cell.get().is_some() {
        return;
    }
    let retained = value.clone();
    if let Some(entry) = accounting.upgrade() {
        let mut bytes = RetainedBytes::new(entry.remaining());
        if bytes.visit(&retained).is_none() || !entry.reserve(bytes.total()) {
            return;
        }
    }
    let _ = cell.set(retained);
}

#[cfg(test)]
mod retention_tests;

pub(in crate::db::executor::prepared_execution_plan) fn build_prepared_execution_plan_core_with_schema_fingerprint(
    authority: &EntityAuthority,
    mut plan: AccessPlannedQuery,
    schema_fingerprint: CommitSchemaFingerprint,
    budget: &dyn ConstructionBudget,
) -> Result<PreparedExecutionPlanCore, InternalError> {
    // Metadata construction belongs to the planner's preparation boundary.
    // Reject incomplete handoffs before lowering or retaining executor state.
    if !plan.has_static_execution_planning_contract() {
        return Err(InternalError::query_executor_invariant());
    }
    let continuation_identity = AcceptedContinuationIdentity::new(
        schema_fingerprint,
        authority.accepted_schema_authority(),
    );

    // Phase 1: lower access-derived execution specs once and retain invariant
    // state. Projection shapes, grouped residents, and retained-slot layouts are
    // lazy residents because each execution surface consumes only one of those
    // families.
    bind_access_with_schema_info(
        authority.entity_tag(),
        &mut plan.access,
        authority.accepted_schema_info(),
        budget,
    )
    .map_err(LoweredAccessError::into_internal_error)?;

    build_prepared_execution_plan_core_from_bound_plan(
        authority,
        plan,
        continuation_identity,
        budget,
    )
}

// Rebuild prepared metadata from one already-finalized logical plan plus
// already-lowered access specs. Logical rewrites that preserve the access plan
// can reuse the lowered specs while refreshing continuation metadata locally.
pub(in crate::db::executor::prepared_execution_plan) fn build_prepared_execution_plan_core_from_bound_plan(
    authority: &EntityAuthority,
    plan: AccessPlannedQuery,
    continuation_identity: AcceptedContinuationIdentity,

    budget: &dyn ConstructionBudget,
) -> Result<PreparedExecutionPlanCore, InternalError> {
    build_prepared_execution_plan_core_from_shared_bound_plan(
        authority,
        Rc::new(plan),
        continuation_identity,
        budget,
    )
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(PreparedExecutionPlanCore {
Self{residents} => [residents],
});
// The accounting handle's control block is charged by the owning cache entry.
crate::retained::retained_fields!(PreparedExecutionPlanResidents {
    Self {
        cache_retention: _, plan, execution_shape_fingerprint_prefix, continuation_identity,
        prepared_projection_contract, projection_covering_read_execution_plan,
        hybrid_covering_read_plan, prepared_grouped_runtime_residents,
        aggregate_execution_preparation, scalar_execution_preparation,
        initial_scalar_route_plan,
        cursorless_retained_slot_layout,
        continuation
    } => [plan, execution_shape_fingerprint_prefix, continuation_identity,
        prepared_projection_contract, projection_covering_read_execution_plan,
        hybrid_covering_read_plan, prepared_grouped_runtime_residents,
        aggregate_execution_preparation, scalar_execution_preparation,
        initial_scalar_route_plan,
        cursorless_retained_slot_layout,
        continuation],
});
crate::retained::retained_fields!(PreparedGroupedRuntimeResidents {
Self{execution_preparation,grouped_slot_layout} => [execution_preparation,grouped_slot_layout],
});

// Rebuild prepared metadata from one shared logical plan plus already-lowered
// access specs. This avoids cloning large cached plans when an aggregate path
// falls back into scalar materialization with the same access contract.
pub(in crate::db::executor::prepared_execution_plan) fn build_prepared_execution_plan_core_from_shared_bound_plan(
    authority: &EntityAuthority,
    plan: Rc<AccessPlannedQuery>,
    continuation_identity: AcceptedContinuationIdentity,

    budget: &dyn ConstructionBudget,
) -> Result<PreparedExecutionPlanCore, InternalError> {
    // Recompute continuation after the logical-shape rewrite so grouped cursor
    // signatures and boundary arity reflect the grouped plan, not the scalar
    // aggregate source plan.
    let continuation = plan.planned_continuation_contract_with_accepted_identity(
        authority.entity_path(),
        Some(continuation_identity),
        budget,
    )?;
    let execution_shape_fingerprint_prefix = read_shape_fingerprint_prefix(authority, &plan)?;

    Ok(PreparedExecutionPlanCore::new(
        plan,
        execution_shape_fingerprint_prefix,
        continuation_identity,
        continuation,
    ))
}
