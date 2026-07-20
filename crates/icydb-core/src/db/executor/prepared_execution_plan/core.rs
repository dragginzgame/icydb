use super::contracts::{
    AcceptedContinuationIdentity, AccessPlannedQuery, CoveringHybridReadExecutionPlan,
    CoveringReadExecutionPlan, ExecutionOrdering, OrderSpec, PlannedContinuationContract,
    QueryMode,
};
use crate::{
    db::{
        access::{
            LoweredAccessError, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            lower_access_with_schema_info,
        },
        commit::CommitSchemaFingerprint,
        cursor::{ContinuationSignature, CursorPlanError, ValidatedCursor, ValidatedGroupedCursor},
        executor::{
            EntityAuthority, ExecutionPreparation, ExecutionRoutePlan, ExecutorPlanError,
            GroupedPaginationWindow, ScalarContinuationContext,
            pipeline::{
                contracts::{CursorEmissionMode, ProjectionMaterializationMode},
                runtime::{
                    compile_grouped_row_slot_layout_from_inputs,
                    compile_retained_slot_layout_for_mode,
                },
            },
            planning::preparation::slot_map_for_model_plan,
            planning::route::{RoutePlanRequest, build_execution_route_plan},
            projection::{PreparedProjectionContract, prepare_projection_contract_from_plan},
            terminal::RetainedSlotLayout,
            traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
    },
    error::InternalError,
};
use std::{
    rc::Rc,
    sync::{Arc, OnceLock},
};

#[cfg(feature = "sql")]
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
    PrimaryKey,
    Ordered,
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
    pub(in crate::db::executor::prepared_execution_plan) plan: Arc<AccessPlannedQuery>,
    pub(in crate::db::executor::prepared_execution_plan) continuation_identity:
        Option<AcceptedContinuationIdentity>,
    pub(in crate::db::executor::prepared_execution_plan) prepared_projection_contract:
        OnceLock<Option<Rc<PreparedProjectionContract>>>,
    pub(in crate::db::executor::prepared_execution_plan) projection_covering_read_execution_plan:
        OnceLock<Option<Rc<CoveringReadExecutionPlan>>>,
    pub(in crate::db::executor::prepared_execution_plan) hybrid_covering_read_plan:
        OnceLock<Option<Rc<CoveringHybridReadExecutionPlan>>>,
    pub(in crate::db::executor::prepared_execution_plan) prepared_grouped_runtime_residents:
        OnceLock<Option<Rc<PreparedGroupedRuntimeResidents>>>,
    pub(in crate::db::executor::prepared_execution_plan) aggregate_execution_preparation:
        OnceLock<ExecutionPreparation>,
    pub(in crate::db::executor::prepared_execution_plan) scalar_execution_preparation:
        OnceLock<ExecutionPreparation>,
    pub(in crate::db::executor::prepared_execution_plan) initial_scalar_route_plan:
        OnceLock<ExecutionRoutePlan>,
    pub(in crate::db::executor::prepared_execution_plan) shared_validation_emit_retained_slot_layout:
        OnceLock<Option<RetainedSlotLayout>>,
    pub(in crate::db::executor::prepared_execution_plan) retain_slot_rows_suppress_retained_slot_layout:
        OnceLock<Option<RetainedSlotLayout>>,
    pub(in crate::db::executor::prepared_execution_plan) none_suppress_retained_slot_layout:
        OnceLock<Option<RetainedSlotLayout>>,
    pub(in crate::db::executor::prepared_execution_plan) continuation:
        Option<PlannedContinuationContract>,
    pub(in crate::db::executor::prepared_execution_plan) index_prefix_specs:
        Arc<[LoweredIndexPrefixSpec]>,
    pub(in crate::db::executor::prepared_execution_plan) index_range_specs:
        Arc<[LoweredIndexRangeSpec]>,
}

impl std::fmt::Debug for PreparedExecutionPlanResidents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparedExecutionPlanResidents(..)")
    }
}

impl Clone for PreparedExecutionPlanResidents {
    fn clone(&self) -> Self {
        Self {
            plan: Arc::clone(&self.plan),
            continuation_identity: self.continuation_identity,
            prepared_projection_contract: clone_once_lock(&self.prepared_projection_contract),
            projection_covering_read_execution_plan: clone_once_lock(
                &self.projection_covering_read_execution_plan,
            ),
            hybrid_covering_read_plan: clone_once_lock(&self.hybrid_covering_read_plan),
            prepared_grouped_runtime_residents: clone_once_lock(
                &self.prepared_grouped_runtime_residents,
            ),
            aggregate_execution_preparation: clone_once_lock(&self.aggregate_execution_preparation),
            scalar_execution_preparation: clone_once_lock(&self.scalar_execution_preparation),
            initial_scalar_route_plan: clone_once_lock(&self.initial_scalar_route_plan),
            shared_validation_emit_retained_slot_layout: clone_once_lock(
                &self.shared_validation_emit_retained_slot_layout,
            ),
            retain_slot_rows_suppress_retained_slot_layout: clone_once_lock(
                &self.retain_slot_rows_suppress_retained_slot_layout,
            ),
            none_suppress_retained_slot_layout: clone_once_lock(
                &self.none_suppress_retained_slot_layout,
            ),
            continuation: self.continuation.clone(),
            index_prefix_specs: Arc::clone(&self.index_prefix_specs),
            index_range_specs: Arc::clone(&self.index_range_specs),
        }
    }
}

// Clone initialized lazy residents when the prepared core has to be cloned out
// of an `Arc`; uninitialized residents intentionally stay lazy.
fn clone_once_lock<T: Clone>(source: &OnceLock<T>) -> OnceLock<T> {
    let cloned = OnceLock::new();
    if let Some(value) = source.get() {
        let _ = cloned.set(value.clone());
    }

    cloned
}

///
/// PreparedExecutionPlanCore
///
/// Generic-free prepared execution-plan payload shared by typed
/// `PreparedExecutionPlan<E>` wrappers. This keeps cursor, ordering, and
/// lowered structural plan state monomorphic while typed access and
/// model-driven behavior remain at the outer executor boundary.
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
/// Grouped load wrappers clone this resident as a unit so execution preparation
/// and retained-slot layout cannot drift across separate cache lookups.
///

#[derive(Clone)]
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
    pub(in crate::db::executor) const fn new(
        execution_preparation: ExecutionPreparation,
        grouped_slot_layout: RetainedSlotLayout,
    ) -> Self {
        Self {
            execution_preparation,
            grouped_slot_layout,
        }
    }

    /// Consume the grouped resident bundle at the grouped runtime boundary.
    pub(in crate::db::executor) fn into_parts(self) -> (ExecutionPreparation, RetainedSlotLayout) {
        (self.execution_preparation, self.grouped_slot_layout)
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

    #[cfg(feature = "sql")]
    pub(in crate::db::executor) fn continuation_signature_for_runtime(
        &self,
    ) -> Result<ContinuationSignature, InternalError> {
        self.core.continuation_signature_for_runtime()
    }

    pub(in crate::db::executor) fn index_prefix_specs(&self) -> &[LoweredIndexPrefixSpec] {
        self.core.residents.index_prefix_specs.as_ref()
    }

    pub(in crate::db::executor) fn index_range_specs(&self) -> &[LoweredIndexRangeSpec] {
        self.core.residents.index_range_specs.as_ref()
    }

    pub(in crate::db::executor) fn get_or_init_initial_scalar_route_plan(
        &self,
        authority: EntityAuthority,
    ) -> Result<ExecutionRoutePlan, InternalError> {
        self.core.get_or_init_initial_scalar_route_plan(authority)
    }

    #[cfg(feature = "sql")]
    pub(in crate::db::executor) fn get_or_init_scalar_layout(
        &self,
        authority: EntityAuthority,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
    ) -> Result<Option<RetainedSlotLayout>, InternalError> {
        self.core
            .get_or_init_scalar_layout(authority, projection_materialization, cursor_emission)
    }
}

impl PreparedExecutionPlanCore {
    #[must_use]
    fn new(
        plan: Arc<AccessPlannedQuery>,
        continuation_identity: Option<AcceptedContinuationIdentity>,
        continuation: Option<PlannedContinuationContract>,
        index_prefix_specs: Arc<[LoweredIndexPrefixSpec]>,
        index_range_specs: Arc<[LoweredIndexRangeSpec]>,
    ) -> Self {
        Self {
            residents: Rc::new(PreparedExecutionPlanResidents {
                plan,
                continuation_identity,
                prepared_projection_contract: OnceLock::new(),
                projection_covering_read_execution_plan: OnceLock::new(),
                hybrid_covering_read_plan: OnceLock::new(),
                prepared_grouped_runtime_residents: OnceLock::new(),
                aggregate_execution_preparation: OnceLock::new(),
                scalar_execution_preparation: OnceLock::new(),
                initial_scalar_route_plan: OnceLock::new(),
                shared_validation_emit_retained_slot_layout: OnceLock::new(),
                retain_slot_rows_suppress_retained_slot_layout: OnceLock::new(),
                none_suppress_retained_slot_layout: OnceLock::new(),
                continuation,
                index_prefix_specs,
                index_range_specs,
            }),
        }
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn plan(&self) -> &AccessPlannedQuery {
        &self.residents.plan
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn plan_hash_hex(&self) -> String {
        self.plan().plan_hash_hex()
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_projection_shape(
        &self,
        authority: EntityAuthority,
    ) -> Result<Option<Rc<PreparedProjectionContract>>, InternalError> {
        // Projection adapters consume this shape directly; scalar validation
        // callers request it explicitly before execution.
        if let Some(cached) = self.residents.prepared_projection_contract.get() {
            return Ok(cached.clone());
        }

        let prepared = if self.residents.plan.scalar_projection_plan().is_some() {
            Some(Rc::new(prepare_projection_contract_from_plan(
                authority.row_layout_ref()?,
                &self.residents.plan,
            )?))
        } else {
            None
        };
        let _ = self
            .residents
            .prepared_projection_contract
            .set(prepared.clone());

        Ok(prepared)
    }

    #[cfg(feature = "sql")]
    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_projection_covering_read_execution_plan(
        &self,
        authority: EntityAuthority,
    ) -> Option<Rc<CoveringReadExecutionPlan>> {
        self.residents
            .projection_covering_read_execution_plan
            .get_or_init(|| {
                let strict_predicate_compatible =
                    covering_strict_predicate_compatible_for_plan(&self.residents.plan);

                authority
                    .covering_read_execution_plan(&self.residents.plan, strict_predicate_compatible)
                    .map(Rc::new)
            })
            .clone()
    }

    #[cfg(feature = "sql")]
    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_hybrid_covering_read_plan(
        &self,
        authority: EntityAuthority,
    ) -> Option<Rc<CoveringHybridReadExecutionPlan>> {
        self.residents
            .hybrid_covering_read_plan
            .get_or_init(|| {
                let strict_predicate_compatible =
                    covering_strict_predicate_compatible_for_plan(&self.residents.plan);

                authority
                    .covering_hybrid_projection_plan(
                        &self.residents.plan,
                        strict_predicate_compatible,
                    )
                    .map(Rc::new)
            })
            .clone()
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_grouped_runtime_residents(
        &self,
        authority: EntityAuthority,
    ) -> Result<Option<Rc<PreparedGroupedRuntimeResidents>>, InternalError> {
        // Grouped execution needs both the runtime preparation and slot layout
        // together, so cache them behind one grouped-resident initializer.
        if let Some(cached) = self.residents.prepared_grouped_runtime_residents.get() {
            return Ok(cached.clone());
        }

        let prepared = if let Some(grouped_plan) = self.residents.plan.grouped_plan() {
            if let Some(grouped_distinct_execution_strategy) =
                self.residents.plan.grouped_distinct_execution_strategy()
            {
                let execution_preparation = ExecutionPreparation::from_runtime_plan(
                    &self.residents.plan,
                    self.residents.plan.slot_map().map(<[usize]>::to_vec),
                );
                let grouped_slot_layout = compile_grouped_row_slot_layout_from_inputs(
                    authority.row_layout()?,
                    grouped_plan.group.group_fields.as_slice(),
                    self.residents
                        .plan
                        .grouped_aggregate_execution_specs()
                        .unwrap_or(&[]),
                    grouped_distinct_execution_strategy,
                    execution_preparation.effective_runtime_filter_program(),
                );

                Some(Rc::new(PreparedGroupedRuntimeResidents::new(
                    execution_preparation,
                    grouped_slot_layout,
                )))
            } else {
                None
            }
        } else {
            None
        };
        let _ = self
            .residents
            .prepared_grouped_runtime_residents
            .set(prepared.clone());

        Ok(prepared)
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_scalar_execution_preparation(
        &self,
    ) -> ExecutionPreparation {
        // Scalar execution preparation is fully plan-deterministic: it depends
        // on the effective runtime predicate and slot map, but not on store
        // handles, cursor state, route retry policy, diagnostics, or
        // materialization mode.
        self.residents
            .scalar_execution_preparation
            .get_or_init(|| {
                ExecutionPreparation::from_runtime_plan(
                    &self.residents.plan,
                    slot_map_for_model_plan(&self.residents.plan),
                )
            })
            .clone()
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_initial_scalar_route_plan(
        &self,
        authority: EntityAuthority,
    ) -> Result<ExecutionRoutePlan, InternalError> {
        if let Some(route_plan) = self.residents.initial_scalar_route_plan.get() {
            return Ok(route_plan.clone());
        }

        let continuation = ScalarContinuationContext::initial();
        let route_plan = build_execution_route_plan(
            &self.residents.plan,
            RoutePlanRequest::Load {
                continuation: &continuation,
                probe_fetch_hint: None,
                authority: Some(authority),
                load_terminal_fast_path: None,
            },
        )?;
        let _ = self
            .residents
            .initial_scalar_route_plan
            .set(route_plan.clone());

        Ok(route_plan)
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_aggregate_execution_preparation(
        &self,
    ) -> ExecutionPreparation {
        // Aggregate execution still consumes the full preparation contract
        // because route planning needs the capability snapshot and strict
        // predicate program. The inputs are deterministic prepared-plan
        // residents, so cache the bundle beside the scalar/runtime variants.
        self.residents
            .aggregate_execution_preparation
            .get_or_init(|| {
                ExecutionPreparation::from_plan(
                    &self.residents.plan,
                    slot_map_for_model_plan(&self.residents.plan),
                )
            })
            .clone()
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_scalar_layout(
        &self,
        authority: EntityAuthority,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
    ) -> Result<Option<RetainedSlotLayout>, InternalError> {
        // Each scalar entrypoint consumes at most one retained-slot layout
        // family, so compile only the selected `(projection, cursor)` shape.
        let layout_cache = match (projection_materialization, cursor_emission) {
            (ProjectionMaterializationMode::SharedValidation, CursorEmissionMode::Emit) => {
                &self.residents.shared_validation_emit_retained_slot_layout
            }
            (ProjectionMaterializationMode::RetainSlotRows, CursorEmissionMode::Suppress) => {
                &self
                    .residents
                    .retain_slot_rows_suppress_retained_slot_layout
            }
            (ProjectionMaterializationMode::None, CursorEmissionMode::Suppress) => {
                &self.residents.none_suppress_retained_slot_layout
            }
            _ => return Ok(None),
        };

        if let Some(cached) = layout_cache.get() {
            return Ok(cached.clone());
        }

        let layout = compile_retained_slot_layout_for_mode(
            &authority,
            &self.residents.plan,
            projection_materialization,
            cursor_emission,
        )?;
        let _ = layout_cache.set(layout.clone());

        Ok(layout)
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn mode(&self) -> QueryMode {
        self.residents.plan.scalar_plan().mode
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn is_grouped(&self) -> bool {
        match self.residents.continuation {
            Some(ref contract) => contract.is_grouped(),
            None => false,
        }
    }

    pub(in crate::db::executor::prepared_execution_plan) fn execution_ordering(
        &self,
    ) -> Result<ExecutionOrdering, InternalError> {
        let contract = self.continuation_contract()?;
        Ok(contract.order_contract().ordering().clone())
    }

    pub(in crate::db::executor::prepared_execution_plan) fn execution_family(
        &self,
    ) -> Result<ExecutionFamily, InternalError> {
        let ordering = self.execution_ordering()?;

        Ok(match ordering {
            ExecutionOrdering::PrimaryKey => ExecutionFamily::PrimaryKey,
            ExecutionOrdering::Explicit(_) => ExecutionFamily::Ordered,
            ExecutionOrdering::Grouped(_) => ExecutionFamily::Grouped,
        })
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(&self.residents.plan)
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn order_spec(
        &self,
    ) -> Option<&OrderSpec> {
        self.residents.plan.scalar_plan().order.as_ref()
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn has_predicate(&self) -> bool {
        self.residents.plan.has_any_residual_filter()
    }

    #[cfg(test)]
    pub(in crate::db::executor::prepared_execution_plan) fn index_prefix_specs(
        &self,
    ) -> &[LoweredIndexPrefixSpec] {
        self.residents.index_prefix_specs.as_ref()
    }

    #[cfg(test)]
    pub(in crate::db::executor::prepared_execution_plan) fn index_range_specs(
        &self,
    ) -> &[LoweredIndexRangeSpec] {
        self.residents.index_range_specs.as_ref()
    }

    // Recover the prepared-plan resident payload by move when this core is
    // uniquely owned, and fall back to cloning only when another wrapper still
    // holds the resident Arc.
    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn into_residents(
        self,
    ) -> PreparedExecutionPlanResidents {
        Rc::try_unwrap(self.residents).unwrap_or_else(|residents| residents.as_ref().clone())
    }

    pub(in crate::db::executor::prepared_execution_plan) fn prepare_cursor(
        &self,
        authority: EntityAuthority,
        cursor: Option<&[u8]>,
    ) -> Result<ValidatedCursor, ExecutorPlanError> {
        let Some(contract) = self.residents.continuation.as_ref() else {
            return Err(ExecutorPlanError::continuation_cursor_requires_load_plan());
        };

        authority
            .prepare_scalar_cursor(contract, cursor)
            .map_err(ExecutorPlanError::from)
    }

    pub(in crate::db::executor::prepared_execution_plan) fn revalidate_cursor(
        &self,
        authority: EntityAuthority,
        cursor: ValidatedCursor,
    ) -> Result<ValidatedCursor, InternalError> {
        let Some(contract) = self.residents.continuation.as_ref() else {
            return Err(
                ExecutorPlanError::continuation_cursor_requires_load_plan().into_internal_error()
            );
        };

        authority
            .revalidate_scalar_cursor(contract, cursor)
            .map_err(CursorPlanError::into_internal_error)
    }

    pub(in crate::db::executor::prepared_execution_plan) fn revalidate_grouped_cursor(
        &self,
        cursor: ValidatedGroupedCursor,
    ) -> Result<ValidatedGroupedCursor, InternalError> {
        let Some(contract) = self.residents.continuation.as_ref() else {
            return Err(
                ExecutorPlanError::grouped_cursor_revalidation_requires_grouped_plan()
                    .into_internal_error(),
            );
        };

        contract
            .revalidate_grouped_cursor(cursor)
            .map_err(CursorPlanError::into_internal_error)
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
            return Err(
                ExecutorPlanError::grouped_cursor_boundary_arity_requires_grouped_plan()
                    .into_internal_error(),
            );
        }

        Ok(contract.boundary_arity())
    }

    pub(in crate::db::executor::prepared_execution_plan) fn grouped_pagination_window(
        &self,
        cursor: &ValidatedGroupedCursor,
    ) -> Result<GroupedPaginationWindow, InternalError> {
        let contract = self.continuation_contract()?;
        let window = contract
            .project_grouped_paging_window(cursor)
            .map_err(CursorPlanError::into_internal_error)?;
        let (
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        ) = window.into_pagination_window_fields();

        Ok(GroupedPaginationWindow::new(
            limit,
            initial_offset_for_page,
            selection_bound,
            resume_initial_offset,
            resume_boundary,
        ))
    }

    // Borrow immutable continuation contract for load-mode plans.
    pub(in crate::db::executor::prepared_execution_plan) fn continuation_contract(
        &self,
    ) -> Result<&PlannedContinuationContract, InternalError> {
        self.residents.continuation.as_ref().ok_or_else(|| {
            ExecutorPlanError::continuation_contract_requires_load_plan().into_internal_error()
        })
    }
}

// Build one canonical test-only lowered prepared execution-plan core from
// resolved authority plus one generated logical plan.
#[cfg(test)]
pub(in crate::db::executor::prepared_execution_plan) fn build_prepared_execution_plan_core(
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
) -> PreparedExecutionPlanCore {
    build_prepared_execution_plan_core_with_schema_fingerprint(authority, plan, None)
        .expect("test prepared execution plan core should build")
}

pub(in crate::db::executor::prepared_execution_plan) fn build_prepared_execution_plan_core_with_schema_fingerprint(
    authority: EntityAuthority,
    mut plan: AccessPlannedQuery,
    schema_fingerprint: Option<CommitSchemaFingerprint>,
) -> Result<PreparedExecutionPlanCore, InternalError> {
    authority.finalize_static_execution_planning_contract(&mut plan)?;
    let continuation_identity = schema_fingerprint
        .map(|entity_schema_fingerprint| {
            authority.accepted_schema_authority().map(|accepted| {
                AcceptedContinuationIdentity::new(entity_schema_fingerprint, accepted)
            })
        })
        .transpose()?;

    // Phase 1: lower access-derived execution specs once and retain invariant
    // state. Projection shapes, grouped residents, and retained-slot layouts are
    // lazy residents because each execution surface consumes only one of those
    // families.
    let lowered_access = lower_access_with_schema_info(
        authority.entity_tag(),
        &plan.access,
        authority
            .accepted_schema_info()
            .ok_or_else(InternalError::query_executor_invariant)?,
    )
    .map_err(LoweredAccessError::into_internal_error)?;
    let (_, index_prefix_specs, index_range_specs) =
        lowered_access.into_executable_and_index_specs();

    Ok(build_prepared_execution_plan_core_with_lowered_access(
        authority,
        plan,
        continuation_identity,
        Arc::from(index_prefix_specs),
        Arc::from(index_range_specs),
    ))
}

// Rebuild prepared metadata from one already-finalized logical plan plus
// already-lowered access specs. Logical rewrites that preserve the access plan
// can reuse the lowered specs while refreshing continuation metadata locally.
pub(in crate::db::executor::prepared_execution_plan) fn build_prepared_execution_plan_core_with_lowered_access(
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
    continuation_identity: Option<AcceptedContinuationIdentity>,
    index_prefix_specs: Arc<[LoweredIndexPrefixSpec]>,
    index_range_specs: Arc<[LoweredIndexRangeSpec]>,
) -> PreparedExecutionPlanCore {
    build_prepared_execution_plan_core_with_shared_lowered_access(
        authority,
        Arc::new(plan),
        continuation_identity,
        index_prefix_specs,
        index_range_specs,
    )
}

// Rebuild prepared metadata from one shared logical plan plus already-lowered
// access specs. This avoids cloning large cached plans when an aggregate path
// falls back into scalar materialization with the same access contract.
pub(in crate::db::executor::prepared_execution_plan) fn build_prepared_execution_plan_core_with_shared_lowered_access(
    authority: EntityAuthority,
    plan: Arc<AccessPlannedQuery>,
    continuation_identity: Option<AcceptedContinuationIdentity>,
    index_prefix_specs: Arc<[LoweredIndexPrefixSpec]>,
    index_range_specs: Arc<[LoweredIndexRangeSpec]>,
) -> PreparedExecutionPlanCore {
    // Recompute continuation after the logical-shape rewrite so grouped cursor
    // signatures and boundary arity reflect the grouped plan, not the scalar
    // aggregate source plan.
    let continuation = plan.planned_continuation_contract_with_accepted_identity(
        authority.entity_path(),
        continuation_identity,
    );

    PreparedExecutionPlanCore::new(
        plan,
        continuation_identity,
        continuation,
        index_prefix_specs,
        index_range_specs,
    )
}
