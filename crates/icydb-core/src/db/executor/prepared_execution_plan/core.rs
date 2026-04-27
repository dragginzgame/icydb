use crate::{
    db::{
        cursor::{ContinuationSignature, CursorPlanError, GroupedPlannedCursor, PlannedCursor},
        executor::{
            EntityAuthority, ExecutionPreparation, ExecutorPlanError, GroupedPaginationWindow,
            LoweredAccessError, LoweredIndexPrefixSpec, LoweredIndexRangeSpec, lower_access,
            pipeline::{
                contracts::{CursorEmissionMode, ProjectionMaterializationMode},
                runtime::{
                    compile_grouped_row_slot_layout_from_parts,
                    compile_retained_slot_layout_for_mode,
                },
            },
            planning::preparation::slot_map_for_model_plan,
            projection::{PreparedProjectionShape, prepare_projection_shape_from_plan},
            terminal::RetainedSlotLayout,
            traversal::row_read_consistency_for_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, ExecutionOrdering, OrderSpec, PlannedContinuationContract,
            QueryMode,
        },
    },
    error::InternalError,
};
use std::sync::{Arc, OnceLock};

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
/// PreparedExecutionPlanCoreShared
///
/// Shared prepared-plan residents stored behind `PreparedExecutionPlanCore`.
/// The struct keeps immutable plan state, lowered access metadata, and lazy
/// runtime residents together so cloned prepared plans share expensive
/// preparation products without duplicating logical plans.
///

#[derive(Debug)]
pub(in crate::db::executor::prepared_execution_plan) struct PreparedExecutionPlanCoreShared {
    pub(in crate::db::executor::prepared_execution_plan) plan: Arc<AccessPlannedQuery>,
    pub(in crate::db::executor::prepared_execution_plan) prepared_projection_shape:
        OnceLock<Option<Arc<PreparedProjectionShape>>>,
    pub(in crate::db::executor::prepared_execution_plan) prepared_grouped_runtime_residents:
        OnceLock<Option<Arc<PreparedGroupedRuntimeResidents>>>,
    pub(in crate::db::executor::prepared_execution_plan) aggregate_execution_preparation:
        OnceLock<ExecutionPreparation>,
    pub(in crate::db::executor::prepared_execution_plan) scalar_execution_preparation:
        OnceLock<ExecutionPreparation>,
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
    pub(in crate::db::executor::prepared_execution_plan) index_prefix_spec_invalid: bool,
    pub(in crate::db::executor::prepared_execution_plan) index_range_specs:
        Arc<[LoweredIndexRangeSpec]>,
    pub(in crate::db::executor::prepared_execution_plan) index_range_spec_invalid: bool,
}

impl Clone for PreparedExecutionPlanCoreShared {
    fn clone(&self) -> Self {
        Self {
            plan: Arc::clone(&self.plan),
            prepared_projection_shape: clone_once_lock(&self.prepared_projection_shape),
            prepared_grouped_runtime_residents: clone_once_lock(
                &self.prepared_grouped_runtime_residents,
            ),
            aggregate_execution_preparation: clone_once_lock(&self.aggregate_execution_preparation),
            scalar_execution_preparation: clone_once_lock(&self.scalar_execution_preparation),
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
            index_prefix_spec_invalid: self.index_prefix_spec_invalid,
            index_range_specs: Arc::clone(&self.index_range_specs),
            index_range_spec_invalid: self.index_range_spec_invalid,
        }
    }
}

// Clone initialized lazy residents when the shared plan core has to be cloned
// out of an `Arc`; uninitialized residents intentionally stay lazy.
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
    pub(in crate::db::executor::prepared_execution_plan) shared:
        Arc<PreparedExecutionPlanCoreShared>,
}

///
/// PreparedGroupedRuntimeResidents
///
/// Lazily cached grouped runtime preparation pair for one prepared plan.
/// Grouped load wrappers clone this resident as a unit so execution preparation
/// and retained-slot layout cannot drift across separate cache lookups.
///

#[derive(Clone)]
pub(in crate::db::executor::prepared_execution_plan) struct PreparedGroupedRuntimeResidents {
    execution_preparation: ExecutionPreparation,
    grouped_slot_layout: RetainedSlotLayout,
}

impl std::fmt::Debug for PreparedGroupedRuntimeResidents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparedGroupedRuntimeResidents(..)")
    }
}

impl PreparedGroupedRuntimeResidents {
    const fn new(
        execution_preparation: ExecutionPreparation,
        grouped_slot_layout: RetainedSlotLayout,
    ) -> Self {
        Self {
            execution_preparation,
            grouped_slot_layout,
        }
    }

    pub(in crate::db::executor::prepared_execution_plan) fn execution_preparation(
        &self,
    ) -> ExecutionPreparation {
        self.execution_preparation.clone()
    }

    pub(in crate::db::executor::prepared_execution_plan) fn grouped_slot_layout(
        &self,
    ) -> RetainedSlotLayout {
        self.grouped_slot_layout.clone()
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

    pub(in crate::db::executor) fn continuation_signature_for_runtime(
        &self,
    ) -> Result<ContinuationSignature, InternalError> {
        self.core.continuation_signature_for_runtime()
    }

    pub(in crate::db::executor) fn index_prefix_specs(
        &self,
    ) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        if self.core.shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }

        Ok(self.core.shared.index_prefix_specs.as_ref())
    }

    pub(in crate::db::executor) fn index_range_specs(
        &self,
    ) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        if self.core.shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(self.core.shared.index_range_specs.as_ref())
    }
}

impl PreparedExecutionPlanCore {
    #[must_use]
    fn new(
        plan: Arc<AccessPlannedQuery>,
        continuation: Option<PlannedContinuationContract>,
        index_prefix_specs: Arc<[LoweredIndexPrefixSpec]>,
        index_prefix_spec_invalid: bool,
        index_range_specs: Arc<[LoweredIndexRangeSpec]>,
        index_range_spec_invalid: bool,
    ) -> Self {
        Self {
            shared: Arc::new(PreparedExecutionPlanCoreShared {
                plan,
                prepared_projection_shape: OnceLock::new(),
                prepared_grouped_runtime_residents: OnceLock::new(),
                aggregate_execution_preparation: OnceLock::new(),
                scalar_execution_preparation: OnceLock::new(),
                shared_validation_emit_retained_slot_layout: OnceLock::new(),
                retain_slot_rows_suppress_retained_slot_layout: OnceLock::new(),
                none_suppress_retained_slot_layout: OnceLock::new(),
                continuation,
                index_prefix_specs,
                index_prefix_spec_invalid,
                index_range_specs,
                index_range_spec_invalid,
            }),
        }
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn plan(&self) -> &AccessPlannedQuery {
        &self.shared.plan
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_projection_shape(
        &self,
        authority: EntityAuthority,
    ) -> Option<Arc<PreparedProjectionShape>> {
        // Projection adapters consume this shape directly; scalar validation
        // callers request it explicitly before execution.
        self.shared
            .prepared_projection_shape
            .get_or_init(|| {
                self.shared.plan.scalar_projection_plan().map(|_| {
                    Arc::new(prepare_projection_shape_from_plan(
                        authority.model(),
                        &self.shared.plan,
                    ))
                })
            })
            .clone()
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_grouped_runtime_residents(
        &self,
        authority: EntityAuthority,
    ) -> Option<Arc<PreparedGroupedRuntimeResidents>> {
        // Grouped execution needs both the runtime preparation and slot layout
        // together, so cache them behind one grouped-resident initializer.
        self.shared
            .prepared_grouped_runtime_residents
            .get_or_init(|| {
                self.shared.plan.grouped_plan().and_then(|grouped_plan| {
                    let grouped_distinct_execution_strategy =
                        self.shared.plan.grouped_distinct_execution_strategy()?;
                    let execution_preparation = ExecutionPreparation::from_runtime_plan(
                        &self.shared.plan,
                        self.shared.plan.slot_map().map(<[usize]>::to_vec),
                    );
                    let grouped_slot_layout = compile_grouped_row_slot_layout_from_parts(
                        authority.row_layout(),
                        grouped_plan.group.group_fields.as_slice(),
                        self.shared
                            .plan
                            .grouped_aggregate_execution_specs()
                            .unwrap_or(&[]),
                        grouped_distinct_execution_strategy,
                        execution_preparation.effective_runtime_filter_program(),
                    );

                    Some(Arc::new(PreparedGroupedRuntimeResidents::new(
                        execution_preparation,
                        grouped_slot_layout,
                    )))
                })
            })
            .clone()
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_scalar_execution_preparation(
        &self,
    ) -> ExecutionPreparation {
        // Scalar execution preparation is fully plan-deterministic: it depends
        // on the effective runtime predicate and slot map, but not on store
        // handles, cursor state, route retry policy, diagnostics, or
        // materialization mode.
        self.shared
            .scalar_execution_preparation
            .get_or_init(|| {
                ExecutionPreparation::from_runtime_plan(
                    &self.shared.plan,
                    slot_map_for_model_plan(&self.shared.plan),
                )
            })
            .clone()
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_aggregate_execution_preparation(
        &self,
    ) -> ExecutionPreparation {
        // Aggregate execution still consumes the full preparation contract
        // because route planning needs the capability snapshot and strict
        // predicate program. The inputs are deterministic prepared-plan
        // residents, so cache the bundle beside the scalar/runtime variants.
        self.shared
            .aggregate_execution_preparation
            .get_or_init(|| {
                ExecutionPreparation::from_plan(
                    &self.shared.plan,
                    slot_map_for_model_plan(&self.shared.plan),
                )
            })
            .clone()
    }

    pub(in crate::db::executor::prepared_execution_plan) fn get_or_init_scalar_layout(
        &self,
        authority: EntityAuthority,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
    ) -> Option<RetainedSlotLayout> {
        // Each scalar entrypoint consumes at most one retained-slot layout
        // family, so compile only the selected `(projection, cursor)` shape.
        let layout_cache = match (projection_materialization, cursor_emission) {
            (ProjectionMaterializationMode::SharedValidation, CursorEmissionMode::Emit) => {
                &self.shared.shared_validation_emit_retained_slot_layout
            }
            (ProjectionMaterializationMode::RetainSlotRows, CursorEmissionMode::Suppress) => {
                &self.shared.retain_slot_rows_suppress_retained_slot_layout
            }
            (ProjectionMaterializationMode::None, CursorEmissionMode::Suppress) => {
                &self.shared.none_suppress_retained_slot_layout
            }
            _ => return None,
        };

        layout_cache
            .get_or_init(|| {
                compile_retained_slot_layout_for_mode(
                    authority.model(),
                    &self.shared.plan,
                    projection_materialization,
                    cursor_emission,
                )
            })
            .clone()
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn mode(&self) -> QueryMode {
        self.shared.plan.scalar_plan().mode
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn is_grouped(&self) -> bool {
        match self.shared.continuation {
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
        row_read_consistency_for_plan(&self.shared.plan)
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn order_spec(
        &self,
    ) -> Option<&OrderSpec> {
        self.shared.plan.scalar_plan().order.as_ref()
    }

    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn has_predicate(&self) -> bool {
        self.shared.plan.has_residual_filter_expr()
            || self.shared.plan.has_residual_filter_predicate()
    }

    #[cfg(test)]
    pub(in crate::db::executor::prepared_execution_plan) fn index_prefix_specs(
        &self,
    ) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        if self.shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }

        Ok(self.shared.index_prefix_specs.as_ref())
    }

    #[cfg(test)]
    pub(in crate::db::executor::prepared_execution_plan) fn index_range_specs(
        &self,
    ) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        if self.shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(self.shared.index_range_specs.as_ref())
    }

    // Recover the shared prepared-plan payload by move when this core is
    // uniquely owned, and fall back to cloning only when another wrapper still
    // holds the shared Arc.
    #[must_use]
    pub(in crate::db::executor::prepared_execution_plan) fn into_shared(
        self,
    ) -> PreparedExecutionPlanCoreShared {
        Arc::try_unwrap(self.shared).unwrap_or_else(|shared| shared.as_ref().clone())
    }

    pub(in crate::db::executor::prepared_execution_plan) fn prepare_cursor(
        &self,
        authority: EntityAuthority,
        cursor: Option<&[u8]>,
    ) -> Result<PlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.shared.continuation.as_ref() else {
            return Err(ExecutorPlanError::continuation_cursor_requires_load_plan());
        };

        authority
            .prepare_scalar_cursor(contract, cursor)
            .map_err(ExecutorPlanError::from)
    }

    pub(in crate::db::executor::prepared_execution_plan) fn revalidate_cursor(
        &self,
        authority: EntityAuthority,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, InternalError> {
        let Some(contract) = self.shared.continuation.as_ref() else {
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
        cursor: GroupedPlannedCursor,
    ) -> Result<GroupedPlannedCursor, InternalError> {
        let Some(contract) = self.shared.continuation.as_ref() else {
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
        cursor: &GroupedPlannedCursor,
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
        ) = window.into_parts();

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
        self.shared.continuation.as_ref().ok_or_else(|| {
            ExecutorPlanError::continuation_contract_requires_load_plan().into_internal_error()
        })
    }
}

// Build one canonical lowered prepared execution-plan core from resolved
// authority plus one logical plan, regardless of whether the caller started
// from a typed `PreparedExecutionPlan<E>` shell or a structural follow-on
// rewrite.
pub(in crate::db::executor::prepared_execution_plan) fn build_prepared_execution_plan_core(
    authority: EntityAuthority,
    mut plan: AccessPlannedQuery,
) -> PreparedExecutionPlanCore {
    authority.finalize_static_planning_shape(&mut plan);

    // Phase 1: lower access-derived execution specs once and retain invariant
    // state. Projection shapes, grouped residents, and retained-slot layouts are
    // lazy residents because each execution surface consumes only one of those
    // families.
    let (
        index_prefix_specs,
        index_prefix_spec_invalid,
        index_range_specs,
        index_range_spec_invalid,
    ) = match lower_access(authority.entity_tag(), &plan.access) {
        Ok(lowered) => {
            let (_, index_prefix_specs, index_range_specs) = lowered.into_parts();

            (
                Arc::from(index_prefix_specs),
                false,
                Arc::from(index_range_specs),
                false,
            )
        }
        Err(LoweredAccessError::IndexPrefix(_)) => (
            Arc::from(Vec::<LoweredIndexPrefixSpec>::new()),
            true,
            Arc::from(Vec::<LoweredIndexRangeSpec>::new()),
            false,
        ),
        Err(LoweredAccessError::IndexRange(_)) => (
            Arc::from(Vec::<LoweredIndexPrefixSpec>::new()),
            false,
            Arc::from(Vec::<LoweredIndexRangeSpec>::new()),
            true,
        ),
    };

    build_prepared_execution_plan_core_with_lowered_access(
        authority,
        plan,
        index_prefix_specs,
        index_prefix_spec_invalid,
        index_range_specs,
        index_range_spec_invalid,
    )
}

// Rebuild prepared metadata from one already-finalized logical plan plus
// already-lowered access specs. Logical rewrites that preserve the access plan
// can reuse the lowered specs while refreshing continuation metadata locally.
pub(in crate::db::executor::prepared_execution_plan) fn build_prepared_execution_plan_core_with_lowered_access(
    authority: EntityAuthority,
    plan: AccessPlannedQuery,
    index_prefix_specs: Arc<[LoweredIndexPrefixSpec]>,
    index_prefix_spec_invalid: bool,
    index_range_specs: Arc<[LoweredIndexRangeSpec]>,
    index_range_spec_invalid: bool,
) -> PreparedExecutionPlanCore {
    build_prepared_execution_plan_core_with_shared_lowered_access(
        authority,
        Arc::new(plan),
        index_prefix_specs,
        index_prefix_spec_invalid,
        index_range_specs,
        index_range_spec_invalid,
    )
}

// Rebuild prepared metadata from one shared logical plan plus already-lowered
// access specs. This avoids cloning large cached plans when an aggregate path
// falls back into scalar materialization with the same access contract.
pub(in crate::db::executor::prepared_execution_plan) fn build_prepared_execution_plan_core_with_shared_lowered_access(
    authority: EntityAuthority,
    plan: Arc<AccessPlannedQuery>,
    index_prefix_specs: Arc<[LoweredIndexPrefixSpec]>,
    index_prefix_spec_invalid: bool,
    index_range_specs: Arc<[LoweredIndexRangeSpec]>,
    index_range_spec_invalid: bool,
) -> PreparedExecutionPlanCore {
    // Recompute continuation after the logical-shape rewrite so grouped cursor
    // signatures and boundary arity reflect the grouped plan, not the scalar
    // aggregate source plan.
    let continuation = plan.planned_continuation_contract(authority.entity_path());

    PreparedExecutionPlanCore::new(
        plan,
        continuation,
        index_prefix_specs,
        index_prefix_spec_invalid,
        index_range_specs,
        index_range_spec_invalid,
    )
}
