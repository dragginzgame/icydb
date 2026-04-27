//! Module: db::executor::prepared_execution_plan
//! Responsibility: bind validated access-planned queries to executor-ready contracts.
//! Does not own: logical plan semantics or route policy decisions.
//! Boundary: shared plan container for load/delete/aggregate runtime entrypoints.

#[cfg(test)]
use crate::db::executor::planning::route::LoadTerminalFastPathContract;
use crate::{
    db::{
        access::AccessPlan,
        cursor::{ContinuationSignature, CursorPlanError, GroupedPlannedCursor, PlannedCursor},
        executor::{
            EntityAuthority, ExecutionPreparation, ExecutorPlanError, GroupedPaginationWindow,
            LoweredAccessError, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            explain::assemble_load_execution_node_descriptor,
            lower_access,
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
        query::{
            explain::ExplainExecutionNodeDescriptor,
            plan::{
                AccessPlannedQuery, ExecutionOrdering, GroupSpec, OrderSpec,
                PlannedContinuationContract, QueryMode,
                constant_covering_projection_value_from_access, covering_index_projection_context,
            },
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::marker::PhantomData;
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
/// BytesByProjectionMode
///
/// Canonical `bytes_by(field)` projection mode classification used by execution
/// and explain surfaces.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum BytesByProjectionMode {
    Materialized,
    CoveringIndex,
    CoveringConstant,
}

/// Classify canonical `bytes_by(field)` execution mode from one neutral access context.
#[must_use]
pub(in crate::db::executor) fn classify_bytes_by_projection_mode(
    access: &AccessPlan<crate::value::Value>,
    order_spec: Option<&OrderSpec>,
    consistency: MissingRowPolicy,
    has_predicate: bool,
    target_field: &str,
    primary_key_name: &'static str,
) -> BytesByProjectionMode {
    if !matches!(consistency, MissingRowPolicy::Ignore) {
        return BytesByProjectionMode::Materialized;
    }

    if constant_covering_projection_value_from_access(access, target_field).is_some() {
        return BytesByProjectionMode::CoveringConstant;
    }

    if has_predicate {
        return BytesByProjectionMode::Materialized;
    }

    if covering_index_projection_context(access, order_spec, target_field, primary_key_name)
        .is_some()
    {
        return BytesByProjectionMode::CoveringIndex;
    }

    BytesByProjectionMode::Materialized
}

///
/// PreparedExecutionPlanCore
///
/// Generic-free prepared execution-plan payload shared by typed `PreparedExecutionPlan<E>`
/// wrappers. This keeps cursor, ordering, and lowered structural plan state
/// monomorphic while typed access and model-driven behavior remain at the
/// outer executor boundary.
///

#[derive(Debug)]
struct PreparedExecutionPlanCoreShared {
    plan: AccessPlannedQuery,
    prepared_projection_shape: OnceLock<Option<Arc<PreparedProjectionShape>>>,
    prepared_grouped_runtime_residents: OnceLock<Option<Arc<PreparedGroupedRuntimeResidents>>>,
    scalar_execution_preparation: OnceLock<ExecutionPreparation>,
    shared_validation_emit_retained_slot_layout: OnceLock<Option<RetainedSlotLayout>>,
    retain_slot_rows_suppress_retained_slot_layout: OnceLock<Option<RetainedSlotLayout>>,
    none_suppress_retained_slot_layout: OnceLock<Option<RetainedSlotLayout>>,
    continuation: Option<PlannedContinuationContract>,
    index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    index_prefix_spec_invalid: bool,
    index_range_specs: Vec<LoweredIndexRangeSpec>,
    index_range_spec_invalid: bool,
}

impl Clone for PreparedExecutionPlanCoreShared {
    fn clone(&self) -> Self {
        Self {
            plan: self.plan.clone(),
            prepared_projection_shape: clone_once_lock(&self.prepared_projection_shape),
            prepared_grouped_runtime_residents: clone_once_lock(
                &self.prepared_grouped_runtime_residents,
            ),
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
            index_prefix_specs: self.index_prefix_specs.clone(),
            index_prefix_spec_invalid: self.index_prefix_spec_invalid,
            index_range_specs: self.index_range_specs.clone(),
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

#[derive(Clone, Debug)]
struct PreparedExecutionPlanCore {
    shared: Arc<PreparedExecutionPlanCoreShared>,
}

#[derive(Clone)]
struct PreparedGroupedRuntimeResidents {
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

    fn execution_preparation(&self) -> ExecutionPreparation {
        self.execution_preparation.clone()
    }

    fn grouped_slot_layout(&self) -> RetainedSlotLayout {
        self.grouped_slot_layout.clone()
    }
}

///
/// PreparedScalarRuntimeParts
///
/// Structural scalar runtime handoff extracted from one prepared load plan.
/// Scalar entrypoints use this bundle to consume the authority, projection,
/// retained-slot, and lowered-index residents together instead of restating the
/// same wrapper sequence before route/runtime assembly.
///

pub(in crate::db::executor) struct PreparedScalarRuntimeParts {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) execution_preparation: ExecutionPreparation,
    pub(in crate::db::executor) prepared_projection_shape: Option<Arc<PreparedProjectionShape>>,
    pub(in crate::db::executor) retained_slot_layout: Option<RetainedSlotLayout>,
    pub(in crate::db::executor) plan_core: PreparedScalarPlanCore,
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
    core: PreparedExecutionPlanCore,
}

impl PreparedScalarPlanCore {
    /// Build one scalar prepared-plan handle from a logical plan whose lowered
    /// index specs were already derived by a caller-owned boundary.
    ///
    /// This is reserved for scalar projection/materialized helpers that must
    /// preserve a caller-owned retained-slot layout while still entering the
    /// shared scalar runtime without exposing owned plan/spec fields.
    #[must_use]
    pub(in crate::db::executor) fn from_prepared_lowered_parts(
        authority: EntityAuthority,
        plan: AccessPlannedQuery,
        index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
        index_range_specs: Vec<LoweredIndexRangeSpec>,
    ) -> Self {
        let continuation = plan.planned_continuation_contract(authority.entity_path());
        let core = PreparedExecutionPlanCore::new(
            plan,
            continuation,
            index_prefix_specs,
            false,
            index_range_specs,
            false,
        );

        Self { core }
    }

    #[must_use]
    pub(in crate::db::executor) fn plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    pub(in crate::db::executor) fn index_prefix_specs(
        &self,
    ) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        if self.core.shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }

        Ok(self.core.shared.index_prefix_specs.as_slice())
    }

    pub(in crate::db::executor) fn index_range_specs(
        &self,
    ) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        if self.core.shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(self.core.shared.index_range_specs.as_slice())
    }
}

///
/// PreparedGroupedRuntimeParts
///
/// Grouped runtime residents cloned from one prepared load plan.
/// Grouped entrypoints use this pair as one explicit handoff so the grouped
/// runtime boundary does not expose two separate clone-only wrappers.
///

pub(in crate::db::executor) struct PreparedGroupedRuntimeParts {
    pub(in crate::db::executor) execution_preparation: Option<ExecutionPreparation>,
    pub(in crate::db::executor) grouped_slot_layout: Option<RetainedSlotLayout>,
}

///
/// PreparedAccessPlanParts
///
/// Structural prepared-plan payload consumed by delete and grouped/scalar
/// structural entrypoints.
/// It keeps the authority, logical plan, and lowered access specs together so
/// those consumers do not peel the same immutable residents back out through
/// parallel wrappers.
///

pub(in crate::db) struct PreparedAccessPlanParts {
    pub(in crate::db) authority: EntityAuthority,
    pub(in crate::db) plan: AccessPlannedQuery,
    pub(in crate::db) index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    pub(in crate::db) index_range_specs: Vec<LoweredIndexRangeSpec>,
}

///
/// SharedPreparedProjectionRuntimeParts
///
/// Structural shared-prepared payload needed by projection runtime adapters.
/// Projection adapters consume this bundle directly so they do not
/// restate the same authority/plan/projection extraction across separate
/// shared-plan accessor calls.
///

pub(in crate::db) struct SharedPreparedProjectionRuntimeParts {
    pub(in crate::db) authority: EntityAuthority,
    pub(in crate::db) plan: AccessPlannedQuery,
    pub(in crate::db) prepared_projection_shape: Option<Arc<PreparedProjectionShape>>,
}

///
/// SharedPreparedExecutionPlan
///
/// SharedPreparedExecutionPlan is the generic-free prepared executor shell
/// cached below the SQL/fluent frontend split.
/// It preserves one canonical prepared execution contract without retaining
/// runtime cursor state or executor scratch buffers.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct SharedPreparedExecutionPlan {
    authority: EntityAuthority,
    core: PreparedExecutionPlanCore,
}

impl SharedPreparedExecutionPlan {
    #[must_use]
    pub(in crate::db) fn from_plan(
        authority: EntityAuthority,
        mut plan: AccessPlannedQuery,
    ) -> Self {
        authority.finalize_planner_route_profile(&mut plan);

        Self {
            authority,
            core: build_prepared_execution_plan_core(authority, plan),
        }
    }

    #[must_use]
    pub(in crate::db) fn typed_clone<E: EntityKind>(&self) -> PreparedExecutionPlan<E> {
        assert!(
            self.authority.entity_path() == E::PATH,
            "shared prepared plan entity mismatch: cached for '{}', requested '{}'",
            self.authority.entity_path(),
            E::PATH,
        );

        PreparedExecutionPlan {
            core: self.core.clone(),
            marker: PhantomData,
        }
    }

    #[must_use]
    pub(in crate::db) fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    // Projection runtime adapters consume these three shared prepared residents
    // together, so hand them off as one bundle instead of re-reading the same
    // plan shell through parallel field-level accessors.
    #[must_use]
    pub(in crate::db) fn into_projection_runtime_parts(
        self,
    ) -> SharedPreparedProjectionRuntimeParts {
        let Self { authority, core } = self;
        let prepared_projection_shape = core.get_or_init_projection_shape(authority);
        let shared = core.into_shared();

        SharedPreparedProjectionRuntimeParts {
            authority,
            plan: shared.plan,
            prepared_projection_shape,
        }
    }
}

impl PreparedExecutionPlanCore {
    #[must_use]
    fn new(
        plan: AccessPlannedQuery,
        continuation: Option<PlannedContinuationContract>,
        index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
        index_prefix_spec_invalid: bool,
        index_range_specs: Vec<LoweredIndexRangeSpec>,
        index_range_spec_invalid: bool,
    ) -> Self {
        Self {
            shared: Arc::new(PreparedExecutionPlanCoreShared {
                plan,
                prepared_projection_shape: OnceLock::new(),
                prepared_grouped_runtime_residents: OnceLock::new(),
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
    fn plan(&self) -> &AccessPlannedQuery {
        &self.shared.plan
    }

    fn get_or_init_projection_shape(
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

    fn get_or_init_grouped_runtime_residents(
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

    fn grouped_execution_preparation(
        &self,
        authority: EntityAuthority,
    ) -> Option<ExecutionPreparation> {
        self.get_or_init_grouped_runtime_residents(authority)
            .map(|residents| residents.execution_preparation())
    }

    fn grouped_slot_layout(&self, authority: EntityAuthority) -> Option<RetainedSlotLayout> {
        self.get_or_init_grouped_runtime_residents(authority)
            .map(|residents| residents.grouped_slot_layout())
    }

    fn get_or_init_scalar_execution_preparation(&self) -> ExecutionPreparation {
        // Scalar execution preparation is fully plan-deterministic: it depends on
        // the effective runtime predicate and slot map, but not on store handles,
        // cursor state, route retry policy, diagnostics, or materialization mode.
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

    fn get_or_init_scalar_layout(
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
    fn mode(&self) -> QueryMode {
        self.shared.plan.scalar_plan().mode
    }

    #[must_use]
    fn is_grouped(&self) -> bool {
        match self.shared.continuation {
            Some(ref contract) => contract.is_grouped(),
            None => false,
        }
    }

    fn execution_ordering(&self) -> Result<ExecutionOrdering, InternalError> {
        let contract = self.continuation_contract()?;
        Ok(contract.order_contract().ordering().clone())
    }

    fn execution_family(&self) -> Result<ExecutionFamily, InternalError> {
        let ordering = self.execution_ordering()?;

        Ok(match ordering {
            ExecutionOrdering::PrimaryKey => ExecutionFamily::PrimaryKey,
            ExecutionOrdering::Explicit(_) => ExecutionFamily::Ordered,
            ExecutionOrdering::Grouped(_) => ExecutionFamily::Grouped,
        })
    }

    #[must_use]
    fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(&self.shared.plan)
    }

    #[must_use]
    fn order_spec(&self) -> Option<&OrderSpec> {
        self.shared.plan.scalar_plan().order.as_ref()
    }

    #[must_use]
    fn has_predicate(&self) -> bool {
        self.shared.plan.has_residual_filter_expr()
            || self.shared.plan.has_residual_filter_predicate()
    }

    #[cfg(test)]
    fn index_prefix_specs(&self) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        if self.shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }

        Ok(self.shared.index_prefix_specs.as_slice())
    }

    #[cfg(test)]
    fn index_range_specs(&self) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        if self.shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(self.shared.index_range_specs.as_slice())
    }

    #[must_use]
    fn into_inner(self) -> AccessPlannedQuery {
        self.into_shared().plan
    }

    // Recover the shared prepared-plan payload by move when this core is
    // uniquely owned, and fall back to cloning only when another wrapper still
    // holds the shared Arc.
    #[must_use]
    fn into_shared(self) -> PreparedExecutionPlanCoreShared {
        Arc::try_unwrap(self.shared).unwrap_or_else(|shared| shared.as_ref().clone())
    }

    fn prepare_cursor(
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

    fn revalidate_cursor(
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

    fn revalidate_grouped_cursor(
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

    fn continuation_signature_for_runtime(&self) -> Result<ContinuationSignature, InternalError> {
        let contract = self.continuation_contract()?;
        Ok(contract.continuation_signature())
    }

    fn grouped_cursor_boundary_arity(&self) -> Result<usize, InternalError> {
        let contract = self.continuation_contract()?;
        if !contract.is_grouped() {
            return Err(
                ExecutorPlanError::grouped_cursor_boundary_arity_requires_grouped_plan()
                    .into_internal_error(),
            );
        }

        Ok(contract.boundary_arity())
    }

    fn grouped_pagination_window(
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
    fn continuation_contract(&self) -> Result<&PlannedContinuationContract, InternalError> {
        self.shared.continuation.as_ref().ok_or_else(|| {
            ExecutorPlanError::continuation_contract_requires_load_plan().into_internal_error()
        })
    }
}

// Build one canonical lowered prepared execution-plan core from resolved authority
// plus one logical plan, regardless of whether the caller started from a typed
// `PreparedExecutionPlan<E>` shell or a structural follow-on rewrite.
fn build_prepared_execution_plan_core(
    authority: EntityAuthority,
    mut plan: AccessPlannedQuery,
) -> PreparedExecutionPlanCore {
    authority.finalize_static_planning_shape(&mut plan);

    // Phase 1: derive immutable continuation contract once from planner semantics.
    let continuation = plan.planned_continuation_contract(authority.entity_path());

    // Phase 2: lower access-derived execution specs once and retain invariant state.
    // Projection shapes, grouped residents, and retained-slot layouts are lazy
    // residents because each execution surface consumes only one of those
    // families.
    let (
        index_prefix_specs,
        index_prefix_spec_invalid,
        index_range_specs,
        index_range_spec_invalid,
    ) = match lower_access(authority.entity_tag(), &plan.access) {
        Ok(lowered) => {
            let (_, index_prefix_specs, index_range_specs) = lowered.into_parts();

            (index_prefix_specs, false, index_range_specs, false)
        }
        Err(LoweredAccessError::IndexPrefix(_)) => (Vec::new(), true, Vec::new(), false),
        Err(LoweredAccessError::IndexRange(_)) => (Vec::new(), false, Vec::new(), true),
    };

    PreparedExecutionPlanCore::new(
        plan,
        continuation,
        index_prefix_specs,
        index_prefix_spec_invalid,
        index_range_specs,
        index_range_spec_invalid,
    )
}

///
/// PreparedExecutionPlan
///
/// Executor-ready plan bound to a specific entity type.
///

#[derive(Debug)]
pub(in crate::db) struct PreparedExecutionPlan<E: EntityKind> {
    core: PreparedExecutionPlanCore,
    marker: PhantomData<fn() -> E>,
}

///
/// PreparedLoadPlan
///
/// Generic-free load-plan boundary consumed by continuation resolution and
/// load pipeline preparation after the typed `PreparedExecutionPlan<E>` shell is no
/// longer needed.
///

#[derive(Debug)]
pub(in crate::db::executor) struct PreparedLoadPlan {
    authority: EntityAuthority,
    core: PreparedExecutionPlanCore,
}

impl PreparedLoadPlan {
    #[must_use]
    pub(in crate::db::executor) fn from_plan(
        authority: EntityAuthority,
        plan: AccessPlannedQuery,
    ) -> Self {
        Self {
            authority,
            core: build_prepared_execution_plan_core(authority, plan),
        }
    }

    #[must_use]
    pub(in crate::db::executor) const fn authority(&self) -> EntityAuthority {
        self.authority
    }

    #[must_use]
    pub(in crate::db::executor) fn mode(&self) -> QueryMode {
        self.core.mode()
    }

    #[must_use]
    pub(in crate::db::executor) fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    pub(in crate::db::executor) fn execution_ordering(
        &self,
    ) -> Result<ExecutionOrdering, InternalError> {
        self.core.execution_ordering()
    }

    pub(in crate::db::executor) fn revalidate_cursor(
        &self,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, InternalError> {
        self.core.revalidate_cursor(self.authority, cursor)
    }

    pub(in crate::db::executor) fn revalidate_grouped_cursor(
        &self,
        cursor: GroupedPlannedCursor,
    ) -> Result<GroupedPlannedCursor, InternalError> {
        self.core.revalidate_grouped_cursor(cursor)
    }

    pub(in crate::db::executor) fn continuation_signature_for_runtime(
        &self,
    ) -> Result<ContinuationSignature, InternalError> {
        self.core.continuation_signature_for_runtime()
    }

    pub(in crate::db::executor) fn grouped_cursor_boundary_arity(
        &self,
    ) -> Result<usize, InternalError> {
        self.core.grouped_cursor_boundary_arity()
    }

    pub(in crate::db::executor) fn grouped_pagination_window(
        &self,
        cursor: &GroupedPlannedCursor,
    ) -> Result<GroupedPaginationWindow, InternalError> {
        self.core.grouped_pagination_window(cursor)
    }

    // Collapse the scalar runtime handoff into one structural extraction so
    // callers do not restate the same authority/projection/layout/index/plan
    // unpacking sequence at every scalar entrypoint.
    pub(in crate::db::executor) fn into_scalar_runtime_parts(
        self,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
    ) -> Result<PreparedScalarRuntimeParts, InternalError> {
        self.into_scalar_runtime_parts_with_layout_override(
            projection_materialization,
            cursor_emission,
            None,
        )
    }

    /// Consume one typed prepared execution plan into scalar runtime parts
    /// while using a caller-owned retained-slot layout for this execution only.
    pub(in crate::db::executor) fn into_scalar_runtime_parts_with_retained_slot_layout(
        self,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
        retained_slot_layout: RetainedSlotLayout,
    ) -> Result<PreparedScalarRuntimeParts, InternalError> {
        self.into_scalar_runtime_parts_with_layout_override(
            projection_materialization,
            cursor_emission,
            Some(retained_slot_layout),
        )
    }

    fn into_scalar_runtime_parts_with_layout_override(
        self,
        projection_materialization: ProjectionMaterializationMode,
        cursor_emission: CursorEmissionMode,
        retained_slot_layout_override: Option<RetainedSlotLayout>,
    ) -> Result<PreparedScalarRuntimeParts, InternalError> {
        let Self { authority, core } = self;
        let prepared_projection_shape = if projection_materialization.validate_projection()
            && !core.plan().projection_is_model_identity()
        {
            core.get_or_init_projection_shape(authority)
        } else {
            None
        };
        let retained_slot_layout = retained_slot_layout_override.or_else(|| {
            core.get_or_init_scalar_layout(authority, projection_materialization, cursor_emission)
        });
        let execution_preparation = core.get_or_init_scalar_execution_preparation();
        if core.shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }
        if core.shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(PreparedScalarRuntimeParts {
            authority,
            execution_preparation,
            prepared_projection_shape,
            retained_slot_layout,
            plan_core: PreparedScalarPlanCore { core },
        })
    }

    #[must_use]
    pub(in crate::db::executor) fn cloned_grouped_runtime_parts(
        &self,
    ) -> PreparedGroupedRuntimeParts {
        PreparedGroupedRuntimeParts {
            execution_preparation: self.core.grouped_execution_preparation(self.authority),
            grouped_slot_layout: self.core.grouped_slot_layout(self.authority),
        }
    }

    pub(in crate::db::executor) fn into_access_plan_parts(
        self,
    ) -> Result<PreparedAccessPlanParts, InternalError> {
        let Self { authority, core } = self;
        let shared = core.into_shared();

        if shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }
        if shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(PreparedAccessPlanParts {
            authority,
            plan: shared.plan,
            index_prefix_specs: shared.index_prefix_specs,
            index_range_specs: shared.index_range_specs,
        })
    }
}

///
/// PreparedAggregatePlan
///
/// Generic-free aggregate-plan boundary consumed by aggregate terminal and
/// runtime preparation after the typed `PreparedExecutionPlan<E>` shell is no longer
/// needed.
///

#[derive(Debug)]
pub(in crate::db::executor) struct PreparedAggregatePlan {
    authority: EntityAuthority,
    core: PreparedExecutionPlanCore,
}

impl PreparedAggregatePlan {
    #[must_use]
    pub(in crate::db::executor) fn execution_preparation(&self) -> ExecutionPreparation {
        ExecutionPreparation::from_plan(self.core.plan(), slot_map_for_model_plan(self.core.plan()))
    }

    pub(in crate::db::executor) fn into_streaming_parts(
        self,
    ) -> Result<
        (
            EntityAuthority,
            AccessPlannedQuery,
            Vec<LoweredIndexPrefixSpec>,
            Vec<LoweredIndexRangeSpec>,
        ),
        InternalError,
    > {
        let Self { authority, core } = self;
        let shared = core.into_shared();

        if shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }
        if shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok((
            authority,
            shared.plan,
            shared.index_prefix_specs,
            shared.index_range_specs,
        ))
    }

    /// Re-shape one prepared aggregate plan into one grouped prepared load plan
    /// without reconstructing a typed `PreparedExecutionPlan<E>` shell.
    #[must_use]
    pub(in crate::db::executor) fn into_grouped_load_plan(
        self,
        group: GroupSpec,
    ) -> PreparedLoadPlan {
        PreparedLoadPlan::from_plan(self.authority, self.core.into_inner().into_grouped(group))
    }
}

impl<E: EntityKind> PreparedExecutionPlan<E> {
    pub(in crate::db) fn new(plan: AccessPlannedQuery) -> Self {
        Self::build(plan)
    }

    fn build(mut plan: AccessPlannedQuery) -> Self {
        let authority = EntityAuthority::for_type::<E>();
        authority.finalize_planner_route_profile(&mut plan);

        Self {
            core: build_prepared_execution_plan_core(authority, plan),
            marker: PhantomData,
        }
    }

    /// Explain scalar load execution shape as one canonical execution-node descriptor tree.
    pub(in crate::db) fn explain_load_execution_node_descriptor(
        &self,
    ) -> Result<ExplainExecutionNodeDescriptor, InternalError>
    where
        E: EntityValue,
    {
        if !self.mode().is_load() {
            return Err(
                ExecutorPlanError::load_execution_descriptor_requires_load_plan()
                    .into_internal_error(),
            );
        }

        let authority = EntityAuthority::for_type::<E>();

        assemble_load_execution_node_descriptor(
            authority.fields(),
            authority.primary_key_name(),
            self.core.plan(),
        )
    }

    /// Validate and decode a continuation cursor into executor-ready cursor state.
    pub(in crate::db) fn prepare_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<PlannedCursor, ExecutorPlanError> {
        self.core
            .prepare_cursor(EntityAuthority::for_type::<E>(), cursor)
    }

    /// Return the plan mode (load vs delete).
    #[must_use]
    pub(in crate::db) fn mode(&self) -> QueryMode {
        self.core.mode()
    }

    /// Return whether this prepared execution plan carries grouped logical shape.
    #[must_use]
    pub(in crate::db) fn is_grouped(&self) -> bool {
        self.core.is_grouped()
    }

    /// Return planner-projected execution strategy for entrypoint dispatch.
    pub(in crate::db) fn execution_family(&self) -> Result<ExecutionFamily, InternalError> {
        self.core.execution_family()
    }

    /// Borrow the structural logical plan behind this prepared execution plan.
    #[must_use]
    pub(in crate::db) fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    /// Expose planner-projected execution ordering for executor/lowering tests.
    #[cfg(test)]
    pub(in crate::db) fn execution_ordering(&self) -> Result<ExecutionOrdering, InternalError> {
        self.core.execution_ordering()
    }

    pub(in crate::db) fn access(&self) -> &crate::db::access::AccessPlan<crate::value::Value> {
        &self.core.plan().access
    }

    /// Borrow scalar row-consistency policy for runtime row reads.
    #[must_use]
    pub(in crate::db) fn consistency(&self) -> MissingRowPolicy {
        self.core.consistency()
    }

    /// Classify canonical `bytes_by(field)` execution mode for this plan/field.
    #[must_use]
    pub(in crate::db) fn bytes_by_projection_mode(
        &self,
        target_field: &str,
    ) -> BytesByProjectionMode {
        let authority = EntityAuthority::for_type::<E>();

        classify_bytes_by_projection_mode(
            self.access(),
            self.order_spec(),
            self.consistency(),
            self.has_predicate(),
            target_field,
            authority.primary_key_name(),
        )
    }

    /// Return a stable explain/diagnostic label for one bytes-by mode.
    #[must_use]
    pub(in crate::db) const fn bytes_by_projection_mode_label(
        mode: BytesByProjectionMode,
    ) -> &'static str {
        match mode {
            BytesByProjectionMode::Materialized => "field_materialized",
            BytesByProjectionMode::CoveringIndex => "field_covering_index",
            BytesByProjectionMode::CoveringConstant => "field_covering_constant",
        }
    }

    /// Borrow scalar ORDER BY contract for this prepared execution plan, if any.
    #[must_use]
    pub(in crate::db::executor) fn order_spec(&self) -> Option<&OrderSpec> {
        self.core.order_spec()
    }

    /// Borrow lowered index-prefix specs for test-only executor contracts.
    #[cfg(test)]
    pub(in crate::db) fn index_prefix_specs(
        &self,
    ) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        self.core.index_prefix_specs()
    }

    /// Return whether this prepared execution plan has a residual predicate.
    #[must_use]
    pub(in crate::db::executor) fn has_predicate(&self) -> bool {
        self.core.has_predicate()
    }

    /// Render one canonical executor snapshot for test-only planner/executor
    /// contract checks.
    #[cfg(test)]
    #[expect(clippy::too_many_lines)]
    pub(in crate::db) fn render_snapshot_canonical(&self) -> Result<String, InternalError>
    where
        E: EntityValue,
    {
        // Phase 1: project all executor-owned summary fields from the logical plan.
        let plan = self.core.plan();
        let authority = EntityAuthority::for_type::<E>();
        let projection_spec = plan.frozen_projection_spec();
        let projection_selection = if plan.grouped_plan().is_some()
            || projection_spec.len() != authority.row_layout().field_count()
        {
            "Declared"
        } else {
            "All"
        };
        let projection_coverage_flag = plan.grouped_plan().is_some();
        let continuation_signature = self.core.continuation_signature_for_runtime()?;
        let ordering_direction = self
            .core
            .continuation_contract()?
            .order_contract()
            .direction();
        let load_terminal_fast_path =
            crate::db::executor::planning::route::derive_load_terminal_fast_path_contract_for_plan(
                authority, plan,
            );

        // Phase 2: lower index-bound summaries into stable compact text.
        let render_lowered_bound =
            |bound: &std::ops::Bound<crate::db::access::LoweredKey>| match bound {
                std::ops::Bound::Included(key) => {
                    let bytes = key.as_bytes();
                    let head_len = bytes.len().min(8);
                    let tail_len = bytes.len().min(8);
                    let head = crate::db::codec::hex::encode_hex_lower(&bytes[..head_len]);
                    let tail =
                        crate::db::codec::hex::encode_hex_lower(&bytes[bytes.len() - tail_len..]);

                    format!("included(len:{}:head:{head}:tail:{tail})", bytes.len())
                }
                std::ops::Bound::Excluded(key) => {
                    let bytes = key.as_bytes();
                    let head_len = bytes.len().min(8);
                    let tail_len = bytes.len().min(8);
                    let head = crate::db::codec::hex::encode_hex_lower(&bytes[..head_len]);
                    let tail =
                        crate::db::codec::hex::encode_hex_lower(&bytes[bytes.len() - tail_len..]);

                    format!("excluded(len:{}:head:{head}:tail:{tail})", bytes.len())
                }
                std::ops::Bound::Unbounded => "unbounded".to_string(),
            };
        let index_prefix_specs = format!(
            "[{}]",
            self.core
                .index_prefix_specs()?
                .iter()
                .map(|spec| {
                    format!(
                        "{{index:{},bound_type:equality,lower:{},upper:{}}}",
                        spec.index().name(),
                        render_lowered_bound(spec.lower()),
                        render_lowered_bound(spec.upper()),
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        );
        let index_range_specs = format!(
            "[{}]",
            self.core
                .index_range_specs()?
                .iter()
                .map(|spec| {
                    format!(
                        "{{index:{},lower:{},upper:{}}}",
                        spec.index().name(),
                        render_lowered_bound(spec.lower()),
                        render_lowered_bound(spec.upper()),
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        );
        let explain_plan = plan.explain();

        // Phase 3: join the canonical snapshot payload in one stable line order.
        Ok([
            "snapshot_version=1".to_string(),
            format!("plan_hash={}", plan.fingerprint()),
            format!("mode={:?}", self.core.mode()),
            format!("is_grouped={}", self.core.is_grouped()),
            format!("execution_family={:?}", self.core.execution_family()?),
            format!(
                "load_terminal_fast_path={}",
                match load_terminal_fast_path.as_ref() {
                    Some(LoadTerminalFastPathContract::CoveringRead(_)) => "CoveringRead",
                    None => "Materialized",
                }
            ),
            format!("ordering_direction={ordering_direction:?}"),
            format!(
                "distinct_execution_strategy={:?}",
                plan.distinct_execution_strategy()
            ),
            format!("projection_selection={projection_selection}"),
            format!("projection_spec={projection_spec:?}"),
            format!("order_spec={:?}", plan.scalar_plan().order),
            format!("page_spec={:?}", plan.scalar_plan().page),
            format!("projection_coverage_flag={projection_coverage_flag}"),
            format!("continuation_signature={continuation_signature}"),
            format!("index_prefix_specs={index_prefix_specs}"),
            format!("index_range_specs={index_range_specs}"),
            format!("explain_plan={explain_plan:?}"),
        ]
        .join("\n"))
    }

    // Collapse the typed prepared shell into the structural logical plan plus
    // lowered access specs together so structural consumers do not peel those
    // three prepared artifacts back out through separate wrappers.
    pub(in crate::db) fn into_access_plan_parts(
        self,
    ) -> Result<PreparedAccessPlanParts, InternalError> {
        let shared = self.core.into_shared();

        if shared.index_prefix_spec_invalid {
            return Err(
                ExecutorPlanError::lowered_index_prefix_spec_invalid().into_internal_error()
            );
        }
        if shared.index_range_spec_invalid {
            return Err(ExecutorPlanError::lowered_index_range_spec_invalid().into_internal_error());
        }

        Ok(PreparedAccessPlanParts {
            authority: EntityAuthority::for_type::<E>(),
            plan: shared.plan,
            index_prefix_specs: shared.index_prefix_specs,
            index_range_specs: shared.index_range_specs,
        })
    }

    /// Validate and decode grouped continuation cursor state for grouped plans.
    #[cfg(test)]
    pub(in crate::db) fn prepare_grouped_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<GroupedPlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.core.shared.continuation.as_ref() else {
            return Err(ExecutorPlanError::grouped_cursor_preparation_requires_grouped_plan());
        };

        contract
            .prepare_grouped_cursor(EntityAuthority::for_type::<E>().entity_path(), cursor)
            .map_err(ExecutorPlanError::from)
    }

    /// Validate one already-decoded grouped continuation token for grouped plans.
    pub(in crate::db) fn prepare_grouped_cursor_token(
        &self,
        cursor: Option<crate::db::cursor::GroupedContinuationToken>,
    ) -> Result<GroupedPlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.core.shared.continuation.as_ref() else {
            return Err(ExecutorPlanError::grouped_cursor_preparation_requires_grouped_plan());
        };

        contract
            .prepare_grouped_cursor_token(EntityAuthority::for_type::<E>().entity_path(), cursor)
            .map_err(ExecutorPlanError::from)
    }

    /// Consume one typed prepared execution plan into one generic-free boundary
    /// payload for continuation and load-pipeline preparation.
    #[must_use]
    pub(in crate::db::executor) fn into_prepared_load_plan(self) -> PreparedLoadPlan {
        PreparedLoadPlan {
            authority: EntityAuthority::for_type::<E>(),
            core: self.core,
        }
    }

    /// Consume one typed prepared execution plan into one generic-free
    /// boundary payload for aggregate terminal and runtime preparation.
    #[must_use]
    pub(in crate::db::executor) fn into_prepared_aggregate_plan(self) -> PreparedAggregatePlan {
        PreparedAggregatePlan {
            authority: EntityAuthority::for_type::<E>(),
            core: self.core,
        }
    }
}
