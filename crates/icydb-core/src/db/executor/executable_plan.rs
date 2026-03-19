//! Module: db::executor::executable_plan
//! Responsibility: bind validated access-planned queries to executor-ready contracts.
//! Does not own: logical plan semantics or route policy decisions.
//! Boundary: shared plan container for load/delete/aggregate runtime entrypoints.

use crate::{
    db::{
        cursor::{ContinuationSignature, CursorPlanError, GroupedPlannedCursor, PlannedCursor},
        direction::Direction,
        executor::{
            ExecutionPreparation, ExecutorPlanError, GroupedPaginationWindow,
            LOWERED_INDEX_PREFIX_SPEC_INVALID, LOWERED_INDEX_RANGE_SPEC_INVALID,
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            explain::{
                assemble_aggregate_terminal_execution_descriptor,
                assemble_load_execution_node_descriptor,
                assemble_load_execution_verbose_diagnostics,
            },
            lower_index_prefix_specs, lower_index_range_specs,
            preparation::resolved_index_slots_for_access_path,
            traversal::row_read_consistency_for_plan,
            validate_executor_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, ContinuationContract, ExecutionOrdering, GroupedExecutorHandoff,
            OrderDirection, OrderSpec, QueryMode, constant_covering_projection_value_from_access,
            covering_index_projection_context, grouped_executor_handoff,
            index_covering_existing_rows_terminal_eligible,
        },
        query::{
            builder::AggregateExpr,
            explain::{ExplainExecutionDescriptor, ExplainExecutionNodeDescriptor},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::marker::PhantomData;
///
/// ExecutionStrategy
///
/// Executor-facing execution shape contract derived from planner ordering.
/// Session and runtime entrypoints consume this strategy and must not
/// re-derive grouped/scalar routing shape from boolean flags.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionStrategy {
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

/// ExecutablePlanCore
///
/// Generic-free executable-plan payload shared by typed `ExecutablePlan<E>`
/// wrappers. This keeps cursor, ordering, and lowered structural plan state
/// monomorphic while typed access and model-driven behavior remain at the
/// outer executor boundary.
///

#[derive(Debug)]
struct ExecutablePlanCore {
    plan: AccessPlannedQuery,
    continuation: Option<ContinuationContract>,
    index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    index_prefix_spec_invalid: bool,
    index_range_specs: Vec<LoweredIndexRangeSpec>,
    index_range_spec_invalid: bool,
}

impl ExecutablePlanCore {
    #[must_use]
    const fn new(
        plan: AccessPlannedQuery,
        continuation: Option<ContinuationContract>,
        index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
        index_prefix_spec_invalid: bool,
        index_range_specs: Vec<LoweredIndexRangeSpec>,
        index_range_spec_invalid: bool,
    ) -> Self {
        Self {
            plan,
            continuation,
            index_prefix_specs,
            index_prefix_spec_invalid,
            index_range_specs,
            index_range_spec_invalid,
        }
    }

    #[must_use]
    const fn plan(&self) -> &AccessPlannedQuery {
        &self.plan
    }

    #[must_use]
    const fn mode(&self) -> QueryMode {
        self.plan.scalar_plan().mode
    }

    #[must_use]
    const fn is_grouped(&self) -> bool {
        match self.continuation {
            Some(ref contract) => contract.is_grouped(),
            None => false,
        }
    }

    fn execution_ordering(&self) -> Result<ExecutionOrdering, InternalError> {
        let contract = self.continuation_contract()?;
        Ok(contract.order_contract().ordering().clone())
    }

    fn execution_strategy(&self) -> Result<ExecutionStrategy, InternalError> {
        let ordering = self.execution_ordering()?;

        Ok(match ordering {
            ExecutionOrdering::PrimaryKey => ExecutionStrategy::PrimaryKey,
            ExecutionOrdering::Explicit(_) => ExecutionStrategy::Ordered,
            ExecutionOrdering::Grouped(_) => ExecutionStrategy::Grouped,
        })
    }

    #[must_use]
    const fn consistency(&self) -> MissingRowPolicy {
        row_read_consistency_for_plan(&self.plan)
    }

    #[must_use]
    const fn order_spec(&self) -> Option<&OrderSpec> {
        self.plan.scalar_plan().order.as_ref()
    }

    #[must_use]
    const fn has_predicate(&self) -> bool {
        self.plan.scalar_plan().predicate.is_some()
    }

    #[must_use]
    const fn is_distinct(&self) -> bool {
        self.plan.scalar_plan().distinct
    }

    #[must_use]
    const fn has_no_predicate_or_distinct(&self) -> bool {
        !self.has_predicate() && !self.is_distinct()
    }

    fn index_prefix_specs(&self) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        if self.index_prefix_spec_invalid {
            return Err(crate::db::error::query_executor_invariant(
                LOWERED_INDEX_PREFIX_SPEC_INVALID,
            ));
        }

        Ok(self.index_prefix_specs.as_slice())
    }

    fn index_range_specs(&self) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        if self.index_range_spec_invalid {
            return Err(crate::db::error::query_executor_invariant(
                LOWERED_INDEX_RANGE_SPEC_INVALID,
            ));
        }

        Ok(self.index_range_specs.as_slice())
    }

    #[must_use]
    fn into_inner(self) -> AccessPlannedQuery {
        self.plan
    }

    fn prepare_cursor(
        &self,
        entity_path: &'static str,
        entity_tag: crate::types::EntityTag,
        entity_model: &crate::model::entity::EntityModel,
        cursor: Option<&[u8]>,
    ) -> Result<PlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(cursor_plan_error(
                "continuation cursors are only supported for load plans",
            ));
        };

        contract
            .prepare_scalar_cursor(entity_path, entity_tag, entity_model, cursor)
            .map_err(ExecutorPlanError::from)
    }

    fn revalidate_cursor(
        &self,
        entity_tag: crate::types::EntityTag,
        entity_model: &crate::model::entity::EntityModel,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, InternalError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(crate::db::error::query_executor_invariant(
                "continuation cursors are only supported for load plans",
            ));
        };

        contract
            .revalidate_scalar_cursor(entity_tag, entity_model, cursor)
            .map_err(crate::db::error::from_cursor_plan_error)
    }

    fn revalidate_grouped_cursor(
        &self,
        cursor: GroupedPlannedCursor,
    ) -> Result<GroupedPlannedCursor, InternalError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(crate::db::error::query_executor_invariant(
                "grouped cursor revalidation requires grouped logical plans",
            ));
        };

        contract
            .revalidate_grouped_cursor(cursor)
            .map_err(crate::db::error::from_cursor_plan_error)
    }

    fn continuation_signature_for_runtime(&self) -> Result<ContinuationSignature, InternalError> {
        let contract = self.continuation_contract()?;
        Ok(contract.continuation_signature())
    }

    fn grouped_cursor_boundary_arity(&self) -> Result<usize, InternalError> {
        let contract = self.continuation_contract()?;
        if !contract.is_grouped() {
            return Err(crate::db::error::query_executor_invariant(
                "grouped cursor boundary arity requires grouped logical plans",
            ));
        }

        Ok(contract.boundary_arity())
    }

    fn grouped_pagination_window(
        &self,
        cursor: &GroupedPlannedCursor,
    ) -> Result<GroupedPaginationWindow, InternalError> {
        let contract = self.continuation_contract()?;
        let window = contract
            .grouped_paging_window(cursor)
            .map_err(crate::db::error::from_cursor_plan_error)?;
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
    fn continuation_contract(&self) -> Result<&ContinuationContract, InternalError> {
        self.continuation.as_ref().ok_or_else(|| {
            crate::db::error::query_executor_invariant(
                "continuation contracts are only supported for load plans",
            )
        })
    }
}

///
/// ExecutablePlan
///
/// Executor-ready plan bound to a specific entity type.
///

#[derive(Debug)]
pub(in crate::db) struct ExecutablePlan<E: EntityKind> {
    core: ExecutablePlanCore,
    marker: PhantomData<fn() -> E>,
}

impl<E: EntityKind> ExecutablePlan<E> {
    pub(in crate::db) fn new(plan: AccessPlannedQuery) -> Self {
        Self::build(plan)
    }

    fn build(plan: AccessPlannedQuery) -> Self {
        // Phase 0: derive immutable continuation contract once from planner semantics.
        let continuation = plan.continuation_contract(E::PATH);

        // Phase 1: Lower index-prefix specs once and retain invariant state.
        let (index_prefix_specs, index_prefix_spec_invalid) =
            match lower_index_prefix_specs(E::ENTITY_TAG, &plan.access) {
                Ok(specs) => (specs, false),
                Err(_) => (Vec::new(), true),
            };

        // Phase 2: Lower index-range specs once and retain invariant state.
        let (index_range_specs, index_range_spec_invalid) =
            match lower_index_range_specs(E::ENTITY_TAG, &plan.access) {
                Ok(specs) => (specs, false),
                Err(_) => (Vec::new(), true),
            };

        Self {
            core: ExecutablePlanCore::new(
                plan,
                continuation,
                index_prefix_specs,
                index_prefix_spec_invalid,
                index_range_specs,
                index_range_spec_invalid,
            ),
            marker: PhantomData,
        }
    }

    /// Explain one scalar aggregate execution descriptor without executing it.
    #[must_use]
    pub(in crate::db) fn explain_aggregate_terminal_execution_descriptor(
        &self,
        aggregate: AggregateExpr,
    ) -> ExplainExecutionDescriptor
    where
        E: EntityValue,
    {
        assemble_aggregate_terminal_execution_descriptor::<E>(self.core.plan(), aggregate)
    }

    /// Explain scalar load execution shape as one canonical execution-node descriptor tree.
    pub(in crate::db) fn explain_load_execution_node_descriptor(
        &self,
    ) -> Result<ExplainExecutionNodeDescriptor, InternalError>
    where
        E: EntityValue,
    {
        if !self.mode().is_load() {
            return Err(crate::db::error::query_executor_invariant(
                "load execution descriptor requires load-mode executable plans",
            ));
        }

        assemble_load_execution_node_descriptor::<E>(self.core.plan())
    }

    /// Explain scalar load execution route diagnostics for verbose surfaces.
    pub(in crate::db) fn explain_load_execution_verbose_diagnostics(
        &self,
    ) -> Result<Vec<String>, InternalError>
    where
        E: EntityValue,
    {
        if !self.mode().is_load() {
            return Err(crate::db::error::query_executor_invariant(
                "load execution verbose diagnostics require load-mode executable plans",
            ));
        }

        assemble_load_execution_verbose_diagnostics::<E>(self.core.plan())
    }

    /// Validate and decode a continuation cursor into executor-ready cursor state.
    pub(in crate::db) fn prepare_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<PlannedCursor, ExecutorPlanError> {
        self.core
            .prepare_cursor(E::PATH, E::ENTITY_TAG, E::MODEL, cursor)
    }

    /// Return the plan mode (load vs delete).
    #[must_use]
    pub(in crate::db) const fn mode(&self) -> QueryMode {
        self.core.mode()
    }

    /// Return whether this executable plan carries grouped logical shape.
    #[must_use]
    pub(in crate::db) const fn is_grouped(&self) -> bool {
        self.core.is_grouped()
    }

    /// Return planner-projected execution ordering used by runtime dispatch.
    pub(in crate::db) fn execution_ordering(&self) -> Result<ExecutionOrdering, InternalError> {
        self.core.execution_ordering()
    }

    /// Return planner-projected execution strategy for entrypoint dispatch.
    pub(in crate::db) fn execution_strategy(&self) -> Result<ExecutionStrategy, InternalError> {
        self.core.execution_strategy()
    }

    pub(in crate::db) const fn access(
        &self,
    ) -> &crate::db::access::AccessPlan<crate::value::Value> {
        &self.core.plan().access
    }

    /// Borrow scalar row-consistency policy for runtime row reads.
    #[must_use]
    pub(in crate::db) const fn consistency(&self) -> MissingRowPolicy {
        self.core.consistency()
    }

    /// Classify canonical `bytes_by(field)` execution mode for this plan/field.
    #[must_use]
    pub(in crate::db) fn bytes_by_projection_mode(
        &self,
        target_field: &str,
    ) -> BytesByProjectionMode {
        if !matches!(self.consistency(), MissingRowPolicy::Ignore) {
            return BytesByProjectionMode::Materialized;
        }

        if constant_covering_projection_value_from_access(self.access(), target_field).is_some() {
            return BytesByProjectionMode::CoveringConstant;
        }

        if self.has_predicate() {
            return BytesByProjectionMode::Materialized;
        }

        if covering_index_projection_context(
            self.access(),
            self.order_spec(),
            target_field,
            E::MODEL.primary_key.name,
        )
        .is_some()
        {
            return BytesByProjectionMode::CoveringIndex;
        }

        BytesByProjectionMode::Materialized
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

    /// Borrow scalar ORDER BY contract for this executable plan, if any.
    #[must_use]
    pub(in crate::db::executor) const fn order_spec(&self) -> Option<&OrderSpec> {
        self.core.order_spec()
    }

    /// Return whether this executable plan has a residual predicate.
    #[must_use]
    pub(in crate::db::executor) const fn has_predicate(&self) -> bool {
        self.core.has_predicate()
    }

    /// Return whether this plan clears both residual-predicate and DISTINCT
    /// gates required by route-owned scalar fast-path contracts.
    #[must_use]
    pub(in crate::db::executor) const fn has_no_predicate_or_distinct(&self) -> bool {
        self.core.has_no_predicate_or_distinct()
    }

    /// Return one canonical scan direction for unordered plans (`Asc`) or
    /// explicit primary-key-only ordering; return `None` for non-PK ordering.
    #[must_use]
    pub(in crate::db::executor) fn unordered_or_primary_key_order_direction(
        &self,
    ) -> Option<Direction> {
        let Some(order) = self.order_spec() else {
            return Some(Direction::Asc);
        };

        order
            .primary_key_only_direction(E::MODEL.primary_key.name)
            .map(|direction| match direction {
                OrderDirection::Asc => Direction::Asc,
                OrderDirection::Desc => Direction::Desc,
            })
    }

    /// Build canonical execution preparation for this executable plan.
    #[must_use]
    pub(in crate::db::executor) fn execution_preparation(&self) -> ExecutionPreparation
    where
        E: EntityValue,
    {
        ExecutionPreparation::from_plan(
            E::MODEL,
            self.core.plan(),
            resolved_index_slots_for_access_path(
                E::MODEL,
                self.access().resolve_strategy().executable(),
            ),
        )
    }

    /// Return whether COUNT/EXISTS can keep one index-covering existing-row terminal path.
    #[must_use]
    pub(in crate::db::executor) fn index_covering_existing_rows_terminal_eligible(&self) -> bool
    where
        E: EntityValue,
    {
        let strict_predicate_compatible = self.execution_preparation().strict_mode().is_some();

        index_covering_existing_rows_terminal_eligible(
            self.core.plan(),
            strict_predicate_compatible,
        )
    }

    #[must_use]
    pub(in crate::db) const fn logical_plan(&self) -> &AccessPlannedQuery {
        self.core.plan()
    }

    pub(in crate::db) fn index_prefix_specs(
        &self,
    ) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        self.core.index_prefix_specs()
    }

    pub(in crate::db) fn index_range_specs(
        &self,
    ) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        self.core.index_range_specs()
    }

    /// Split the executable plan into its canonical structural logical plan.
    ///
    /// Aggregate/scalar prepared boundaries should prefer this helper when they
    /// no longer need the typed `ExecutablePlan<E>` shell after entering
    /// structural execution preparation.
    pub(in crate::db) fn into_plan(self) -> AccessPlannedQuery {
        self.core.into_inner()
    }

    /// Build grouped executor handoff from this executable plan using one
    /// canonical executor-boundary validation pass.
    pub(in crate::db) fn grouped_handoff(
        &self,
    ) -> Result<GroupedExecutorHandoff<'_>, InternalError> {
        validate_executor_plan::<E>(self.core.plan())?;
        grouped_executor_handoff(self.core.plan())
    }

    /// Revalidate executor-provided cursor state through the canonical cursor spine.
    pub(in crate::db) fn revalidate_cursor(
        &self,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, InternalError> {
        self.core.revalidate_cursor(E::ENTITY_TAG, E::MODEL, cursor)
    }

    /// Validate and decode grouped continuation cursor state for grouped plans.
    pub(in crate::db) fn prepare_grouped_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<GroupedPlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.core.continuation.as_ref() else {
            return Err(cursor_plan_error(
                "grouped cursor preparation requires grouped logical plans",
            ));
        };

        contract
            .prepare_grouped_cursor(E::PATH, cursor)
            .map_err(ExecutorPlanError::from)
    }

    /// Revalidate grouped cursor state before grouped executor entry.
    pub(in crate::db) fn revalidate_grouped_cursor(
        &self,
        cursor: GroupedPlannedCursor,
    ) -> Result<GroupedPlannedCursor, InternalError> {
        self.core.revalidate_grouped_cursor(cursor)
    }

    /// Borrow continuation signature from immutable continuation contract.
    pub(in crate::db) fn continuation_signature_for_runtime(
        &self,
    ) -> Result<ContinuationSignature, InternalError> {
        self.core.continuation_signature_for_runtime()
    }

    /// Borrow grouped cursor boundary arity from immutable continuation contract.
    pub(in crate::db) fn grouped_cursor_boundary_arity(&self) -> Result<usize, InternalError> {
        self.core.grouped_cursor_boundary_arity()
    }

    /// Derive grouped paging window from immutable continuation contract.
    pub(in crate::db::executor) fn grouped_pagination_window(
        &self,
        cursor: &GroupedPlannedCursor,
    ) -> Result<GroupedPaginationWindow, InternalError> {
        self.core.grouped_pagination_window(cursor)
    }
}

fn cursor_plan_error(message: impl Into<String>) -> ExecutorPlanError {
    ExecutorPlanError::from(CursorPlanError::continuation_cursor_invariant(
        crate::db::error::executor_invariant_message(message),
    ))
}
