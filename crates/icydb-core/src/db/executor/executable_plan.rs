//! Module: db::executor::executable_plan
//! Responsibility: bind validated access-planned queries to executor-ready contracts.
//! Does not own: logical plan semantics or route policy decisions.
//! Boundary: shared plan container for load/delete/aggregate runtime entrypoints.

use crate::{
    db::{
        access::AccessPlan,
        cursor::{ContinuationSignature, CursorPlanError, GroupedPlannedCursor, PlannedCursor},
        executor::{
            ExecutionPreparation, ExecutorPlanError, LOWERED_INDEX_PREFIX_SPEC_INVALID,
            LOWERED_INDEX_RANGE_SPEC_INVALID, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            explain::{
                assemble_aggregate_terminal_execution_descriptor,
                assemble_load_execution_node_descriptor,
                assemble_load_execution_verbose_diagnostics,
            },
            lower_index_prefix_specs, lower_index_range_specs, validate_executor_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, ContinuationContract, ExecutionOrdering, ExecutionShapeSignature,
            GroupedContinuationWindow, GroupedExecutorHandoff, OrderSpec, PageSpec, QueryMode,
            grouped_executor_handoff,
        },
        query::{
            builder::AggregateExpr,
            explain::{ExplainExecutionDescriptor, ExplainExecutionNodeDescriptor},
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

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
/// ExecutablePlan
///
/// Executor-ready plan bound to a specific entity type.
///

#[derive(Debug)]
pub(crate) struct ExecutablePlan<E: EntityKind> {
    plan: AccessPlannedQuery<E::Key>,
    execution_shape_signature: ExecutionShapeSignature,
    continuation: Option<ContinuationContract<E::Key>>,
    index_prefix_specs: Vec<LoweredIndexPrefixSpec>,
    index_prefix_spec_invalid: bool,
    index_range_specs: Vec<LoweredIndexRangeSpec>,
    index_range_spec_invalid: bool,
}

impl<E: EntityKind> ExecutablePlan<E> {
    #[cfg(test)]
    pub(crate) fn new(plan: AccessPlannedQuery<E::Key>) -> Self {
        Self::build(plan)
    }

    #[cfg(not(test))]
    pub(in crate::db) fn new(plan: AccessPlannedQuery<E::Key>) -> Self {
        Self::build(plan)
    }

    fn build(plan: AccessPlannedQuery<E::Key>) -> Self {
        // Phase 0: derive immutable execution-shape signature once from planner semantics.
        let execution_shape_signature = plan.execution_shape_signature(E::PATH);
        let continuation = plan.continuation_contract(E::PATH);

        // Phase 1: Lower index-prefix specs once and retain invariant state.
        let (index_prefix_specs, index_prefix_spec_invalid) =
            match lower_index_prefix_specs::<E>(&plan.access) {
                Ok(specs) => (specs, false),
                Err(_) => (Vec::new(), true),
            };

        // Phase 2: Lower index-range specs once and retain invariant state.
        let (index_range_specs, index_range_spec_invalid) =
            match lower_index_range_specs::<E>(&plan.access) {
                Ok(specs) => (specs, false),
                Err(_) => (Vec::new(), true),
            };

        Self {
            plan,
            execution_shape_signature,
            continuation,
            index_prefix_specs,
            index_prefix_spec_invalid,
            index_range_specs,
            index_range_spec_invalid,
        }
    }

    /// Explain this plan without executing it.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn explain(&self) -> crate::db::query::explain::ExplainPlan {
        self.plan.explain_with_model(E::MODEL)
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
        assemble_aggregate_terminal_execution_descriptor::<E>(&self.plan, aggregate)
    }

    /// Explain scalar load execution shape as one canonical execution-node descriptor tree.
    pub(in crate::db) fn explain_load_execution_node_descriptor(
        &self,
    ) -> Result<ExplainExecutionNodeDescriptor, InternalError>
    where
        E: EntityValue,
    {
        if !self.mode().is_load() {
            return Err(invariant(
                "load execution descriptor requires load-mode executable plans",
            ));
        }

        assemble_load_execution_node_descriptor::<E>(&self.plan)
    }

    /// Explain scalar load execution route diagnostics for verbose surfaces.
    pub(in crate::db) fn explain_load_execution_verbose_diagnostics(
        &self,
    ) -> Result<Vec<String>, InternalError>
    where
        E: EntityValue,
    {
        if !self.mode().is_load() {
            return Err(invariant(
                "load execution verbose diagnostics require load-mode executable plans",
            ));
        }

        assemble_load_execution_verbose_diagnostics::<E>(&self.plan)
    }

    /// Compute a stable continuation signature for cursor compatibility checks.
    ///
    /// Unlike `fingerprint()`, this excludes window state such as `limit`/`offset`.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db) const fn continuation_signature(&self) -> ContinuationSignature {
        match self.continuation {
            Some(ref contract) => contract.continuation_signature(),
            None => {
                panic!("continuation signature requires load-mode continuation contract")
            }
        }
    }

    /// Borrow the immutable execution-shape signature for this executable plan.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db) const fn execution_shape_signature(&self) -> ExecutionShapeSignature {
        self.execution_shape_signature
    }

    /// Validate and decode a continuation cursor into executor-ready cursor state.
    pub(in crate::db) fn prepare_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<PlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(cursor_plan_error(
                "continuation cursors are only supported for load plans",
            ));
        };

        contract
            .prepare_scalar_cursor::<E>(cursor)
            .map_err(ExecutorPlanError::from)
    }

    /// Return the plan mode (load vs delete).
    #[must_use]
    pub(in crate::db) const fn mode(&self) -> QueryMode {
        self.plan.scalar_plan().mode
    }

    /// Return whether this executable plan carries grouped logical shape.
    #[must_use]
    pub(in crate::db) const fn is_grouped(&self) -> bool {
        match self.continuation {
            Some(ref contract) => contract.is_grouped(),
            None => false,
        }
    }

    /// Return planner-projected execution ordering used by runtime dispatch.
    pub(in crate::db) fn execution_ordering(&self) -> Result<ExecutionOrdering, InternalError> {
        let contract = self.continuation_contract()?;
        Ok(contract.order_contract().ordering().clone())
    }

    /// Return planner-projected execution strategy for entrypoint dispatch.
    pub(in crate::db) fn execution_strategy(&self) -> Result<ExecutionStrategy, InternalError> {
        let ordering = self.execution_ordering()?;

        Ok(match ordering {
            ExecutionOrdering::PrimaryKey => ExecutionStrategy::PrimaryKey,
            ExecutionOrdering::Explicit(_) => ExecutionStrategy::Ordered,
            ExecutionOrdering::Grouped(_) => ExecutionStrategy::Grouped,
        })
    }

    pub(in crate::db) const fn access(&self) -> &AccessPlan<E::Key> {
        &self.plan.access
    }

    /// Borrow scalar row-consistency policy for runtime row reads.
    #[must_use]
    pub(in crate::db) const fn consistency(&self) -> MissingRowPolicy {
        self.plan.scalar_plan().consistency
    }

    /// Borrow scalar ORDER BY contract for this executable plan, if any.
    #[must_use]
    pub(in crate::db::executor) const fn order_spec(&self) -> Option<&OrderSpec> {
        self.plan.scalar_plan().order.as_ref()
    }

    /// Borrow scalar pagination contract for this executable plan, if any.
    #[must_use]
    pub(in crate::db::executor) const fn page_spec(&self) -> Option<&PageSpec> {
        self.plan.scalar_plan().page.as_ref()
    }

    /// Return whether this executable plan has a residual predicate.
    #[must_use]
    pub(in crate::db::executor) const fn has_predicate(&self) -> bool {
        self.plan.scalar_plan().predicate.is_some()
    }

    /// Build canonical execution preparation for this executable plan.
    #[must_use]
    pub(in crate::db::executor) fn execution_preparation(&self) -> ExecutionPreparation
    where
        E: EntityValue,
    {
        ExecutionPreparation::for_plan::<E>(&self.plan)
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn as_inner(&self) -> &AccessPlannedQuery<E::Key> {
        &self.plan
    }

    pub(in crate::db) fn index_prefix_specs(
        &self,
    ) -> Result<&[LoweredIndexPrefixSpec], InternalError> {
        if self.index_prefix_spec_invalid {
            return Err(invariant(LOWERED_INDEX_PREFIX_SPEC_INVALID));
        }

        Ok(self.index_prefix_specs.as_slice())
    }

    pub(in crate::db) fn index_range_specs(
        &self,
    ) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        if self.index_range_spec_invalid {
            return Err(invariant(LOWERED_INDEX_RANGE_SPEC_INVALID));
        }

        Ok(self.index_range_specs.as_slice())
    }

    pub(in crate::db) fn into_inner(self) -> AccessPlannedQuery<E::Key> {
        self.plan
    }

    /// Build grouped executor handoff from this executable plan using one
    /// canonical executor-boundary validation pass.
    pub(in crate::db) fn grouped_handoff(
        &self,
    ) -> Result<GroupedExecutorHandoff<'_, E::Key>, InternalError> {
        validate_executor_plan::<E>(&self.plan)?;
        grouped_executor_handoff(&self.plan)
    }

    /// Revalidate executor-provided cursor state through the canonical cursor spine.
    pub(in crate::db) fn revalidate_cursor(
        &self,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, InternalError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(invariant(
                "continuation cursors are only supported for load plans",
            ));
        };

        contract
            .revalidate_scalar_cursor::<E>(cursor)
            .map_err(InternalError::from_cursor_plan_error)
    }

    /// Validate and decode grouped continuation cursor state for grouped plans.
    pub(in crate::db) fn prepare_grouped_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<GroupedPlannedCursor, ExecutorPlanError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(cursor_plan_error(
                "grouped cursor preparation requires grouped logical plans",
            ));
        };

        contract
            .prepare_grouped_cursor::<E>(cursor)
            .map_err(ExecutorPlanError::from)
    }

    /// Revalidate grouped cursor state before grouped executor entry.
    pub(in crate::db) fn revalidate_grouped_cursor(
        &self,
        cursor: GroupedPlannedCursor,
    ) -> Result<GroupedPlannedCursor, InternalError> {
        let Some(contract) = self.continuation.as_ref() else {
            return Err(invariant(
                "grouped cursor revalidation requires grouped logical plans",
            ));
        };

        contract
            .revalidate_grouped_cursor(cursor)
            .map_err(InternalError::from_cursor_plan_error)
    }

    /// Borrow continuation signature from immutable continuation contract.
    pub(in crate::db) fn continuation_signature_for_runtime(
        &self,
    ) -> Result<ContinuationSignature, InternalError> {
        let contract = self.continuation_contract()?;
        Ok(contract.continuation_signature())
    }

    /// Borrow grouped cursor boundary arity from immutable continuation contract.
    pub(in crate::db) fn grouped_cursor_boundary_arity(&self) -> Result<usize, InternalError> {
        let contract = self.continuation_contract()?;
        if !contract.is_grouped() {
            return Err(invariant(
                "grouped cursor boundary arity requires grouped logical plans",
            ));
        }

        Ok(contract.boundary_arity())
    }

    /// Derive grouped paging window from immutable continuation contract.
    pub(in crate::db) fn grouped_continuation_window(
        &self,
        cursor: &GroupedPlannedCursor,
    ) -> Result<GroupedContinuationWindow, InternalError> {
        let contract = self.continuation_contract()?;
        contract
            .grouped_paging_window(cursor)
            .map_err(InternalError::from_cursor_plan_error)
    }

    // Borrow immutable continuation contract for load-mode plans.
    fn continuation_contract(&self) -> Result<&ContinuationContract<E::Key>, InternalError> {
        self.continuation
            .as_ref()
            .ok_or_else(|| invariant("continuation contracts are only supported for load plans"))
    }
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}

fn cursor_plan_error(message: impl Into<String>) -> ExecutorPlanError {
    ExecutorPlanError::from(CursorPlanError::continuation_cursor_invariant(
        InternalError::executor_invariant_message(message),
    ))
}
