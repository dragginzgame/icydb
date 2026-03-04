//! Module: db::executor::executable_plan
//! Responsibility: bind validated access-planned queries to executor-ready contracts.
//! Does not own: logical plan semantics or route policy decisions.
//! Boundary: shared plan container for load/delete/aggregate runtime entrypoints.

use crate::{
    db::{
        access::AccessPlan,
        cursor::{ContinuationSignature, CursorPlanError, GroupedPlannedCursor, PlannedCursor},
        executor::{
            ExecutorPlanError, LOWERED_INDEX_PREFIX_SPEC_INVALID, LOWERED_INDEX_RANGE_SPEC_INVALID,
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec, lower_index_prefix_specs,
            lower_index_range_specs, validate_executor_plan,
        },
        predicate::MissingRowPolicy,
        query::plan::{
            AccessPlannedQuery, ContinuationContract, ExecutionShapeSignature,
            GroupedExecutorHandoff, QueryMode, grouped_executor_handoff,
        },
    },
    error::InternalError,
    traits::EntityKind,
};
use std::marker::PhantomData;

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
    _marker: PhantomData<E>,
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
            _marker: PhantomData,
        }
    }

    /// Explain this plan without executing it.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn explain(&self) -> crate::db::query::explain::ExplainPlan {
        self.plan.explain_with_model(E::MODEL)
    }

    /// Compute a stable continuation signature for cursor compatibility checks.
    ///
    /// Unlike `fingerprint()`, this excludes window state such as `limit`/`offset`.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::db) const fn continuation_signature(&self) -> ContinuationSignature {
        self.execution_shape_signature.continuation_signature()
    }

    /// Borrow the immutable execution-shape signature for this executable plan.
    #[must_use]
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

    /// Return whether this executable plan supports continuation cursors.
    #[must_use]
    pub(in crate::db) const fn supports_continuation(&self) -> bool {
        self.continuation.is_some()
    }

    pub(in crate::db) const fn access(&self) -> &AccessPlan<E::Key> {
        &self.plan.access
    }

    /// Borrow scalar row-consistency policy for runtime row reads.
    #[must_use]
    pub(in crate::db) const fn consistency(&self) -> MissingRowPolicy {
        self.plan.scalar_plan().consistency
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
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}

fn cursor_plan_error(message: impl Into<String>) -> ExecutorPlanError {
    ExecutorPlanError::from(CursorPlanError::continuation_cursor_invariant(
        InternalError::executor_invariant_message(message),
    ))
}
