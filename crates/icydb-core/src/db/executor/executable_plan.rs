//! Module: db::executor::executable_plan
//! Responsibility: bind validated access-planned queries to executor-ready contracts.
//! Does not own: logical plan semantics or route policy decisions.
//! Boundary: shared plan container for load/delete/aggregate runtime entrypoints.

use crate::{
    db::{
        access::AccessPlan,
        cursor::{ContinuationSignature, GroupedPlannedCursor, PlannedCursor},
        executor::{
            ContinuationEngine, ExecutorPlanError, LOWERED_INDEX_PREFIX_SPEC_INVALID,
            LOWERED_INDEX_RANGE_SPEC_INVALID, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            lower_index_prefix_specs, lower_index_range_specs,
        },
        query::plan::{
            AccessPlannedQuery, GroupDistinctPolicyReason, QueryMode,
            grouped_distinct_policy_violation_for_executor,
        },
    },
    error::InternalError,
    traits::{EntityKind, FieldValue},
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
    pub(in crate::db) fn continuation_signature(&self) -> ContinuationSignature {
        self.plan.continuation_signature(E::PATH)
    }

    /// Validate and decode a continuation cursor into executor-ready cursor state.
    pub(in crate::db) fn prepare_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<PlannedCursor, ExecutorPlanError>
    where
        E::Key: FieldValue,
    {
        ContinuationEngine::prepare_scalar_cursor_for_plan::<E>(
            &self.plan,
            self.continuation_signature(),
            cursor,
        )
    }

    /// Return the plan mode (load vs delete).
    #[must_use]
    pub(in crate::db) const fn mode(&self) -> QueryMode {
        self.plan.scalar_plan().mode
    }

    /// Return whether this executable plan carries grouped logical shape.
    #[must_use]
    pub(in crate::db) const fn is_grouped(&self) -> bool {
        self.plan.grouped_plan().is_some()
    }

    /// Return grouped DISTINCT policy violation reason for executor boundary guards.
    #[must_use]
    pub(in crate::db) fn grouped_distinct_policy_violation_for_executor(
        &self,
    ) -> Option<GroupDistinctPolicyReason> {
        self.plan
            .grouped_plan()
            .and_then(grouped_distinct_policy_violation_for_executor)
    }

    pub(in crate::db) const fn access(&self) -> &AccessPlan<E::Key> {
        &self.plan.access
    }

    #[must_use]
    pub(in crate::db) const fn as_inner(&self) -> &AccessPlannedQuery<E::Key> {
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

    /// Revalidate executor-provided cursor state through the canonical cursor spine.
    pub(in crate::db) fn revalidate_cursor(
        &self,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, InternalError>
    where
        E::Key: FieldValue,
    {
        ContinuationEngine::revalidate_scalar_cursor_for_plan::<E>(&self.plan, cursor)
    }

    /// Validate and decode grouped continuation cursor state for grouped plans.
    pub(in crate::db) fn prepare_grouped_cursor(
        &self,
        cursor: Option<&[u8]>,
    ) -> Result<GroupedPlannedCursor, ExecutorPlanError> {
        ContinuationEngine::prepare_grouped_cursor_for_plan(
            E::PATH,
            &self.plan,
            self.continuation_signature(),
            cursor,
        )
    }

    /// Revalidate grouped cursor state before grouped executor entry.
    pub(in crate::db) fn revalidate_grouped_cursor(
        &self,
        cursor: GroupedPlannedCursor,
    ) -> Result<GroupedPlannedCursor, InternalError> {
        ContinuationEngine::revalidate_grouped_cursor_for_plan(&self.plan, cursor)
    }
}

fn invariant(message: impl Into<String>) -> InternalError {
    InternalError::query_executor_invariant(message)
}
