use crate::{
    db::{
        access::AccessPlan,
        cursor::{ContinuationSignature, PlannedCursor},
        executor::{
            ExecutorPlanError, LOWERED_INDEX_PREFIX_SPEC_INVALID, LOWERED_INDEX_RANGE_SPEC_INVALID,
            LoweredIndexPrefixSpec, LoweredIndexRangeSpec, lower_index_prefix_specs,
            lower_index_range_specs,
        },
        plan::derive_primary_scan_direction,
        query::{
            explain::ExplainPlan,
            fingerprint::PlanFingerprint,
            plan::{AccessPlannedQuery, LogicalPlan, QueryMode, validate::PlanError},
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
pub struct ExecutablePlan<E: EntityKind> {
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
        let (index_prefix_specs, index_prefix_spec_invalid) =
            match lower_index_prefix_specs::<E>(&plan.access) {
                Ok(specs) => (specs, false),
                Err(_) => (Vec::new(), true),
            };
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

    // Initial page offset used for continuation compatibility on first-page shape.
    const fn initial_page_offset(plan: &LogicalPlan) -> u32 {
        match plan.page {
            Some(ref page) => page.offset,
            None => 0,
        }
    }

    /// Explain this plan without executing it.
    #[must_use]
    pub fn explain(&self) -> ExplainPlan {
        self.plan.explain_with_model(E::MODEL)
    }

    /// Compute a stable fingerprint for this plan.
    #[must_use]
    pub fn fingerprint(&self) -> PlanFingerprint {
        self.plan.fingerprint()
    }

    /// Compute a stable continuation signature for cursor compatibility checks.
    ///
    /// Unlike `fingerprint()`, this excludes window state such as `limit`/`offset`.
    #[must_use]
    pub fn continuation_signature(&self) -> ContinuationSignature {
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
        let direction = derive_primary_scan_direction(self.plan.order.as_ref());
        crate::db::cursor::prepare_cursor::<E>(
            &self.plan,
            direction,
            self.continuation_signature(),
            Self::initial_page_offset(&self.plan),
            cursor,
        )
        .map_err(ExecutorPlanError::from)
    }

    /// Return the plan mode (load vs delete).
    #[must_use]
    pub(in crate::db) const fn mode(&self) -> QueryMode {
        self.plan.logical.mode
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
            return Err(InternalError::query_executor_invariant(
                LOWERED_INDEX_PREFIX_SPEC_INVALID,
            ));
        }

        Ok(self.index_prefix_specs.as_slice())
    }

    pub(in crate::db) fn index_range_specs(
        &self,
    ) -> Result<&[LoweredIndexRangeSpec], InternalError> {
        if self.index_range_spec_invalid {
            return Err(InternalError::query_executor_invariant(
                LOWERED_INDEX_RANGE_SPEC_INVALID,
            ));
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
        let direction = derive_primary_scan_direction(self.plan.order.as_ref());
        crate::db::cursor::revalidate_cursor::<E>(
            &self.plan,
            direction,
            Self::initial_page_offset(&self.plan),
            cursor,
        )
        .map_err(|err| InternalError::from_cursor_plan_error(PlanError::from(err)))
    }
}
