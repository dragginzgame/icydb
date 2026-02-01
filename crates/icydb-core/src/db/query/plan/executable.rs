use crate::{
    db::query::{
        QueryMode,
        plan::{ExplainPlan, LogicalPlan, PlanFingerprint},
    },
    traits::EntityKind,
    types::Ref,
};
use std::marker::PhantomData;

///
/// ExecutablePlan
///
/// Executor-ready plan bound to a specific entity type.
///

#[derive(Debug)]
pub struct ExecutablePlan<E: EntityKind<PrimaryKey = Ref<E>>> {
    plan: LogicalPlan<E::PrimaryKey>,
    _marker: PhantomData<E>,
}

impl<E: EntityKind<PrimaryKey = Ref<E>>> ExecutablePlan<E> {
    pub(crate) const fn new(plan: LogicalPlan<E::PrimaryKey>) -> Self {
        Self {
            plan,
            _marker: PhantomData,
        }
    }

    /// Explain this plan without executing it.
    #[must_use]
    pub fn explain(&self) -> ExplainPlan {
        self.plan.explain()
    }

    /// Compute a stable fingerprint for this plan.
    #[must_use]
    pub fn fingerprint(&self) -> PlanFingerprint {
        self.plan.fingerprint()
    }

    /// Return the plan mode (load vs delete).
    #[must_use]
    pub(crate) const fn mode(&self) -> QueryMode {
        self.plan.mode
    }

    pub(crate) const fn access(&self) -> &crate::db::query::plan::AccessPlan<E::PrimaryKey> {
        &self.plan.access
    }

    pub(crate) fn into_inner(self) -> LogicalPlan<E::PrimaryKey> {
        self.plan
    }
}
