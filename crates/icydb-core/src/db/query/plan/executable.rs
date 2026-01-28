use crate::{
    db::query::{
        QueryMode,
        plan::{ExplainPlan, LogicalPlan, PlanFingerprint},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::EntityKind,
};
use std::marker::PhantomData;

///
/// ExecutablePlan
///
/// Executor-ready plan bound to a specific entity type.
///

pub struct ExecutablePlan<E: EntityKind> {
    plan: LogicalPlan,
    _marker: PhantomData<E>,
}

impl<E: EntityKind> ExecutablePlan<E> {
    pub(crate) const fn new(plan: LogicalPlan) -> Self {
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

    pub(crate) const fn access(&self) -> &crate::db::query::plan::AccessPlan {
        &self.plan.access
    }

    pub(crate) fn into_inner(self) -> LogicalPlan {
        self.plan
    }

    /// Erase the entity type while preserving the validated plan and entity path.
    #[must_use]
    pub fn erase(self) -> ExecutablePlanErased {
        ExecutablePlanErased {
            plan: self.plan,
            entity_path: E::PATH,
        }
    }
}

/// Opaque, entity-tagged plan used for dynamic dispatch.
#[doc(hidden)]
pub struct ExecutablePlanErased {
    plan: LogicalPlan,
    entity_path: &'static str,
}

impl ExecutablePlanErased {
    #[doc(hidden)]
    pub fn into_typed<E: EntityKind>(self) -> Result<ExecutablePlan<E>, InternalError> {
        if self.entity_path != E::PATH {
            return Err(InternalError::new(
                ErrorClass::Unsupported,
                ErrorOrigin::Query,
                format!(
                    "plan entity mismatch: expected {}, found {}",
                    E::PATH,
                    self.entity_path
                ),
            ));
        }

        Ok(ExecutablePlan::new(self.plan))
    }
}
