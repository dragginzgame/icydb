//! Test-only constructors and adapters for the parent module.

use super::*;

impl SharedPreparedExecutionPlan {
    pub(in crate::db) fn resident_lifetime_for_tests(&self) -> impl Fn() -> bool + use<> {
        let residents = Rc::downgrade(&self.core.residents);
        move || residents.strong_count() != 0
    }

    pub(in crate::db) fn has_projection_covering_read_plan_for_tests(&self) -> bool {
        self.projection_covering_read_execution_plan().is_some()
    }

    pub(in crate::db) fn has_hybrid_covering_read_plan_for_tests(&self) -> bool {
        self.hybrid_covering_read_plan().is_some()
    }
}
