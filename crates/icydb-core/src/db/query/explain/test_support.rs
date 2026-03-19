use crate::db::query::{explain::ExplainPlan, plan::AccessPlannedQuery};

impl AccessPlannedQuery {
    /// Produce a stable, deterministic explanation of this logical plan for tests.
    #[must_use]
    pub(crate) fn explain(&self) -> ExplainPlan {
        self.explain_inner(None)
    }
}
