//! Test-only constructors and adapters for the parent module.

use super::*;

impl CompiledAcceptedTargetedRules {
    pub(in crate::db::schema::check) fn evaluate_with_limits_for_tests(
        &self,
        values_by_slot: &[Option<Cow<'_, Value>>],
        limits: TargetedEvaluationLimits,
    ) -> Result<Option<AcceptedTargetedRuleViolation>, AcceptedTargetedRuleEvaluationError> {
        self.evaluate_with_limits(values_by_slot, limits, None)
    }
}

impl TargetedEvaluationLimits {
    #[must_use]
    pub(in crate::db) const fn for_tests(
        depth: u16,
        value_nodes: u32,
        operations: u32,
        path_components: usize,
    ) -> Self {
        Self {
            depth,
            value_nodes,
            operations,
            path_components,
        }
    }
}
