//! Module: db::query::intent::policy
//! Responsibility: intent-policy validation before planner compilation.
//! Does not own: logical-plan construction or executor runtime behavior.
//! Boundary: enforces query-shape policy on intent-owned state.

use crate::db::query::{
    intent::{IntentError, state::QueryIntent},
    plan::validate_intent_plan_shape,
};

impl QueryIntent {
    /// Validate intent policy shape before planning.
    pub(in crate::db::query::intent) fn validate_policy_shape(&self) -> Result<(), IntentError> {
        let scalar_intent = self.scalar();
        validate_intent_plan_shape(self.mode(), scalar_intent.order.as_ref(), self.is_grouped())
            .map_err(IntentError::from)?;

        Ok(())
    }
}
