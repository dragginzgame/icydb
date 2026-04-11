//! Module: db::query::intent::policy
//! Responsibility: intent-policy validation before planner compilation.
//! Does not own: logical-plan construction or executor runtime behavior.
//! Boundary: enforces query-shape and key-access policy on intent-owned state.

use crate::db::query::{
    intent::{IntentError, KeyAccessKind, state::QueryIntent},
    plan::{
        IntentKeyAccessKind as IntentValidationKeyAccessKind, validate_intent_key_access_policy,
        validate_intent_plan_shape,
    },
};

impl<K> QueryIntent<K> {
    /// Validate intent policy shape before planning.
    pub(in crate::db::query::intent) fn validate_policy_shape(&self) -> Result<(), IntentError> {
        let scalar_intent = self.scalar();
        validate_intent_plan_shape(
            self.mode(),
            scalar_intent.order.as_ref(),
            self.is_grouped(),
            self.has_delete_offset_violation(),
        )
        .map_err(IntentError::from)?;

        let key_access_kind = scalar_intent
            .key_access
            .as_ref()
            .map(|state| match state.kind {
                KeyAccessKind::Single => IntentValidationKeyAccessKind::Single,
                KeyAccessKind::Many => IntentValidationKeyAccessKind::Many,
                KeyAccessKind::Only => IntentValidationKeyAccessKind::Only,
            });
        validate_intent_key_access_policy(
            scalar_intent.key_access_conflict,
            key_access_kind,
            scalar_intent.predicate.is_some(),
        )
        .map_err(IntentError::from)?;

        Ok(())
    }
}
