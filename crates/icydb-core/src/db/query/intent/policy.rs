//! Module: db::query::intent::policy
//! Responsibility: module-local ownership and contracts for db::query::intent::policy.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

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
