//! Test-only constructors and adapters for the parent module.

use super::*;

impl CompiledAcceptedRowConstraints {
    pub(in crate::db) fn evaluate_targeted_rules_with_limits(
        &self,
        current_fingerprint: CommitSchemaFingerprint,
        values_by_slot: &[Option<Cow<'_, Value>>],
        limits: super::super::targeted::TargetedEvaluationLimits,
    ) -> Result<(), AcceptedRowConstraintEvaluationError> {
        if current_fingerprint != self.fingerprint {
            return Err(AcceptedRowConstraintEvaluationError::FingerprintMismatch);
        }
        let violation = self
            .targeted_rules
            .evaluate_with_limits_for_tests(values_by_slot, limits)
            .map_err(AcceptedRowConstraintEvaluationError::from)?;
        if let Some(violation) = violation {
            return Err(targeted_rule_violation_error(violation));
        }
        Ok(())
    }
}
