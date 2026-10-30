//! Mixed accepted families keep their write/integrity identity and gate scope.

use super::*;

#[test]
fn mixed_constraint_sources_preserve_violation_order_and_activation_scope() {
    let fixture = targeted_rule_fixture();
    let snapshot = fixture.accepted.persisted_snapshot();
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check(
            "accepted_true".into(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::True,
        )
        .unwrap()
        .with_added_check_activation(
            "pending_false".into(),
            ConstraintOrigin::SqlDdl,
            AcceptedCheckExprV1::False,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
            2,
        )
        .unwrap();
    let pending_id = catalog.activations()[0].id();
    let accepted =
        AcceptedSchemaSnapshot::try_new(snapshot.clone().with_constraint_catalog(catalog)).unwrap();
    let program = CompiledAcceptedRowConstraints::compile(
        &accepted,
        &fixture.value_catalog,
        FINGERPRINT,
        &MaintenanceConstructionBudget::new(),
    )
    .unwrap();
    let valid = borrowed_values(&fixture.values);
    assert_eq!(
        program.evaluate(FINGERPRINT, &valid),
        Err(AcceptedRowConstraintEvaluationError::Violation {
            constraint_id: pending_id,
            kind: AcceptedRowConstraintViolationKind::Check,
        }),
    );
    // The accepted CHECK is added to integrity; the pending false gate is not.
    assert_eq!(
        program.integrity_constraint_count(),
        fixture.program.integrity_constraint_count() + 1,
    );
    for ordinal in 0..program.integrity_constraint_count() {
        program
            .evaluate_integrity_constraint(ordinal, FINGERPRINT, &valid)
            .unwrap();
    }
    for earlier_not_null in [false, true] {
        let mut invalid = fixture.values.clone();
        invalid[10] = Some(Value::Nat64(15));
        if earlier_not_null {
            invalid[8] = Some(Value::Null);
        }
        let invalid = borrowed_values(&invalid);
        let expected = fixture.program.evaluate(FINGERPRINT, &invalid).unwrap_err();
        if earlier_not_null {
            assert!(matches!(
                expected,
                AcceptedRowConstraintEvaluationError::Violation {
                    kind: AcceptedRowConstraintViolationKind::NotNull,
                    ..
                }
            ));
        } else {
            assert!(matches!(
                expected,
                AcceptedRowConstraintEvaluationError::TargetedRuleViolation { .. }
            ));
        }
        assert_eq!(program.evaluate(FINGERPRINT, &invalid), Err(expected));
    }
}
