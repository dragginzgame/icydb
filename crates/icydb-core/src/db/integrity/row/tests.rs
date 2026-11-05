use super::*;

#[test]
fn identity_row_classification_precedes_generic_row_checks() {
    assert_eq!(
        classify_identity_scalar(
            &AcceptedFieldKind::Nat64,
            Some(PrimaryKeyComponent::Nat64(9)),
            8,
        )
        .expect("Nat64 identity should classify"),
        (
            IdentityScalarIntegrity::HighWaterExceeded,
            Some(u128::from(9_u8)),
        ),
    );
    assert_eq!(
        classify_identity_scalar(
            &AcceptedFieldKind::Nat8,
            Some(PrimaryKeyComponent::Nat64(0)),
            8,
        )
        .expect("zero identity should classify"),
        (IdentityScalarIntegrity::Invalid, Some(0)),
    );
    assert_eq!(
        classify_identity_scalar(
            &AcceptedFieldKind::Nat128,
            Some(PrimaryKeyComponent::Nat128(8)),
            8,
        )
        .expect("Nat128 identity should classify"),
        (IdentityScalarIntegrity::Clean, Some(8)),
    );
    assert_eq!(
        classify_identity_scalar(
            &AcceptedFieldKind::Nat128,
            Some(PrimaryKeyComponent::Nat64(8)),
            8,
        )
        .expect("wrong key shape should classify"),
        (IdentityScalarIntegrity::Invalid, None),
    );
}
