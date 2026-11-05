use super::{
    AcceptedConstraintCatalog, AcceptedConstraintCatalogError, AcceptedConstraintKind,
    AcceptedRuleOperation, ConstraintActivationState, ConstraintOrigin, not_null_constraint_name,
};
use crate::db::{
    codec::{finalize_hash_sha256, new_hash_sha256},
    schema::{
        AcceptedCheckExprV1, AcceptedCheckLiteralV1, AcceptedFieldKind, AcceptedSchemaFingerprint,
        FieldId, FieldStorageDecode, LeafCodec, ScalarCodec,
    },
};
use sha2::Digest;

#[test]
fn reserved_names_reject_authoring_but_preserve_accepted_engine_constraints() {
    for name in [
        "__icydb_primary_key",
        "__icydb_not_null_2",
        "__icydb_custom",
    ] {
        assert_eq!(
            super::validate_authored_constraint_name(name),
            Err(AcceptedConstraintCatalogError::InvalidName)
        );
        assert!(super::validate_generated_constraint_name(name).is_err());
        assert!(super::accepted_constraint_name_is_valid(name));
    }
    let mut catalog = AcceptedConstraintCatalog::default();
    catalog
        .push(
            super::primary_key_constraint_name().into(),
            ConstraintOrigin::Generated,
            AcceptedConstraintKind::PrimaryKey,
        )
        .unwrap();
    catalog
        .push(
            not_null_constraint_name(FieldId::new(2)),
            ConstraintOrigin::SqlDdl,
            AcceptedConstraintKind::NotNull {
                field_id: FieldId::new(2),
            },
        )
        .unwrap();
    assert_eq!(catalog.constraints().len(), 2);
}

#[test]
fn dense_field_remapping_preserves_constraint_identity_and_allows_new_not_null_owners() {
    let mut catalog = AcceptedConstraintCatalog::default();
    for (id, origin) in [
        (1, ConstraintOrigin::Generated),
        (2, ConstraintOrigin::SqlDdl),
        (3, ConstraintOrigin::Generated),
    ] {
        let field_id = FieldId::new(id);
        catalog
            .push(
                not_null_constraint_name(field_id),
                origin,
                AcceptedConstraintKind::NotNull { field_id },
            )
            .unwrap();
    }
    catalog = catalog
        .with_added_check(
            "authored_check".into(),
            ConstraintOrigin::SqlDdl,
            AcceptedCheckExprV1::True,
        )
        .unwrap();
    let authored = catalog.constraints().last().unwrap().clone();
    for _ in 0..2 {
        let before = catalog.clone();
        catalog = catalog
            .with_removed_not_null(FieldId::new(1))
            .unwrap()
            .with_mapped_field_ids(|id| {
                id.get()
                    .checked_sub(1)
                    .filter(|id| *id > 0)
                    .map(FieldId::new)
            })
            .unwrap();
        assert_eq!(catalog.allocator(), before.allocator());
        for constraint in catalog.constraints() {
            let previous = before
                .constraints()
                .iter()
                .find(|entry| entry.id() == constraint.id())
                .unwrap();
            assert_eq!(constraint.origin(), previous.origin());
            if let AcceptedConstraintKind::NotNull { field_id } = constraint.kind() {
                assert_eq!(constraint.name(), not_null_constraint_name(*field_id));
                assert_eq!(
                    previous.kind(),
                    &AcceptedConstraintKind::NotNull {
                        field_id: FieldId::new(field_id.get() + 1)
                    }
                );
            } else {
                assert_eq!(constraint, &authored);
            }
        }
        let field_id = FieldId::new(3);
        catalog
            .push(
                not_null_constraint_name(field_id),
                ConstraintOrigin::SqlDdl,
                AcceptedConstraintKind::NotNull { field_id },
            )
            .unwrap();
        assert_eq!(
            catalog.allocator().high_water(),
            before.allocator().high_water() + 1
        );
    }
}

#[test]
fn appended_targeted_rule_fingerprint_tags_are_four_and_five() {
    let literal = AcceptedCheckLiteralV1::from_accepted_parts(
        AcceptedFieldKind::Nat64,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
        vec![5],
    );
    for (operation, tag) in [
        (
            AcceptedRuleOperation::NumericMaximumInclusive {
                value: literal.clone(),
            },
            4,
        ),
        (
            AcceptedRuleOperation::MultipleOf {
                divisor: literal.clone(),
            },
            5,
        ),
    ] {
        let mut actual = new_hash_sha256();
        super::update_targeted_rule_operation_fingerprint(&mut actual, &operation);

        let mut expected = new_hash_sha256();
        expected.update([tag]);
        let key = literal.canonical_key();
        expected.update(u64::try_from(key.len()).unwrap_or(u64::MAX).to_be_bytes());
        expected.update(key);
        assert_eq!(finalize_hash_sha256(actual), finalize_hash_sha256(expected));
    }
}

#[test]
fn accepted_constraint_catalog_rejects_noncanonical_or_unbounded_names() {
    for (name, expected) in [
        ("", AcceptedConstraintCatalogError::EmptyName),
        (
            "contains space",
            AcceptedConstraintCatalogError::InvalidName,
        ),
        ("contains\0nul", AcceptedConstraintCatalogError::InvalidName),
    ] {
        let mut catalog = AcceptedConstraintCatalog::default();
        assert_eq!(
            catalog.push(
                name.to_string(),
                ConstraintOrigin::Generated,
                AcceptedConstraintKind::PrimaryKey,
            ),
            Err(expected),
        );
    }

    let mut catalog = AcceptedConstraintCatalog::default();
    assert_eq!(
        catalog.push(
            "a".repeat(257),
            ConstraintOrigin::Generated,
            AcceptedConstraintKind::PrimaryKey,
        ),
        Err(AcceptedConstraintCatalogError::NameTooLong),
    );
}

#[test]
fn check_activation_preserves_identity_through_validation_and_promotion() {
    let catalog = AcceptedConstraintCatalog::default()
        .with_added_check_activation(
            "pending_check".to_string(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::True,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
            7,
        )
        .expect("activation should reserve identity");
    let allocator = catalog.allocator();
    let activation = catalog.activations()[0].clone();
    let validating = catalog
        .with_validation_started(activation.id())
        .expect("activation should enter validation");
    assert_eq!(
        validating.activations()[0].state(),
        ConstraintActivationState::Validating
    );
    assert_ne!(
        validating.activations()[0].fingerprint(),
        activation.fingerprint(),
    );

    let promoted = validating
        .with_promoted_activation(activation.id())
        .expect("validated check should promote");
    assert!(promoted.activations().is_empty());
    assert_eq!(promoted.constraints()[0].id(), activation.id());
    assert_eq!(promoted.constraints()[0].name(), activation.name());
    assert_eq!(promoted.allocator(), allocator);
}

#[test]
fn aborted_activation_retires_its_identity_without_reuse() {
    let catalog = AcceptedConstraintCatalog::default()
        .with_added_check_activation(
            "first".to_string(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::True,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
            7,
        )
        .expect("activation should reserve identity");
    let retired = catalog.activations()[0].id();
    let aborted = catalog
        .with_aborted_activation(retired)
        .expect("activation should abort");
    let next = aborted
        .with_added_check_activation(
            "second".to_string(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::True,
            AcceptedSchemaFingerprint::new([0xB5; 32]),
            8,
        )
        .expect("new activation should reserve a new identity");
    assert!(next.activations()[0].id() > retired);
}

#[test]
fn removed_generated_check_retires_its_identity_without_reuse() {
    let catalog = AcceptedConstraintCatalog::default()
        .with_added_check(
            "first".to_string(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::True,
        )
        .expect("generated check should admit");
    let retired = catalog.constraints()[0].id();
    let allocator = catalog.allocator();
    let removed = catalog
        .with_removed_generated_constraint(retired)
        .expect("generated check should be removable");

    assert!(removed.constraints().is_empty());
    assert_eq!(removed.allocator(), allocator);

    let next = removed
        .with_added_check(
            "second".to_string(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::True,
        )
        .expect("new generated check should admit");
    assert!(next.constraints()[0].id() > retired);

    let sql_owned = AcceptedConstraintCatalog::default()
        .with_added_check(
            "sql_owned".to_string(),
            ConstraintOrigin::SqlDdl,
            AcceptedCheckExprV1::True,
        )
        .expect("SQL-owned check should admit");
    let sql_owned_id = sql_owned.constraints()[0].id();
    assert_eq!(
        sql_owned.with_removed_generated_constraint(sql_owned_id),
        Err(AcceptedConstraintCatalogError::OwnerMismatch),
        "generated removal must not take ownership of SQL DDL state",
    );
}
