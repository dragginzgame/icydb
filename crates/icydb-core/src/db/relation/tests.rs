use super::{
    AcceptedRelationCardinality, AcceptedRelationTargetAuthority, AcceptedRelationTargetContract,
    accepted_scalar_relation_cardinality, validate_accepted_relation_primary_key_kinds,
    validate_relation_primary_key_component_kind,
};
use crate::{db::schema::AcceptedFieldKind, error::ErrorClass, types::EntityTag};

fn relation_key_kind(key_kind: AcceptedFieldKind) -> AcceptedFieldKind {
    AcceptedFieldKind::Relation {
        target_path: "Target".to_string(),
        target_entity_name: "Target".to_string(),
        target_entity_tag: EntityTag::new(11),
        target_store_path: "TargetStore".to_string(),
        key_kind: Box::new(key_kind),
    }
}

#[test]
fn scalar_relation_contract_checks_selected_identity_and_key_kind() {
    let kind = relation_key_kind(AcceptedFieldKind::Nat64);
    let contract = AcceptedRelationTargetContract {
        target: AcceptedRelationTargetAuthority::try_new(
            "Target",
            "Target",
            EntityTag::new(11),
            "TargetStore",
        )
        .unwrap(),
        primary_key_kinds: vec![AcceptedFieldKind::Nat64],
    };
    assert_eq!(
        accepted_scalar_relation_cardinality(&kind, &contract).unwrap(),
        Some(AcceptedRelationCardinality::Single)
    );
    for (path, name, tag, store) in [
        ("Other", "Target", 11, "TargetStore"),
        ("Target", "Other", 11, "TargetStore"),
        ("Target", "Target", 12, "TargetStore"),
        ("Target", "Target", 11, "OtherStore"),
    ] {
        let changed = AcceptedRelationTargetContract {
            target: AcceptedRelationTargetAuthority::try_new(
                path,
                name,
                EntityTag::new(tag),
                store,
            )
            .unwrap(),
            primary_key_kinds: contract.primary_key_kinds.clone(),
        };
        assert!(accepted_scalar_relation_cardinality(&kind, &changed).is_err());
    }
    let changed_key = AcceptedRelationTargetContract {
        primary_key_kinds: vec![AcceptedFieldKind::Nat128],
        ..contract
    };
    assert!(accepted_scalar_relation_cardinality(&kind, &changed_key).is_err());
}

#[test]
fn relation_primary_key_component_kind_accepts_admitted_scalar_lanes() {
    for kind in [
        AcceptedFieldKind::Account,
        AcceptedFieldKind::Int64,
        AcceptedFieldKind::Int128,
        AcceptedFieldKind::Nat64,
        AcceptedFieldKind::Nat128,
        AcceptedFieldKind::Principal,
        AcceptedFieldKind::Subaccount,
        AcceptedFieldKind::Timestamp,
        AcceptedFieldKind::U256,
        AcceptedFieldKind::Ulid,
        AcceptedFieldKind::Unit,
    ] {
        validate_relation_primary_key_component_kind(&kind)
            .expect("admitted relation primary-key component kind should validate");
    }
}

#[test]
fn relation_primary_key_component_kind_unwraps_relation_key_kind() {
    let kind = relation_key_kind(AcceptedFieldKind::Nat128);

    validate_relation_primary_key_component_kind(&kind)
        .expect("relation field wrapper should validate through its key kind");
}

#[test]
fn accepted_relation_target_authority_rejects_primary_key_arity_drift() {
    let err = validate_accepted_relation_primary_key_kinds(
        &[AcceptedFieldKind::Nat64],
        &[AcceptedFieldKind::Nat64, AcceptedFieldKind::Ulid],
    )
    .expect_err("relation target authority must reject primary-key arity drift");

    assert_eq!(err.class, ErrorClass::Internal);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInternal,
    );
}

#[test]
fn accepted_relation_target_authority_rejects_primary_key_kind_drift() {
    let err = validate_accepted_relation_primary_key_kinds(
        &[AcceptedFieldKind::Nat64],
        &[AcceptedFieldKind::Nat128],
    )
    .expect_err("relation target authority must reject primary-key kind drift");

    assert_eq!(err.class, ErrorClass::Internal);
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInternal,
    );
}

#[test]
fn accepted_relation_target_authority_accepts_matching_ordered_primary_key_kinds() {
    validate_accepted_relation_primary_key_kinds(
        &[AcceptedFieldKind::Nat64, AcceptedFieldKind::Ulid],
        &[AcceptedFieldKind::Nat64, AcceptedFieldKind::Ulid],
    )
    .expect("matching ordered relation target authority should validate");
}
