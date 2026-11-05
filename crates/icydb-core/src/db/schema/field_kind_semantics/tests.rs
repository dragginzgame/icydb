use super::{AcceptedFieldKindCategory, classify_accepted_field_kind};
use crate::{db::schema::AcceptedFieldKind, types::EntityTag};
use icydb_schema::ScalarKind;

fn relation_to_key(key_kind: AcceptedFieldKind) -> AcceptedFieldKind {
    AcceptedFieldKind::Relation {
        target_path: "target::Entity".into(),
        target_entity_name: "Target".into(),
        target_entity_tag: EntityTag::new(77),
        target_store_path: "target::Store".into(),
        key_kind: Box::new(key_kind),
    }
}

#[test]
fn classify_persisted_numeric_scalar_kind() {
    let semantics = classify_accepted_field_kind(&AcceptedFieldKind::Nat64);

    assert_eq!(
        semantics.category(),
        AcceptedFieldKindCategory::Scalar(ScalarKind::Nat),
    );
    assert!(semantics.is_scalar());
    assert!(semantics.is_numeric());
    assert!(!semantics.is_signed_numeric());
    assert!(semantics.is_unsigned_numeric());
    assert!(semantics.is_orderable());
    assert!(semantics.is_sql_comparable());
    assert!(semantics.is_relation_key_eligible());
}

#[test]
fn classify_u256_keeps_checked_arithmetic_outside_numeric_widening() {
    let semantics = classify_accepted_field_kind(&AcceptedFieldKind::U256);

    assert!(!semantics.is_numeric());
    assert!(!semantics.is_signed_numeric());
    assert!(!semantics.is_unsigned_numeric());
    assert!(semantics.supports_arithmetic_numeric());
    assert!(!semantics.supports_predicate_numeric_widen());
    assert!(semantics.is_orderable());
}

#[test]
fn classify_relation_delegates_to_key_semantics_without_becoming_scalar() {
    let relation = relation_to_key(AcceptedFieldKind::Nat128);
    let semantics = classify_accepted_field_kind(&relation);

    assert_eq!(
        semantics.category(),
        AcceptedFieldKindCategory::Relation(Some(ScalarKind::Nat128)),
    );
    assert!(!semantics.is_scalar());
    assert!(semantics.is_numeric());
    assert!(semantics.is_unsigned_numeric());
    assert!(semantics.is_orderable());
    assert!(semantics.is_sql_comparable());
    assert!(semantics.is_relation_key_eligible());
}

#[test]
fn classify_collection_and_composite_kinds_stay_non_scalar() {
    let collection = classify_accepted_field_kind(&AcceptedFieldKind::List(Box::new(
        AcceptedFieldKind::Text { max_len: None },
    )));
    let composite = classify_accepted_field_kind(&AcceptedFieldKind::test_composite());

    assert!(collection.is_collection());
    assert!(!collection.is_scalar());
    assert!(!collection.is_sql_comparable());

    assert!(composite.is_composite());
    assert!(!composite.is_collection());
    assert!(!composite.is_orderable());
}

#[test]
fn classify_scalar_edges_match_current_persisted_contracts() {
    let blob = classify_accepted_field_kind(&AcceptedFieldKind::Blob { max_len: None });
    let unit = classify_accepted_field_kind(&AcceptedFieldKind::Unit);
    let date = classify_accepted_field_kind(&AcceptedFieldKind::Date);
    let timestamp = classify_accepted_field_kind(&AcceptedFieldKind::Timestamp);

    assert!(blob.is_sql_comparable());
    assert!(!blob.is_orderable());

    assert!(!unit.is_sql_comparable());
    assert!(unit.is_orderable());
    assert!(unit.is_relation_key_eligible());

    assert!(date.is_orderable());
    assert!(!date.is_relation_key_eligible());

    assert!(timestamp.is_numeric());
    assert!(timestamp.is_relation_key_eligible());
}
