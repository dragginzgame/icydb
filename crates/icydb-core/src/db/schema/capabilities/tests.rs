use crate::{db::schema::AcceptedFieldKind, types::EntityTag};

use crate::db::schema::capabilities::sql_capabilities;

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
fn sql_capabilities_keep_blob_selectable_and_comparable_but_not_orderable() {
    let capabilities = sql_capabilities(&AcceptedFieldKind::Blob { max_len: None });

    assert!(capabilities.selectable());
    assert!(!capabilities.orderable());
    assert!(capabilities.groupable());
    assert!(capabilities.aggregate_input().count());
    assert!(!capabilities.aggregate_input().sum());
    assert!(!capabilities.aggregate_input().average());
    assert!(!capabilities.aggregate_input().extrema());
}

#[test]
fn sql_capabilities_keep_numeric_arithmetic_and_extrema_distinct() {
    let amount = sql_capabilities(&AcceptedFieldKind::Decimal { scale: 3 });
    let timestamp = sql_capabilities(&AcceptedFieldKind::Timestamp);

    assert!(amount.aggregate_input().sum());
    assert!(amount.aggregate_input().average());
    assert!(amount.aggregate_input().extrema());
    assert!(!timestamp.aggregate_input().sum());
    assert!(!timestamp.aggregate_input().average());
    assert!(timestamp.aggregate_input().extrema());
}

#[test]
fn sql_capabilities_keep_enum_equality_only_without_catalog_key_proof() {
    let capabilities = sql_capabilities(&AcceptedFieldKind::Enum {
        type_id: crate::value::EnumTypeId::new(1).expect("test enum type ID should be valid"),
    });

    assert!(capabilities.selectable());
    assert!(!capabilities.orderable());
    assert!(!capabilities.groupable());
    #[cfg(feature = "sql")]
    assert_eq!(capabilities.enum_equality(), None);
    assert!(capabilities.aggregate_input().count());
    assert!(!capabilities.aggregate_input().sum());
    assert!(!capabilities.aggregate_input().average());
    assert!(!capabilities.aggregate_input().extrema());
}

#[test]
fn sql_capabilities_admit_unit_ordering_without_group_or_numeric_semantics() {
    let capabilities = sql_capabilities(&AcceptedFieldKind::Unit);

    assert!(capabilities.selectable());
    assert!(capabilities.orderable());
    assert!(!capabilities.groupable());
    assert!(!capabilities.aggregate_input().count());
    assert!(!capabilities.aggregate_input().sum());
    assert!(!capabilities.aggregate_input().average());
    assert!(capabilities.aggregate_input().extrema());
}

#[test]
fn sql_capabilities_transport_collections_and_composites_without_scalar_operations() {
    let list = sql_capabilities(&AcceptedFieldKind::List(Box::new(
        AcceptedFieldKind::Text { max_len: None },
    )));
    let composite = sql_capabilities(&AcceptedFieldKind::test_composite());

    assert!(list.selectable());
    assert!(!list.orderable());
    assert!(!list.groupable());
    assert!(composite.selectable());
    assert!(!composite.orderable());
    assert!(!composite.groupable());
    assert!(!composite.aggregate_input().count());
}

#[test]
fn sql_capabilities_relation_inherits_key_capabilities() {
    let relation = sql_capabilities(&relation_to_key(AcceptedFieldKind::Nat64));

    assert!(relation.selectable());
    assert!(relation.orderable());
    assert!(relation.aggregate_input().sum());
    assert!(relation.aggregate_input().average());
}

#[test]
fn sql_capabilities_admit_u256_sum_without_numeric_widening_or_average() {
    let capabilities = sql_capabilities(&AcceptedFieldKind::U256);

    assert!(capabilities.selectable());
    assert!(capabilities.orderable());
    assert!(capabilities.groupable());
    assert!(capabilities.aggregate_input().sum());
    assert!(!capabilities.aggregate_input().average());
    assert!(capabilities.aggregate_input().extrema());
}

#[test]
fn sql_capabilities_fail_closed_for_non_scalar_relation_keys() {
    let relation = sql_capabilities(&relation_to_key(AcceptedFieldKind::test_composite()));

    assert!(relation.selectable());
    assert!(!relation.orderable());
    assert!(!relation.groupable());
    assert!(!relation.aggregate_input().count());
}
