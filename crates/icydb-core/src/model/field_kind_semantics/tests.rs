use crate::{
    model::{
        canonicalize_grouped_having_numeric_literal_for_field_kind,
        canonicalize_strict_sql_literal_for_kind, classify_field_kind,
        field::FieldKind,
        field_kind_has_identity_group_canonical_form,
        field_kind_semantics::{FieldKindCategory, FieldKindNumericClass, FieldKindScalarClass},
    },
    value::Value,
};

#[test]
fn classify_numeric_scalar_field_kind() {
    let semantics = classify_field_kind(&FieldKind::Nat64);

    assert_eq!(
        semantics.category(),
        FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::Unsigned64,
        )),
    );
    assert!(semantics.supports_aggregate_numeric());
    assert!(semantics.supports_aggregate_ordering());
    assert!(semantics.supports_predicate_numeric_widen());
}

#[test]
fn classify_relation_uses_key_semantics_without_expr_numeric() {
    static NAT_KEY_KIND: FieldKind = FieldKind::Nat64;
    static RELATION_KIND: FieldKind = FieldKind::Relation {
        target_path: "demo::Target",
        target_entity_name: "Target",
        target_entity_tag: crate::types::EntityTag::new(1),
        target_store_path: "demo::store::TargetStore",
        key_kind: &NAT_KEY_KIND,
        strength: crate::model::field::RelationStrength::Strong,
    };

    let semantics = classify_field_kind(&RELATION_KIND);

    assert_eq!(
        semantics.category(),
        FieldKindCategory::Relation(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::Unsigned64,
        )),
    );
    assert!(semantics.supports_aggregate_numeric());
    assert!(semantics.supports_aggregate_ordering());
    assert!(semantics.supports_predicate_numeric_widen());
}

#[test]
fn classify_collection_and_blob_stay_non_orderable() {
    let collection = classify_field_kind(&FieldKind::List(&FieldKind::Text { max_len: None }));
    let blob = classify_field_kind(&FieldKind::Blob { max_len: None });

    assert_eq!(collection.category(), FieldKindCategory::Collection);
    assert!(!collection.supports_aggregate_ordering());

    assert_eq!(
        blob.category(),
        FieldKindCategory::Scalar(FieldKindScalarClass::Opaque),
    );
    assert!(!blob.supports_aggregate_ordering());
}

#[test]
fn classify_wide_integer_and_temporal_kinds_keep_distinct_numeric_facets() {
    let wide = classify_field_kind(&FieldKind::Int128);
    let duration = classify_field_kind(&FieldKind::Duration);
    let timestamp = classify_field_kind(&FieldKind::Timestamp);

    assert_eq!(
        wide.category(),
        FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::SignedWide,
        )),
    );
    assert_eq!(
        duration.category(),
        FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::DurationLike,
        )),
    );
    assert_eq!(
        timestamp.category(),
        FieldKindCategory::Scalar(FieldKindScalarClass::Numeric(
            FieldKindNumericClass::TimestampLike,
        )),
    );

    assert!(!wide.supports_predicate_numeric_widen());
    assert!(!duration.supports_predicate_numeric_widen());
    assert!(!timestamp.supports_predicate_numeric_widen());
}

#[test]
fn grouped_field_kind_helpers_keep_decimal_relation_and_unit_edges_explicit() {
    static NAT_KEY_KIND: FieldKind = FieldKind::Nat64;
    static RELATION_KIND: FieldKind = FieldKind::Relation {
        target_path: "demo::Target",
        target_entity_name: "Target",
        target_entity_tag: crate::types::EntityTag::new(1),
        target_store_path: "demo::store::TargetStore",
        key_kind: &NAT_KEY_KIND,
        strength: crate::model::field::RelationStrength::Strong,
    };

    assert!(field_kind_has_identity_group_canonical_form(
        FieldKind::Text { max_len: None }
    ));
    assert!(!field_kind_has_identity_group_canonical_form(
        FieldKind::Decimal { scale: 2 }
    ));
    assert!(!field_kind_has_identity_group_canonical_form(RELATION_KIND));

    assert!(FieldKind::Decimal { scale: 2 }.supports_group_probe());
    assert!(RELATION_KIND.supports_group_probe());
    assert!(!FieldKind::Unit.supports_group_probe());
}

#[test]
fn runtime_value_acceptance_recurses_through_nested_field_kinds() {
    static TEXT_KIND: FieldKind = FieldKind::Text { max_len: None };
    static NAT_KIND: FieldKind = FieldKind::Nat64;
    static RELATION_KIND: FieldKind = FieldKind::Relation {
        target_path: "demo::Target",
        target_entity_name: "Target",
        target_entity_tag: crate::types::EntityTag::new(1),
        target_store_path: "demo::store::TargetStore",
        key_kind: &NAT_KIND,
        strength: crate::model::field::RelationStrength::Strong,
    };

    assert!(
        FieldKind::Map {
            key: &TEXT_KIND,
            value: &NAT_KIND,
        }
        .accepts_value(&Value::Map(vec![(
            Value::Text("a".into()),
            Value::Nat64(1)
        )]))
    );
    assert!(RELATION_KIND.accepts_value(&Value::Nat64(9)));
    assert!(!FieldKind::List(&TEXT_KIND).accepts_value(&Value::List(vec![Value::Nat64(1)])));
}

#[test]
fn grouped_having_numeric_canonicalization_keeps_numeric_relation_recursion() {
    static NAT_KIND: FieldKind = FieldKind::Nat64;
    static RELATION_KIND: FieldKind = FieldKind::Relation {
        target_path: "demo::Target",
        target_entity_name: "Target",
        target_entity_tag: crate::types::EntityTag::new(1),
        target_store_path: "demo::store::TargetStore",
        key_kind: &NAT_KIND,
        strength: crate::model::field::RelationStrength::Strong,
    };

    assert_eq!(
        canonicalize_grouped_having_numeric_literal_for_field_kind(
            Some(FieldKind::Int64),
            &Value::Nat64(7),
        ),
        Some(Value::Int64(7)),
    );
    assert_eq!(
        canonicalize_grouped_having_numeric_literal_for_field_kind(
            Some(RELATION_KIND),
            &Value::Int64(7),
        ),
        Some(Value::Nat64(7)),
    );
    assert_eq!(
        canonicalize_grouped_having_numeric_literal_for_field_kind(
            Some(FieldKind::Ulid),
            &Value::Text("01ARZ3NDEKTSV4RRFFQ69G5FAV".into()),
        ),
        None,
    );
}

#[test]
fn strict_sql_literal_canonicalization_adds_ulid_without_widening_other_kinds() {
    let ulid_text = "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string();

    std::assert_matches!(
        canonicalize_strict_sql_literal_for_kind(&FieldKind::Ulid, &Value::Text(ulid_text),),
        Some(Value::Ulid(_)),
    );
    assert_eq!(
        canonicalize_strict_sql_literal_for_kind(&FieldKind::Nat64, &Value::Int64(4)),
        Some(Value::Nat64(4)),
    );
    assert_eq!(
        canonicalize_strict_sql_literal_for_kind(
            &FieldKind::Text { max_len: None },
            &Value::Text("x".into())
        ),
        None,
    );
}
