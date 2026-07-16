use super::*;
use crate::build::schema_write;

fn primitive_item(primitive: Primitive) -> Item {
    Item::new(
        ItemTarget::Primitive(primitive),
        None,
        RelationEnforcement::Enforced,
        None,
        None,
        None,
        &[],
        &[],
        false,
    )
}

fn relation_item(target_path: &'static str, primitive: Primitive) -> Item {
    Item::new(
        ItemTarget::Primitive(primitive),
        Some(target_path),
        RelationEnforcement::Enforced,
        None,
        None,
        None,
        &[],
        &[],
        false,
    )
}

fn field(ident: &'static str, primitive: Primitive) -> Field {
    Field::new(
        ident,
        Value::new(Cardinality::One, primitive_item(primitive)),
        None,
        None,
        None,
    )
}

fn item_with_metadata(
    primitive: Primitive,
    scale: Option<u32>,
    max_len: Option<u32>,
    max_bytes: Option<u32>,
) -> Item {
    Item::new(
        ItemTarget::Primitive(primitive),
        None,
        RelationEnforcement::Enforced,
        scale,
        max_len,
        max_bytes,
        &[],
        &[],
        false,
    )
}

fn insert_entity(
    module: &'static str,
    ident: &'static str,
    pk_fields: &'static [&'static str],
    fields: &'static [Field],
) -> &'static str {
    let path = Box::leak(format!("{module}::{ident}").into_boxed_str());
    schema_write().insert_node(SchemaNode::Entity(Entity::new(
        Def::new(module, ident),
        "SchemaItemRelationStore",
        1,
        PrimaryKey::new(pk_fields, PrimaryKeySource::External),
        None,
        &[],
        &[],
        FieldList::new(fields),
        Type::new(&[], &[]),
    )));
    path
}

#[test]
fn relation_item_carries_explicit_enforcement() {
    let item = Item::new(
        ItemTarget::Primitive(Primitive::Ulid),
        Some("schema_item_relation_enforcement::Target"),
        RelationEnforcement::Unchecked,
        None,
        None,
        None,
        &[],
        &[],
        false,
    );

    assert_eq!(item.enforcement(), RelationEnforcement::Unchecked);
}

#[test]
fn unchecked_enforcement_requires_a_relation() {
    let item = Item::new(
        ItemTarget::Primitive(Primitive::Ulid),
        None,
        RelationEnforcement::Unchecked,
        None,
        None,
        None,
        &[],
        &[],
        false,
    );

    assert!(item.validate().is_err());
}

#[test]
fn relation_to_composite_target_rejects_even_when_first_component_matches() {
    let fields = Box::leak(
        vec![
            field("tenant_id", Primitive::Nat64),
            field("local_id", Primitive::Nat64),
        ]
        .into_boxed_slice(),
    );
    let target_path = insert_entity(
        "schema_item_relation_composite_target",
        "CompositeTarget",
        &["tenant_id", "local_id"],
        fields,
    );

    let err = relation_item(target_path, Primitive::Nat64)
        .validate()
        .expect_err("relation to composite target must fail before first-field matching");

    assert!(
        err.messages().iter().any(|message| {
            message.contains("uses composite primary key fields")
                && message.contains("single-field relation targets require a scalar primary key")
        }),
        "unexpected relation validation errors: {err}",
    );
}

#[test]
fn scalar_128_bit_relation_targets_validate_at_schema_node_boundary() {
    for (module, ident, primitive) in [
        (
            "schema_item_relation_int128_target",
            "Int128Target",
            Primitive::Int128,
        ),
        (
            "schema_item_relation_nat128_target",
            "Nat128Target",
            Primitive::Nat128,
        ),
    ] {
        let fields = Box::leak(vec![field("id", primitive)].into_boxed_slice());
        let target_path = insert_entity(module, ident, &["id"], fields);

        relation_item(target_path, primitive)
            .validate()
            .expect("scalar 128-bit relation target should validate");
    }
}

#[test]
fn scalar_relation_target_descriptor_compares_type_and_bounds() {
    for (primitive, expected_metadata, wrong_metadata) in [
        (
            Primitive::Decimal,
            (Some(4), None, None),
            (Some(2), None, None),
        ),
        (
            Primitive::Text,
            (None, Some(64), None),
            (None, Some(32), None),
        ),
        (
            Primitive::IntBig,
            (None, None, Some(32)),
            (None, None, Some(16)),
        ),
    ] {
        let expected = item_with_metadata(
            primitive,
            expected_metadata.0,
            expected_metadata.1,
            expected_metadata.2,
        );
        let same = item_with_metadata(
            primitive,
            expected_metadata.0,
            expected_metadata.1,
            expected_metadata.2,
        );
        let wrong_bounds = item_with_metadata(
            primitive,
            wrong_metadata.0,
            wrong_metadata.1,
            wrong_metadata.2,
        );
        let wrong_target = item_with_metadata(
            Primitive::Nat64,
            expected_metadata.0,
            expected_metadata.1,
            expected_metadata.2,
        );

        let expected = RelationComponentContract::from_item(&expected);
        assert!(!expected.mismatches(RelationComponentContract::from_item(&same)));
        assert!(expected.mismatches(RelationComponentContract::from_item(&wrong_bounds)));
        assert!(expected.mismatches(RelationComponentContract::from_item(&wrong_target)));
    }
}

#[test]
fn scalar_relation_target_validation_rejects_mismatched_scalar_kind() {
    let fields = Box::leak(vec![field("id", Primitive::Nat64)].into_boxed_slice());
    let target_path = insert_entity(
        "schema_item_relation_scalar_target_mismatch",
        "Nat64Target",
        &["id"],
        fields,
    );

    let err = relation_item(target_path, Primitive::Int64)
        .validate()
        .expect_err("mismatched scalar relation target should reject");

    assert!(
        err.messages()
            .iter()
            .any(|message| message.contains("relation target type mismatch")),
        "unexpected relation validation errors: {err}",
    );
}

#[test]
fn scalar_relation_target_validation_accepts_matching_scalar_kind() {
    let fields = Box::leak(vec![field("id", Primitive::Nat64)].into_boxed_slice());
    let target_path = insert_entity(
        "schema_item_relation_scalar_target_match",
        "Nat64Target",
        &["id"],
        fields,
    );

    relation_item(target_path, Primitive::Nat64)
        .validate()
        .expect("matching scalar relation target should validate");
}

#[test]
fn scalar_relation_target_from_field_preserves_metadata_descriptor() {
    let field = Field::new(
        "id",
        Value::new(
            Cardinality::One,
            item_with_metadata(Primitive::Text, None, Some(64), None),
        ),
        None,
        None,
        None,
    );

    let descriptor = RelationComponentContract::from_field(&field);
    assert_eq!(descriptor.target(), &ItemTarget::Primitive(Primitive::Text));
    assert_eq!(descriptor.scale(), None);
    assert_eq!(descriptor.max_len(), Some(64));
    assert_eq!(descriptor.max_bytes(), None);
}
