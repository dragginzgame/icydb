use super::*;
use crate::build::schema_write;

fn primitive_item(primitive: Primitive) -> Item {
    Item::new(
        ItemTarget::Primitive(primitive),
        None,
        None,
        None,
        &[],
        &[],
        false,
    )
}

fn item_with_metadata(primitive: Primitive, scale: Option<u32>, max_len: Option<u32>) -> Item {
    Item::new(
        ItemTarget::Primitive(primitive),
        None,
        scale,
        max_len,
        &[],
        &[],
        false,
    )
}

fn field(ident: &'static str, primitive: Primitive) -> Field {
    field_with_item(ident, primitive_item(primitive))
}

fn generated_field(ident: &'static str, primitive: Primitive) -> Field {
    Field::new(
        ident,
        Value::new(Cardinality::One, primitive_item(primitive)),
        None,
        Some(FieldGeneration::Insert(Arg::FuncPath(
            "generate_relation_component",
        ))),
        None,
    )
}

fn field_with_item(ident: &'static str, item: Item) -> Field {
    Field::new(ident, Value::new(Cardinality::One, item), None, None, None)
}

fn optional_field(ident: &'static str, primitive: Primitive) -> Field {
    Field::new(
        ident,
        Value::new(Cardinality::Opt, primitive_item(primitive)),
        None,
        None,
        None,
    )
}

fn entity(
    module: &'static str,
    ident: &'static str,
    pk_fields: &'static [&'static str],
    fields: &'static [Field],
) -> Entity {
    Entity::new(
        Def::new(module, ident),
        "RelationEdgeStore",
        1,
        PrimaryKey::new(pk_fields, PrimaryKeySource::External),
        &[],
        &[],
        &[],
        FieldList::new(fields),
        Type::new(&[], &[], &[]),
    )
}

fn insert_entity(
    module: &'static str,
    ident: &'static str,
    pk_fields: &'static [&'static str],
    fields: &'static [Field],
) -> (&'static str, Entity) {
    let path = Box::leak(format!("{module}::{ident}").into_boxed_str());
    let entity = entity(module, ident, pk_fields, fields);
    schema_write().insert_node(SchemaNode::Entity(entity.clone()));
    (path, entity)
}

#[test]
fn relation_edge_accepts_ordered_composite_target_tuple() {
    let source_fields = Box::leak(
        vec![
            field("author_tenant_id", Primitive::Nat64),
            field("author_user_id", Primitive::Ulid),
        ]
        .into_boxed_slice(),
    );
    let target_fields = Box::leak(
        vec![
            field("tenant_id", Primitive::Nat64),
            field("user_id", Primitive::Ulid),
        ]
        .into_boxed_slice(),
    );
    let source = entity(
        "schema_relation_edge_accepts_tuple",
        "Post",
        &["author_user_id"],
        source_fields,
    );
    let target = entity(
        "schema_relation_edge_accepts_tuple",
        "User",
        &["tenant_id", "user_id"],
        target_fields,
    );

    RelationEdge::new(
        "author",
        "schema_relation_edge_accepts_tuple::User",
        &["author_tenant_id", "author_user_id"],
    )
    .validate_against_entities(&source, &target)
    .expect("matching ordered composite relation tuple should validate");
}

#[test]
fn relation_edge_rejects_scalar_local_field_for_composite_target() {
    let source_fields =
        Box::leak(vec![field("author_user_id", Primitive::Ulid)].into_boxed_slice());
    let target_fields = Box::leak(
        vec![
            field("tenant_id", Primitive::Nat64),
            field("user_id", Primitive::Ulid),
        ]
        .into_boxed_slice(),
    );
    let source = entity(
        "schema_relation_edge_rejects_scalar_for_composite",
        "Post",
        &["author_user_id"],
        source_fields,
    );
    let target = entity(
        "schema_relation_edge_rejects_scalar_for_composite",
        "User",
        &["tenant_id", "user_id"],
        target_fields,
    );

    let err = RelationEdge::new(
        "author",
        "schema_relation_edge_rejects_scalar_for_composite::User",
        &["author_user_id"],
    )
    .validate_against_entities(&source, &target)
    .expect_err("scalar local component must not validate as composite target tuple");

    assert!(
        err.messages()
            .iter()
            .any(|message| message.contains("arity mismatch")),
        "unexpected relation edge validation errors: {err}",
    );
}

#[test]
fn relation_edge_rejects_wrong_component_order() {
    let source_fields = Box::leak(
        vec![
            field("author_tenant_id", Primitive::Nat64),
            field("author_user_id", Primitive::Ulid),
        ]
        .into_boxed_slice(),
    );
    let target_fields = Box::leak(
        vec![
            field("tenant_id", Primitive::Nat64),
            field("user_id", Primitive::Ulid),
        ]
        .into_boxed_slice(),
    );
    let source = entity(
        "schema_relation_edge_rejects_order",
        "Post",
        &["author_user_id"],
        source_fields,
    );
    let target = entity(
        "schema_relation_edge_rejects_order",
        "User",
        &["tenant_id", "user_id"],
        target_fields,
    );

    let err = RelationEdge::new(
        "author",
        "schema_relation_edge_rejects_order::User",
        &["author_user_id", "author_tenant_id"],
    )
    .validate_against_entities(&source, &target)
    .expect_err("local tuple order must match target primary-key order");

    assert!(
        err.messages()
            .iter()
            .any(|message| message.contains("component 0 type mismatch")),
        "unexpected relation edge validation errors: {err}",
    );
}

#[test]
fn relation_edge_rejects_missing_local_component_field() {
    let source_fields =
        Box::leak(vec![field("author_tenant_id", Primitive::Nat64)].into_boxed_slice());
    let target_fields = Box::leak(
        vec![
            field("tenant_id", Primitive::Nat64),
            field("user_id", Primitive::Ulid),
        ]
        .into_boxed_slice(),
    );
    let source = entity(
        "schema_relation_edge_rejects_missing_local",
        "Post",
        &["author_tenant_id"],
        source_fields,
    );
    let target = entity(
        "schema_relation_edge_rejects_missing_local",
        "User",
        &["tenant_id", "user_id"],
        target_fields,
    );

    let err = RelationEdge::new(
        "author",
        "schema_relation_edge_rejects_missing_local::User",
        &["author_tenant_id", "author_user_id"],
    )
    .validate_against_entities(&source, &target)
    .expect_err("missing local tuple component should reject");

    assert!(
        err.messages()
            .iter()
            .any(|message| message.contains("local field 'author_user_id' not found")),
        "unexpected relation edge validation errors: {err}",
    );
}

#[test]
fn relation_edge_rejects_non_admissible_target_primary_key_component() {
    let source_fields =
        Box::leak(vec![field("author_score", Primitive::Float64)].into_boxed_slice());
    let target_fields = Box::leak(vec![field("score", Primitive::Float64)].into_boxed_slice());
    let source = entity(
        "schema_relation_edge_rejects_float64_target",
        "Post",
        &["author_score"],
        source_fields,
    );
    let target = entity(
        "schema_relation_edge_rejects_float64_target",
        "User",
        &["score"],
        target_fields,
    );

    let err = RelationEdge::new(
        "author",
        "schema_relation_edge_rejects_float64_target::User",
        &["author_score"],
    )
    .validate_against_entities(&source, &target)
    .expect_err("float64 target primary key component should reject");

    assert!(
        err.messages()
            .iter()
            .any(|message| message.contains("non-admissible component")),
        "unexpected relation edge validation errors: {err}",
    );
}

#[test]
fn relation_edge_rejects_generated_local_component_field() {
    let source_fields =
        Box::leak(vec![generated_field("author_id", Primitive::Ulid)].into_boxed_slice());
    let target_fields = Box::leak(vec![field("id", Primitive::Ulid)].into_boxed_slice());
    let source = entity(
        "schema_relation_edge_rejects_generated_local",
        "Post",
        &["author_id"],
        source_fields,
    );
    let target = entity(
        "schema_relation_edge_rejects_generated_local",
        "User",
        &["id"],
        target_fields,
    );

    let err = RelationEdge::new(
        "author",
        "schema_relation_edge_rejects_generated_local::User",
        &["author_id"],
    )
    .validate_against_entities(&source, &target)
    .expect_err("generated local component field should reject");

    assert!(
        err.messages()
            .iter()
            .any(|message| message.contains("is generated")),
        "unexpected relation edge validation errors: {err}",
    );
}

#[test]
fn relation_edge_rejects_mixed_local_component_cardinality() {
    let source_fields = Box::leak(
        vec![
            field("author_tenant_id", Primitive::Nat64),
            optional_field("author_user_id", Primitive::Ulid),
        ]
        .into_boxed_slice(),
    );
    let target_fields = Box::leak(
        vec![
            field("tenant_id", Primitive::Nat64),
            field("user_id", Primitive::Ulid),
        ]
        .into_boxed_slice(),
    );
    let source = entity(
        "schema_relation_edge_rejects_mixed_cardinality",
        "Post",
        &["author_tenant_id"],
        source_fields,
    );
    let target = entity(
        "schema_relation_edge_rejects_mixed_cardinality",
        "User",
        &["tenant_id", "user_id"],
        target_fields,
    );

    let err = RelationEdge::new(
        "author",
        "schema_relation_edge_rejects_mixed_cardinality::User",
        &["author_tenant_id", "author_user_id"],
    )
    .validate_against_entities(&source, &target)
    .expect_err("mixed local tuple cardinality should reject");

    assert!(
        err.messages()
            .iter()
            .any(|message| message.contains("cardinality mismatch")),
        "unexpected relation edge validation errors: {err}",
    );
}

#[test]
fn relation_edge_validate_for_source_uses_schema_target_lookup() {
    let source_fields = Box::leak(vec![field("author_id", Primitive::Ulid)].into_boxed_slice());
    let target_fields = Box::leak(vec![field("id", Primitive::Ulid)].into_boxed_slice());
    let source = entity(
        "schema_relation_edge_lookup",
        "Post",
        &["author_id"],
        source_fields,
    );
    let (target_path, _) = insert_entity(
        "schema_relation_edge_lookup",
        "User",
        &["id"],
        target_fields,
    );

    RelationEdge::new("author", target_path, &["author_id"])
        .validate_for_source(&source)
        .expect("schema target lookup should validate matching scalar edge");
}

#[test]
fn relation_edge_component_contract_preserves_bounds() {
    let expected = field_with_item("body", item_with_metadata(Primitive::Text, None, Some(64)));
    let same = field_with_item(
        "body_copy",
        item_with_metadata(Primitive::Text, None, Some(64)),
    );
    let wrong = field_with_item(
        "body_short",
        item_with_metadata(Primitive::Text, None, Some(32)),
    );

    let expected = RelationComponentContract::from_field(&expected);
    assert!(!expected.mismatches(RelationComponentContract::from_field(&same)));
    assert!(expected.mismatches(RelationComponentContract::from_field(&wrong)));
}
