//! Module: node::entity::tests
//! Responsibility: regression coverage for this module.
//! Does not own: production behavior.
//! Boundary: test-only contracts.

use super::{Entity, composite_primary_key_type_part, entity_typed_adapter_tokens};
use crate::authoring_types::Primitive;
use crate::node::{
    Def, Field, FieldList, FieldWriteManagement, HasSchemaPart, Index, Item, PrimaryKey,
    PrimaryKeySource, Relation, Type, ValidateNode, Value,
};
use darling::{FromMeta, ast::NestedMeta};
use proc_macro2::Span;
use quote::format_ident;
use quote::quote;
use syn::LitStr;

fn scalar_field(ident: &str) -> Field {
    primitive_field(ident, Primitive::Ulid)
}

fn primitive_field(ident: &str, primitive: Primitive) -> Field {
    Field {
        name: format_ident!("{ident}"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(primitive),
                ..Item::default()
            },
        },
        default: None,
        generated: None,
        write_management: None,
    }
}

fn many_scalar_field(ident: &str) -> Field {
    Field {
        name: format_ident!("{ident}"),
        value: Value {
            opt: false,
            many: true,
            item: Item {
                primitive: Some(Primitive::Text),
                unbounded: true,
                ..Item::default()
            },
        },
        default: None,
        generated: None,
        write_management: None,
    }
}

fn unit_field(ident: &str) -> Field {
    Field {
        name: format_ident!("{ident}"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(Primitive::Unit),
                ..Item::default()
            },
        },
        default: None,
        generated: None,
        write_management: None,
    }
}

fn field_list(values: &[&str]) -> Vec<LitStr> {
    values
        .iter()
        .map(|value| LitStr::new(value, Span::call_site()))
        .collect()
}

fn entity_with_fields_and_indexes(fields: Vec<Field>, indexes: Vec<Index>) -> Entity {
    Entity {
        def: Def::new(syn::parse_quote!(
            struct TestEntity;
        )),
        store: syn::parse_quote!(UiDataStore),
        schema_version: 1,
        primary_key: PrimaryKey {
            fields: vec![format_ident!("id")],
            source: PrimaryKeySource::Internal,
        },
        typed_adapters: false,
        indexes,
        relations: Vec::new(),
        constraints: Vec::new(),
        timestamps: false,
        fields: FieldList { fields },
        ty: Type::default(),
        traits: crate::trait_kind::TraitBuilder::default(),
    }
}

#[test]
fn scalar_primary_key_does_not_emit_generated_key_struct() {
    let entity = entity_with_fields_and_indexes(vec![scalar_field("id")], vec![]);

    assert!(composite_primary_key_type_part(&entity).is_empty());
}

#[test]
fn composite_primary_key_emits_deterministic_public_key_struct() {
    let mut entity = entity_with_fields_and_indexes(
        vec![scalar_field("tenant_id"), scalar_field("local_id")],
        vec![],
    );
    entity.primary_key.fields = vec![format_ident!("tenant_id"), format_ident!("local_id")];

    let tokens = composite_primary_key_type_part(&entity).to_string();

    assert!(
        tokens.contains("pub struct TestEntityKey"),
        "unexpected key struct tokens: {tokens}",
    );
    assert!(
        tokens.contains("pub tenant_id"),
        "unexpected key struct tokens: {tokens}",
    );
    assert!(
        tokens.contains("pub local_id"),
        "unexpected key struct tokens: {tokens}",
    );
}

#[test]
fn composite_primary_key_struct_implements_key_contracts() {
    let mut entity = entity_with_fields_and_indexes(
        vec![scalar_field("tenant_id"), scalar_field("local_id")],
        vec![],
    );
    entity.primary_key.fields = vec![format_ident!("tenant_id"), format_ident!("local_id")];

    let tokens = composite_primary_key_type_part(&entity).to_string();

    for expected in [
        "impl :: icydb :: __macro :: KeyValueCodec for TestEntityKey",
        "impl :: icydb :: __macro :: PrimaryKeyEncode for TestEntityKey",
        "impl :: icydb :: __macro :: PrimaryKeyDecode for TestEntityKey",
        "impl :: icydb :: __macro :: EntityKeyBytes for TestEntityKey",
    ] {
        assert!(
            tokens.contains(expected),
            "expected generated key contract `{expected}` in tokens: {tokens}",
        );
    }
}

#[test]
fn typed_adapter_generation_is_explicitly_opted_in() {
    let entity = entity_with_fields_and_indexes(vec![scalar_field("id")], vec![]);

    assert!(entity_typed_adapter_tokens(&entity).is_empty());
}

#[test]
fn typed_adapter_generation_separates_row_and_operation_shapes() {
    let mut created_at = primitive_field("created_at", Primitive::Timestamp);
    created_at.write_management = Some(FieldWriteManagement::CreatedAt);
    let mut entity = entity_with_fields_and_indexes(
        vec![
            scalar_field("id"),
            primitive_field("name", Primitive::Text),
            created_at,
        ],
        vec![],
    );
    entity.typed_adapters = true;

    let tokens = entity_typed_adapter_tokens(&entity).to_string();
    for expected in [
        "pub struct TestEntityInsert",
        "pub struct TestEntityPatch",
        "pub struct TestEntityReplace",
        "impl :: icydb :: db :: TypedEntityAdapter for TestEntity",
        "impl :: icydb :: db :: TypedRowAdapter for TestEntity",
        "impl :: icydb :: db :: TypedWriteAdapter for TestEntityInsert",
        "impl :: icydb :: db :: TypedWriteAdapter for TestEntityPatch",
        "impl :: icydb :: db :: TypedWriteAdapter for TestEntityReplace",
        "TypedFieldBindingRequest :: new",
        "TypedFieldType :: Scalar",
        "ScalarType :: Timestamp",
    ] {
        assert!(
            tokens.contains(expected),
            "expected generated adapter contract `{expected}` in tokens: {tokens}",
        );
    }
    assert!(
        !tokens.contains("pub created_at : :: icydb :: db :: WriteCell"),
        "managed fields must be absent from authored write inputs: {tokens}",
    );
}

#[test]
fn fatal_errors_validate_each_ordered_primary_key_field() {
    let mut entity = entity_with_fields_and_indexes(
        vec![scalar_field("id"), many_scalar_field("tenant_id")],
        vec![],
    );
    entity.primary_key.fields = vec![format_ident!("id"), format_ident!("tenant_id")];

    let errors = entity.fatal_errors();
    let error_text = errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        error_text.contains("primary key field 'tenant_id' must have cardinality One"),
        "unexpected fatal errors: {error_text}",
    );
}

#[test]
fn fatal_errors_reject_unit_inside_composite_primary_key() {
    let mut entity = entity_with_fields_and_indexes(
        vec![scalar_field("tenant_id"), unit_field("singleton")],
        vec![],
    );
    entity.primary_key.fields = vec![format_ident!("tenant_id"), format_ident!("singleton")];

    let errors = entity.fatal_errors();
    let error_text = errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        error_text.contains(
            "primary key field 'singleton' cannot use Unit inside a composite primary key"
        ),
        "unexpected fatal errors: {error_text}",
    );
}

#[test]
fn fatal_errors_admit_fixed_128_bit_primary_keys() {
    for primitive in [Primitive::Int128, Primitive::Nat128] {
        let entity = entity_with_fields_and_indexes(vec![primitive_field("id", primitive)], vec![]);

        let errors = entity.fatal_errors();

        assert!(
            errors.is_empty(),
            "fixed 128-bit primitive {primitive:?} should be primary-key admissible: {errors:?}",
        );
    }
}

#[test]
fn fatal_errors_reject_big_integer_primary_keys() {
    for primitive in [Primitive::IntBig, Primitive::NatBig] {
        let entity = entity_with_fields_and_indexes(vec![primitive_field("id", primitive)], vec![]);
        let error_text = entity
            .fatal_errors()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            error_text.contains("must use a scalar key primitive"),
            "bounded big integer primitive {primitive:?} must stay non-primary-key: {error_text}",
        );
    }
}

#[test]
fn fatal_errors_report_missing_ordered_primary_key_field() {
    let mut entity = entity_with_fields_and_indexes(vec![scalar_field("id")], vec![]);
    entity.primary_key.fields = vec![format_ident!("id"), format_ident!("tenant_id")];

    let errors = entity.fatal_errors();
    let error_text = errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        error_text.contains("primary key field 'tenant_id' not found in entity fields"),
        "unexpected fatal errors: {error_text}",
    );
}

#[test]
fn validate_rejects_index_field_not_found() {
    let entity = entity_with_fields_and_indexes(
        vec![scalar_field("id")],
        vec![Index {
            fields: field_list(&["missing_field"]),
            unique: false,
            predicate: None,
        }],
    );
    let err = entity
        .validate()
        .expect_err("missing index field should fail entity validation");
    assert!(
        err.to_string()
            .contains("index field 'missing_field' not found"),
        "unexpected validation error: {err}",
    );
}

#[test]
fn validate_rejects_many_cardinality_index_field() {
    let entity = entity_with_fields_and_indexes(
        vec![scalar_field("id"), many_scalar_field("tags")],
        vec![Index {
            fields: field_list(&["tags"]),
            unique: false,
            predicate: None,
        }],
    );
    let err = entity
        .validate()
        .expect_err("indexing many-cardinality fields should fail");
    assert!(
        err.to_string()
            .contains("cannot add an index field with many cardinality"),
        "unexpected validation error: {err}",
    );
}

#[test]
fn validate_rejects_expression_index_field_not_found() {
    let entity = entity_with_fields_and_indexes(
        vec![scalar_field("id"), scalar_field("email")],
        vec![Index {
            fields: field_list(&["LOWER(name)"]),
            unique: false,
            predicate: None,
        }],
    );
    let err = entity
        .validate()
        .expect_err("missing expression index field should fail entity validation");
    assert!(
        err.to_string().contains("index field 'name' not found"),
        "unexpected validation error: {err}",
    );
}

#[test]
fn from_list_parses_nested_indexes_and_fields() {
    let args = NestedMeta::parse_meta_list(quote!(
        store = "UiDataStore",
        version = 1,
        pk(fields = ["id"]),
        index(fields = ["missing_field"]),
        fields(field(
            name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ))
    ))
    .expect("entity args should parse");

    let node = Entity::from_list(&args).expect("entity meta should lower");

    assert_eq!(
        node.indexes.len(),
        1,
        "index(...) should parse into indexes"
    );
    assert_eq!(
        node.fields.len(),
        1,
        "omitting timestamps must not synthesize hidden fields"
    );
    assert!(
        node.fields.get(&format_ident!("id")).is_some(),
        "declared nested field should be preserved in the lowered field list",
    );
    assert!(node.fields.get(&format_ident!("created_at")).is_none());
    assert!(node.fields.get(&format_ident!("updated_at")).is_none());
}

#[test]
fn explicit_timestamps_lower_to_managed_fields() {
    let args = NestedMeta::parse_meta_list(quote!(
        store = "UiDataStore",
        version = 1,
        pk(fields = ["id"]),
        fields(field(
            name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        )),
        timestamps
    ))
    .expect("entity args should parse");

    let node = Entity::from_list(&args).expect("explicit timestamp policy should lower");

    assert_eq!(node.fields.len(), 3);
    assert!(!node.timestamps, "parser-only marker should be consumed");
    let created = node
        .fields
        .get(&format_ident!("created_at"))
        .expect("created field should be present");
    assert_eq!(created.name.to_string(), "created_at");
    assert_eq!(
        created.write_management,
        Some(FieldWriteManagement::CreatedAt)
    );
    let updated = node
        .fields
        .get(&format_ident!("updated_at"))
        .expect("updated field should be present");
    assert_eq!(updated.name.to_string(), "updated_at");
    assert_eq!(
        updated.write_management,
        Some(FieldWriteManagement::UpdatedAt)
    );
}

#[test]
fn explicit_timestamps_reject_fixed_field_name_collisions() {
    let args = NestedMeta::parse_meta_list(quote!(
        store = "UiDataStore",
        version = 1,
        pk(fields = ["id"]),
        fields(
            field(
                name = "id",
                value(item(prim = "Ulid")),
                generated(insert = "Ulid::generate")
            ),
            field(name = "created_at", value(item(prim = "Timestamp")))
        ),
        timestamps
    ))
    .expect("entity args should parse");

    let error = Entity::from_list(&args).expect_err("duplicate field name must reject");

    assert!(
        error
            .to_string()
            .contains("managed timestamp field 'created_at' conflicts"),
        "unexpected diagnostic: {error}",
    );
}

#[test]
fn from_list_parses_relation_edges() {
    let args = NestedMeta::parse_meta_list(quote!(
        store = "UiDataStore",
        version = 1,
        pk(fields = ["id"]),
        relation(
            name = "author",
            rel = "User",
            fields = ["author_tenant_id", "author_id"]
        ),
        fields(
            field(name = "id", value(item(prim = "Ulid"))),
            field(name = "author_tenant_id", value(item(prim = "Nat64"))),
            field(name = "author_id", value(item(prim = "Ulid")))
        )
    ))
    .expect("entity args should parse");

    let node = Entity::from_list(&args).expect("entity meta should lower");

    assert_eq!(node.relations.len(), 1);
    assert_eq!(node.relations[0].name.value(), "author");
    assert_eq!(
        node.relations[0]
            .fields
            .iter()
            .map(LitStr::value)
            .collect::<Vec<_>>(),
        ["author_tenant_id", "author_id"],
    );
}

#[test]
fn schema_part_emits_relation_edge_metadata() {
    let mut entity = entity_with_fields_and_indexes(
        vec![
            scalar_field("id"),
            primitive_field("author_tenant_id", Primitive::Nat64),
            scalar_field("author_id"),
        ],
        vec![],
    );
    entity.relations.push(Relation {
        name: LitStr::new("author", Span::call_site()),
        target: syn::parse_quote!(User),
        fields: field_list(&["author_tenant_id", "author_id"]),
    });

    let tokens = entity.schema_part().to_string();

    assert!(
        tokens.contains("RelationEdge :: new"),
        "unexpected schema tokens: {tokens}",
    );
    assert!(
        tokens.contains("const __RELATIONS"),
        "unexpected schema tokens: {tokens}",
    );
}
