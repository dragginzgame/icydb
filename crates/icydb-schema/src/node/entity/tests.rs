use super::*;
use crate::build::schema_write;

fn primitive_item(primitive: Primitive) -> Item {
    Item::new(
        ItemTarget::Primitive(primitive),
        None,
        None,
        None,
        None,
        &[],
        &[],
        false,
    )
}

fn relation_item(primitive: Primitive, target: &'static str) -> Item {
    Item::new(
        ItemTarget::Primitive(primitive),
        Some(target),
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

fn relation_field(ident: &'static str, primitive: Primitive, target: &'static str) -> Field {
    Field::new(
        ident,
        Value::new(Cardinality::One, relation_item(primitive, target)),
        None,
        None,
        None,
    )
}

fn store(path: &'static str) -> Store {
    Store::new_stable(
        Def::new("schema_entity_relation_edge", "Store"),
        "STORE",
        "schema_entity_relation_edge_store",
        path,
        StoreStableMemoryConfig::new(110, 111, 112),
    )
}

fn stable_store_in_module(module: &'static str, ident: &'static str) -> Store {
    Store::new_stable(
        Def::new(module, ident),
        "STORE",
        "schema_entity_relation_edge_store",
        "schema_entity_relation_edge_store",
        StoreStableMemoryConfig::new(120, 121, 122),
    )
}

fn heap_store_in_module(module: &'static str, ident: &'static str) -> Store {
    Store::new_heap(
        Def::new(module, ident),
        "HEAP_STORE",
        "schema_entity_relation_edge_heap_store",
        "schema_entity_relation_edge_heap_store",
        StoreHeapConfig::new(),
    )
}

fn entity(
    ident: &'static str,
    store_path: &'static str,
    pk_fields: &'static [&'static str],
    relations: &'static [RelationEdge],
    fields: &'static [Field],
) -> Entity {
    entity_in_module(
        "schema_entity_relation_edge",
        ident,
        pk_fields,
        store_path,
        relations,
        fields,
    )
}

fn entity_in_module(
    module: &'static str,
    ident: &'static str,
    pk_fields: &'static [&'static str],
    store_path: &'static str,
    relations: &'static [RelationEdge],
    fields: &'static [Field],
) -> Entity {
    Entity::new(
        Def::new(module, ident),
        store_path,
        1,
        PrimaryKey::new(pk_fields, PrimaryKeySource::External),
        None,
        &[],
        relations,
        FieldList::new(fields),
        Type::new(&[], &[]),
    )
}

#[test]
fn entity_validation_checks_owned_relation_edges() {
    let store_path = "schema_entity_relation_edge::Store";
    schema_write().insert_node(SchemaNode::Store(store(store_path)));
    let target_fields = Box::leak(
        vec![
            field("tenant_id", Primitive::Nat64),
            field("id", Primitive::Ulid),
        ]
        .into_boxed_slice(),
    );
    schema_write().insert_node(SchemaNode::Entity(entity(
        "User",
        store_path,
        &["tenant_id", "id"],
        &[],
        target_fields,
    )));

    let source_fields = Box::leak(
        vec![
            field("author_tenant_id", Primitive::Nat64),
            field("author_id", Primitive::Ulid),
        ]
        .into_boxed_slice(),
    );
    let source_relations = Box::leak(
        vec![RelationEdge::new(
            "author",
            "schema_entity_relation_edge::User",
            &["author_tenant_id", "author_id"],
        )]
        .into_boxed_slice(),
    );
    let source = entity(
        "Post",
        store_path,
        &["author_id"],
        source_relations,
        source_fields,
    );

    source
        .validate()
        .expect("entity-owned matching relation edge should validate");
}

#[test]
fn entity_validation_rejects_zero_schema_version() {
    let store_path = "schema_entity_relation_edge::Store";
    schema_write().insert_node(SchemaNode::Store(store(store_path)));
    let fields = Box::leak(vec![field("id", Primitive::Ulid)].into_boxed_slice());
    let mut source = entity("Versioned", store_path, &["id"], &[], fields);
    source.schema_version = 0;

    let err = source
        .validate()
        .expect_err("zero schema_version should fail schema node validation");
    assert!(
        err.to_string()
            .contains("entity schema_version must be a positive integer"),
        "unexpected schema_version validation error: {err}",
    );
}

#[test]
fn entity_validation_rejects_stable_source_relation_field_to_heap_target() {
    let module = "schema_entity_relation_field_stable_to_heap";
    let source_store_path = "schema_entity_relation_field_stable_to_heap::StableStore";
    let target_store_path = "schema_entity_relation_field_stable_to_heap::HeapStore";
    let target_path = "schema_entity_relation_field_stable_to_heap::User";
    schema_write().insert_node(SchemaNode::Store(stable_store_in_module(
        module,
        "StableStore",
    )));
    schema_write().insert_node(SchemaNode::Store(heap_store_in_module(module, "HeapStore")));
    schema_write().insert_node(SchemaNode::Entity(entity_in_module(
        module,
        "User",
        &["id"],
        target_store_path,
        &[],
        Box::leak(vec![field("id", Primitive::Ulid)].into_boxed_slice()),
    )));

    let source = entity_in_module(
        module,
        "Post",
        &["id"],
        source_store_path,
        &[],
        Box::leak(
            vec![
                field("id", Primitive::Ulid),
                relation_field("author_id", Primitive::Ulid, target_path),
            ]
            .into_boxed_slice(),
        ),
    );

    let err = source
        .validate()
        .expect_err("stable source relation into heap target should reject");
    assert_eq!(err.messages().len(), 1);
    assert!(err.children().is_empty());
}

#[test]
fn entity_validation_allows_heap_source_relation_field_to_heap_target() {
    let module = "schema_entity_relation_field_heap_to_heap";
    let store_path = "schema_entity_relation_field_heap_to_heap::HeapStore";
    let target_path = "schema_entity_relation_field_heap_to_heap::User";
    schema_write().insert_node(SchemaNode::Store(heap_store_in_module(module, "HeapStore")));
    schema_write().insert_node(SchemaNode::Entity(entity_in_module(
        module,
        "User",
        &["id"],
        store_path,
        &[],
        Box::leak(vec![field("id", Primitive::Ulid)].into_boxed_slice()),
    )));

    let source = entity_in_module(
        module,
        "Post",
        &["id"],
        store_path,
        &[],
        Box::leak(
            vec![
                field("id", Primitive::Ulid),
                relation_field("author_id", Primitive::Ulid, target_path),
            ]
            .into_boxed_slice(),
        ),
    );

    source
        .validate()
        .expect("heap source relation into heap target should keep live validation semantics");
}

#[test]
fn entity_validation_rejects_stable_source_relation_edge_to_heap_target() {
    let module = "schema_entity_relation_edge_stable_to_heap";
    let source_store_path = "schema_entity_relation_edge_stable_to_heap::StableStore";
    let target_store_path = "schema_entity_relation_edge_stable_to_heap::HeapStore";
    schema_write().insert_node(SchemaNode::Store(stable_store_in_module(
        module,
        "StableStore",
    )));
    schema_write().insert_node(SchemaNode::Store(heap_store_in_module(module, "HeapStore")));
    let target_fields = Box::leak(
        vec![
            field("tenant_id", Primitive::Nat64),
            field("id", Primitive::Ulid),
        ]
        .into_boxed_slice(),
    );
    schema_write().insert_node(SchemaNode::Entity(entity_in_module(
        module,
        "User",
        &["tenant_id", "id"],
        target_store_path,
        &[],
        target_fields,
    )));

    let source_relations = Box::leak(
        vec![RelationEdge::new(
            "author",
            "schema_entity_relation_edge_stable_to_heap::User",
            &["author_tenant_id", "author_id"],
        )]
        .into_boxed_slice(),
    );
    let source = entity_in_module(
        module,
        "Post",
        &["id"],
        source_store_path,
        source_relations,
        Box::leak(
            vec![
                field("id", Primitive::Ulid),
                field("author_tenant_id", Primitive::Nat64),
                field("author_id", Primitive::Ulid),
            ]
            .into_boxed_slice(),
        ),
    );

    let err = source
        .validate()
        .expect_err("stable source relation edge into heap target should reject");
    assert_eq!(err.messages().len(), 1);
    assert!(err.children().is_empty());
}

#[test]
fn entity_validation_reports_relation_edge_errors_under_relation_name() {
    let store_path = "schema_entity_relation_edge_error::Store";
    schema_write().insert_node(SchemaNode::Store(Store::new_stable(
        Def::new("schema_entity_relation_edge_error", "Store"),
        "STORE",
        "schema_entity_relation_edge_error_store",
        store_path,
        StoreStableMemoryConfig::new(113, 114, 115),
    )));
    let target_fields = Box::leak(
        vec![
            field("tenant_id", Primitive::Nat64),
            field("id", Primitive::Ulid),
        ]
        .into_boxed_slice(),
    );
    schema_write().insert_node(SchemaNode::Entity(entity(
        "User",
        store_path,
        &["tenant_id", "id"],
        &[],
        target_fields,
    )));

    let source_fields = Box::leak(vec![field("author_id", Primitive::Ulid)].into_boxed_slice());
    let source_relations = Box::leak(
        vec![RelationEdge::new(
            "author",
            "schema_entity_relation_edge_error::User",
            &["author_id"],
        )]
        .into_boxed_slice(),
    );
    let source = entity(
        "BrokenPost",
        store_path,
        &["author_id"],
        source_relations,
        source_fields,
    );

    let err = source
        .validate()
        .expect_err("entity validation should reject invalid relation edge");

    assert!(
        err.children().contains_key("author"),
        "relation edge errors should be nested under relation name: {err}",
    );
}
