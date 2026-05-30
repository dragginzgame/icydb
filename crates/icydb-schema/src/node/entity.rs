use crate::prelude::*;
use std::any::Any;

///
/// Entity
///

#[derive(Clone, Debug, Serialize)]
pub struct Entity {
    def: Def,
    store: &'static str,
    primary_key: PrimaryKey,

    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'static str>,

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    indexes: &'static [Index],

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    relations: &'static [RelationEdge],

    fields: FieldList,
    ty: Type,
}

impl Entity {
    #[must_use]
    #[expect(
        clippy::too_many_arguments,
        reason = "schema entity construction keeps store, key, index, relation, field, and type metadata explicit"
    )]
    pub const fn new(
        def: Def,
        store: &'static str,
        primary_key: PrimaryKey,
        name: Option<&'static str>,
        indexes: &'static [Index],
        relations: &'static [RelationEdge],
        fields: FieldList,
        ty: Type,
    ) -> Self {
        Self {
            def,
            store,
            primary_key,
            name,
            indexes,
            relations,
            fields,
            ty,
        }
    }

    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    #[must_use]
    pub const fn store(&self) -> &'static str {
        self.store
    }

    #[must_use]
    pub const fn primary_key(&self) -> &PrimaryKey {
        &self.primary_key
    }

    #[must_use]
    pub const fn name(&self) -> Option<&'static str> {
        self.name
    }

    #[must_use]
    pub const fn indexes(&self) -> &'static [Index] {
        self.indexes
    }

    #[must_use]
    pub const fn relations(&self) -> &'static [RelationEdge] {
        self.relations
    }

    #[must_use]
    pub const fn fields(&self) -> &FieldList {
        &self.fields
    }

    #[must_use]
    pub const fn ty(&self) -> &Type {
        &self.ty
    }

    /// Return the scalar primary key field if this entity uses a scalar
    /// primary-key contract.
    #[must_use]
    pub fn scalar_primary_key_field(&self) -> Option<&Field> {
        self.fields().get(self.primary_key().scalar_field()?)
    }

    /// Resolve the entity name used for schema identity.
    #[must_use]
    pub fn resolved_name(&self) -> &'static str {
        self.name().unwrap_or_else(|| self.def().ident())
    }

    fn validate_relation_storage_policy(&self, errs: &mut ErrorTree) {
        for field in self.fields().fields() {
            if let Some(target) = field.value().item().relation() {
                self.validate_relation_target_storage_policy(errs, field.ident(), target);
            }
        }

        for relation in self.relations() {
            self.validate_relation_target_storage_policy(errs, relation.ident(), relation.target());
        }
    }

    fn validate_relation_target_storage_policy(
        &self,
        errs: &mut ErrorTree,
        relation_name: &str,
        target_path: &str,
    ) {
        let schema = schema_read();
        let Ok(source_store) = schema.cast_node::<Store>(self.store()) else {
            return;
        };
        let Ok(target) = schema.cast_node::<Self>(target_path) else {
            return;
        };
        let Ok(target_store) = schema.cast_node::<Store>(target.store()) else {
            return;
        };

        if source_store.is_stable_storage() && target_store.is_heap_storage() {
            err!(
                errs,
                "relation '{}' from stable store '{}' to heap target store '{}' is not supported in 0.169; stable stores cannot own referential integrity against volatile heap targets",
                relation_name,
                self.store(),
                target.store(),
            );
        }
    }
}

impl MacroNode for Entity {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ValidateNode for Entity {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        // store
        match schema.cast_node::<Store>(self.store()) {
            Ok(_) => {}
            Err(e) => errs.add(e),
        }

        for relation in self.relations() {
            if let Err(e) = relation.validate_for_source(self) {
                errs.merge_for(relation.ident(), e);
            }
        }
        self.validate_relation_storage_policy(&mut errs);

        errs.result()
    }
}

impl VisitableNode for Entity {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
        self.fields().accept(v);
        self.ty().accept(v);
    }
}

#[cfg(test)]
mod tests {
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
}
