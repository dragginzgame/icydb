use crate::prelude::*;

///
/// RelationComponentContract
///
/// Schema-side type contract for one relation key component.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RelationComponentContract<'a> {
    target: &'a ItemTarget,
    scale: Option<u32>,
    max_len: Option<u32>,
    max_bytes: Option<u32>,
}

impl<'a> RelationComponentContract<'a> {
    pub(crate) const fn from_field(field: &'a Field) -> Self {
        Self::from_item(field.value().item())
    }

    pub(crate) const fn from_item(item: &'a Item) -> Self {
        Self {
            target: item.target(),
            scale: item.scale(),
            max_len: item.max_len(),
            max_bytes: item.max_bytes(),
        }
    }

    pub(crate) const fn target(&self) -> &'a ItemTarget {
        self.target
    }

    pub(crate) const fn scale(&self) -> Option<u32> {
        self.scale
    }

    pub(crate) const fn max_len(&self) -> Option<u32> {
        self.max_len
    }

    pub(crate) const fn max_bytes(&self) -> Option<u32> {
        self.max_bytes
    }

    pub(crate) fn mismatches(self, other: Self) -> bool {
        self != other
    }
}

///
/// RelationEdge
///
/// Schema-side relation edge declaration over one or more local component
/// fields. Runtime acceptance still owns durable field IDs and slots; this
/// helper proves arity/order/kind compatibility before a tuple relation shape
/// can be admitted.
///

#[derive(Clone, Debug, Serialize)]
pub struct RelationEdge {
    ident: &'static str,
    target: &'static str,
    local_fields: &'static [&'static str],
}

impl RelationEdge {
    /// Build one relation-edge declaration from a relation name, target entity
    /// path, and ordered local component fields.
    #[must_use]
    pub const fn new(
        ident: &'static str,
        target: &'static str,
        local_fields: &'static [&'static str],
    ) -> Self {
        Self {
            ident,
            target,
            local_fields,
        }
    }

    /// Borrow the relation-edge name used by diagnostics.
    #[must_use]
    pub const fn ident(&self) -> &'static str {
        self.ident
    }

    /// Borrow the target entity path.
    #[must_use]
    pub const fn target(&self) -> &'static str {
        self.target
    }

    /// Borrow ordered local source fields that map to the target primary key.
    #[must_use]
    pub const fn local_fields(&self) -> &'static [&'static str] {
        self.local_fields
    }

    /// Validate this edge against one source entity and the target entity
    /// stored in the current schema graph.
    pub fn validate_for_source(&self, source: &Entity) -> Result<(), ErrorTree> {
        let schema = schema_read();

        match schema.cast_node::<Entity>(self.target()) {
            Ok(target) => self.validate_against_entities(source, target),
            Err(_) => Err(ErrorTree::from(format!(
                "relation edge '{}' target entity '{}' not found",
                self.ident(),
                self.target()
            ))),
        }
    }

    /// Validate this edge against explicit source and target entity metadata.
    pub fn validate_against_entities(
        &self,
        source: &Entity,
        target: &Entity,
    ) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let target_fields = target.primary_key().fields();

        if self.local_fields().is_empty() {
            err!(
                errs,
                "relation edge '{}' must declare at least one local field",
                self.ident()
            );
        }

        if self.local_fields().len() != target_fields.len() {
            err!(
                errs,
                "relation edge '{}' arity mismatch: local fields {:?} target primary key fields {:?}",
                self.ident(),
                self.local_fields(),
                target_fields,
            );
            return errs.result();
        }

        let mut local_component_cardinality = None;
        for (index, (local_name, target_name)) in self
            .local_fields()
            .iter()
            .zip(target_fields.iter())
            .enumerate()
        {
            let Some(local_field) = source.fields().get(local_name) else {
                err!(
                    errs,
                    "relation edge '{}' local field '{}' not found",
                    self.ident(),
                    local_name
                );
                continue;
            };
            let Some(target_field) = target.fields().get(target_name) else {
                err!(
                    errs,
                    "relation edge '{}' target primary key field '{}' not found",
                    self.ident(),
                    target_name
                );
                continue;
            };

            if !self.validate_local_component_shape(
                &mut errs,
                local_name,
                local_field,
                &mut local_component_cardinality,
            ) {
                continue;
            }

            self.validate_component_contract(
                &mut errs,
                index,
                local_name,
                local_field,
                target_name,
                target_field,
            );
        }

        errs.result()
    }

    fn validate_local_component_shape(
        &self,
        errs: &mut ErrorTree,
        local_name: &str,
        local_field: &Field,
        local_component_cardinality: &mut Option<Cardinality>,
    ) -> bool {
        let local_cardinality = local_field.value().cardinality();
        if local_cardinality == Cardinality::Many {
            err!(
                errs,
                "relation edge '{}' local field '{}' cannot have many cardinality",
                self.ident(),
                local_name
            );
            return false;
        }
        match *local_component_cardinality {
            Some(expected) if expected != local_cardinality => {
                err!(
                    errs,
                    "relation edge '{}' local field '{}' cardinality mismatch: all local component fields must be required or all optional",
                    self.ident(),
                    local_name
                );
                return false;
            }
            Some(_) => {}
            None => *local_component_cardinality = Some(local_cardinality),
        }

        if local_field.generated().is_some() {
            err!(
                errs,
                "relation edge '{}' local field '{}' is generated and cannot be a relation component",
                self.ident(),
                local_name
            );
            return false;
        }

        true
    }

    fn validate_component_contract(
        &self,
        errs: &mut ErrorTree,
        index: usize,
        local_name: &str,
        local_field: &Field,
        target_name: &str,
        target_field: &Field,
    ) {
        let expected = RelationComponentContract::from_field(target_field);
        if !target_primary_key_component_is_admissible(expected) {
            err!(
                errs,
                "relation edge '{}' target primary key field '{}' uses non-admissible component {:?}",
                self.ident(),
                target_name,
                expected.target(),
            );
            return;
        }

        let actual = RelationComponentContract::from_field(local_field);
        if expected.mismatches(actual) {
            err!(
                errs,
                "relation edge '{}' component {index} type mismatch: local field '{}' has ({:?}, scale={:?}, max_len={:?}, max_bytes={:?}); target field '{}' requires ({:?}, scale={:?}, max_len={:?}, max_bytes={:?})",
                self.ident(),
                local_name,
                actual.target(),
                actual.scale(),
                actual.max_len(),
                actual.max_bytes(),
                target_name,
                expected.target(),
                expected.scale(),
                expected.max_len(),
                expected.max_bytes(),
            );
        }
    }
}

const fn target_primary_key_component_is_admissible(
    contract: RelationComponentContract<'_>,
) -> bool {
    match contract.target() {
        ItemTarget::Primitive(primitive) => primitive.is_primary_key_component_encodable(),
        ItemTarget::Is(_) => false,
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

    fn item_with_metadata(
        primitive: Primitive,
        scale: Option<u32>,
        max_len: Option<u32>,
        max_bytes: Option<u32>,
    ) -> Item {
        Item::new(
            ItemTarget::Primitive(primitive),
            None,
            scale,
            max_len,
            max_bytes,
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
            None,
            &[],
            &[],
            FieldList::new(fields),
            Type::new(&[], &[]),
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
            Box::leak(vec![field("author_score", Primitive::IntBig)].into_boxed_slice());
        let target_fields = Box::leak(vec![field("score", Primitive::IntBig)].into_boxed_slice());
        let source = entity(
            "schema_relation_edge_rejects_int_big_target",
            "Post",
            &["author_score"],
            source_fields,
        );
        let target = entity(
            "schema_relation_edge_rejects_int_big_target",
            "User",
            &["score"],
            target_fields,
        );

        let err = RelationEdge::new(
            "author",
            "schema_relation_edge_rejects_int_big_target::User",
            &["author_score"],
        )
        .validate_against_entities(&source, &target)
        .expect_err("int_big target primary key component should reject");

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
        let expected = field_with_item(
            "body",
            item_with_metadata(Primitive::Text, None, Some(64), None),
        );
        let same = field_with_item(
            "body_copy",
            item_with_metadata(Primitive::Text, None, Some(64), None),
        );
        let wrong = field_with_item(
            "body_short",
            item_with_metadata(Primitive::Text, None, Some(32), None),
        );

        let expected = RelationComponentContract::from_field(&expected);
        assert!(!expected.mismatches(RelationComponentContract::from_field(&same)));
        assert!(expected.mismatches(RelationComponentContract::from_field(&wrong)));
    }
}
