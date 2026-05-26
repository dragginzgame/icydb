use crate::prelude::*;
use std::ops::Not;

///
/// Item
///
/// Canonical schema item descriptor for one scalar, relation, or primitive
/// field target plus its attached sanitizers and validators.
///

#[derive(Clone, Debug, Serialize)]
pub struct Item {
    target: ItemTarget,

    #[serde(skip_serializing_if = "Option::is_none")]
    relation: Option<&'static str>,

    #[serde(skip_serializing_if = "Option::is_none")]
    scale: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    max_len: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    max_bytes: Option<u32>,

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    validators: &'static [TypeValidator],

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    sanitizers: &'static [TypeSanitizer],

    #[serde(skip_serializing_if = "Not::not")]
    indirect: bool,
}

impl Item {
    #[must_use]
    #[expect(
        clippy::too_many_arguments,
        reason = "schema item construction keeps generated scalar, relation, and validation metadata explicit"
    )]
    pub const fn new(
        target: ItemTarget,
        relation: Option<&'static str>,
        scale: Option<u32>,
        max_len: Option<u32>,
        max_bytes: Option<u32>,
        validators: &'static [TypeValidator],
        sanitizers: &'static [TypeSanitizer],
        indirect: bool,
    ) -> Self {
        Self {
            target,
            relation,
            scale,
            max_len,
            max_bytes,
            validators,
            sanitizers,
            indirect,
        }
    }

    #[must_use]
    pub const fn target(&self) -> &ItemTarget {
        &self.target
    }

    #[must_use]
    pub const fn relation(&self) -> Option<&'static str> {
        self.relation
    }

    #[must_use]
    pub const fn scale(&self) -> Option<u32> {
        self.scale
    }

    #[must_use]
    pub const fn max_len(&self) -> Option<u32> {
        self.max_len
    }

    #[must_use]
    pub const fn max_bytes(&self) -> Option<u32> {
        self.max_bytes
    }

    #[must_use]
    pub const fn validators(&self) -> &'static [TypeValidator] {
        self.validators
    }

    #[must_use]
    pub const fn sanitizers(&self) -> &'static [TypeSanitizer] {
        self.sanitizers
    }

    #[must_use]
    pub const fn indirect(&self) -> bool {
        self.indirect
    }

    #[must_use]
    pub const fn is_relation(&self) -> bool {
        self.relation().is_some()
    }
}

struct ScalarRelationTarget<'a> {
    target: &'a ItemTarget,
    scale: Option<u32>,
    max_len: Option<u32>,
    max_bytes: Option<u32>,
}

impl<'a> ScalarRelationTarget<'a> {
    const fn from_field(field: &'a Field) -> Self {
        let item = field.value().item();

        Self {
            target: item.target(),
            scale: item.scale(),
            max_len: item.max_len(),
            max_bytes: item.max_bytes(),
        }
    }

    const fn from_item(item: &'a Item) -> Self {
        Self {
            target: item.target(),
            scale: item.scale(),
            max_len: item.max_len(),
            max_bytes: item.max_bytes(),
        }
    }
}

fn scalar_relation_target_mismatch(
    expected: &ScalarRelationTarget<'_>,
    actual: &ScalarRelationTarget<'_>,
) -> bool {
    expected.target != actual.target
        || expected.scale != actual.scale
        || expected.max_len != actual.max_len
        || expected.max_bytes != actual.max_bytes
}

impl ValidateNode for Item {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        // Phase 1: validate target shape.
        match self.target() {
            ItemTarget::Is(path) => {
                // cannot be an entity
                if schema.check_node_as::<Entity>(path).is_ok() {
                    err!(errs, "a non-relation Item cannot reference an Entity");
                }
            }

            ItemTarget::Primitive(_) => {}
        }

        // Phase 2: validate relation target compatibility.
        if let Some(relation) = self.relation() {
            match schema.cast_node::<Entity>(relation) {
                Ok(entity) => {
                    if entity.primary_key().fields().len() != 1 {
                        err!(
                            errs,
                            "relation entity '{relation}' uses composite primary key fields {:?}; single-field relation targets require a scalar primary key; use ordered relation tuple metadata for composite targets",
                            entity.primary_key().fields()
                        );
                    } else if let Some(primary_field) = entity.scalar_primary_key_field() {
                        let expected = ScalarRelationTarget::from_field(primary_field);
                        let actual = ScalarRelationTarget::from_item(self);
                        if scalar_relation_target_mismatch(&expected, &actual) {
                            err!(
                                errs,
                                "relation target type mismatch: expected ({:?}, scale={:?}, max_len={:?}, max_bytes={:?}), found ({:?}, scale={:?}, max_len={:?}, max_bytes={:?})",
                                expected.target,
                                expected.scale,
                                expected.max_len,
                                expected.max_bytes,
                                actual.target,
                                actual.scale,
                                actual.max_len,
                                actual.max_bytes,
                            );
                        }
                    } else {
                        let primary_key_field =
                            entity.primary_key().scalar_field().unwrap_or("<composite>");
                        err!(
                            errs,
                            "relation entity '{relation}' missing primary key field '{0}'",
                            primary_key_field
                        );
                    }
                }
                Err(_) => {
                    err!(errs, "relation entity '{relation}' not found");
                }
            }
        }

        errs.result()
    }
}

impl VisitableNode for Item {
    fn drive<V: Visitor>(&self, v: &mut V) {
        for node in self.validators() {
            node.accept(v);
        }
    }
}

///
/// ItemTarget
///
/// Local item target declaration, either by schema path or primitive runtime
/// kind.
///

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ItemTarget {
    Is(&'static str),
    Primitive(Primitive),
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

    fn relation_item(target_path: &'static str, primitive: Primitive) -> Item {
        Item::new(
            ItemTarget::Primitive(primitive),
            Some(target_path),
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
            PrimaryKey::new(pk_fields, PrimaryKeySource::External),
            None,
            &[],
            FieldList::new(fields),
            Type::new(&[], &[]),
        )));
        path
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
                    && message
                        .contains("single-field relation targets require a scalar primary key")
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

            let expected = ScalarRelationTarget::from_item(&expected);
            assert!(!scalar_relation_target_mismatch(
                &expected,
                &ScalarRelationTarget::from_item(&same),
            ));
            assert!(scalar_relation_target_mismatch(
                &expected,
                &ScalarRelationTarget::from_item(&wrong_bounds),
            ));
            assert!(scalar_relation_target_mismatch(
                &expected,
                &ScalarRelationTarget::from_item(&wrong_target),
            ));
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

        let descriptor = ScalarRelationTarget::from_field(&field);
        assert_eq!(descriptor.target, &ItemTarget::Primitive(Primitive::Text));
        assert_eq!(descriptor.scale, None);
        assert_eq!(descriptor.max_len, Some(64));
        assert_eq!(descriptor.max_bytes, None);
    }
}
