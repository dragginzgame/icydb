//! Module: db::schema::describe
//! Responsibility: deterministic entity-schema introspection DTOs for runtime consumers.
//! Does not own: query planning, execution routing, or relation enforcement semantics.
//! Boundary: projects `EntityModel`/`FieldKind` into stable describe surfaces.

use crate::model::{
    entity::EntityModel,
    field::{FieldKind, RelationStrength},
};
use candid::CandidType;
use serde::Deserialize;
use std::fmt::Write;

#[cfg_attr(
    doc,
    doc = "EntitySchemaDescription\n\nStable describe payload for one entity model."
)]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct EntitySchemaDescription {
    pub(crate) entity_path: String,
    pub(crate) entity_name: String,
    pub(crate) primary_key: String,
    pub(crate) fields: Vec<EntityFieldDescription>,
    pub(crate) indexes: Vec<EntityIndexDescription>,
    pub(crate) relations: Vec<EntityRelationDescription>,
}

impl EntitySchemaDescription {
    /// Construct one entity schema description payload.
    #[must_use]
    pub const fn new(
        entity_path: String,
        entity_name: String,
        primary_key: String,
        fields: Vec<EntityFieldDescription>,
        indexes: Vec<EntityIndexDescription>,
        relations: Vec<EntityRelationDescription>,
    ) -> Self {
        Self {
            entity_path,
            entity_name,
            primary_key,
            fields,
            indexes,
            relations,
        }
    }

    /// Borrow the entity module path.
    #[must_use]
    pub const fn entity_path(&self) -> &str {
        self.entity_path.as_str()
    }

    /// Borrow the entity display name.
    #[must_use]
    pub const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the primary-key field name.
    #[must_use]
    pub const fn primary_key(&self) -> &str {
        self.primary_key.as_str()
    }

    /// Borrow field description entries.
    #[must_use]
    pub const fn fields(&self) -> &[EntityFieldDescription] {
        self.fields.as_slice()
    }

    /// Borrow index description entries.
    #[must_use]
    pub const fn indexes(&self) -> &[EntityIndexDescription] {
        self.indexes.as_slice()
    }

    /// Borrow relation description entries.
    #[must_use]
    pub const fn relations(&self) -> &[EntityRelationDescription] {
        self.relations.as_slice()
    }
}

#[cfg_attr(
    doc,
    doc = "EntityFieldDescription\n\nOne field entry in a describe payload."
)]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct EntityFieldDescription {
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) primary_key: bool,
    pub(crate) queryable: bool,
}

impl EntityFieldDescription {
    /// Construct one field description entry.
    #[must_use]
    pub const fn new(name: String, kind: String, primary_key: bool, queryable: bool) -> Self {
        Self {
            name,
            kind,
            primary_key,
            queryable,
        }
    }

    /// Borrow the field name.
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Borrow the rendered field kind label.
    #[must_use]
    pub const fn kind(&self) -> &str {
        self.kind.as_str()
    }

    /// Return whether this field is the primary key.
    #[must_use]
    pub const fn primary_key(&self) -> bool {
        self.primary_key
    }

    /// Return whether this field is queryable.
    #[must_use]
    pub const fn queryable(&self) -> bool {
        self.queryable
    }
}

#[cfg_attr(
    doc,
    doc = "EntityIndexDescription\n\nOne index entry in a describe payload."
)]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct EntityIndexDescription {
    pub(crate) name: String,
    pub(crate) unique: bool,
    pub(crate) fields: Vec<String>,
}

impl EntityIndexDescription {
    /// Construct one index description entry.
    #[must_use]
    pub const fn new(name: String, unique: bool, fields: Vec<String>) -> Self {
        Self {
            name,
            unique,
            fields,
        }
    }

    /// Borrow the index name.
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return whether the index enforces uniqueness.
    #[must_use]
    pub const fn unique(&self) -> bool {
        self.unique
    }

    /// Borrow ordered index field names.
    #[must_use]
    pub const fn fields(&self) -> &[String] {
        self.fields.as_slice()
    }
}

#[cfg_attr(
    doc,
    doc = "EntityRelationDescription\n\nOne relation entry in a describe payload."
)]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct EntityRelationDescription {
    pub(crate) field: String,
    pub(crate) target_path: String,
    pub(crate) target_entity_name: String,
    pub(crate) target_store_path: String,
    pub(crate) strength: EntityRelationStrength,
    pub(crate) cardinality: EntityRelationCardinality,
}

impl EntityRelationDescription {
    /// Construct one relation description entry.
    #[must_use]
    pub const fn new(
        field: String,
        target_path: String,
        target_entity_name: String,
        target_store_path: String,
        strength: EntityRelationStrength,
        cardinality: EntityRelationCardinality,
    ) -> Self {
        Self {
            field,
            target_path,
            target_entity_name,
            target_store_path,
            strength,
            cardinality,
        }
    }

    /// Borrow the source relation field name.
    #[must_use]
    pub const fn field(&self) -> &str {
        self.field.as_str()
    }

    /// Borrow the relation target path.
    #[must_use]
    pub const fn target_path(&self) -> &str {
        self.target_path.as_str()
    }

    /// Borrow the relation target entity name.
    #[must_use]
    pub const fn target_entity_name(&self) -> &str {
        self.target_entity_name.as_str()
    }

    /// Borrow the relation target store path.
    #[must_use]
    pub const fn target_store_path(&self) -> &str {
        self.target_store_path.as_str()
    }

    /// Return relation strength.
    #[must_use]
    pub const fn strength(&self) -> EntityRelationStrength {
        self.strength
    }

    /// Return relation cardinality.
    #[must_use]
    pub const fn cardinality(&self) -> EntityRelationCardinality {
        self.cardinality
    }
}

#[cfg_attr(doc, doc = "EntityRelationStrength\n\nDescribe relation strength.")]
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum EntityRelationStrength {
    Strong,
    Weak,
}

#[cfg_attr(
    doc,
    doc = "EntityRelationCardinality\n\nDescribe relation cardinality."
)]
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum EntityRelationCardinality {
    Single,
    List,
    Set,
}

#[cfg_attr(
    doc,
    doc = "Build one stable entity-schema description from one runtime `EntityModel`."
)]
#[must_use]
pub(in crate::db) fn describe_entity_model(model: &EntityModel) -> EntitySchemaDescription {
    let mut fields = Vec::with_capacity(model.fields.len());
    let mut relations = Vec::new();
    for field in model.fields {
        let field_kind = summarize_field_kind(&field.kind);
        let queryable = field.kind.value_kind().is_queryable();
        let primary_key = field.name == model.primary_key.name;

        fields.push(EntityFieldDescription::new(
            field.name.to_string(),
            field_kind,
            primary_key,
            queryable,
        ));

        if let Some(relation) = relation_from_field_kind(field.name, &field.kind) {
            relations.push(relation);
        }
    }

    let mut indexes = Vec::with_capacity(model.indexes.len());
    for index in model.indexes {
        indexes.push(EntityIndexDescription::new(
            index.name().to_string(),
            index.is_unique(),
            index
                .fields()
                .iter()
                .map(|field| (*field).to_string())
                .collect(),
        ));
    }

    EntitySchemaDescription::new(
        model.path.to_string(),
        model.entity_name.to_string(),
        model.primary_key.name.to_string(),
        fields,
        indexes,
        relations,
    )
}

#[cfg_attr(
    doc,
    doc = "Resolve relation metadata from one field kind, including list/set relation forms."
)]
fn relation_from_field_kind(
    field_name: &str,
    kind: &FieldKind,
) -> Option<EntityRelationDescription> {
    match kind {
        FieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            strength,
            ..
        } => Some(EntityRelationDescription::new(
            field_name.to_string(),
            (*target_path).to_string(),
            (*target_entity_name).to_string(),
            (*target_store_path).to_string(),
            relation_strength(*strength),
            EntityRelationCardinality::Single,
        )),
        FieldKind::List(inner) => {
            relation_from_collection_relation(field_name, inner, EntityRelationCardinality::List)
        }
        FieldKind::Set(inner) => {
            relation_from_collection_relation(field_name, inner, EntityRelationCardinality::Set)
        }
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Enum { .. }
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid
        | FieldKind::Unit
        | FieldKind::Map { .. }
        | FieldKind::Structured { .. } => None,
    }
}

#[cfg_attr(
    doc,
    doc = "Resolve list/set relation metadata only when the collection inner shape is relation."
)]
fn relation_from_collection_relation(
    field_name: &str,
    inner: &FieldKind,
    cardinality: EntityRelationCardinality,
) -> Option<EntityRelationDescription> {
    let FieldKind::Relation {
        target_path,
        target_entity_name,
        target_store_path,
        strength,
        ..
    } = inner
    else {
        return None;
    };

    Some(EntityRelationDescription::new(
        field_name.to_string(),
        (*target_path).to_string(),
        (*target_entity_name).to_string(),
        (*target_store_path).to_string(),
        relation_strength(*strength),
        cardinality,
    ))
}

#[cfg_attr(
    doc,
    doc = "Project runtime relation strength into the describe DTO surface."
)]
const fn relation_strength(strength: RelationStrength) -> EntityRelationStrength {
    match strength {
        RelationStrength::Strong => EntityRelationStrength::Strong,
        RelationStrength::Weak => EntityRelationStrength::Weak,
    }
}

#[cfg_attr(doc, doc = "Render one stable field-kind label for describe output.")]
fn summarize_field_kind(kind: &FieldKind) -> String {
    let mut out = String::new();
    write_field_kind_summary(&mut out, kind);

    out
}

// Stream one stable field-kind label directly into the output buffer so
// describe/sql surfaces do not retain a large recursive `format!` family.
fn write_field_kind_summary(out: &mut String, kind: &FieldKind) {
    match kind {
        FieldKind::Account => out.push_str("account"),
        FieldKind::Blob => out.push_str("blob"),
        FieldKind::Bool => out.push_str("bool"),
        FieldKind::Date => out.push_str("date"),
        FieldKind::Decimal { scale } => {
            let _ = write!(out, "decimal(scale={scale})");
        }
        FieldKind::Duration => out.push_str("duration"),
        FieldKind::Enum { path, .. } => {
            out.push_str("enum(");
            out.push_str(path);
            out.push(')');
        }
        FieldKind::Float32 => out.push_str("float32"),
        FieldKind::Float64 => out.push_str("float64"),
        FieldKind::Int => out.push_str("int"),
        FieldKind::Int128 => out.push_str("int128"),
        FieldKind::IntBig => out.push_str("int_big"),
        FieldKind::Principal => out.push_str("principal"),
        FieldKind::Subaccount => out.push_str("subaccount"),
        FieldKind::Text => out.push_str("text"),
        FieldKind::Timestamp => out.push_str("timestamp"),
        FieldKind::Uint => out.push_str("uint"),
        FieldKind::Uint128 => out.push_str("uint128"),
        FieldKind::UintBig => out.push_str("uint_big"),
        FieldKind::Ulid => out.push_str("ulid"),
        FieldKind::Unit => out.push_str("unit"),
        FieldKind::Relation {
            target_entity_name,
            key_kind,
            strength,
            ..
        } => {
            out.push_str("relation(target=");
            out.push_str(target_entity_name);
            out.push_str(", key=");
            write_field_kind_summary(out, key_kind);
            out.push_str(", strength=");
            out.push_str(summarize_relation_strength(*strength));
            out.push(')');
        }
        FieldKind::List(inner) => {
            out.push_str("list<");
            write_field_kind_summary(out, inner);
            out.push('>');
        }
        FieldKind::Set(inner) => {
            out.push_str("set<");
            write_field_kind_summary(out, inner);
            out.push('>');
        }
        FieldKind::Map { key, value } => {
            out.push_str("map<");
            write_field_kind_summary(out, key);
            out.push_str(", ");
            write_field_kind_summary(out, value);
            out.push('>');
        }
        FieldKind::Structured { queryable } => {
            let _ = write!(out, "structured(queryable={queryable})");
        }
    }
}

#[cfg_attr(
    doc,
    doc = "Render one stable relation-strength label for field-kind summaries."
)]
const fn summarize_relation_strength(strength: RelationStrength) -> &'static str {
    match strength {
        RelationStrength::Strong => "strong",
        RelationStrength::Weak => "weak",
    }
}

//
// TESTS
//

#[cfg(test)]
mod tests {
    use crate::db::{
        EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
        EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription,
    };
    use candid::types::{CandidType, Label, Type, TypeInner};

    fn expect_record_fields(ty: Type) -> Vec<String> {
        match ty.as_ref() {
            TypeInner::Record(fields) => fields
                .iter()
                .map(|field| match field.id.as_ref() {
                    Label::Named(name) => name.clone(),
                    other => panic!("expected named record field, got {other:?}"),
                })
                .collect(),
            other => panic!("expected candid record, got {other:?}"),
        }
    }

    fn expect_variant_labels(ty: Type) -> Vec<String> {
        match ty.as_ref() {
            TypeInner::Variant(fields) => fields
                .iter()
                .map(|field| match field.id.as_ref() {
                    Label::Named(name) => name.clone(),
                    other => panic!("expected named variant label, got {other:?}"),
                })
                .collect(),
            other => panic!("expected candid variant, got {other:?}"),
        }
    }

    #[test]
    fn entity_schema_description_candid_shape_is_stable() {
        let fields = expect_record_fields(EntitySchemaDescription::ty());

        for field in [
            "entity_path",
            "entity_name",
            "primary_key",
            "fields",
            "indexes",
            "relations",
        ] {
            assert!(
                fields.iter().any(|candidate| candidate == field),
                "EntitySchemaDescription must keep `{field}` field key",
            );
        }
    }

    #[test]
    fn entity_field_description_candid_shape_is_stable() {
        let fields = expect_record_fields(EntityFieldDescription::ty());

        for field in ["name", "kind", "primary_key", "queryable"] {
            assert!(
                fields.iter().any(|candidate| candidate == field),
                "EntityFieldDescription must keep `{field}` field key",
            );
        }
    }

    #[test]
    fn entity_index_description_candid_shape_is_stable() {
        let fields = expect_record_fields(EntityIndexDescription::ty());

        for field in ["name", "unique", "fields"] {
            assert!(
                fields.iter().any(|candidate| candidate == field),
                "EntityIndexDescription must keep `{field}` field key",
            );
        }
    }

    #[test]
    fn entity_relation_description_candid_shape_is_stable() {
        let fields = expect_record_fields(EntityRelationDescription::ty());

        for field in [
            "field",
            "target_path",
            "target_entity_name",
            "target_store_path",
            "strength",
            "cardinality",
        ] {
            assert!(
                fields.iter().any(|candidate| candidate == field),
                "EntityRelationDescription must keep `{field}` field key",
            );
        }
    }

    #[test]
    fn relation_enum_variant_labels_are_stable() {
        let mut strength_labels = expect_variant_labels(EntityRelationStrength::ty());
        strength_labels.sort_unstable();
        assert_eq!(
            strength_labels,
            vec!["Strong".to_string(), "Weak".to_string()]
        );

        let mut cardinality_labels = expect_variant_labels(EntityRelationCardinality::ty());
        cardinality_labels.sort_unstable();
        assert_eq!(
            cardinality_labels,
            vec!["List".to_string(), "Set".to_string(), "Single".to_string()],
        );
    }

    #[test]
    fn describe_fixture_constructors_stay_usable() {
        let payload = EntitySchemaDescription::new(
            "entities::User".to_string(),
            "User".to_string(),
            "id".to_string(),
            vec![EntityFieldDescription::new(
                "id".to_string(),
                "ulid".to_string(),
                true,
                true,
            )],
            vec![EntityIndexDescription::new(
                "idx_email".to_string(),
                true,
                vec!["email".to_string()],
            )],
            vec![EntityRelationDescription::new(
                "account_id".to_string(),
                "entities::Account".to_string(),
                "Account".to_string(),
                "accounts".to_string(),
                EntityRelationStrength::Strong,
                EntityRelationCardinality::Single,
            )],
        );

        assert_eq!(payload.entity_name(), "User");
        assert_eq!(payload.fields().len(), 1);
        assert_eq!(payload.indexes().len(), 1);
        assert_eq!(payload.relations().len(), 1);
    }
}
