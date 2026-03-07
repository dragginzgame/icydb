//! Module: describe
//! Responsibility: deterministic entity-schema introspection DTOs for runtime consumers.
//! Does not own: query planning, execution routing, or relation enforcement semantics.
//! Boundary: projects `EntityModel`/`FieldKind` into stable describe surfaces.

use crate::model::{
    entity::EntityModel,
    field::{FieldKind, RelationStrength},
};
use candid::CandidType;
use serde::{Deserialize, Serialize};

///
/// EntitySchemaDescription
///
/// Stable schema-introspection payload for one runtime entity model.
/// This mirrors SQL-style `DESCRIBE` intent for fields, indexes, and relations.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

///
/// EntityFieldDescription
///
/// One field-level projection inside `EntitySchemaDescription`.
/// Keeps field type and queryability metadata explicit for diagnostics.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

///
/// EntityIndexDescription
///
/// One secondary-index projection inside `EntitySchemaDescription`.
/// Includes uniqueness and ordered field list.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

///
/// EntityRelationDescription
///
/// One relation-field projection inside `EntitySchemaDescription`.
/// Captures relation target identity plus strength/cardinality metadata.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

///
/// EntityRelationStrength
///
/// Describe-surface relation strength projection.
///
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum EntityRelationStrength {
    Strong,
    Weak,
}

///
/// EntityRelationCardinality
///
/// Describe-surface relation cardinality projection.
///
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum EntityRelationCardinality {
    Single,
    List,
    Set,
}

/// Build one stable entity-schema description from one runtime `EntityModel`.
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
            index.name.to_string(),
            index.unique,
            index
                .fields
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

// Resolve relation metadata from one field kind, including list/set relation forms.
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

// Resolve list/set relation metadata only when the collection inner shape is relation.
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

// Project runtime relation strength into the describe DTO surface.
const fn relation_strength(strength: RelationStrength) -> EntityRelationStrength {
    match strength {
        RelationStrength::Strong => EntityRelationStrength::Strong,
        RelationStrength::Weak => EntityRelationStrength::Weak,
    }
}

// Render one stable field-kind label for describe output.
fn summarize_field_kind(kind: &FieldKind) -> String {
    match kind {
        FieldKind::Account => "account".to_string(),
        FieldKind::Blob => "blob".to_string(),
        FieldKind::Bool => "bool".to_string(),
        FieldKind::Date => "date".to_string(),
        FieldKind::Decimal { scale } => format!("decimal(scale={scale})"),
        FieldKind::Duration => "duration".to_string(),
        FieldKind::Enum { path } => format!("enum({path})"),
        FieldKind::Float32 => "float32".to_string(),
        FieldKind::Float64 => "float64".to_string(),
        FieldKind::Int => "int".to_string(),
        FieldKind::Int128 => "int128".to_string(),
        FieldKind::IntBig => "int_big".to_string(),
        FieldKind::Principal => "principal".to_string(),
        FieldKind::Subaccount => "subaccount".to_string(),
        FieldKind::Text => "text".to_string(),
        FieldKind::Timestamp => "timestamp".to_string(),
        FieldKind::Uint => "uint".to_string(),
        FieldKind::Uint128 => "uint128".to_string(),
        FieldKind::UintBig => "uint_big".to_string(),
        FieldKind::Ulid => "ulid".to_string(),
        FieldKind::Unit => "unit".to_string(),
        FieldKind::Relation {
            target_entity_name,
            key_kind,
            strength,
            ..
        } => format!(
            "relation(target={target_entity_name}, key={}, strength={})",
            summarize_field_kind(key_kind),
            summarize_relation_strength(*strength),
        ),
        FieldKind::List(inner) => format!("list<{}>", summarize_field_kind(inner)),
        FieldKind::Set(inner) => format!("set<{}>", summarize_field_kind(inner)),
        FieldKind::Map { key, value } => {
            format!(
                "map<{}, {}>",
                summarize_field_kind(key),
                summarize_field_kind(value)
            )
        }
        FieldKind::Structured { queryable } => format!("structured(queryable={queryable})"),
    }
}

// Render one stable relation-strength label for field-kind summaries.
const fn summarize_relation_strength(strength: RelationStrength) -> &'static str {
    match strength {
        RelationStrength::Strong => "strong",
        RelationStrength::Weak => "weak",
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::db::{
        EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
        EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription,
    };
    use serde::Serialize;
    use serde_cbor::Value as CborValue;
    use std::collections::BTreeMap;

    fn to_cbor_value<T: Serialize>(value: &T) -> CborValue {
        let bytes =
            serde_cbor::to_vec(value).expect("test fixtures must serialize into CBOR payloads");
        serde_cbor::from_slice::<CborValue>(&bytes)
            .expect("test fixtures must deserialize into CBOR value trees")
    }

    fn expect_cbor_map(value: &CborValue) -> &BTreeMap<CborValue, CborValue> {
        match value {
            CborValue::Map(map) => map,
            other => panic!("expected CBOR map, got {other:?}"),
        }
    }

    fn map_field<'a>(map: &'a BTreeMap<CborValue, CborValue>, key: &str) -> Option<&'a CborValue> {
        map.get(&CborValue::Text(key.to_string()))
    }

    #[test]
    fn entity_schema_description_serialization_shape_is_stable() {
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
        let encoded = to_cbor_value(&payload);
        let root = expect_cbor_map(&encoded);

        assert!(
            map_field(root, "entity_path").is_some(),
            "EntitySchemaDescription must keep `entity_path` field key",
        );
        assert!(
            map_field(root, "entity_name").is_some(),
            "EntitySchemaDescription must keep `entity_name` field key",
        );
        assert!(
            map_field(root, "primary_key").is_some(),
            "EntitySchemaDescription must keep `primary_key` field key",
        );
        assert!(
            map_field(root, "fields").is_some(),
            "EntitySchemaDescription must keep `fields` field key",
        );
        assert!(
            map_field(root, "indexes").is_some(),
            "EntitySchemaDescription must keep `indexes` field key",
        );
        assert!(
            map_field(root, "relations").is_some(),
            "EntitySchemaDescription must keep `relations` field key",
        );
    }

    #[test]
    fn entity_relation_description_serialization_shape_is_stable() {
        let encoded = to_cbor_value(&EntityRelationDescription::new(
            "owner_id".to_string(),
            "entities::User".to_string(),
            "User".to_string(),
            "users".to_string(),
            EntityRelationStrength::Weak,
            EntityRelationCardinality::Set,
        ));
        let root = expect_cbor_map(&encoded);

        assert!(
            map_field(root, "field").is_some(),
            "EntityRelationDescription must keep `field` field key",
        );
        assert!(
            map_field(root, "target_path").is_some(),
            "EntityRelationDescription must keep `target_path` field key",
        );
        assert!(
            map_field(root, "target_entity_name").is_some(),
            "EntityRelationDescription must keep `target_entity_name` field key",
        );
        assert!(
            map_field(root, "target_store_path").is_some(),
            "EntityRelationDescription must keep `target_store_path` field key",
        );
        assert!(
            map_field(root, "strength").is_some(),
            "EntityRelationDescription must keep `strength` field key",
        );
        assert!(
            map_field(root, "cardinality").is_some(),
            "EntityRelationDescription must keep `cardinality` field key",
        );
    }

    #[test]
    fn relation_enum_variant_labels_are_stable() {
        assert_eq!(
            to_cbor_value(&EntityRelationStrength::Strong),
            CborValue::Text("Strong".to_string())
        );
        assert_eq!(
            to_cbor_value(&EntityRelationStrength::Weak),
            CborValue::Text("Weak".to_string())
        );
        assert_eq!(
            to_cbor_value(&EntityRelationCardinality::Single),
            CborValue::Text("Single".to_string())
        );
        assert_eq!(
            to_cbor_value(&EntityRelationCardinality::List),
            CborValue::Text("List".to_string())
        );
        assert_eq!(
            to_cbor_value(&EntityRelationCardinality::Set),
            CborValue::Text("Set".to_string())
        );
    }
}
