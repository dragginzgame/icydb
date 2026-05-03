//! Module: db::schema::describe
//! Responsibility: deterministic entity-schema introspection DTOs for runtime consumers.
//! Does not own: query planning, execution routing, or relation enforcement semantics.
//! Boundary: projects `EntityModel`/`FieldKind` into stable describe surfaces.

use crate::{
    db::{
        relation::{
            RelationDescriptor, RelationDescriptorCardinality, relation_descriptors_for_model_iter,
        },
        schema::PersistedSchemaSnapshot,
    },
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel, RelationStrength},
    },
};
use candid::CandidType;
use serde::Deserialize;
use std::{collections::BTreeMap, fmt::Write};

const ENTITY_FIELD_DESCRIPTION_NO_SLOT: u16 = u16::MAX;

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
    pub(crate) slot: u16,
    pub(crate) kind: String,
    pub(crate) primary_key: bool,
    pub(crate) queryable: bool,
}

impl EntityFieldDescription {
    /// Construct one field description entry.
    #[must_use]
    pub const fn new(
        name: String,
        slot: Option<u16>,
        kind: String,
        primary_key: bool,
        queryable: bool,
    ) -> Self {
        let slot = match slot {
            Some(slot) => slot,
            None => ENTITY_FIELD_DESCRIPTION_NO_SLOT,
        };

        Self {
            name,
            slot,
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

    /// Return the physical row slot for top-level fields.
    #[must_use]
    pub const fn slot(&self) -> Option<u16> {
        if self.slot == ENTITY_FIELD_DESCRIPTION_NO_SLOT {
            None
        } else {
            Some(self.slot)
        }
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
    let fields = describe_entity_fields(model);

    describe_entity_model_with_fields(model, fields)
}

#[cfg_attr(
    doc,
    doc = "Build one entity-schema description using accepted persisted schema slot metadata."
)]
#[must_use]
pub(in crate::db) fn describe_entity_model_with_persisted_schema(
    model: &EntityModel,
    schema: &PersistedSchemaSnapshot,
) -> EntitySchemaDescription {
    let fields = describe_entity_fields_with_persisted_schema(model, schema);

    describe_entity_model_with_fields(model, fields)
}

// Assemble the common DESCRIBE payload once field rows have already been built.
// This keeps live-schema slot overlays local to field description while index
// and relation description remain generated-model owned for this phase.
fn describe_entity_model_with_fields(
    model: &EntityModel,
    fields: Vec<EntityFieldDescription>,
) -> EntitySchemaDescription {
    let relations = relation_descriptors_for_model_iter(model)
        .map(relation_description_from_descriptor)
        .collect();

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

// Build the stable field-description subset once from one runtime model so
// metadata surfaces that only need columns do not rebuild indexes and
// relations through the heavier DESCRIBE payload path.
#[must_use]
pub(in crate::db) fn describe_entity_fields(model: &EntityModel) -> Vec<EntityFieldDescription> {
    describe_entity_fields_with_slot_lookup(model, |slot, _field| {
        Some(u16::try_from(slot).expect("generated field slot should fit in u16"))
    })
}

#[cfg_attr(
    doc,
    doc = "Build field descriptors using accepted persisted schema slot metadata."
)]
#[must_use]
pub(in crate::db) fn describe_entity_fields_with_persisted_schema(
    model: &EntityModel,
    schema: &PersistedSchemaSnapshot,
) -> Vec<EntityFieldDescription> {
    let slots_by_name = schema
        .fields()
        .iter()
        .map(|field| (field.name(), field.slot().get()))
        .collect::<BTreeMap<_, _>>();

    describe_entity_fields_with_slot_lookup(model, |_slot, field| {
        slots_by_name.get(field.name()).copied()
    })
}

// Build field descriptors with an injected top-level slot lookup. Generated
// model introspection uses generated positions; live-schema introspection uses
// accepted persisted row layout metadata while preserving nested-field behavior.
fn describe_entity_fields_with_slot_lookup(
    model: &EntityModel,
    mut slot_for_field: impl FnMut(usize, &FieldModel) -> Option<u16>,
) -> Vec<EntityFieldDescription> {
    let mut fields = Vec::with_capacity(model.fields.len());

    for (slot, field) in model.fields.iter().enumerate() {
        let primary_key = field.name == model.primary_key.name;
        describe_field_recursive(
            &mut fields,
            field.name,
            slot_for_field(slot, field),
            field,
            primary_key,
            None,
        );
    }

    fields
}

// Add one top-level field and any generated structured-record leaves under
// dotted names so DESCRIBE/SHOW COLUMNS expose the same field paths SQL can
// project and filter.
fn describe_field_recursive(
    fields: &mut Vec<EntityFieldDescription>,
    name: &str,
    slot: Option<u16>,
    field: &FieldModel,
    primary_key: bool,
    tree_prefix: Option<&'static str>,
) {
    let field_kind = summarize_field_kind(&field.kind);
    let queryable = field.kind.value_kind().is_queryable();

    // Generated nested field rows keep a compact tree marker so
    // table-oriented describe output scans as a hierarchy.
    let display_name = if let Some(prefix) = tree_prefix {
        format!("{prefix}{name}")
    } else {
        name.to_string()
    };

    fields.push(EntityFieldDescription::new(
        display_name,
        slot,
        field_kind,
        primary_key,
        queryable,
    ));

    let nested_fields = field.nested_fields();
    for (index, nested) in nested_fields.iter().enumerate() {
        let prefix = if index + 1 == nested_fields.len() {
            "└─ "
        } else {
            "├─ "
        };
        describe_field_recursive(fields, nested.name(), None, nested, false, Some(prefix));
    }
}

// Project the relation-owned descriptor into the stable describe DTO surface.
fn relation_description_from_descriptor(
    descriptor: RelationDescriptor<'_>,
) -> EntityRelationDescription {
    let strength = match descriptor.strength() {
        RelationStrength::Strong => EntityRelationStrength::Strong,
        RelationStrength::Weak => EntityRelationStrength::Weak,
    };

    let cardinality = match descriptor.cardinality() {
        RelationDescriptorCardinality::Single => EntityRelationCardinality::Single,
        RelationDescriptorCardinality::List => EntityRelationCardinality::List,
        RelationDescriptorCardinality::Set => EntityRelationCardinality::Set,
    };

    EntityRelationDescription::new(
        descriptor.field_name().to_string(),
        descriptor.target_path().to_string(),
        descriptor.target_entity_name().to_string(),
        descriptor.target_store_path().to_string(),
        strength,
        cardinality,
    )
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
        FieldKind::Text { max_len } => match max_len {
            Some(max_len) => {
                let _ = write!(out, "text(max_len={max_len})");
            }
            None => out.push_str("text"),
        },
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
        FieldKind::Structured { .. } => {
            out.push_str("structured");
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
    use crate::{
        db::{
            EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
            EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription,
            relation::{RelationDescriptorCardinality, relation_descriptors_for_model_iter},
            schema::describe::describe_entity_model,
        },
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel, FieldStorageDecode, RelationStrength},
        },
        types::EntityTag,
    };
    use candid::types::{CandidType, Label, Type, TypeInner};

    static DESCRIBE_SINGLE_RELATION_KIND: FieldKind = FieldKind::Relation {
        target_path: "entities::Target",
        target_entity_name: "Target",
        target_entity_tag: EntityTag::new(0xD001),
        target_store_path: "stores::Target",
        key_kind: &FieldKind::Ulid,
        strength: RelationStrength::Strong,
    };
    static DESCRIBE_LIST_RELATION_INNER_KIND: FieldKind = FieldKind::Relation {
        target_path: "entities::Account",
        target_entity_name: "Account",
        target_entity_tag: EntityTag::new(0xD002),
        target_store_path: "stores::Account",
        key_kind: &FieldKind::Uint,
        strength: RelationStrength::Weak,
    };
    static DESCRIBE_SET_RELATION_INNER_KIND: FieldKind = FieldKind::Relation {
        target_path: "entities::Team",
        target_entity_name: "Team",
        target_entity_tag: EntityTag::new(0xD003),
        target_store_path: "stores::Team",
        key_kind: &FieldKind::Text { max_len: None },
        strength: RelationStrength::Strong,
    };
    static DESCRIBE_RELATION_FIELDS: [FieldModel; 4] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("target", DESCRIBE_SINGLE_RELATION_KIND),
        FieldModel::generated(
            "accounts",
            FieldKind::List(&DESCRIBE_LIST_RELATION_INNER_KIND),
        ),
        FieldModel::generated("teams", FieldKind::Set(&DESCRIBE_SET_RELATION_INNER_KIND)),
    ];
    static DESCRIBE_RELATION_INDEXES: [&crate::model::index::IndexModel; 0] = [];
    static DESCRIBE_RELATION_MODEL: EntityModel = EntityModel::generated(
        "entities::Source",
        "Source",
        &DESCRIBE_RELATION_FIELDS[0],
        0,
        &DESCRIBE_RELATION_FIELDS,
        &DESCRIBE_RELATION_INDEXES,
    );

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

        for field in ["name", "slot", "kind", "primary_key", "queryable"] {
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
                Some(0),
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

    #[test]
    fn schema_describe_relations_match_relation_descriptors() {
        let descriptors =
            relation_descriptors_for_model_iter(&DESCRIBE_RELATION_MODEL).collect::<Vec<_>>();
        let described = describe_entity_model(&DESCRIBE_RELATION_MODEL);
        let relations = described.relations();

        assert_eq!(descriptors.len(), relations.len());

        for (descriptor, relation) in descriptors.iter().zip(relations) {
            assert_eq!(relation.field(), descriptor.field_name());
            assert_eq!(relation.target_path(), descriptor.target_path());
            assert_eq!(
                relation.target_entity_name(),
                descriptor.target_entity_name()
            );
            assert_eq!(relation.target_store_path(), descriptor.target_store_path());
            assert_eq!(
                relation.strength(),
                match descriptor.strength() {
                    RelationStrength::Strong => EntityRelationStrength::Strong,
                    RelationStrength::Weak => EntityRelationStrength::Weak,
                }
            );
            assert_eq!(
                relation.cardinality(),
                match descriptor.cardinality() {
                    RelationDescriptorCardinality::Single => EntityRelationCardinality::Single,
                    RelationDescriptorCardinality::List => EntityRelationCardinality::List,
                    RelationDescriptorCardinality::Set => EntityRelationCardinality::Set,
                }
            );
        }
    }

    #[test]
    fn schema_describe_includes_text_max_len_contract() {
        static FIELDS: [FieldModel; 2] = [
            FieldModel::generated("id", FieldKind::Ulid),
            FieldModel::generated("name", FieldKind::Text { max_len: Some(16) }),
        ];
        static INDEXES: [&crate::model::index::IndexModel; 0] = [];
        static MODEL: EntityModel = EntityModel::generated(
            "entities::BoundedName",
            "BoundedName",
            &FIELDS[0],
            0,
            &FIELDS,
            &INDEXES,
        );

        let described = describe_entity_model(&MODEL);
        let name_field = described
            .fields()
            .iter()
            .find(|field| field.name() == "name")
            .expect("bounded text field should be described");

        assert_eq!(name_field.kind(), "text(max_len=16)");
    }

    #[test]
    fn schema_describe_expands_generated_structured_field_leaves() {
        static NESTED_FIELDS: [FieldModel; 3] = [
            FieldModel::generated("name", FieldKind::Text { max_len: None }),
            FieldModel::generated("level", FieldKind::Uint),
            FieldModel::generated("pid", FieldKind::Principal),
        ];
        static FIELDS: [FieldModel; 2] = [
            FieldModel::generated("id", FieldKind::Ulid),
            FieldModel::generated_with_storage_decode_nullability_write_policies_and_nested_fields(
                "mentor",
                FieldKind::Structured { queryable: false },
                FieldStorageDecode::Value,
                false,
                None,
                None,
                &NESTED_FIELDS,
            ),
        ];
        static INDEXES: [&crate::model::index::IndexModel; 0] = [];
        static MODEL: EntityModel = EntityModel::generated(
            "entities::Character",
            "Character",
            &FIELDS[0],
            0,
            &FIELDS,
            &INDEXES,
        );

        let described = describe_entity_model(&MODEL);
        let described_fields = described
            .fields()
            .iter()
            .map(|field| (field.name(), field.slot(), field.kind(), field.queryable()))
            .collect::<Vec<_>>();

        assert_eq!(
            described_fields,
            vec![
                ("id", Some(0), "ulid", true),
                ("mentor", Some(1), "structured", false),
                ("├─ name", None, "text", true),
                ("├─ level", None, "uint", true),
                ("└─ pid", None, "principal", true),
            ],
        );
    }
}
