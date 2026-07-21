//! Module: db::schema::describe
//! Responsibility: deterministic entity-schema introspection DTOs for runtime consumers.
//! Does not own: query planning, execution routing, or relation enforcement semantics.
//! Boundary: projects generated or accepted schema metadata into stable describe surfaces.

use crate::{
    db::{
        data::decode_admitted_value_from_accepted_field_contract,
        relation::{
            RelationFieldCardinality, RelationFieldMetadata, relation_field_metadata_for_model_iter,
        },
        schema::{
            AcceptedFieldKind, AcceptedFieldPersistenceContract, AcceptedInsertOmissionPolicy,
            AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle,
            PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedNestedLeafSnapshot,
            SchemaHistoricalFill,
            composite_catalog::{AcceptedCompositeElement, AcceptedCompositeShape},
            field_type_from_persisted_kind, output_value_from_runtime,
            runtime::AcceptedRowLayoutRuntimeField,
        },
    },
    error::InternalError,
    model::{
        entity::EntityModel,
        field::{
            CompositeCodec, CompositeElementModel, CompositeShapeModel, FieldDatabaseDefault,
            FieldKind, FieldModel,
        },
    },
    value::{OutputValue, render_output_value_text},
};
use std::fmt::Write;

use candid::CandidType;
use serde::Deserialize;
use sha2::{Digest, Sha256};

const ENTITY_FIELD_DESCRIPTION_NO_SLOT: u16 = u16::MAX;
const MAX_SCHEMA_VALUE_RENDER_CHARS: usize = 128;

#[cfg_attr(
    doc,
    doc = "EntitySchemaDescription\n\nStable describe payload for one entity model."
)]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct EntitySchemaDescription {
    pub(crate) entity_path: String,
    pub(crate) entity_name: String,
    pub(crate) primary_key: String,
    pub(crate) primary_key_fields: Vec<String>,
    pub(crate) fields: Vec<EntityFieldDescription>,
    pub(crate) indexes: Vec<EntityIndexDescription>,
    pub(crate) relations: Vec<EntityRelationDescription>,
    pub(crate) row_layout_current: u32,
    pub(crate) row_layout_history_floor: u32,
}

#[cfg_attr(
    doc,
    doc = "EntitySchemaCheckDescription\n\nGenerated-vs-accepted schema description payload for one entity."
)]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct EntitySchemaCheckDescription {
    pub(crate) generated: EntitySchemaDescription,
    pub(crate) accepted: EntitySchemaDescription,
}

impl EntitySchemaCheckDescription {
    /// Construct one generated-vs-accepted schema check payload.
    #[must_use]
    pub const fn new(
        generated: EntitySchemaDescription,
        accepted: EntitySchemaDescription,
    ) -> Self {
        Self {
            generated,
            accepted,
        }
    }

    /// Borrow the generated schema proposal description.
    #[must_use]
    pub const fn generated(&self) -> &EntitySchemaDescription {
        &self.generated
    }

    /// Borrow the accepted live-schema description.
    #[must_use]
    pub const fn accepted(&self) -> &EntitySchemaDescription {
        &self.accepted
    }
}

impl EntitySchemaDescription {
    /// Construct one scalar-compatible entity schema description payload.
    #[expect(
        clippy::too_many_arguments,
        reason = "schema description construction keeps identity, collections, and layout explicit"
    )]
    #[must_use]
    pub fn new(
        entity_path: String,
        entity_name: String,
        primary_key: String,
        fields: Vec<EntityFieldDescription>,
        indexes: Vec<EntityIndexDescription>,
        relations: Vec<EntityRelationDescription>,
        row_layout_current: u32,
        row_layout_history_floor: u32,
    ) -> Self {
        Self::new_with_primary_key_fields(
            entity_path,
            entity_name,
            primary_key.clone(),
            vec![primary_key],
            fields,
            indexes,
            relations,
            row_layout_current,
            row_layout_history_floor,
        )
    }

    /// Construct one entity schema description payload with ordered
    /// primary-key fields.
    #[expect(
        clippy::too_many_arguments,
        reason = "schema description construction keeps identity, collections, and layout explicit"
    )]
    #[must_use]
    pub const fn new_with_primary_key_fields(
        entity_path: String,
        entity_name: String,
        primary_key: String,
        primary_key_fields: Vec<String>,
        fields: Vec<EntityFieldDescription>,
        indexes: Vec<EntityIndexDescription>,
        relations: Vec<EntityRelationDescription>,
        row_layout_current: u32,
        row_layout_history_floor: u32,
    ) -> Self {
        Self {
            entity_path,
            entity_name,
            primary_key,
            primary_key_fields,
            fields,
            indexes,
            relations,
            row_layout_current,
            row_layout_history_floor,
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

    /// Borrow the rendered primary-key field list.
    #[must_use]
    pub const fn primary_key(&self) -> &str {
        self.primary_key.as_str()
    }

    /// Borrow ordered primary-key field names.
    #[must_use]
    pub const fn primary_key_fields(&self) -> &[String] {
        self.primary_key_fields.as_slice()
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

    /// Return the current accepted physical row-layout identity.
    #[must_use]
    pub const fn row_layout_current(&self) -> u32 {
        self.row_layout_current
    }

    /// Return the oldest admitted physical row-layout identity.
    #[must_use]
    pub const fn row_layout_history_floor(&self) -> u32 {
        self.row_layout_history_floor
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
    pub(crate) nullable: bool,
    pub(crate) primary_key: bool,
    pub(crate) queryable: bool,
    pub(crate) origin: String,
    pub(crate) insert_omission: Option<String>,
    pub(crate) insert_default: Option<String>,
    pub(crate) insert_default_bytes: Option<u32>,
    pub(crate) insert_default_hash: Option<String>,
    pub(crate) introduced_in_layout: Option<u32>,
    pub(crate) historical_fill: Option<String>,
    pub(crate) historical_fill_bytes: Option<u32>,
    pub(crate) historical_fill_hash: Option<String>,
}

///
/// EntityFieldTemporalFacts
///
/// One internally assembled projection of the independent accepted insert and
/// historical-absence contracts. Nested rows carry an explicitly empty bundle.
///

struct EntityFieldTemporalFacts {
    insert_omission: Option<String>,
    insert_default: Option<String>,
    insert_default_bytes: Option<u32>,
    insert_default_hash: Option<String>,
    introduced_in_layout: Option<u32>,
    historical_fill: Option<String>,
    historical_fill_bytes: Option<u32>,
    historical_fill_hash: Option<String>,
}

impl EntityFieldTemporalFacts {
    const fn nested() -> Self {
        Self {
            insert_omission: None,
            insert_default: None,
            insert_default_bytes: None,
            insert_default_hash: None,
            introduced_in_layout: None,
            historical_fill: None,
            historical_fill_bytes: None,
            historical_fill_hash: None,
        }
    }

    fn generated(field: &FieldModel) -> Self {
        let insert_omission = if field.insert_generation().is_some() {
            "generated"
        } else if field.write_management().is_some() {
            "managed"
        } else {
            match field.database_default() {
                FieldDatabaseDefault::EncodedSlotPayload(_)
                | FieldDatabaseDefault::AuthoredEnumUnit { .. } => "default",
                FieldDatabaseDefault::None if field.nullable() => "null",
                FieldDatabaseDefault::None => "required",
            }
        };
        let (insert_default, insert_default_bytes, insert_default_hash) =
            generated_insert_default_facts(field.database_default());

        Self {
            insert_omission: Some(insert_omission.to_string()),
            insert_default,
            insert_default_bytes,
            insert_default_hash,
            introduced_in_layout: Some(1),
            historical_fill: Some("reject".to_string()),
            historical_fill_bytes: None,
            historical_fill_hash: None,
        }
    }
}

impl EntityFieldDescription {
    /// Construct one field description entry.
    #[expect(
        clippy::too_many_arguments,
        reason = "schema description construction keeps every temporal field fact explicit"
    )]
    #[must_use]
    pub fn new(
        name: String,
        slot: Option<u16>,
        kind: String,
        nullable: bool,
        primary_key: bool,
        queryable: bool,
        origin: String,
        insert_omission: Option<String>,
        insert_default: Option<String>,
        insert_default_bytes: Option<u32>,
        insert_default_hash: Option<String>,
        introduced_in_layout: Option<u32>,
        historical_fill: Option<String>,
        historical_fill_bytes: Option<u32>,
        historical_fill_hash: Option<String>,
    ) -> Self {
        Self::new_with_temporal_facts(
            name,
            slot,
            primary_key,
            DescribeFieldMetadata::new(kind, nullable, queryable, origin),
            EntityFieldTemporalFacts {
                insert_omission,
                insert_default,
                insert_default_bytes,
                insert_default_hash,
                introduced_in_layout,
                historical_fill,
                historical_fill_bytes,
                historical_fill_hash,
            },
        )
    }

    fn new_with_temporal_facts(
        name: String,
        slot: Option<u16>,
        primary_key: bool,
        metadata: DescribeFieldMetadata,
        temporal: EntityFieldTemporalFacts,
    ) -> Self {
        let slot = match slot {
            Some(slot) => slot,
            None => ENTITY_FIELD_DESCRIPTION_NO_SLOT,
        };

        Self {
            name,
            slot,
            kind: metadata.kind,
            nullable: metadata.nullable,
            primary_key,
            queryable: metadata.queryable,
            origin: metadata.origin,
            insert_omission: temporal.insert_omission,
            insert_default: temporal.insert_default,
            insert_default_bytes: temporal.insert_default_bytes,
            insert_default_hash: temporal.insert_default_hash,
            introduced_in_layout: temporal.introduced_in_layout,
            historical_fill: temporal.historical_fill,
            historical_fill_bytes: temporal.historical_fill_bytes,
            historical_fill_hash: temporal.historical_fill_hash,
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

    /// Return whether this field permits explicit `NULL`.
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
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

    /// Borrow the accepted/generated field origin label.
    #[must_use]
    pub const fn origin(&self) -> &str {
        self.origin.as_str()
    }

    /// Borrow the accepted insert-omission policy label for a top-level field.
    #[must_use]
    pub fn insert_omission(&self) -> Option<&str> {
        self.insert_omission.as_deref()
    }

    /// Borrow the bounded canonical accepted insert-default rendering.
    #[must_use]
    pub fn insert_default(&self) -> Option<&str> {
        self.insert_default.as_deref()
    }

    /// Return the accepted insert-default payload byte count.
    #[must_use]
    pub const fn insert_default_bytes(&self) -> Option<u32> {
        self.insert_default_bytes
    }

    /// Borrow the stable accepted insert-default payload hash.
    #[must_use]
    pub fn insert_default_hash(&self) -> Option<&str> {
        self.insert_default_hash.as_deref()
    }

    /// Return the row layout that first physically contained this field.
    #[must_use]
    pub const fn introduced_in_layout(&self) -> Option<u32> {
        self.introduced_in_layout
    }

    /// Borrow the accepted frozen historical-absence rendering.
    #[must_use]
    pub fn historical_fill(&self) -> Option<&str> {
        self.historical_fill.as_deref()
    }

    /// Return the historical-fill payload byte count when one is stored.
    #[must_use]
    pub const fn historical_fill_bytes(&self) -> Option<u32> {
        self.historical_fill_bytes
    }

    /// Borrow the stable historical-fill payload hash.
    #[must_use]
    pub fn historical_fill_hash(&self) -> Option<&str> {
        self.historical_fill_hash.as_deref()
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
    pub(crate) origin: String,
}

impl EntityIndexDescription {
    /// Construct one index description entry.
    #[must_use]
    pub const fn new(name: String, unique: bool, fields: Vec<String>, origin: String) -> Self {
        Self {
            name,
            unique,
            fields,
            origin,
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

    /// Borrow the accepted index origin label.
    #[must_use]
    pub const fn origin(&self) -> &str {
        self.origin.as_str()
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
        cardinality: EntityRelationCardinality,
    ) -> Self {
        Self {
            field,
            target_path,
            target_entity_name,
            target_store_path,
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

    /// Return relation cardinality.
    #[must_use]
    pub const fn cardinality(&self) -> EntityRelationCardinality {
        self.cardinality
    }
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
    let primary_key_fields = primary_key_field_names_from_model(model);
    let primary_key = render_primary_key_fields(primary_key_fields.as_slice());

    describe_entity_model_from_description_rows(
        model.path,
        model.entity_name,
        primary_key.as_str(),
        primary_key_fields,
        fields,
        describe_entity_indexes_from_model(model),
        describe_entity_relations_from_model(model),
        1,
        1,
    )
}

#[cfg_attr(
    doc,
    doc = "Build one entity-schema description using accepted persisted schema slot metadata."
)]
pub(in crate::db) fn describe_entity_model_with_persisted_schema(
    model: &EntityModel,
    schema: &AcceptedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<EntitySchemaDescription, InternalError> {
    let row_layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(schema)?;
    let fields = describe_entity_fields_with_runtime_contract(schema, &row_layout, value_catalog)?;
    let primary_key_fields = schema.primary_key_field_names();
    let primary_key_fields = if primary_key_fields.is_empty() {
        vec![model.primary_key.name.to_string()]
    } else {
        primary_key_fields
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    let primary_key = render_primary_key_fields(primary_key_fields.as_slice());

    Ok(describe_entity_model_from_description_rows(
        schema.entity_path(),
        schema.entity_name(),
        primary_key.as_str(),
        primary_key_fields,
        fields,
        describe_entity_indexes_with_persisted_schema(schema),
        describe_entity_relations_with_persisted_schema(schema),
        row_layout.current_layout_version().get(),
        row_layout.history_floor().get(),
    ))
}

// Assemble the common DESCRIBE payload once field rows have already been built.
// Callers project relation descriptions from the same authority as their field
// and index rows, so accepted DESCRIBE output does not fall back to generated
// relation metadata.
#[expect(
    clippy::too_many_arguments,
    reason = "one final schema DTO assembly keeps every already-owned section explicit"
)]
fn describe_entity_model_from_description_rows(
    entity_path: &str,
    entity_name: &str,
    primary_key: &str,
    primary_key_fields: Vec<String>,
    fields: Vec<EntityFieldDescription>,
    indexes: Vec<EntityIndexDescription>,
    relations: Vec<EntityRelationDescription>,
    row_layout_current: u32,
    row_layout_history_floor: u32,
) -> EntitySchemaDescription {
    EntitySchemaDescription::new_with_primary_key_fields(
        entity_path.to_string(),
        entity_name.to_string(),
        primary_key.to_string(),
        primary_key_fields,
        fields,
        indexes,
        relations,
        row_layout_current,
        row_layout_history_floor,
    )
}

fn describe_entity_relations_from_model(model: &EntityModel) -> Vec<EntityRelationDescription> {
    relation_field_metadata_for_model_iter(model)
        .map(relation_description_from_metadata)
        .collect()
}

fn primary_key_field_names_from_model(model: &EntityModel) -> Vec<String> {
    model
        .primary_key_model()
        .fields()
        .iter()
        .map(|field| field.name.to_string())
        .collect()
}

fn render_primary_key_fields(fields: &[String]) -> String {
    fields.join(", ")
}

fn describe_entity_indexes_from_model(model: &EntityModel) -> Vec<EntityIndexDescription> {
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
            "generated".to_string(),
        ));
    }

    indexes
}

fn describe_entity_indexes_with_persisted_schema(
    schema: &AcceptedSchemaSnapshot,
) -> Vec<EntityIndexDescription> {
    schema
        .persisted_snapshot()
        .indexes()
        .iter()
        .map(|index| {
            EntityIndexDescription::new(
                index.name().to_string(),
                index.unique(),
                describe_persisted_index_fields(index.key()),
                if index.generated() {
                    "generated".to_string()
                } else {
                    "ddl".to_string()
                },
            )
        })
        .collect()
}

fn describe_persisted_index_fields(key: &PersistedIndexKeySnapshot) -> Vec<String> {
    match key {
        PersistedIndexKeySnapshot::FieldPath(paths) => paths
            .iter()
            .map(|field_path| field_path.path().join("."))
            .collect(),
        PersistedIndexKeySnapshot::Items(items) => items
            .iter()
            .map(|item| match item {
                PersistedIndexKeyItemSnapshot::FieldPath(field_path) => field_path.path().join("."),
                PersistedIndexKeyItemSnapshot::Expression(expression) => {
                    expression.canonical_text().to_string()
                }
            })
            .collect(),
    }
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
pub(in crate::db) fn describe_entity_fields_with_persisted_schema(
    schema: &AcceptedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<Vec<EntityFieldDescription>, InternalError> {
    let row_layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(schema)?;
    describe_entity_fields_with_runtime_contract(schema, &row_layout, value_catalog)
}

fn describe_entity_fields_with_runtime_contract(
    schema: &AcceptedSchemaSnapshot,
    row_layout: &AcceptedRowLayoutRuntimeContract<'_>,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<Vec<EntityFieldDescription>, InternalError> {
    let snapshot = schema.persisted_snapshot();
    if snapshot.fields().len() != row_layout.fields().len() {
        return Err(InternalError::store_invariant());
    }
    let mut fields = Vec::with_capacity(snapshot.fields().len());

    // Accepted-schema describe surfaces must follow the stored schema payload,
    // not the generated model's current field order.
    for (field, runtime_field) in snapshot.fields().iter().zip(row_layout.fields()) {
        if field.id() != runtime_field.field_id() {
            return Err(InternalError::store_invariant());
        }
        let primary_key = snapshot.primary_key_field_ids().contains(&field.id());
        let slot = Some(runtime_field.slot().get());
        let metadata = DescribeFieldMetadata::new(
            summarize_persisted_field_kind(field.kind(), value_catalog)?,
            field.nullable(),
            field_type_from_persisted_kind(field.kind())
                .value_kind()
                .is_queryable(),
            field_origin_label(field.generated()),
        );
        let temporal = accepted_field_temporal_facts(runtime_field, value_catalog)?;

        push_described_field_row(
            &mut fields,
            field.name(),
            slot,
            primary_key,
            None,
            metadata,
            temporal,
        );

        if !field.nested_leaves().is_empty() {
            describe_persisted_nested_leaves(
                &mut fields,
                field.nested_leaves(),
                field_origin_label(field.generated()),
                value_catalog,
            )?;
        }
    }

    Ok(fields)
}

// Build model-only field descriptors with an injected top-level slot lookup.
// Accepted-schema introspection has a separate catalog-backed entrypoint above.
fn describe_entity_fields_with_slot_lookup(
    model: &EntityModel,
    mut slot_for_field: impl FnMut(usize, &FieldModel) -> Option<u16>,
) -> Vec<EntityFieldDescription> {
    let mut fields = Vec::with_capacity(model.fields.len());
    let primary_key_fields = primary_key_field_names_from_model(model);

    for (slot, field) in model.fields.iter().enumerate() {
        let primary_key = primary_key_fields
            .iter()
            .any(|primary_key_field| primary_key_field == field.name);
        describe_field_recursive(
            &mut fields,
            field.name,
            slot_for_field(slot, field),
            field,
            primary_key,
            None,
            None,
        );
    }

    fields
}

///
/// DescribeFieldMetadata
///
/// Field-description metadata selected before one field row is rendered.
///

struct DescribeFieldMetadata {
    kind: String,
    nullable: bool,
    queryable: bool,
    origin: String,
}

impl DescribeFieldMetadata {
    // Build one metadata bundle from already-rendered field facts.
    const fn new(kind: String, nullable: bool, queryable: bool, origin: String) -> Self {
        Self {
            kind,
            nullable,
            queryable,
            origin,
        }
    }
}

// Add one generated field and any generated composite-record leaves so
// DESCRIBE/SHOW COLUMNS expose the same nested rows SQL can project and filter.
fn describe_field_recursive(
    fields: &mut Vec<EntityFieldDescription>,
    name: &str,
    slot: Option<u16>,
    field: &FieldModel,
    primary_key: bool,
    tree_prefix: Option<&'static str>,
    metadata_override: Option<DescribeFieldMetadata>,
) {
    let temporal = if slot.is_some() {
        EntityFieldTemporalFacts::generated(field)
    } else {
        EntityFieldTemporalFacts::nested()
    };
    let metadata = metadata_override.unwrap_or_else(|| {
        DescribeFieldMetadata::new(
            summarize_field_kind(&field.kind),
            field.nullable(),
            field.kind.value_kind().is_queryable(),
            "generated".to_string(),
        )
    });

    push_described_field_row(
        fields,
        name,
        slot,
        primary_key,
        tree_prefix,
        metadata,
        temporal,
    );
    describe_generated_nested_fields(fields, field.nested_fields());
}

// Add one already-resolved field row to the stable describe DTO list. The
// caller owns where metadata came from: generated model or accepted schema.
fn push_described_field_row(
    fields: &mut Vec<EntityFieldDescription>,
    name: &str,
    slot: Option<u16>,
    primary_key: bool,
    tree_prefix: Option<&'static str>,
    metadata: DescribeFieldMetadata,
    temporal: EntityFieldTemporalFacts,
) {
    // Nested field rows keep a compact tree marker so table-oriented describe
    // output scans as a hierarchy without assigning nested leaves row slots.
    let display_name = if let Some(prefix) = tree_prefix {
        format!("{prefix}{name}")
    } else {
        name.to_string()
    };

    fields.push(EntityFieldDescription::new_with_temporal_facts(
        display_name,
        slot,
        primary_key,
        metadata,
        temporal,
    ));
}

// Render generated nested field metadata recursively for model-only
// introspection. Accepted introspection consumes persisted catalog-owned leaves.
fn describe_generated_nested_fields(
    fields: &mut Vec<EntityFieldDescription>,
    nested_fields: &[FieldModel],
) {
    for (index, nested) in nested_fields.iter().enumerate() {
        let prefix = if index + 1 == nested_fields.len() {
            "└─ "
        } else {
            "├─ "
        };
        describe_field_recursive(
            fields,
            nested.name(),
            None,
            nested,
            false,
            Some(prefix),
            None,
        );
    }
}

// Render accepted nested leaf descriptors. Nested leaves do not own physical
// row slots, so they always appear with the no-slot sentinel in the Candid DTO.
fn describe_persisted_nested_leaves(
    fields: &mut Vec<EntityFieldDescription>,
    nested_leaves: &[PersistedNestedLeafSnapshot],
    origin: String,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<(), InternalError> {
    for (index, leaf) in nested_leaves.iter().enumerate() {
        let prefix = if index + 1 == nested_leaves.len() {
            "└─ "
        } else {
            "├─ "
        };
        let name = leaf.path().last().map_or("", String::as_str);
        let metadata = DescribeFieldMetadata::new(
            summarize_persisted_field_kind(leaf.kind(), value_catalog)?,
            leaf.nullable(),
            field_type_from_persisted_kind(leaf.kind())
                .value_kind()
                .is_queryable(),
            origin.clone(),
        );

        push_described_field_row(
            fields,
            name,
            None,
            false,
            Some(prefix),
            metadata,
            EntityFieldTemporalFacts::nested(),
        );
    }

    Ok(())
}

fn field_origin_label(generated: bool) -> String {
    if generated {
        "generated".to_string()
    } else {
        "ddl".to_string()
    }
}

fn describe_entity_relations_with_persisted_schema(
    schema: &AcceptedSchemaSnapshot,
) -> Vec<EntityRelationDescription> {
    schema
        .persisted_snapshot()
        .fields()
        .iter()
        .filter_map(relation_description_from_persisted_field)
        .collect()
}

fn relation_description_from_persisted_field(
    field: &crate::db::schema::PersistedFieldSnapshot,
) -> Option<EntityRelationDescription> {
    let relation = persisted_relation_description_metadata(field.kind())?;

    Some(EntityRelationDescription::new(
        field.name().to_string(),
        relation.target_path.to_string(),
        relation.target_entity_name.to_string(),
        relation.target_store_path.to_string(),
        relation.cardinality,
    ))
}

struct PersistedRelationDescriptionMetadata<'a> {
    target_path: &'a str,
    target_entity_name: &'a str,
    target_store_path: &'a str,
    cardinality: EntityRelationCardinality,
}

fn persisted_relation_description_metadata(
    kind: &AcceptedFieldKind,
) -> Option<PersistedRelationDescriptionMetadata<'_>> {
    const fn from_relation_kind(
        kind: &AcceptedFieldKind,
        cardinality: EntityRelationCardinality,
    ) -> Option<PersistedRelationDescriptionMetadata<'_>> {
        let AcceptedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_store_path,
            ..
        } = kind
        else {
            return None;
        };

        Some(PersistedRelationDescriptionMetadata {
            target_path: target_path.as_str(),
            target_entity_name: target_entity_name.as_str(),
            target_store_path: target_store_path.as_str(),
            cardinality,
        })
    }

    match kind {
        AcceptedFieldKind::Relation { .. } => {
            from_relation_kind(kind, EntityRelationCardinality::Single)
        }
        AcceptedFieldKind::List(inner) => {
            from_relation_kind(inner, EntityRelationCardinality::List)
        }
        AcceptedFieldKind::Set(inner) => from_relation_kind(inner, EntityRelationCardinality::Set),
        _ => None,
    }
}

// Project relation-owned metadata into the stable describe DTO surface.
fn relation_description_from_metadata(
    metadata: RelationFieldMetadata,
) -> EntityRelationDescription {
    let cardinality = match metadata.cardinality() {
        RelationFieldCardinality::Single => EntityRelationCardinality::Single,
        RelationFieldCardinality::List => EntityRelationCardinality::List,
        RelationFieldCardinality::Set => EntityRelationCardinality::Set,
    };

    EntityRelationDescription::new(
        metadata.field_name().to_string(),
        metadata.target_path().to_string(),
        metadata.target_entity_name().to_string(),
        metadata.target_store_path().to_string(),
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
    if let Some(name) = kind.describe_kind_name() {
        out.push_str(name);
        return;
    }

    match kind {
        FieldKind::Blob { max_len } => {
            write_length_bounded_field_kind_summary(out, "blob", *max_len);
        }
        FieldKind::Decimal { scale } => {
            let _ = write!(out, "decimal(scale={scale})");
        }
        FieldKind::IntBig { max_bytes } => {
            write_byte_bounded_field_kind_summary(out, "int_big", *max_bytes);
        }
        FieldKind::Enum { path, .. } => {
            out.push_str("enum(");
            out.push_str(path);
            out.push(')');
        }
        FieldKind::Text { max_len } => {
            write_length_bounded_field_kind_summary(out, "text", *max_len);
        }
        FieldKind::Relation {
            target_entity_name,
            key_kind,
            ..
        } => {
            out.push_str("relation(target=");
            out.push_str(target_entity_name);
            out.push_str(", key=");
            write_field_kind_summary(out, key_kind);
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
        FieldKind::Composite { path, codec, shape } => {
            out.push_str("composite(path=");
            out.push_str(path);
            out.push_str(", codec=");
            write_composite_codec_summary(out, *codec);
            out.push_str(", shape=");
            write_generated_composite_shape_summary(out, shape);
            out.push(')');
        }
        FieldKind::Account
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int8
        | FieldKind::Int16
        | FieldKind::Int32
        | FieldKind::Int64
        | FieldKind::Int128
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Timestamp
        | FieldKind::Nat8
        | FieldKind::Nat16
        | FieldKind::Nat32
        | FieldKind::Nat64
        | FieldKind::Nat128
        | FieldKind::Ulid
        | FieldKind::Unit => unreachable!("schema describe invariant"),
        FieldKind::NatBig { max_bytes } => {
            write_byte_bounded_field_kind_summary(out, "nat_big", *max_bytes);
        }
    }
}

fn write_composite_codec_summary(out: &mut String, codec: CompositeCodec) {
    match codec {
        CompositeCodec::StructuralV1 => out.push_str("structural_v1"),
    }
}

fn write_generated_composite_shape_summary(out: &mut String, shape: &CompositeShapeModel) {
    match shape {
        CompositeShapeModel::Record(fields) => {
            out.push_str("record{");
            for (index, field) in fields.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                out.push_str(field.name());
                out.push(':');
                write_field_kind_summary(out, &field.kind());
                write_composite_nullability_summary(out, field.nullable());
            }
            out.push('}');
        }
        CompositeShapeModel::Tuple(elements) => {
            out.push_str("tuple<");
            for (index, element) in elements.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                write_generated_composite_element_summary(out, element);
            }
            out.push('>');
        }
        CompositeShapeModel::Newtype(inner) => {
            out.push_str("newtype<");
            write_generated_composite_element_summary(out, inner);
            out.push('>');
        }
    }
}

fn write_generated_composite_element_summary(out: &mut String, element: &CompositeElementModel) {
    write_field_kind_summary(out, &element.kind());
    write_composite_nullability_summary(out, element.nullable());
}

fn write_accepted_composite_shape_summary(
    out: &mut String,
    shape: &AcceptedCompositeShape,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<(), InternalError> {
    match shape {
        AcceptedCompositeShape::Record(fields) => {
            out.push_str("record{");
            for (index, field) in fields.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                out.push_str(field.name());
                out.push(':');
                write_accepted_composite_element_summary(out, field.contract(), value_catalog)?;
            }
            out.push('}');
        }
        AcceptedCompositeShape::Tuple(elements) => {
            out.push_str("tuple<");
            for (index, element) in elements.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                write_accepted_composite_element_summary(out, element, value_catalog)?;
            }
            out.push('>');
        }
        AcceptedCompositeShape::Newtype(inner) => {
            out.push_str("newtype<");
            write_accepted_composite_element_summary(out, inner, value_catalog)?;
            out.push('>');
        }
    }

    Ok(())
}

fn write_accepted_composite_element_summary(
    out: &mut String,
    element: &AcceptedCompositeElement,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<(), InternalError> {
    write_persisted_field_kind_summary(out, element.kind(), value_catalog)?;
    write_composite_nullability_summary(out, element.nullable());
    Ok(())
}

fn write_composite_nullability_summary(out: &mut String, nullable: bool) {
    if nullable {
        out.push('?');
    }
}

trait DescribeKindName {
    fn describe_kind_name(&self) -> Option<&'static str>;
}

impl DescribeKindName for FieldKind {
    fn describe_kind_name(&self) -> Option<&'static str> {
        Some(match self {
            Self::Account => "account",
            Self::Bool => "bool",
            Self::Date => "date",
            Self::Duration => "duration",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::Int128 => "int128",
            Self::Principal => "principal",
            Self::Subaccount => "subaccount",
            Self::Timestamp => "timestamp",
            Self::Nat8 => "nat8",
            Self::Nat16 => "nat16",
            Self::Nat32 => "nat32",
            Self::Nat64 => "nat64",
            Self::Nat128 => "nat128",
            Self::Ulid => "ulid",
            Self::Unit => "unit",
            Self::Blob { .. }
            | Self::Decimal { .. }
            | Self::Enum { .. }
            | Self::IntBig { .. }
            | Self::NatBig { .. }
            | Self::Text { .. }
            | Self::Relation { .. }
            | Self::List(_)
            | Self::Set(_)
            | Self::Map { .. }
            | Self::Composite { .. } => return None,
        })
    }
}

// Write the common text/blob describe label. Both generated and accepted schema
// summaries use this path so bounded and explicitly unbounded contracts stay
// visibly identical across `DESCRIBE` and `SHOW COLUMNS`.
fn write_length_bounded_field_kind_summary(
    out: &mut String,
    kind_name: &str,
    max_len: Option<u32>,
) {
    out.push_str(kind_name);
    if let Some(max_len) = max_len {
        out.push_str("(max_len=");
        out.push_str(&max_len.to_string());
        out.push(')');
    } else {
        out.push_str("(unbounded)");
    }
}

fn write_byte_bounded_field_kind_summary(out: &mut String, kind_name: &str, max_bytes: u32) {
    out.push_str(kind_name);
    out.push_str("(max_bytes=");
    out.push_str(&max_bytes.to_string());
    out.push(')');
}

// Project generated proposal metadata without pretending that generated code
// owns accepted runtime semantics. Encoded generated defaults can expose their
// exact byte identity; accepted introspection below additionally decodes them.
fn generated_insert_default_facts(
    default: FieldDatabaseDefault,
) -> (Option<String>, Option<u32>, Option<String>) {
    match default {
        FieldDatabaseDefault::None => (None, None, None),
        FieldDatabaseDefault::EncodedSlotPayload(payload) => {
            let bytes = u32::try_from(payload.len()).ok();
            (
                Some(encoded_payload_summary(payload)),
                bytes,
                Some(short_default_payload_fingerprint(payload)),
            )
        }
        FieldDatabaseDefault::AuthoredEnumUnit { enum_path, variant } => {
            (Some(format!("{enum_path}::{variant}")), None, None)
        }
    }
}

///
/// RenderedTemporalPayload
///
/// One accepted temporal payload projected as an inseparable bounded value,
/// byte count, and stable diagnostic hash.
///

struct RenderedTemporalPayload {
    value: String,
    bytes: u32,
    hash: String,
}

fn accepted_field_temporal_facts(
    field: &AcceptedRowLayoutRuntimeField<'_>,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<EntityFieldTemporalFacts, InternalError> {
    let write_policy = field.write_policy();
    let insert_omission = if write_policy.insert_generation().is_some() {
        "generated"
    } else if write_policy.write_management().is_some() {
        "managed"
    } else {
        match field.insert_omission_policy() {
            AcceptedInsertOmissionPolicy::NullIfMissing => "null",
            AcceptedInsertOmissionPolicy::DefaultIfMissing => "default",
            AcceptedInsertOmissionPolicy::Required => "required",
        }
    };
    let insert_default = field
        .insert_default()
        .slot_payload()
        .map(|payload| accepted_payload_facts(field, value_catalog, payload))
        .transpose()?;
    let (insert_default, insert_default_bytes, insert_default_hash) = match insert_default {
        Some(payload) => (Some(payload.value), Some(payload.bytes), Some(payload.hash)),
        None => (None, None, None),
    };
    let (historical_fill, historical_fill_bytes, historical_fill_hash) =
        match field.historical_fill() {
            SchemaHistoricalFill::Reject => (Some("reject".to_string()), None, None),
            SchemaHistoricalFill::Null => (Some("null".to_string()), None, None),
            SchemaHistoricalFill::SlotPayload(payload) => {
                let rendered = accepted_payload_facts(field, value_catalog, payload.as_slice())?;
                (
                    Some(rendered.value),
                    Some(rendered.bytes),
                    Some(rendered.hash),
                )
            }
        };

    Ok(EntityFieldTemporalFacts {
        insert_omission: Some(insert_omission.to_string()),
        insert_default,
        insert_default_bytes,
        insert_default_hash,
        introduced_in_layout: Some(field.introduced_in_layout().get()),
        historical_fill,
        historical_fill_bytes,
        historical_fill_hash,
    })
}

fn accepted_payload_facts(
    field: &AcceptedRowLayoutRuntimeField<'_>,
    value_catalog: &AcceptedValueCatalogHandle,
    payload: &[u8],
) -> Result<RenderedTemporalPayload, InternalError> {
    let persistence = AcceptedFieldPersistenceContract::new(value_catalog, field.decode_contract())
        .map_err(|_| InternalError::store_invariant())?;
    let admitted = decode_admitted_value_from_accepted_field_contract(persistence, payload)?;
    let output = output_value_from_runtime(value_catalog.enum_catalog(), admitted.value())
        .map_err(|_| InternalError::store_invariant())?;
    let rendered = bounded_schema_value_rendering(&output, payload);
    let bytes = u32::try_from(payload.len()).map_err(|_| InternalError::store_invariant())?;

    Ok(RenderedTemporalPayload {
        value: rendered,
        bytes,
        hash: short_default_payload_fingerprint(payload),
    })
}

fn bounded_schema_value_rendering(value: &OutputValue, payload: &[u8]) -> String {
    let rendered = match value {
        OutputValue::Text(value) => format!("'{}'", value.escape_default()),
        _ => render_output_value_text(value),
    };
    if rendered.len() <= MAX_SCHEMA_VALUE_RENDER_CHARS {
        return rendered;
    }

    format!(
        "{}(bytes={}, sha256={})",
        output_value_kind_label(value),
        payload.len(),
        short_default_payload_fingerprint(payload),
    )
}

const fn output_value_kind_label(value: &OutputValue) -> &'static str {
    match value {
        OutputValue::Account(_) => "account",
        OutputValue::Blob(_) => "blob",
        OutputValue::Bool(_) => "bool",
        OutputValue::Date(_) => "date",
        OutputValue::Decimal(_) => "decimal",
        OutputValue::Duration(_) => "duration",
        OutputValue::Enum(_) => "enum",
        OutputValue::Float32(_) => "float32",
        OutputValue::Float64(_) => "float64",
        OutputValue::Int64(_) => "int64",
        OutputValue::Int128(_) => "int128",
        OutputValue::IntBig(_) => "int_big",
        OutputValue::List(_) => "list",
        OutputValue::Map(_) => "map",
        OutputValue::Null => "null",
        OutputValue::Principal(_) => "principal",
        OutputValue::Subaccount(_) => "subaccount",
        OutputValue::Text(_) => "text",
        OutputValue::Timestamp(_) => "timestamp",
        OutputValue::Nat64(_) => "nat64",
        OutputValue::Nat128(_) => "nat128",
        OutputValue::NatBig(_) => "nat_big",
        OutputValue::Ulid(_) => "ulid",
        OutputValue::Unit => "unit",
    }
}

fn encoded_payload_summary(payload: &[u8]) -> String {
    format!(
        "slot_payload(bytes={}, sha256={})",
        payload.len(),
        short_default_payload_fingerprint(payload),
    )
}

fn short_default_payload_fingerprint(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    let mut out = String::with_capacity(16);
    for byte in &digest[..8] {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg_attr(
    doc,
    doc = "Render one stable field-kind label from accepted persisted schema metadata."
)]
fn summarize_persisted_field_kind(
    kind: &AcceptedFieldKind,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<String, InternalError> {
    let mut out = String::new();
    write_persisted_field_kind_summary(&mut out, kind, value_catalog)?;

    Ok(out)
}

// Stream the accepted persisted field-kind label in the same public format as
// generated `FieldKind` summaries. Top-level live-schema metadata can then
// drive DESCRIBE output without converting back into generated static types.
fn write_persisted_field_kind_summary(
    out: &mut String,
    kind: &AcceptedFieldKind,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<(), InternalError> {
    if let Some(name) = kind.describe_kind_name() {
        out.push_str(name);
        return Ok(());
    }

    match kind {
        AcceptedFieldKind::Blob { max_len } => {
            write_length_bounded_field_kind_summary(out, "blob", *max_len);
        }
        AcceptedFieldKind::Decimal { scale } => {
            let _ = write!(out, "decimal(scale={scale})");
        }
        AcceptedFieldKind::IntBig { max_bytes } => {
            write_byte_bounded_field_kind_summary(out, "int_big", *max_bytes);
        }
        AcceptedFieldKind::Enum { type_id } => {
            let definition = value_catalog
                .enum_catalog()
                .enum_type(*type_id)
                .ok_or_else(InternalError::store_invariant)?;
            out.push_str("enum(");
            out.push_str(definition.path());
            out.push(')');
        }
        AcceptedFieldKind::Text { max_len } => {
            write_length_bounded_field_kind_summary(out, "text", *max_len);
        }
        AcceptedFieldKind::Relation {
            target_entity_name,
            key_kind,
            ..
        } => {
            out.push_str("relation(target=");
            out.push_str(target_entity_name);
            out.push_str(", key=");
            write_persisted_field_kind_summary(out, key_kind, value_catalog)?;
            out.push(')');
        }
        AcceptedFieldKind::List(inner) => {
            out.push_str("list<");
            write_persisted_field_kind_summary(out, inner, value_catalog)?;
            out.push('>');
        }
        AcceptedFieldKind::Set(inner) => {
            out.push_str("set<");
            write_persisted_field_kind_summary(out, inner, value_catalog)?;
            out.push('>');
        }
        AcceptedFieldKind::Map { key, value } => {
            out.push_str("map<");
            write_persisted_field_kind_summary(out, key, value_catalog)?;
            out.push_str(", ");
            write_persisted_field_kind_summary(out, value, value_catalog)?;
            out.push('>');
        }
        AcceptedFieldKind::Composite { type_id } => {
            let composite_catalog = value_catalog.composite_catalog();
            let definition = composite_catalog
                .composite_type(*type_id)
                .ok_or_else(InternalError::store_invariant)?;
            out.push_str("composite(path=");
            out.push_str(definition.path());
            out.push_str(", codec=");
            write_composite_codec_summary(out, definition.codec());
            out.push_str(", shape=");
            write_accepted_composite_shape_summary(out, definition.shape(), value_catalog)?;
            out.push(')');
        }
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Date
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit => unreachable!("schema describe invariant"),
        AcceptedFieldKind::NatBig { max_bytes } => {
            write_byte_bounded_field_kind_summary(out, "nat_big", *max_bytes);
        }
    }

    Ok(())
}

impl DescribeKindName for AcceptedFieldKind {
    fn describe_kind_name(&self) -> Option<&'static str> {
        Some(match self {
            Self::Account => "account",
            Self::Bool => "bool",
            Self::Date => "date",
            Self::Duration => "duration",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
            Self::Int8 => "int8",
            Self::Int16 => "int16",
            Self::Int32 => "int32",
            Self::Int64 => "int64",
            Self::Int128 => "int128",
            Self::Principal => "principal",
            Self::Subaccount => "subaccount",
            Self::Timestamp => "timestamp",
            Self::Nat8 => "nat8",
            Self::Nat16 => "nat16",
            Self::Nat32 => "nat32",
            Self::Nat64 => "nat64",
            Self::Nat128 => "nat128",
            Self::Ulid => "ulid",
            Self::Unit => "unit",
            Self::Blob { .. }
            | Self::Decimal { .. }
            | Self::Enum { .. }
            | Self::IntBig { .. }
            | Self::NatBig { .. }
            | Self::Text { .. }
            | Self::Relation { .. }
            | Self::List(_)
            | Self::Set(_)
            | Self::Map { .. }
            | Self::Composite { .. } => return None,
        })
    }
}

//
// TESTS
//

#[cfg(test)]
mod tests;
