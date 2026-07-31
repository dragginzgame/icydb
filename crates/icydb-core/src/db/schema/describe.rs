//! Module: db::schema::describe
//! Responsibility: deterministic entity-schema introspection DTOs for runtime consumers.
//! Does not own: query planning, execution routing, or relation enforcement semantics.
//! Boundary: projects accepted schema metadata into stable describe surfaces.

use crate::{
    db::schema::CompositeCodec,
    db::{
        data::decode_admitted_value_from_accepted_field_contract,
        schema::{
            AcceptedConstraintKind, AcceptedFieldKind, AcceptedFieldPersistenceContract,
            AcceptedIdentityInspection, AcceptedInsertOmissionPolicy,
            AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle,
            ConstraintActivationKind, ConstraintActivationSnapshot, ConstraintActivationState,
            ConstraintOrigin, ConstraintValidationJob, FieldId, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot,
            SchemaHistoricalFill,
            composite_catalog::{AcceptedCompositeElement, AcceptedCompositeShape},
            field_type_from_persisted_kind, identity_kind_maximum, output_value_from_runtime,
            render_accepted_check_expr_sql,
            runtime::AcceptedRowLayoutRuntimeField,
        },
    },
    error::InternalError,
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
    pub(crate) identity: Option<Box<EntityIdentityDescription>>,
    pub(crate) fields: Vec<EntityFieldDescription>,
    pub(crate) indexes: Vec<EntityIndexDescription>,
    pub(crate) relations: Vec<EntityRelationDescription>,
    pub(crate) constraints: Vec<EntityConstraintDescription>,
    pub(crate) row_layout_current: u32,
    pub(crate) row_layout_history_floor: u32,
}

impl EntitySchemaDescription {
    /// Construct one entity schema description payload.
    #[expect(
        clippy::too_many_arguments,
        reason = "schema description construction keeps identity, collections, and layout explicit"
    )]
    #[must_use]
    pub const fn new(
        entity_path: String,
        entity_name: String,
        primary_key: String,
        primary_key_fields: Vec<String>,
        fields: Vec<EntityFieldDescription>,
        indexes: Vec<EntityIndexDescription>,
        relations: Vec<EntityRelationDescription>,
        constraints: Vec<EntityConstraintDescription>,
        row_layout_current: u32,
        row_layout_history_floor: u32,
    ) -> Self {
        Self {
            entity_path,
            entity_name,
            primary_key,
            primary_key_fields,
            identity: None,
            fields,
            indexes,
            relations,
            constraints,
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

    /// Borrow the accepted Identity policy and lifetime allocation state.
    #[must_use]
    pub fn identity(&self) -> Option<&EntityIdentityDescription> {
        self.identity.as_deref()
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

    /// Borrow accepted or generated structural constraint descriptions.
    #[must_use]
    pub const fn constraints(&self) -> &[EntityConstraintDescription] {
        self.constraints.as_slice()
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

    fn with_identity(mut self, identity: Option<EntityIdentityDescription>) -> Self {
        self.identity = identity.map(Box::new);
        self
    }
}

/// Accepted Identity generator policy, exact unsigned domain, and current
/// lifetime allocation state for one entity.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct EntityIdentityDescription {
    field: String,
    generator: String,
    accepted_kind: String,
    minimum: u128,
    maximum: u128,
    high_water: u128,
    remaining: u128,
    exhausted: bool,
}

impl EntityIdentityDescription {
    pub(in crate::db) fn new(
        field: String,
        accepted_kind: String,
        maximum: u128,
        high_water: u128,
    ) -> Result<Self, InternalError> {
        let remaining = maximum
            .checked_sub(high_water)
            .ok_or_else(InternalError::identity_state_corruption)?;
        Ok(Self {
            field,
            generator: "Identity::next".to_string(),
            accepted_kind,
            minimum: 1,
            maximum,
            high_water,
            remaining,
            exhausted: high_water == maximum,
        })
    }

    /// Borrow the accepted Identity field name.
    #[must_use]
    pub const fn field(&self) -> &str {
        self.field.as_str()
    }

    /// Borrow the fixed accepted generator spelling.
    #[must_use]
    pub const fn generator(&self) -> &str {
        self.generator.as_str()
    }

    /// Borrow the exact accepted unsigned field kind.
    #[must_use]
    pub const fn accepted_kind(&self) -> &str {
        self.accepted_kind.as_str()
    }

    /// Return the first generated value.
    #[must_use]
    pub const fn minimum(&self) -> u128 {
        self.minimum
    }

    /// Return the exact accepted lifetime allocation maximum.
    #[must_use]
    pub const fn maximum(&self) -> u128 {
        self.maximum
    }

    /// Return the greatest committed value, or zero before the first commit.
    #[must_use]
    pub const fn high_water(&self) -> u128 {
        self.high_water
    }

    /// Return the remaining lifetime allocation capacity.
    #[must_use]
    pub const fn remaining(&self) -> u128 {
        self.remaining
    }

    /// Return whether the exact accepted unsigned domain is exhausted.
    #[must_use]
    pub const fn exhausted(&self) -> bool {
        self.exhausted
    }
}

pub(in crate::db) fn describe_accepted_identity(
    identity: &AcceptedIdentityInspection,
    high_water: u128,
) -> Result<EntityIdentityDescription, InternalError> {
    let accepted_kind = describe_kind_name(identity.accepted_kind())
        .ok_or_else(InternalError::identity_state_corruption)?;
    let maximum = identity_kind_maximum(identity.accepted_kind())
        .ok_or_else(InternalError::identity_state_corruption)?;
    EntityIdentityDescription::new(
        identity.field_name().to_string(),
        accepted_kind.to_string(),
        maximum,
        high_water,
    )
}

#[cfg_attr(
    doc,
    doc = "EntityConstraintDescription\n\nOne accepted structural constraint entry in a describe payload."
)]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct EntityConstraintDescription {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) origin: String,
    pub(crate) validation_state: String,
    pub(crate) validation_progress: Option<ConstraintValidationProgressDescription>,
    pub(crate) field_id: Option<u32>,
    pub(crate) index_id: Option<u32>,
    pub(crate) relation_id: Option<u32>,
    pub(crate) fields: Vec<String>,
    pub(crate) index: Option<String>,
    pub(crate) relation: Option<String>,
    pub(crate) target_entity: Option<String>,
    pub(crate) action: Option<String>,
    pub(crate) semantics: String,
    pub(crate) check_sql: Option<String>,
}

/// Current bounded validation-job counters for one activating constraint.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ConstraintValidationProgressDescription {
    phase: String,
    rows_scanned: u64,
    findings_seen: u64,
    restarts: u64,
}

impl ConstraintValidationProgressDescription {
    fn from_job(job: &ConstraintValidationJob) -> Self {
        Self {
            phase: job.phase().as_str().to_string(),
            rows_scanned: job.rows_scanned(),
            findings_seen: job.findings_seen(),
            restarts: job.restarts(),
        }
    }

    /// Borrow the current bounded proof phase.
    #[must_use]
    pub const fn phase(&self) -> &str {
        self.phase.as_str()
    }

    /// Return the cumulative classified-row count.
    #[must_use]
    pub const fn rows_scanned(&self) -> u64 {
        self.rows_scanned
    }

    /// Return the cumulative finding count.
    #[must_use]
    pub const fn findings_seen(&self) -> u64 {
        self.findings_seen
    }

    /// Return the cumulative proof-restart count.
    #[must_use]
    pub const fn restarts(&self) -> u64 {
        self.restarts
    }
}

impl EntityConstraintDescription {
    /// Return the stable entity-local constraint identity.
    #[must_use]
    pub const fn id(&self) -> u32 {
        self.id
    }

    /// Borrow the stable accepted constraint name.
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Borrow the structural constraint kind label.
    #[must_use]
    pub const fn kind(&self) -> &str {
        self.kind.as_str()
    }

    /// Borrow the constraint origin label.
    #[must_use]
    pub const fn origin(&self) -> &str {
        self.origin.as_str()
    }

    /// Borrow the validation-state label.
    #[must_use]
    pub const fn validation_state(&self) -> &str {
        self.validation_state.as_str()
    }

    /// Borrow current bounded validation progress, when activation has begun.
    #[must_use]
    pub const fn validation_progress(&self) -> Option<&ConstraintValidationProgressDescription> {
        self.validation_progress.as_ref()
    }

    /// Return the referenced field identity for a not-null constraint.
    #[must_use]
    pub const fn field_id(&self) -> Option<u32> {
        self.field_id
    }

    /// Return the referenced logical index identity for a unique constraint.
    #[must_use]
    pub const fn index_id(&self) -> Option<u32> {
        self.index_id
    }

    /// Return the referenced logical relation identity.
    #[must_use]
    pub const fn relation_id(&self) -> Option<u32> {
        self.relation_id
    }

    /// Borrow current accepted field names participating in the constraint.
    #[must_use]
    pub const fn fields(&self) -> &[String] {
        self.fields.as_slice()
    }

    /// Borrow the current accepted index display name, when applicable.
    #[must_use]
    pub fn index(&self) -> Option<&str> {
        self.index.as_deref()
    }

    /// Borrow the current accepted relation display name, when applicable.
    #[must_use]
    pub fn relation(&self) -> Option<&str> {
        self.relation.as_deref()
    }

    /// Borrow the current relation target entity path, when applicable.
    #[must_use]
    pub fn target_entity(&self) -> Option<&str> {
        self.target_entity.as_deref()
    }

    /// Borrow the derived referential action, when applicable.
    #[must_use]
    pub fn action(&self) -> Option<&str> {
        self.action.as_deref()
    }

    /// Borrow the derived structural semantics label.
    #[must_use]
    pub const fn semantics(&self) -> &str {
        self.semantics.as_str()
    }

    /// Borrow the canonical accepted check expression, when applicable.
    #[must_use]
    pub fn check_sql(&self) -> Option<&str> {
        self.check_sql.as_deref()
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

/// Build one entity-schema description solely from accepted persisted authority.
pub(in crate::db) fn describe_accepted_entity_with_persisted_schema(
    schema: &AcceptedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
    validation_jobs: &[ConstraintValidationJob],
    identity: Option<EntityIdentityDescription>,
) -> Result<EntitySchemaDescription, InternalError> {
    describe_entity_with_persisted_schema(schema, value_catalog, validation_jobs, identity)
}

fn describe_entity_with_persisted_schema(
    schema: &AcceptedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
    validation_jobs: &[ConstraintValidationJob],
    identity: Option<EntityIdentityDescription>,
) -> Result<EntitySchemaDescription, InternalError> {
    let row_layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(schema)?;
    let fields = describe_entity_fields_with_runtime_contract(schema, &row_layout, value_catalog)?;
    let primary_key_fields = schema.primary_key_field_names();
    if primary_key_fields.is_empty() {
        return Err(InternalError::store_invariant());
    }
    let primary_key_fields = primary_key_fields
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let primary_key = render_primary_key_fields(primary_key_fields.as_slice());

    Ok(describe_entity_model_from_description_rows(
        schema.entity_path(),
        schema.entity_name(),
        primary_key.as_str(),
        primary_key_fields,
        fields,
        describe_entity_indexes_with_persisted_schema(schema),
        describe_entity_relations_with_persisted_schema(schema),
        describe_entity_constraints_with_persisted_schema(schema, value_catalog, validation_jobs)?,
        row_layout.current_layout_version().get(),
        row_layout.history_floor().get(),
    )
    .with_identity(identity))
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
    constraints: Vec<EntityConstraintDescription>,
    row_layout_current: u32,
    row_layout_history_floor: u32,
) -> EntitySchemaDescription {
    EntitySchemaDescription::new(
        entity_path.to_string(),
        entity_name.to_string(),
        primary_key.to_string(),
        primary_key_fields,
        fields,
        indexes,
        relations,
        constraints,
        row_layout_current,
        row_layout_history_floor,
    )
}

fn describe_entity_constraints_with_persisted_schema(
    schema: &AcceptedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
    validation_jobs: &[ConstraintValidationJob],
) -> Result<Vec<EntityConstraintDescription>, InternalError> {
    let snapshot = schema.persisted_snapshot();
    let mut descriptions = snapshot
        .constraints()
        .iter()
        .map(|constraint| describe_accepted_constraint(snapshot, value_catalog, constraint))
        .collect::<Result<Vec<_>, InternalError>>()?;
    descriptions.extend(
        snapshot
            .constraint_activations()
            .iter()
            .map(|activation| {
                let job = validation_jobs
                    .iter()
                    .find(|job| job.constraint_id() == activation.id());
                describe_constraint_activation(snapshot, value_catalog, activation, job)
            })
            .collect::<Result<Vec<_>, InternalError>>()?,
    );
    if validation_jobs.iter().any(|job| {
        !snapshot
            .constraint_activations()
            .iter()
            .any(|activation| activation.id() == job.constraint_id())
    }) {
        return Err(InternalError::store_invariant());
    }
    descriptions.sort_by_key(|description| {
        (
            description.id(),
            description.validation_state() != "validated",
        )
    });
    Ok(descriptions)
}

fn describe_accepted_constraint(
    snapshot: &PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
    constraint: &crate::db::schema::AcceptedConstraintSnapshot,
) -> Result<EntityConstraintDescription, InternalError> {
    let mut description = accepted_constraint_description(
        constraint.id().get(),
        constraint.name(),
        constraint.origin(),
    );
    match constraint.kind() {
        AcceptedConstraintKind::PrimaryKey => {
            description.kind = "primary_key".to_string();
            description.fields = snapshot
                .primary_key_field_ids()
                .iter()
                .map(|field_id| accepted_field_name(snapshot, *field_id))
                .collect::<Result<Vec<_>, _>>()?;
            description.semantics = "primary_key_v1".to_string();
        }
        AcceptedConstraintKind::NotNull { field_id } => {
            description.kind = "not_null".to_string();
            description.field_id = Some(field_id.get());
            description.fields = vec![accepted_field_name(snapshot, *field_id)?];
            description.semantics = "not_null_v1".to_string();
        }
        AcceptedConstraintKind::Unique { index_id } => {
            let index = snapshot
                .indexes()
                .iter()
                .find(|index| index.schema_id() == *index_id)
                .ok_or_else(InternalError::store_invariant)?;
            description.kind = "unique".to_string();
            description.index_id = Some(index_id.get());
            description.fields = describe_persisted_index_fields(index.key());
            description.index = Some(index.name().to_string());
            description.semantics = "unique_index_v1".to_string();
        }
        AcceptedConstraintKind::Relation { relation_id } => {
            let relation = snapshot
                .relations()
                .iter()
                .find(|relation| relation.id() == *relation_id)
                .ok_or_else(InternalError::store_invariant)?;
            description.kind = "relation".to_string();
            description.relation_id = Some(relation_id.get());
            description.fields = relation
                .local_field_ids()
                .iter()
                .map(|field_id| accepted_field_name(snapshot, *field_id))
                .collect::<Result<Vec<_>, _>>()?;
            description.relation = Some(relation.name().to_string());
            description.target_entity = Some(relation.target_path().to_string());
            description.action = Some("restrict".to_string());
            description.semantics = "relation_pk_restrict_v1".to_string();
        }
        AcceptedConstraintKind::Check { expression } => {
            description.kind = "check".to_string();
            description.fields = expression
                .dependencies()
                .into_iter()
                .map(|field_id| accepted_field_name(snapshot, field_id))
                .collect::<Result<Vec<_>, _>>()?;
            description.semantics = "check_expr_v1".to_string();
            description.check_sql = Some(render_accepted_check_expr_sql(
                expression,
                snapshot,
                value_catalog,
            )?);
        }
        AcceptedConstraintKind::TargetedRule { target, operation } => {
            description.kind = "targeted_rule".to_string();
            description.field_id = Some(target.root_field_id().get());
            description.fields = vec![accepted_field_name(snapshot, target.root_field_id())?];
            description.semantics = match operation.as_ref() {
                crate::db::schema::AcceptedRuleOperation::LengthRangeInclusive { .. } => {
                    "targeted_length_range_v1"
                }
                crate::db::schema::AcceptedRuleOperation::MultipleOf { .. } => {
                    "targeted_multiple_of_v1"
                }
                crate::db::schema::AcceptedRuleOperation::NumericMaximumInclusive { .. } => {
                    "targeted_numeric_maximum_v1"
                }
                crate::db::schema::AcceptedRuleOperation::NumericMinimumInclusive { .. } => {
                    "targeted_numeric_minimum_v1"
                }
                crate::db::schema::AcceptedRuleOperation::NumericRangeInclusive { .. } => {
                    "targeted_numeric_range_v1"
                }
            }
            .to_string();
        }
    }
    Ok(description)
}

fn describe_constraint_activation(
    snapshot: &PersistedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
    activation: &ConstraintActivationSnapshot,
    validation_job: Option<&ConstraintValidationJob>,
) -> Result<EntityConstraintDescription, InternalError> {
    let mut description = accepted_constraint_description(
        activation.id().get(),
        activation.name(),
        activation.origin(),
    );
    match activation.state() {
        ConstraintActivationState::EnforcingNewWrites if validation_job.is_none() => {
            description.validation_state = "enforcing_new_writes".to_string();
        }
        ConstraintActivationState::Validating => {
            let job = validation_job.ok_or_else(InternalError::store_invariant)?;
            job.validate(Some(activation))?;
            description.validation_state = "validating".to_string();
            description.validation_progress =
                Some(ConstraintValidationProgressDescription::from_job(job));
        }
        ConstraintActivationState::EnforcingNewWrites => {
            return Err(InternalError::store_invariant());
        }
    }
    match activation.kind() {
        ConstraintActivationKind::NotNull { field_id } => {
            description.kind = "not_null".to_string();
            description.field_id = Some(field_id.get());
            description.fields = vec![accepted_field_name(snapshot, *field_id)?];
            description.semantics = "not_null_v1".to_string();
        }
        ConstraintActivationKind::Unique { index_id } => {
            let index = snapshot
                .candidate_indexes()
                .iter()
                .find(|index| index.schema_id() == *index_id)
                .ok_or_else(InternalError::store_invariant)?;
            description.kind = "unique".to_string();
            description.index_id = Some(index_id.get());
            description.fields = describe_persisted_index_fields(index.key());
            description.index = Some(index.name().to_string());
            description.semantics = "unique_index_v1".to_string();
        }
        ConstraintActivationKind::Relation { relation_id } => {
            let relation = snapshot
                .candidate_relations()
                .iter()
                .find(|relation| relation.id() == *relation_id)
                .ok_or_else(InternalError::store_invariant)?;
            description.kind = "relation".to_string();
            description.relation_id = Some(relation_id.get());
            description.fields = relation
                .local_field_ids()
                .iter()
                .map(|field_id| accepted_field_name(snapshot, *field_id))
                .collect::<Result<Vec<_>, _>>()?;
            description.relation = Some(relation.name().to_string());
            description.target_entity = Some(relation.target_path().to_string());
            description.action = Some("restrict".to_string());
            description.semantics = "relation_pk_restrict_v1".to_string();
        }
        ConstraintActivationKind::Check { expression } => {
            description.kind = "check".to_string();
            description.fields = expression
                .dependencies()
                .into_iter()
                .map(|field_id| accepted_field_name(snapshot, field_id))
                .collect::<Result<Vec<_>, _>>()?;
            description.semantics = "check_expr_v1".to_string();
            description.check_sql = Some(render_accepted_check_expr_sql(
                expression,
                snapshot,
                value_catalog,
            )?);
        }
        ConstraintActivationKind::TargetedRule { target, operation } => {
            description.kind = "targeted_rule".to_string();
            description.field_id = Some(target.root_field_id().get());
            description.fields = vec![accepted_field_name(snapshot, target.root_field_id())?];
            description.semantics = match operation.as_ref() {
                crate::db::schema::AcceptedRuleOperation::LengthRangeInclusive { .. } => {
                    "targeted_length_range_v1"
                }
                crate::db::schema::AcceptedRuleOperation::MultipleOf { .. } => {
                    "targeted_multiple_of_v1"
                }
                crate::db::schema::AcceptedRuleOperation::NumericMaximumInclusive { .. } => {
                    "targeted_numeric_maximum_v1"
                }
                crate::db::schema::AcceptedRuleOperation::NumericMinimumInclusive { .. } => {
                    "targeted_numeric_minimum_v1"
                }
                crate::db::schema::AcceptedRuleOperation::NumericRangeInclusive { .. } => {
                    "targeted_numeric_range_v1"
                }
            }
            .to_string();
        }
    }
    Ok(description)
}

fn accepted_constraint_description(
    id: u32,
    name: &str,
    origin: ConstraintOrigin,
) -> EntityConstraintDescription {
    EntityConstraintDescription {
        id,
        name: name.to_string(),
        kind: String::new(),
        origin: accepted_constraint_origin_label(origin).to_string(),
        validation_state: "validated".to_string(),
        validation_progress: None,
        field_id: None,
        index_id: None,
        relation_id: None,
        fields: Vec::new(),
        index: None,
        relation: None,
        target_entity: None,
        action: None,
        semantics: String::new(),
        check_sql: None,
    }
}

const fn accepted_constraint_origin_label(origin: ConstraintOrigin) -> &'static str {
    match origin {
        ConstraintOrigin::Generated => "generated",
        ConstraintOrigin::SqlDdl => "sql_ddl",
    }
}

fn accepted_field_name(
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    field_id: FieldId,
) -> Result<String, InternalError> {
    snapshot
        .fields()
        .iter()
        .find(|field| field.id() == field_id)
        .map(|field| field.name().to_string())
        .ok_or_else(InternalError::store_invariant)
}

fn render_primary_key_fields(fields: &[String]) -> String {
    fields.join(", ")
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

#[cfg_attr(
    doc,
    doc = "Build field descriptors using accepted persisted schema slot metadata."
)]
#[cfg(any(test, feature = "query"))]
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
            field_type_from_persisted_kind(field.kind()).is_queryable(),
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
            field_type_from_persisted_kind(leaf.kind()).is_queryable(),
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

fn write_composite_codec_summary(out: &mut String, codec: CompositeCodec) {
    match codec {
        CompositeCodec::StructuralV1 => out.push_str("structural_v1"),
    }
}

fn write_composite_nullability_summary(out: &mut String, nullable: bool) {
    if nullable {
        out.push('?');
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
    let hash = short_default_payload_fingerprint(payload);
    let rendered = bounded_schema_value_rendering(&output, payload, hash.as_str());
    let bytes = u32::try_from(payload.len()).map_err(|_| InternalError::store_invariant())?;

    Ok(RenderedTemporalPayload {
        value: rendered,
        bytes,
        hash,
    })
}

fn bounded_schema_value_rendering(value: &OutputValue, payload: &[u8], hash: &str) -> String {
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
        hash,
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

// Stream the accepted persisted field-kind label in the stable public
// `DESCRIBE` format directly from live schema metadata.
fn write_persisted_field_kind_summary(
    out: &mut String,
    kind: &AcceptedFieldKind,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<(), InternalError> {
    if let Some(name) = describe_kind_name(kind) {
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
        | AcceptedFieldKind::Unit => return Err(InternalError::store_invariant()),
        AcceptedFieldKind::NatBig { max_bytes } => {
            write_byte_bounded_field_kind_summary(out, "nat_big", *max_bytes);
        }
    }

    Ok(())
}

const fn describe_kind_name(kind: &AcceptedFieldKind) -> Option<&'static str> {
    Some(match kind {
        AcceptedFieldKind::Account => "account",
        AcceptedFieldKind::Bool => "bool",
        AcceptedFieldKind::Date => "date",
        AcceptedFieldKind::Duration => "duration",
        AcceptedFieldKind::Float32 => "float32",
        AcceptedFieldKind::Float64 => "float64",
        AcceptedFieldKind::Int8 => "int8",
        AcceptedFieldKind::Int16 => "int16",
        AcceptedFieldKind::Int32 => "int32",
        AcceptedFieldKind::Int64 => "int64",
        AcceptedFieldKind::Int128 => "int128",
        AcceptedFieldKind::Principal => "principal",
        AcceptedFieldKind::Subaccount => "subaccount",
        AcceptedFieldKind::Timestamp => "timestamp",
        AcceptedFieldKind::Nat8 => "nat8",
        AcceptedFieldKind::Nat16 => "nat16",
        AcceptedFieldKind::Nat32 => "nat32",
        AcceptedFieldKind::Nat64 => "nat64",
        AcceptedFieldKind::Nat128 => "nat128",
        AcceptedFieldKind::Ulid => "ulid",
        AcceptedFieldKind::Unit => "unit",
        AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Decimal { .. }
        | AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::IntBig { .. }
        | AcceptedFieldKind::NatBig { .. }
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Relation { .. }
        | AcceptedFieldKind::List(_)
        | AcceptedFieldKind::Set(_)
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Composite { .. } => return None,
    })
}

//
// TESTS
//

#[cfg(test)]
mod tests {
    use super::EntityIdentityDescription;

    #[test]
    fn identity_description_reports_exact_remaining_capacity_and_exhaustion() {
        let available =
            EntityIdentityDescription::new("id".to_string(), "nat8".to_string(), 255, 254)
                .expect("in-domain Identity description should build");
        assert_eq!(available.minimum(), 1);
        assert_eq!(available.maximum(), 255);
        assert_eq!(available.high_water(), 254);
        assert_eq!(available.remaining(), 1);
        assert!(!available.exhausted());

        let exhausted =
            EntityIdentityDescription::new("id".to_string(), "nat8".to_string(), 255, 255)
                .expect("exact-domain exhaustion should remain describable");
        assert_eq!(exhausted.remaining(), 0);
        assert!(exhausted.exhausted());

        assert!(
            EntityIdentityDescription::new("id".to_string(), "nat8".to_string(), 255, 256).is_err(),
            "state beyond the accepted domain must not be described",
        );
    }
}
