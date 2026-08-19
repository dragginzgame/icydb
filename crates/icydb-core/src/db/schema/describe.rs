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
            ConstraintOrigin, ConstraintValidationJob, FieldId, FieldInsertGeneration,
            PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
            PersistedNestedLeafSnapshot, PersistedRelationEdgeSnapshot, PersistedSchemaSnapshot,
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
const MAX_SQL_COLUMN_EXTRA_FLAGS: usize = 3;
const MAX_SQL_COMPACT_COLUMN_ROWS: usize =
    icydb_schema::MAX_FRAGMENT_FIELDS * (1 + icydb_schema::MAX_FRAGMENT_FIELDS);

/// Compact accepted index-membership hint for one SQL column row.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlColumnKey {
    /// Accepted primary-key field.
    Primary,
    /// Sole field path in one accepted unique secondary index.
    Unique,
    /// Member of a compound or non-unique accepted secondary index.
    Multiple,
    /// No accepted primary or secondary index membership.
    None,
}

/// Compact accepted insert-default policy for one SQL column row.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlColumnDefault {
    /// Database-owned insert synthesis.
    Auto,
    /// Missing inserts produce `NULL`.
    Null,
    /// Bounded canonical accepted literal.
    Literal {
        /// Canonical rendered literal text.
        text: String,
    },
    /// A value is required and no accepted default exists.
    Required,
    /// Nested paths own no independent insert slot.
    NotApplicable,
}

impl SqlColumnDefault {
    /// Borrow canonical literal text when this is a literal default.
    #[must_use]
    pub const fn literal_text(&self) -> Option<&str> {
        match self {
            Self::Literal { text } => Some(text.as_str()),
            Self::Auto | Self::Null | Self::Required | Self::NotApplicable => None,
        }
    }
}

/// Closed compact extra-fact vocabulary for one SQL column row.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlColumnExtra {
    /// Accepted Identity generation owns this field.
    Identity,
    /// Accepted write policy synthesizes this field on insert.
    Generated,
    /// This field participates in an accepted relation edge.
    Relation,
}

/// Compact accepted-schema column projection.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlColumnSummary {
    name: String,
    field_type: String,
    nullable: bool,
    key: SqlColumnKey,
    default: SqlColumnDefault,
    extra: Vec<SqlColumnExtra>,
}

impl SqlColumnSummary {
    fn new(
        name: String,
        field_type: String,
        nullable: bool,
        key: SqlColumnKey,
        default: SqlColumnDefault,
        extra: Vec<SqlColumnExtra>,
    ) -> Result<Self, InternalError> {
        if extra.len() > MAX_SQL_COLUMN_EXTRA_FLAGS
            || default
                .literal_text()
                .is_some_and(|text| text.len() > MAX_SCHEMA_VALUE_RENDER_CHARS)
        {
            return Err(InternalError::store_invariant());
        }
        Ok(Self {
            name,
            field_type,
            nullable,
            key,
            default,
            extra,
        })
    }

    /// Borrow the canonical accepted query path.
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Borrow the accepted field-kind rendering.
    #[must_use]
    pub const fn field_type(&self) -> &str {
        self.field_type.as_str()
    }

    /// Return effective accepted explicit-nullability.
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the compact accepted index hint.
    #[must_use]
    pub const fn key(&self) -> SqlColumnKey {
        self.key
    }

    /// Borrow the compact accepted insert-default policy.
    #[must_use]
    pub const fn default(&self) -> &SqlColumnDefault {
        &self.default
    }

    /// Borrow ordered accepted extra facts.
    #[must_use]
    pub const fn extra(&self) -> &[SqlColumnExtra] {
        self.extra.as_slice()
    }
}

/// Discriminated public `DESCRIBE` result.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlDescribeOutput {
    /// Conventional compact column table.
    Compact {
        /// Accepted entity display name.
        entity: String,
        /// Canonical compact column rows.
        columns: Vec<SqlColumnSummary>,
    },
    /// Complete maintained operational dossier.
    Verbose {
        /// Complete accepted entity description.
        description: EntitySchemaDescription,
    },
}

/// Discriminated public `SHOW COLUMNS` result.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum SqlShowColumnsOutput {
    /// Compact column projection shared with `DESCRIBE`.
    Compact {
        /// Accepted entity display name.
        entity: String,
        /// Canonical compact column rows.
        columns: Vec<SqlColumnSummary>,
    },
    /// Detailed accepted field/layout rows only.
    Verbose {
        /// Accepted entity display name.
        entity: String,
        /// Maintained verbose field descriptions.
        columns: Vec<EntityFieldDescription>,
    },
}

/// Public `SHOW RELATIONS` result.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SqlShowRelationsOutput {
    entity: String,
    relations: Vec<EntityRelationDescription>,
}

impl SqlShowRelationsOutput {
    /// Build one bounded relation-only result.
    pub(in crate::db) fn new(
        entity: String,
        relations: Vec<EntityRelationDescription>,
    ) -> Result<Self, InternalError> {
        if relations.len() > icydb_schema::MAX_FRAGMENT_RELATIONS {
            return Err(InternalError::store_invariant());
        }
        Ok(Self { entity, relations })
    }

    /// Borrow the accepted entity display name.
    #[must_use]
    pub const fn entity(&self) -> &str {
        self.entity.as_str()
    }

    /// Borrow accepted relation rows in stable relation-ID order.
    #[must_use]
    pub const fn relations(&self) -> &[EntityRelationDescription] {
        self.relations.as_slice()
    }
}

#[cfg_attr(
    doc,
    doc = "EntitySchemaDescription\n\nStable describe payload for one entity model."
)]
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct EntitySchemaDescription {
    pub(crate) entity_path: String,
    pub(crate) entity_name: String,
    pub(crate) entity_tag: u64,
    pub(crate) accepted_schema_fingerprint_method: u8,
    pub(crate) accepted_schema_fingerprint: [u8; 16],
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
        entity_tag: u64,
        accepted_schema_fingerprint_method: u8,
        accepted_schema_fingerprint: [u8; 16],
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
            entity_tag,
            accepted_schema_fingerprint_method,
            accepted_schema_fingerprint,
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

    /// Return the accepted durable entity identity used by diagnostic facts.
    #[must_use]
    pub const fn entity_tag(&self) -> u64 {
        self.entity_tag
    }

    /// Return the accepted schema-fingerprint method used by diagnostic facts.
    #[must_use]
    pub const fn accepted_schema_fingerprint_method(&self) -> u8 {
        self.accepted_schema_fingerprint_method
    }

    /// Return the exact accepted entity-schema fingerprint.
    #[must_use]
    pub const fn accepted_schema_fingerprint(&self) -> [u8; 16] {
        self.accepted_schema_fingerprint
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
    pub(crate) predicate_sql: Option<String>,
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

    /// Borrow the accepted backing-index predicate, when the unique
    /// constraint describes partial uniqueness.
    #[must_use]
    pub fn predicate_sql(&self) -> Option<&str> {
        self.predicate_sql.as_deref()
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

/// Accepted identity and fingerprint metadata projected into one entity description.
pub(in crate::db) struct AcceptedEntityDescriptionMetadata {
    identity: Option<EntityIdentityDescription>,
    entity_tag: u64,
    accepted_schema_fingerprint_method: u8,
    accepted_schema_fingerprint: [u8; 16],
}

impl AcceptedEntityDescriptionMetadata {
    /// Capture the accepted metadata that accompanies persisted schema authority.
    pub(in crate::db) const fn new(
        identity: Option<EntityIdentityDescription>,
        entity_tag: u64,
        accepted_schema_fingerprint_method: u8,
        accepted_schema_fingerprint: [u8; 16],
    ) -> Self {
        Self {
            identity,
            entity_tag,
            accepted_schema_fingerprint_method,
            accepted_schema_fingerprint,
        }
    }
}

/// Build one entity-schema description solely from accepted persisted authority.
pub(in crate::db) fn describe_accepted_entity_with_persisted_schema(
    schema: &AcceptedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
    validation_jobs: &[ConstraintValidationJob],
    metadata: AcceptedEntityDescriptionMetadata,
    resolve_relation_target: impl Fn(&str) -> Result<(String, String), InternalError>,
) -> Result<EntitySchemaDescription, InternalError> {
    describe_entity_with_persisted_schema(
        schema,
        value_catalog,
        validation_jobs,
        metadata,
        &resolve_relation_target,
    )
}

fn describe_entity_with_persisted_schema(
    schema: &AcceptedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
    validation_jobs: &[ConstraintValidationJob],
    metadata: AcceptedEntityDescriptionMetadata,
    resolve_relation_target: &impl Fn(&str) -> Result<(String, String), InternalError>,
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
        metadata.entity_tag,
        metadata.accepted_schema_fingerprint_method,
        metadata.accepted_schema_fingerprint,
        primary_key.as_str(),
        primary_key_fields,
        fields,
        describe_entity_indexes_with_persisted_schema(schema),
        describe_entity_relations_with_persisted_schema(schema, resolve_relation_target)?,
        describe_entity_constraints_with_persisted_schema(schema, value_catalog, validation_jobs)?,
        row_layout.current_layout_version().get(),
        row_layout.history_floor().get(),
    )
    .with_identity(metadata.identity))
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
    entity_tag: u64,
    accepted_schema_fingerprint_method: u8,
    accepted_schema_fingerprint: [u8; 16],
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
        entity_tag,
        accepted_schema_fingerprint_method,
        accepted_schema_fingerprint,
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
    descriptions.sort_unstable_by_key(|description| {
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
            apply_unique_index_description(&mut description, index);
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
            apply_unique_index_description(&mut description, index);
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
        predicate_sql: None,
        relation: None,
        target_entity: None,
        action: None,
        semantics: String::new(),
        check_sql: None,
    }
}

fn apply_unique_index_description(
    description: &mut EntityConstraintDescription,
    index: &PersistedIndexSnapshot,
) {
    description.kind = "unique".to_string();
    description.index_id = Some(index.schema_id().get());
    description.fields = describe_persisted_index_fields(index.key());
    description.index = Some(index.name().to_string());
    description.predicate_sql = index.predicate_sql().map(str::to_string);
    description.semantics = if index.predicate_sql().is_some() {
        "partial_unique_index_v1"
    } else {
        "unique_index_v1"
    }
    .to_string();
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

/// Build the canonical compact SQL column projection from accepted authority.
pub(in crate::db) fn describe_compact_columns_with_persisted_schema(
    schema: &AcceptedSchemaSnapshot,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<Vec<SqlColumnSummary>, InternalError> {
    let row_layout = AcceptedRowLayoutRuntimeContract::from_accepted_schema(schema)?;
    let snapshot = schema.persisted_snapshot();
    if snapshot.fields().len() != row_layout.fields().len()
        || snapshot.fields().len() > icydb_schema::MAX_FRAGMENT_FIELDS
    {
        return Err(InternalError::store_invariant());
    }

    let capacity = compact_column_capacity(snapshot.fields())?;
    let mut accepted_fields = snapshot
        .fields()
        .iter()
        .zip(row_layout.fields())
        .collect::<Vec<_>>();
    accepted_fields.sort_unstable_by_key(|(field, _)| field.id());
    let mut columns = Vec::with_capacity(capacity);
    for (field, runtime_field) in accepted_fields {
        let matching_identity = field.id() == runtime_field.field_id();
        let matching_name = field.name() == runtime_field.name();
        if !matching_identity || !matching_name {
            return Err(InternalError::store_invariant());
        }

        let generated = accepted_write_policy_generates(runtime_field);
        let relation = snapshot
            .relations()
            .iter()
            .any(|relation| relation.local_field_ids().contains(&field.id()));
        let extra = compact_column_extras(
            runtime_field.write_policy().insert_generation()
                == Some(FieldInsertGeneration::Identity),
            generated,
            relation,
        );

        columns.push(SqlColumnSummary::new(
            field.name().to_string(),
            summarize_persisted_field_kind(field.kind(), value_catalog)?,
            field.nullable(),
            compact_column_key(snapshot, field.name()),
            compact_column_default(runtime_field, value_catalog)?,
            extra,
        )?);

        let mut nested = field.nested_leaves().iter().collect::<Vec<_>>();
        nested.sort_unstable_by(|left, right| left.path().cmp(right.path()));
        for leaf in nested {
            let mut canonical_path = Vec::with_capacity(leaf.path().len().saturating_add(1));
            canonical_path.push(field.name());
            canonical_path.extend(leaf.path().iter().map(String::as_str));
            let canonical_name = canonical_path.join(".");
            columns.push(SqlColumnSummary::new(
                canonical_name.clone(),
                summarize_persisted_field_kind(leaf.kind(), value_catalog)?,
                nested_path_nullable(field.nullable(), field.nested_leaves(), leaf.path()),
                compact_column_key(snapshot, canonical_name.as_str()),
                SqlColumnDefault::NotApplicable,
                compact_column_extras(false, generated, false),
            )?);
        }
    }

    if columns.len() != capacity {
        return Err(InternalError::store_invariant());
    }
    Ok(columns)
}

fn compact_column_capacity(
    fields: &[crate::db::schema::PersistedFieldSnapshot],
) -> Result<usize, InternalError> {
    compact_column_capacity_from_counts(
        fields.len(),
        fields.iter().map(|field| field.nested_leaves().len()),
    )
}

fn compact_column_capacity_from_counts(
    field_count: usize,
    nested_counts: impl IntoIterator<Item = usize>,
) -> Result<usize, InternalError> {
    if field_count > icydb_schema::MAX_FRAGMENT_FIELDS {
        return Err(InternalError::store_invariant());
    }
    let mut seen_fields = 0usize;
    let mut total = field_count;
    for nested_count in nested_counts {
        seen_fields = seen_fields
            .checked_add(1)
            .ok_or_else(InternalError::store_invariant)?;
        if nested_count > icydb_schema::MAX_FRAGMENT_FIELDS {
            return Err(InternalError::store_invariant());
        }
        total = total
            .checked_add(nested_count)
            .ok_or_else(InternalError::store_invariant)?;
    }
    if seen_fields != field_count {
        return Err(InternalError::store_invariant());
    }
    if total > MAX_SQL_COMPACT_COLUMN_ROWS {
        return Err(InternalError::store_invariant());
    }
    Ok(total)
}

const fn accepted_write_policy_generates(field: &AcceptedRowLayoutRuntimeField<'_>) -> bool {
    let policy = field.write_policy();
    policy.insert_generation().is_some() || policy.write_management().is_some()
}

fn compact_column_extras(identity: bool, generated: bool, relation: bool) -> Vec<SqlColumnExtra> {
    let mut extra = Vec::with_capacity(MAX_SQL_COLUMN_EXTRA_FLAGS);
    if identity {
        extra.push(SqlColumnExtra::Identity);
    }
    if generated {
        extra.push(SqlColumnExtra::Generated);
    }
    if relation {
        extra.push(SqlColumnExtra::Relation);
    }
    extra
}

fn compact_column_default(
    field: &AcceptedRowLayoutRuntimeField<'_>,
    value_catalog: &AcceptedValueCatalogHandle,
) -> Result<SqlColumnDefault, InternalError> {
    if accepted_write_policy_generates(field) {
        return Ok(SqlColumnDefault::Auto);
    }
    match field.insert_omission_policy() {
        AcceptedInsertOmissionPolicy::NullIfMissing => Ok(SqlColumnDefault::Null),
        AcceptedInsertOmissionPolicy::DefaultIfMissing => {
            let payload = field
                .insert_default()
                .slot_payload()
                .ok_or_else(InternalError::store_invariant)?;
            let rendered = accepted_payload_facts(field, value_catalog, payload)?;
            Ok(SqlColumnDefault::Literal {
                text: rendered.value,
            })
        }
        AcceptedInsertOmissionPolicy::Required => Ok(SqlColumnDefault::Required),
    }
}

fn nested_path_nullable(
    top_level_nullable: bool,
    leaves: &[PersistedNestedLeafSnapshot],
    path: &[String],
) -> bool {
    top_level_nullable
        || leaves.iter().any(|candidate| {
            candidate.path().len() <= path.len()
                && path.starts_with(candidate.path())
                && candidate.nullable()
        })
}

fn compact_column_key(snapshot: &PersistedSchemaSnapshot, path: &str) -> SqlColumnKey {
    let top_level_field = snapshot.fields().iter().find(|field| field.name() == path);
    let primary =
        top_level_field.is_some_and(|field| snapshot.primary_key_field_ids().contains(&field.id()));
    let memberships = snapshot.indexes().iter().filter_map(|index| {
        let key_items = match index.key() {
            PersistedIndexKeySnapshot::FieldPath(paths) => paths.len(),
            PersistedIndexKeySnapshot::Items(items) => items.len(),
        };
        let exact_path_member = match index.key() {
            PersistedIndexKeySnapshot::FieldPath(paths) => {
                paths.iter().any(|item| item.path().join(".") == path)
            }
            PersistedIndexKeySnapshot::Items(items) => items.iter().any(|item| {
                matches!(
                    item,
                    PersistedIndexKeyItemSnapshot::FieldPath(field_path)
                        if field_path.path().join(".") == path
                )
            }),
        };
        if !exact_path_member {
            return None;
        }
        Some((index.unique(), key_items))
    });
    classify_compact_column_key(primary, memberships)
}

fn classify_compact_column_key(
    primary: bool,
    memberships: impl IntoIterator<Item = (bool, usize)>,
) -> SqlColumnKey {
    if primary {
        return SqlColumnKey::Primary;
    }
    let mut multiple = false;
    for (unique, key_items) in memberships {
        if unique && key_items == 1 {
            return SqlColumnKey::Unique;
        }
        multiple = true;
    }
    if multiple {
        SqlColumnKey::Multiple
    } else {
        SqlColumnKey::None
    }
}

#[cfg_attr(
    doc,
    doc = "Build field descriptors using accepted persisted schema slot metadata."
)]
#[cfg(any(test, feature = "sql"))]
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

pub(in crate::db) fn describe_entity_relations_with_persisted_schema(
    schema: &AcceptedSchemaSnapshot,
    resolve_target: &impl Fn(&str) -> Result<(String, String), InternalError>,
) -> Result<Vec<EntityRelationDescription>, InternalError> {
    let snapshot = schema.persisted_snapshot();
    if snapshot.relations().len() > icydb_schema::MAX_FRAGMENT_RELATIONS {
        return Err(InternalError::store_invariant());
    }
    let mut relations = snapshot.relations().iter().collect::<Vec<_>>();
    relations.sort_unstable_by_key(|relation| relation.id());
    relations
        .into_iter()
        .map(|relation| {
            let local_fields = relation
                .local_field_ids()
                .iter()
                .map(|field_id| accepted_field_name(snapshot, *field_id))
                .collect::<Result<Vec<_>, _>>()?;
            let (target_entity_name, target_store_path) = resolve_target(relation.target_path())?;

            Ok(EntityRelationDescription::new(
                render_primary_key_fields(local_fields.as_slice()),
                relation.target_path().to_string(),
                target_entity_name,
                target_store_path,
                persisted_relation_cardinality(snapshot, relation)?,
            ))
        })
        .collect()
}

fn persisted_relation_cardinality(
    snapshot: &PersistedSchemaSnapshot,
    relation: &PersistedRelationEdgeSnapshot,
) -> Result<EntityRelationCardinality, InternalError> {
    let [field_id] = relation.local_field_ids() else {
        return Ok(EntityRelationCardinality::Single);
    };
    let field = snapshot
        .fields()
        .iter()
        .find(|field| field.id() == *field_id)
        .ok_or_else(InternalError::store_invariant)?;

    Ok(match field.kind() {
        AcceptedFieldKind::List(_) => EntityRelationCardinality::List,
        AcceptedFieldKind::Set(_) => EntityRelationCardinality::Set,
        _ => EntityRelationCardinality::Single,
    })
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
    use std::collections::BTreeMap;

    use super::{
        EntityIdentityDescription, EntityRelationCardinality, EntityRelationDescription,
        MAX_SCHEMA_VALUE_RENDER_CHARS, SqlColumnDefault, SqlColumnExtra, SqlColumnKey,
        SqlColumnSummary, SqlDescribeOutput, SqlShowRelationsOutput, classify_compact_column_key,
        compact_column_capacity_from_counts, compact_column_extras, describe_accepted_constraint,
        describe_compact_columns_with_persisted_schema, nested_path_nullable,
    };
    use crate::db::schema::{
        AcceptedCompositeCatalog, AcceptedConstraintCatalog, AcceptedFieldKind,
        AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle,
        CompositeFieldId, CompositeTypeId, FieldId, FieldStorageDecode, LeafCodec,
        MAX_SCHEMA_SNAPSHOT_BYTES, PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot,
        PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedNestedLeafSnapshot,
        PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot, SchemaIndexId, SchemaInsertDefault,
        SchemaRowLayout, SchemaVersion,
        composite_catalog::{
            AcceptedCompositeElement, AcceptedCompositeField, AcceptedCompositeShape,
            decode_accepted_composite_catalog, encode_accepted_composite_catalog,
        },
        decode_persisted_schema_snapshot, empty_accepted_enum_catalog_for_tests,
        encode_persisted_schema_snapshot,
    };

    use candid::Encode;

    const REACHABLE_COMPACT_REPLY_COMPOSITE_FIELDS: usize = icydb_schema::MAX_FRAGMENT_FIELDS;
    const REACHABLE_COMPACT_REPLY_TOP_LEVEL_COMPOSITES: usize = 95;
    const IC_QUERY_REPLY_BYTES: usize = 3 * 1024 * 1024;

    #[test]
    fn filtered_unique_constraint_description_exposes_partial_backing_contract() {
        let snapshot = PersistedSchemaSnapshot::new_with_indexes(
            SchemaVersion::initial(),
            "tests::Account".to_string(),
            "Account".to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ]),
            vec![
                PersistedFieldSnapshot::new_initial(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    AcceptedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaInsertDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new_initial(
                    FieldId::new(2),
                    "email".to_string(),
                    SchemaFieldSlot::new(1),
                    AcceptedFieldKind::Text { max_len: None },
                    Vec::new(),
                    true,
                    SchemaInsertDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Text),
                ),
            ],
            vec![PersistedIndexSnapshot::new(
                SchemaIndexId::new(1).expect("test index identity should be non-zero"),
                1,
                "account_email".to_string(),
                "tests::Account::account_email".to_string(),
                true,
                PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(2),
                    SchemaFieldSlot::new(1),
                    vec!["email".to_string()],
                    AcceptedFieldKind::Text { max_len: None },
                    true,
                )]),
                Some("email IS NOT NULL".to_string()),
            )],
        );
        let catalog = AcceptedConstraintCatalog::initial(
            snapshot.fields(),
            snapshot.indexes(),
            snapshot.relations(),
        )
        .expect("fixture constraints should build");
        let snapshot = snapshot.with_constraint_catalog(catalog);
        let value_catalog = AcceptedValueCatalogHandle::new_for_tests(
            empty_accepted_enum_catalog_for_tests(),
            AcceptedCompositeCatalog::empty(),
            AcceptedSchemaRevision::INITIAL,
        );
        let constraint = snapshot
            .constraints()
            .iter()
            .find(|constraint| constraint.name() == "account_email")
            .expect("unique constraint should exist");

        let description = describe_accepted_constraint(&snapshot, &value_catalog, constraint)
            .expect("accepted unique constraint should describe");
        assert_eq!(description.index_id(), Some(1));
        assert_eq!(description.index(), Some("account_email"));
        assert_eq!(description.predicate_sql(), Some("email IS NOT NULL"));
        assert_eq!(description.semantics(), "partial_unique_index_v1");
    }

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

    #[test]
    fn compact_key_contract_distinguishes_single_unique_from_compound_membership() {
        assert_eq!(
            classify_compact_column_key(true, [(true, 1), (false, 2)]),
            SqlColumnKey::Primary
        );
        assert_eq!(
            classify_compact_column_key(false, [(true, 2)]),
            SqlColumnKey::Multiple,
            "compound unique membership must not imply independent uniqueness",
        );
        assert_eq!(
            classify_compact_column_key(false, [(false, 1), (true, 1)]),
            SqlColumnKey::Unique,
            "single-field unique membership has precedence over non-unique membership",
        );
        assert_eq!(
            classify_compact_column_key(false, std::iter::empty()),
            SqlColumnKey::None
        );
    }

    #[test]
    fn compact_extra_contract_is_closed_and_deterministically_ordered() {
        assert_eq!(
            compact_column_extras(true, true, true),
            vec![
                SqlColumnExtra::Identity,
                SqlColumnExtra::Generated,
                SqlColumnExtra::Relation,
            ]
        );
        assert_eq!(
            compact_column_extras(false, true, false),
            vec![SqlColumnExtra::Generated]
        );
        assert!(compact_column_extras(false, false, false).is_empty());
    }

    #[test]
    fn compact_projection_bounds_accept_maximum_and_reject_max_plus_one() {
        assert_eq!(
            compact_column_capacity_from_counts(
                icydb_schema::MAX_FRAGMENT_FIELDS,
                std::iter::repeat_n(
                    icydb_schema::MAX_FRAGMENT_FIELDS,
                    icydb_schema::MAX_FRAGMENT_FIELDS,
                ),
            )
            .expect("accepted maximum should remain projectable"),
            super::MAX_SQL_COMPACT_COLUMN_ROWS,
        );
        assert!(
            compact_column_capacity_from_counts(
                icydb_schema::MAX_FRAGMENT_FIELDS + 1,
                std::iter::repeat_n(0, icydb_schema::MAX_FRAGMENT_FIELDS + 1),
            )
            .is_err()
        );
        assert!(
            compact_column_capacity_from_counts(1, [icydb_schema::MAX_FRAGMENT_FIELDS + 1],)
                .is_err()
        );

        let valid = SqlColumnSummary::new(
            "value".to_string(),
            "text".to_string(),
            false,
            SqlColumnKey::None,
            SqlColumnDefault::Literal {
                text: "x".repeat(MAX_SCHEMA_VALUE_RENDER_CHARS),
            },
            vec![
                SqlColumnExtra::Identity,
                SqlColumnExtra::Generated,
                SqlColumnExtra::Relation,
            ],
        );
        let valid = valid.expect("the complete admitted compact row should remain valid");
        assert!(
            SqlColumnSummary::new(
                "value".to_string(),
                "text".to_string(),
                false,
                SqlColumnKey::None,
                SqlColumnDefault::Literal {
                    text: "x".repeat(MAX_SCHEMA_VALUE_RENDER_CHARS + 1),
                },
                Vec::new(),
            )
            .is_err()
        );
        assert!(
            SqlColumnSummary::new(
                "value".to_string(),
                "text".to_string(),
                false,
                SqlColumnKey::None,
                SqlColumnDefault::Required,
                vec![SqlColumnExtra::Generated; 4],
            )
            .is_err()
        );

        let maximum = SqlDescribeOutput::Compact {
            entity: "AcceptedMaximum".to_string(),
            columns: vec![valid; super::MAX_SQL_COMPACT_COLUMN_ROWS],
        };
        let first = Encode!(&maximum).expect("accepted maximum should encode to bounded Candid");
        let second = Encode!(&maximum).expect("accepted maximum should encode deterministically");
        assert_eq!(first, second);
        assert_eq!(first.len(), 9_737_853);

        let relation = EntityRelationDescription::new(
            "owner_id".to_string(),
            "entities::Owner".to_string(),
            "Owner".to_string(),
            "stores::Owner".to_string(),
            EntityRelationCardinality::Single,
        );
        assert!(
            SqlShowRelationsOutput::new(
                "Entry".to_string(),
                vec![relation.clone(); icydb_schema::MAX_FRAGMENT_RELATIONS],
            )
            .is_ok()
        );
        assert!(
            SqlShowRelationsOutput::new(
                "Entry".to_string(),
                vec![relation; icydb_schema::MAX_FRAGMENT_RELATIONS + 1],
            )
            .is_err()
        );
    }

    #[test]
    fn reachable_accepted_compact_projection_exceeds_the_public_query_reply_limit() {
        let accepted = reachable_compact_reply_schema();
        let value_catalog = reachable_compact_reply_value_catalog();
        let columns = describe_compact_columns_with_persisted_schema(&accepted, &value_catalog)
            .expect("reachable accepted schema should project compact columns");

        assert_eq!(
            columns.len(),
            1 + REACHABLE_COMPACT_REPLY_TOP_LEVEL_COMPOSITES
                * (1 + REACHABLE_COMPACT_REPLY_COMPOSITE_FIELDS),
        );
        let output = SqlDescribeOutput::Compact {
            entity: accepted.entity_name().to_string(),
            columns,
        };
        let encoded_output =
            Encode!(&output).expect("reachable accepted compact output should encode");
        assert_eq!(encoded_output.len(), 3_693_116);
        assert!(
            encoded_output.len() > IC_QUERY_REPLY_BYTES,
            "a valid accepted schema must exercise the generated endpoint reply guard",
        );
    }

    #[test]
    fn nested_nullability_includes_nullable_ancestors() {
        let leaves = vec![
            PersistedNestedLeafSnapshot::new(
                vec!["address".to_string()],
                AcceptedFieldKind::Unit,
                true,
            ),
            PersistedNestedLeafSnapshot::new(
                vec!["address".to_string(), "city".to_string()],
                AcceptedFieldKind::Unit,
                false,
            ),
        ];
        assert!(nested_path_nullable(
            false,
            leaves.as_slice(),
            &["address".to_string(), "city".to_string()],
        ));
        assert!(nested_path_nullable(
            true,
            leaves.as_slice(),
            &["other".to_string()],
        ));
        assert!(!nested_path_nullable(
            false,
            leaves.as_slice(),
            &["other".to_string()],
        ));
    }

    fn compact_reply_leaf_name(index: usize) -> String {
        let first = u8::try_from(index / 26).expect("bounded leaf prefix fits u8") + b'a';
        let second = u8::try_from(index % 26).expect("bounded leaf suffix fits u8") + b'a';
        String::from_utf8(vec![first, second]).expect("ASCII leaf name should be UTF-8")
    }

    fn compact_reply_top_level_name(index: usize) -> String {
        let prefix = format!("field_{index:03}_");
        format!("{prefix}{}", "x".repeat(128 - prefix.len()))
    }

    fn reachable_compact_reply_schema() -> AcceptedSchemaSnapshot {
        let composite_type_id = CompositeTypeId::new(1).expect("one is non-zero");
        let nested_leaves = (0..REACHABLE_COMPACT_REPLY_COMPOSITE_FIELDS)
            .map(|index| {
                PersistedNestedLeafSnapshot::new(
                    vec![compact_reply_leaf_name(index)],
                    AcceptedFieldKind::Unit,
                    false,
                )
            })
            .collect::<Vec<_>>();
        let mut fields = vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )];
        let mut layout = vec![(FieldId::new(1), SchemaFieldSlot::new(0))];
        for index in 0..REACHABLE_COMPACT_REPLY_TOP_LEVEL_COMPOSITES {
            let raw_id = u32::try_from(index)
                .expect("bounded top-level index fits u32")
                .checked_add(2)
                .expect("bounded top-level identity has a successor");
            let raw_slot = u16::try_from(index)
                .expect("bounded top-level index fits u16")
                .checked_add(1)
                .expect("bounded top-level slot has a successor");
            let id = FieldId::new(raw_id);
            let slot = SchemaFieldSlot::new(raw_slot);
            fields.push(PersistedFieldSnapshot::new_initial(
                id,
                compact_reply_top_level_name(index),
                slot,
                AcceptedFieldKind::Composite {
                    type_id: composite_type_id,
                },
                nested_leaves.clone(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Structural,
            ));
            layout.push((id, slot));
        }
        let persisted = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "tests::ReachableCompactReply".to_string(),
            "ReachableCompactReply".to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(layout),
            fields,
        );
        let encoded = encode_persisted_schema_snapshot(&persisted)
            .expect("reachable compact-reply schema should fit its persisted payload limit");
        assert_eq!(encoded.len(), 310_861);
        assert!(encoded.len() <= MAX_SCHEMA_SNAPSHOT_BYTES as usize);
        AcceptedSchemaSnapshot::try_new(
            decode_persisted_schema_snapshot(encoded.as_slice())
                .expect("persisted compact-reply schema should decode"),
        )
        .expect("decoded compact-reply schema should satisfy accepted integrity")
    }

    fn reachable_compact_reply_value_catalog() -> AcceptedValueCatalogHandle {
        let enum_catalog = empty_accepted_enum_catalog_for_tests();
        let composite_type_id = CompositeTypeId::new(1).expect("one is non-zero");
        let composite_fields = (0..REACHABLE_COMPACT_REPLY_COMPOSITE_FIELDS)
            .map(|index| {
                let raw_id = u32::try_from(index)
                    .expect("bounded composite index fits u32")
                    .checked_add(1)
                    .expect("bounded composite identity has a successor");
                AcceptedCompositeField::new(
                    CompositeFieldId::new(raw_id).expect("composite field identity is non-zero"),
                    compact_reply_leaf_name(index),
                    AcceptedCompositeElement::new(AcceptedFieldKind::Unit, false),
                )
            })
            .collect::<Vec<_>>();
        let composite_catalog = AcceptedCompositeCatalog::from_initial_definitions(
            BTreeMap::from([(
                composite_type_id,
                (
                    "tests::CompactReplyRecord".to_string(),
                    AcceptedCompositeShape::Record(composite_fields),
                ),
            )]),
            &enum_catalog,
        )
        .expect("bounded reusable record composite should admit");
        let encoded = encode_accepted_composite_catalog(&composite_catalog, &enum_catalog)
            .expect("reachable composite authority should fit its persisted payload limit");
        assert!(encoded.len() <= MAX_SCHEMA_SNAPSHOT_BYTES as usize);
        let composite_catalog = decode_accepted_composite_catalog(&encoded, &enum_catalog)
            .expect("persisted composite authority should decode");
        AcceptedValueCatalogHandle::new_for_tests(
            enum_catalog,
            composite_catalog,
            AcceptedSchemaRevision::INITIAL,
        )
    }
}
