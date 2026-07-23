//! Module: db::schema::codec
//! Responsibility: typed persisted-schema snapshot encoding.
//! Does not own: reconciliation policy, schema proposal construction, or row decoding.
//! Boundary: converts schema-owned snapshot DTOs to/from raw `SchemaStore` payload bytes.

use crate::{
    db::schema::{
        AcceptedCheckCompareOpV1, AcceptedCheckExprV1, AcceptedCheckLiteralV1,
        AcceptedCheckValueExprV1, AcceptedConstraintCatalog, AcceptedConstraintKind,
        AcceptedConstraintSnapshot, AcceptedFieldKind, AcceptedSchemaFingerprint,
        ConstraintActivationFingerprint, ConstraintActivationKind, ConstraintActivationSnapshot,
        ConstraintActivationState, ConstraintId, ConstraintIdAllocator, ConstraintOrigin, FieldId,
        PersistedFieldOrigin, PersistedFieldSnapshot, PersistedIndexExpressionOp,
        PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
        PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexOrigin,
        PersistedIndexSnapshot, PersistedNestedLeafSnapshot, PersistedRelationEdgeSnapshot,
        PersistedSchemaSnapshot, RelationId, RowLayoutVersion, SchemaFieldSlot,
        SchemaFieldWritePolicy, SchemaHistoricalFill, SchemaIndexId, SchemaInsertDefault,
        SchemaRowLayout, SchemaVersion, composite_catalog::CompositeTypeId,
    },
    error::InternalError,
    model::field::{
        FieldInsertGeneration, FieldStorageDecode, FieldWriteManagement, LeafCodec, ScalarCodec,
    },
    types::EntityTag,
    value::EnumTypeId,
};
#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    static PERSISTED_SCHEMA_SNAPSHOT_DECODE_CALLS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
pub(in crate::db) fn reset_persisted_schema_snapshot_decode_count_for_tests() {
    PERSISTED_SCHEMA_SNAPSHOT_DECODE_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(in crate::db) fn persisted_schema_snapshot_decode_count_for_tests() -> u64 {
    PERSISTED_SCHEMA_SNAPSHOT_DECODE_CALLS.with(Cell::get)
}
use candid::{CandidType, Decode, Encode};
use serde::Deserialize;

const SCHEMA_SNAPSHOT_CODEC_VERSION: u32 = 1;
const SCHEMA_SNAPSHOT_CONTRACT_PROFILE: u32 = u32::from_be_bytes(*b"ICYZ");
/// Maximum canonical bytes for one persisted entity-schema snapshot.
pub(in crate::db) const MAX_SCHEMA_SNAPSHOT_BYTES: u32 = 512 * 1024;

// Candid wire container for one persisted schema snapshot.
//
// The public/internal schema DTOs remain normal Rust types; this wire shape is
// the only place that commits their current durable binary encoding.
#[derive(CandidType, Deserialize)]
struct PersistedSchemaSnapshotWire {
    codec_version: u32,
    contract_profile: u32,
    version: u32,
    entity_path: String,
    entity_name: String,
    primary_key_field_ids: Vec<u32>,
    row_layout: SchemaRowLayoutWire,
    constraint_id_high_water: u32,
    constraints: Vec<AcceptedConstraintSnapshotWire>,
    activations: Vec<ConstraintActivationSnapshotWire>,
    fields: Vec<PersistedFieldSnapshotWire>,
    indexes: Vec<PersistedIndexSnapshotWire>,
    relations: Vec<PersistedRelationEdgeSnapshotWire>,
    candidate_indexes: Vec<PersistedIndexSnapshotWire>,
    candidate_relations: Vec<PersistedRelationEdgeSnapshotWire>,
}

// Candid wire container for one accepted constraint registry entry.
#[derive(CandidType, Deserialize)]
struct AcceptedConstraintSnapshotWire {
    id: u32,
    name: String,
    origin: ConstraintOriginWire,
    kind: AcceptedConstraintKindWire,
}

// Candid wire container for one live accepted-schema activation.
#[derive(CandidType, Deserialize)]
struct ConstraintActivationSnapshotWire {
    id: u32,
    name: String,
    origin: ConstraintOriginWire,
    kind: ConstraintActivationKindWire,
    state: ConstraintActivationStateWire,
    base_schema_fingerprint: [u8; 32],
    activation_epoch: u64,
    fingerprint: [u8; 32],
}

// Candid wire enum for candidate constraint semantics.
#[derive(CandidType, Deserialize)]
enum ConstraintActivationKindWire {
    NotNull {
        field_id: u32,
    },
    Unique {
        index_id: u32,
    },
    Relation {
        relation_id: u32,
    },
    Check {
        expression: Box<AcceptedCheckExprV1Wire>,
    },
}

// Candid wire enum for the two live activation phases.
#[derive(CandidType, Deserialize)]
enum ConstraintActivationStateWire {
    EnforcingNewWrites,
    Validating,
}

// Candid wire enum for accepted constraint ownership.
#[derive(CandidType, Deserialize)]
enum ConstraintOriginWire {
    Generated,
    SqlDdl,
}

// Candid wire enum for one accepted structural constraint reference.
#[derive(CandidType, Deserialize)]
enum AcceptedConstraintKindWire {
    PrimaryKey,
    NotNull {
        field_id: u32,
    },
    Unique {
        index_id: u32,
    },
    Relation {
        relation_id: u32,
    },
    Check {
        expression: Box<AcceptedCheckExprV1Wire>,
    },
}

// Candid wire enum for the bounded canonical check expression.
#[derive(CandidType, Deserialize)]
enum AcceptedCheckExprV1Wire {
    True,
    False,
    Not(Box<Self>),
    And(Vec<Self>),
    Or(Vec<Self>),
    Compare {
        left: AcceptedCheckValueExprV1Wire,
        op: AcceptedCheckCompareOpV1Wire,
        right: AcceptedCheckValueExprV1Wire,
    },
    IsNull(AcceptedCheckValueExprV1Wire),
    IsNotNull(AcceptedCheckValueExprV1Wire),
}

// Candid wire enum for one canonical check value operand.
#[derive(CandidType, Deserialize)]
enum AcceptedCheckValueExprV1Wire {
    Field(u32),
    Literal(AcceptedCheckLiteralV1Wire),
    CharLength(u32),
    OctetLength(u32),
    Cardinality(u32),
}

// Candid wire container for one exact accepted literal payload.
#[derive(CandidType, Deserialize)]
struct AcceptedCheckLiteralV1Wire {
    kind: AcceptedFieldKindWire,
    storage_decode: FieldStorageDecodeWire,
    leaf_codec: LeafCodecWire,
    payload: Vec<u8>,
}

// Candid wire enum for one exact comparison operation.
#[derive(CandidType, Deserialize)]
enum AcceptedCheckCompareOpV1Wire {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

// Candid wire container for schema row-layout identity.
#[derive(CandidType, Deserialize)]
struct SchemaRowLayoutWire {
    current_version: u32,
    history_floor: u32,
    field_to_slot: Vec<(u32, u16)>,
}

// Candid wire container for one persisted schema field.
#[derive(CandidType, Deserialize)]
struct PersistedFieldSnapshotWire {
    id: u32,
    name: String,
    slot: u16,
    kind: AcceptedFieldKindWire,
    nested_leaves: Vec<PersistedNestedLeafSnapshotWire>,
    nullable: bool,
    introduced_in_layout: u32,
    insert_default: SchemaInsertDefaultWire,
    historical_fill: SchemaHistoricalFillWire,
    write_policy: SchemaFieldWritePolicyWire,
    origin: PersistedFieldOriginWire,
    storage_decode: FieldStorageDecodeWire,
    leaf_codec: LeafCodecWire,
}

// Candid wire enum for accepted field origin.
#[derive(CandidType, Deserialize)]
enum PersistedFieldOriginWire {
    Generated,
    SqlDdl,
}

// Candid wire container for one nested leaf rooted at a top-level field.
#[derive(CandidType, Deserialize)]
struct PersistedNestedLeafSnapshotWire {
    path: Vec<String>,
    kind: AcceptedFieldKindWire,
    nullable: bool,
}

// Candid wire container for one accepted index contract.
#[derive(CandidType, Deserialize)]
struct PersistedIndexSnapshotWire {
    schema_id: u32,
    ordinal: u16,
    physical_generation: u64,
    name: String,
    store: String,
    unique: bool,
    origin: PersistedIndexOriginWire,
    key: PersistedIndexKeySnapshotWire,
    predicate_sql: Option<String>,
}

// Candid wire container for one accepted relation-edge contract.
#[derive(CandidType, Deserialize)]
struct PersistedRelationEdgeSnapshotWire {
    relation_id: u32,
    physical_generation: u64,
    name: String,
    target_path: String,
    local_field_ids: Vec<u32>,
}

// Candid wire enum for accepted index origin.
#[derive(CandidType, Deserialize)]
enum PersistedIndexOriginWire {
    Generated,
    SqlDdl,
}

// Candid wire enum for accepted index key contracts.
#[derive(CandidType, Deserialize)]
enum PersistedIndexKeySnapshotWire {
    FieldPath(Vec<PersistedIndexFieldPathSnapshotWire>),
    Items(Vec<PersistedIndexKeyItemSnapshotWire>),
}

// Candid wire enum for one accepted explicit index key item.
#[derive(CandidType, Deserialize)]
enum PersistedIndexKeyItemSnapshotWire {
    FieldPath(PersistedIndexFieldPathSnapshotWire),
    Expression(Box<PersistedIndexExpressionSnapshotWire>),
}

// Candid wire container for one accepted field-path index key item.
#[derive(CandidType, Deserialize)]
struct PersistedIndexFieldPathSnapshotWire {
    field_id: u32,
    slot: u16,
    path: Vec<String>,
    kind: AcceptedFieldKindWire,
    nullable: bool,
}

// Candid wire container for one accepted expression index key item.
#[derive(CandidType, Deserialize)]
struct PersistedIndexExpressionSnapshotWire {
    op: PersistedIndexExpressionOpWire,
    source: PersistedIndexFieldPathSnapshotWire,
    input_kind: AcceptedFieldKindWire,
    output_kind: AcceptedFieldKindWire,
    canonical_text: String,
}

// Candid wire enum for accepted expression index operations.
#[derive(CandidType, Deserialize)]
enum PersistedIndexExpressionOpWire {
    Lower,
    Upper,
    Trim,
    LowerTrim,
    Date,
    Year,
    Month,
    Day,
}

// Candid wire enum for database-level default metadata.
#[derive(CandidType, Deserialize)]
enum SchemaInsertDefaultWire {
    None,
    SlotPayload(Vec<u8>),
}

// Candid wire enum for one frozen historical physical-absence response.
#[derive(CandidType, Deserialize)]
enum SchemaHistoricalFillWire {
    Reject,
    Null,
    SlotPayload(Vec<u8>),
}

// Candid wire container for database-level write policy metadata.
#[derive(CandidType, Deserialize)]
struct SchemaFieldWritePolicyWire {
    insert_generation: Option<FieldInsertGenerationWire>,
    write_management: Option<FieldWriteManagementWire>,
}

// Candid wire enum for insert-time generated value metadata.
#[derive(CandidType, Deserialize)]
enum FieldInsertGenerationWire {
    Ulid,
    Timestamp,
}

// Candid wire enum for managed write metadata.
#[derive(CandidType, Deserialize)]
enum FieldWriteManagementWire {
    CreatedAt,
    UpdatedAt,
}

// Candid wire enum for the complete persisted field-kind shape.
#[derive(CandidType, Deserialize)]
enum AcceptedFieldKindWire {
    Account,
    Blob {
        max_len: Option<u32>,
    },
    Bool,
    Date,
    Decimal {
        scale: u32,
    },
    Duration,
    Enum {
        type_id: u32,
    },
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    IntBig {
        max_bytes: u32,
    },
    Principal,
    Subaccount,
    Text {
        max_len: Option<u32>,
    },
    Timestamp,
    Nat8,
    Nat16,
    Nat32,
    Nat64,
    Nat128,
    NatBig {
        max_bytes: u32,
    },
    Ulid,
    Unit,
    Relation {
        target_path: String,
        target_entity_name: String,
        target_entity_tag: u64,
        target_store_path: String,
        key_kind: Box<Self>,
    },
    List(Box<Self>),
    Set(Box<Self>),
    Map {
        key: Box<Self>,
        value: Box<Self>,
    },
    Composite {
        type_id: u32,
    },
}

// Candid wire enum for slot payload decode policy.
#[derive(CandidType, Deserialize)]
enum FieldStorageDecodeWire {
    ByKind,
    CatalogValue,
}

// Candid wire enum for leaf payload codecs.
#[derive(CandidType, Deserialize)]
enum LeafCodecWire {
    Scalar(ScalarCodecWire),
    Structural,
}

// Candid wire enum for scalar leaf payload codecs.
#[derive(CandidType, Deserialize)]
enum ScalarCodecWire {
    Blob,
    Bool,
    Date,
    Duration,
    Float32,
    Float64,
    Int64,
    Principal,
    Subaccount,
    Text,
    Timestamp,
    Nat64,
    Ulid,
    Unit,
}

/// Encode one typed persisted-schema snapshot into durable raw bytes.
pub(in crate::db) fn encode_persisted_schema_snapshot(
    snapshot: &PersistedSchemaSnapshot,
) -> Result<Vec<u8>, InternalError> {
    if !snapshot.has_valid_integrity() {
        return Err(InternalError::store_invariant());
    }
    let wire = PersistedSchemaSnapshotWire::from_snapshot(snapshot);

    encode_persisted_schema_snapshot_wire(&wire)
}

/// Encode an intentionally malformed typed fixture for raw decode-boundary tests.
#[cfg(test)]
pub(in crate::db) fn encode_unchecked_persisted_schema_snapshot_for_tests(
    snapshot: &PersistedSchemaSnapshot,
) -> Result<Vec<u8>, InternalError> {
    let wire = PersistedSchemaSnapshotWire::from_snapshot(snapshot);

    encode_persisted_schema_snapshot_wire(&wire)
}

/// Decode one typed persisted-schema snapshot from durable raw bytes.
pub(in crate::db) fn decode_persisted_schema_snapshot(
    bytes: &[u8],
) -> Result<PersistedSchemaSnapshot, InternalError> {
    if bytes.len() > MAX_SCHEMA_SNAPSHOT_BYTES as usize {
        return Err(InternalError::store_corruption());
    }

    #[cfg(test)]
    PERSISTED_SCHEMA_SNAPSHOT_DECODE_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));

    let wire = Decode!(bytes, PersistedSchemaSnapshotWire)
        .map_err(|_| InternalError::store_corruption())?;

    wire.into_snapshot()
}

/// Encode one schema wire while enforcing the same bound as persisted decode.
fn encode_persisted_schema_snapshot_wire(
    wire: &PersistedSchemaSnapshotWire,
) -> Result<Vec<u8>, InternalError> {
    let encoded = Encode!(wire).map_err(|_| InternalError::store_corruption())?;
    if encoded.len() > MAX_SCHEMA_SNAPSHOT_BYTES as usize {
        return Err(InternalError::store_unsupported());
    }

    Ok(encoded)
}

impl PersistedSchemaSnapshotWire {
    fn from_snapshot(snapshot: &PersistedSchemaSnapshot) -> Self {
        Self {
            codec_version: SCHEMA_SNAPSHOT_CODEC_VERSION,
            contract_profile: SCHEMA_SNAPSHOT_CONTRACT_PROFILE,
            version: snapshot.version().get(),
            entity_path: snapshot.entity_path().to_string(),
            entity_name: snapshot.entity_name().to_string(),
            primary_key_field_ids: snapshot
                .primary_key_field_ids()
                .iter()
                .map(|field_id| field_id.get())
                .collect(),
            row_layout: SchemaRowLayoutWire::from_layout(snapshot.row_layout()),
            constraint_id_high_water: snapshot.constraint_id_allocator().high_water(),
            constraints: snapshot
                .constraints()
                .iter()
                .map(AcceptedConstraintSnapshotWire::from_constraint)
                .collect(),
            activations: snapshot
                .constraint_activations()
                .iter()
                .map(ConstraintActivationSnapshotWire::from_activation)
                .collect(),
            fields: snapshot
                .fields()
                .iter()
                .map(PersistedFieldSnapshotWire::from_field)
                .collect(),
            indexes: snapshot
                .indexes()
                .iter()
                .map(PersistedIndexSnapshotWire::from_index)
                .collect(),
            relations: snapshot
                .relations()
                .iter()
                .map(PersistedRelationEdgeSnapshotWire::from_relation)
                .collect(),
            candidate_indexes: snapshot
                .candidate_indexes()
                .iter()
                .map(PersistedIndexSnapshotWire::from_index)
                .collect(),
            candidate_relations: snapshot
                .candidate_relations()
                .iter()
                .map(PersistedRelationEdgeSnapshotWire::from_relation)
                .collect(),
        }
    }

    fn into_snapshot(self) -> Result<PersistedSchemaSnapshot, InternalError> {
        if self.codec_version != SCHEMA_SNAPSHOT_CODEC_VERSION
            || self.contract_profile != SCHEMA_SNAPSHOT_CONTRACT_PROFILE
        {
            return Err(InternalError::serialize_incompatible_persisted_format());
        }

        let version = SchemaVersion::new(self.version);
        let row_layout = self.row_layout.into_layout()?;
        let fields = self
            .fields
            .into_iter()
            .map(PersistedFieldSnapshotWire::into_field)
            .collect::<Result<Vec<_>, _>>()?;
        let primary_key_field_ids = self
            .primary_key_field_ids
            .into_iter()
            .map(FieldId::new)
            .collect::<Vec<_>>();
        let indexes = self
            .indexes
            .into_iter()
            .map(PersistedIndexSnapshotWire::into_index)
            .collect::<Result<Vec<_>, _>>()?;
        let relations = self
            .relations
            .into_iter()
            .map(PersistedRelationEdgeSnapshotWire::into_relation)
            .collect::<Result<Vec<_>, _>>()?;
        let candidate_indexes = self
            .candidate_indexes
            .into_iter()
            .map(PersistedIndexSnapshotWire::into_index)
            .collect::<Result<Vec<_>, _>>()?;
        let candidate_relations = self
            .candidate_relations
            .into_iter()
            .map(PersistedRelationEdgeSnapshotWire::into_relation)
            .collect::<Result<Vec<_>, _>>()?;
        let constraints = self
            .constraints
            .into_iter()
            .map(AcceptedConstraintSnapshotWire::into_constraint)
            .collect::<Result<Vec<_>, _>>()?;
        let activations = self
            .activations
            .into_iter()
            .map(ConstraintActivationSnapshotWire::into_activation)
            .collect::<Result<Vec<_>, _>>()?;
        let constraint_catalog = AcceptedConstraintCatalog::from_persisted_parts(
            ConstraintIdAllocator::new(self.constraint_id_high_water),
            constraints,
            activations,
        );
        let snapshot = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            version,
            self.entity_path,
            self.entity_name,
            primary_key_field_ids,
            row_layout,
            fields,
            indexes,
        )
        .with_constraint_catalog(constraint_catalog)
        .with_relations(relations)
        .with_constraint_candidates(candidate_indexes, candidate_relations);
        if !snapshot.has_valid_integrity() {
            return Err(InternalError::store_corruption());
        }

        Ok(snapshot)
    }
}

impl ConstraintActivationSnapshotWire {
    fn from_activation(activation: &ConstraintActivationSnapshot) -> Self {
        Self {
            id: activation.id().get(),
            name: activation.name().to_string(),
            origin: ConstraintOriginWire::from_origin(activation.origin()),
            kind: ConstraintActivationKindWire::from_kind(activation.kind()),
            state: ConstraintActivationStateWire::from_state(activation.state()),
            base_schema_fingerprint: activation.base_schema_fingerprint().as_bytes(),
            activation_epoch: activation.activation_epoch(),
            fingerprint: activation.fingerprint().as_bytes(),
        }
    }

    fn into_activation(self) -> Result<ConstraintActivationSnapshot, InternalError> {
        let id = ConstraintId::new(self.id).ok_or_else(InternalError::store_corruption)?;
        Ok(ConstraintActivationSnapshot::from_persisted_parts(
            id,
            self.name,
            self.origin.into_origin(),
            self.kind.into_kind()?,
            self.state.into_state(),
            AcceptedSchemaFingerprint::new(self.base_schema_fingerprint),
            self.activation_epoch,
            ConstraintActivationFingerprint::new(self.fingerprint),
        ))
    }
}

impl ConstraintActivationKindWire {
    fn from_kind(kind: &ConstraintActivationKind) -> Self {
        match kind {
            ConstraintActivationKind::NotNull { field_id } => Self::NotNull {
                field_id: field_id.get(),
            },
            ConstraintActivationKind::Unique { index_id } => Self::Unique {
                index_id: index_id.get(),
            },
            ConstraintActivationKind::Relation { relation_id } => Self::Relation {
                relation_id: relation_id.get(),
            },
            ConstraintActivationKind::Check { expression } => Self::Check {
                expression: Box::new(AcceptedCheckExprV1Wire::from_expression(expression)),
            },
        }
    }

    fn into_kind(self) -> Result<ConstraintActivationKind, InternalError> {
        match self {
            Self::NotNull { field_id } => Ok(ConstraintActivationKind::NotNull {
                field_id: FieldId::new(field_id),
            }),
            Self::Unique { index_id } => Ok(ConstraintActivationKind::Unique {
                index_id: SchemaIndexId::new(index_id)
                    .ok_or_else(InternalError::store_corruption)?,
            }),
            Self::Relation { relation_id } => Ok(ConstraintActivationKind::Relation {
                relation_id: RelationId::new(relation_id)
                    .ok_or_else(InternalError::store_corruption)?,
            }),
            Self::Check { expression } => Ok(ConstraintActivationKind::Check {
                expression: Box::new((*expression).into_expression()?),
            }),
        }
    }
}

impl ConstraintActivationStateWire {
    const fn from_state(state: ConstraintActivationState) -> Self {
        match state {
            ConstraintActivationState::EnforcingNewWrites => Self::EnforcingNewWrites,
            ConstraintActivationState::Validating => Self::Validating,
        }
    }

    const fn into_state(self) -> ConstraintActivationState {
        match self {
            Self::EnforcingNewWrites => ConstraintActivationState::EnforcingNewWrites,
            Self::Validating => ConstraintActivationState::Validating,
        }
    }
}

impl AcceptedConstraintSnapshotWire {
    fn from_constraint(constraint: &AcceptedConstraintSnapshot) -> Self {
        Self {
            id: constraint.id().get(),
            name: constraint.name().to_string(),
            origin: ConstraintOriginWire::from_origin(constraint.origin()),
            kind: AcceptedConstraintKindWire::from_kind(constraint.kind()),
        }
    }

    fn into_constraint(self) -> Result<AcceptedConstraintSnapshot, InternalError> {
        let id = ConstraintId::new(self.id).ok_or_else(InternalError::store_corruption)?;
        Ok(AcceptedConstraintSnapshot::new(
            id,
            self.name,
            self.origin.into_origin(),
            self.kind.into_kind()?,
        ))
    }
}

impl ConstraintOriginWire {
    const fn from_origin(origin: ConstraintOrigin) -> Self {
        match origin {
            ConstraintOrigin::Generated => Self::Generated,
            ConstraintOrigin::SqlDdl => Self::SqlDdl,
        }
    }

    const fn into_origin(self) -> ConstraintOrigin {
        match self {
            Self::Generated => ConstraintOrigin::Generated,
            Self::SqlDdl => ConstraintOrigin::SqlDdl,
        }
    }
}

impl AcceptedConstraintKindWire {
    fn from_kind(kind: &AcceptedConstraintKind) -> Self {
        match kind {
            AcceptedConstraintKind::PrimaryKey => Self::PrimaryKey,
            AcceptedConstraintKind::NotNull { field_id } => Self::NotNull {
                field_id: field_id.get(),
            },
            AcceptedConstraintKind::Unique { index_id } => Self::Unique {
                index_id: index_id.get(),
            },
            AcceptedConstraintKind::Relation { relation_id } => Self::Relation {
                relation_id: relation_id.get(),
            },
            AcceptedConstraintKind::Check { expression } => Self::Check {
                expression: Box::new(AcceptedCheckExprV1Wire::from_expression(expression)),
            },
        }
    }

    fn into_kind(self) -> Result<AcceptedConstraintKind, InternalError> {
        match self {
            Self::PrimaryKey => Ok(AcceptedConstraintKind::PrimaryKey),
            Self::NotNull { field_id } => Ok(AcceptedConstraintKind::NotNull {
                field_id: FieldId::new(field_id),
            }),
            Self::Unique { index_id } => Ok(AcceptedConstraintKind::Unique {
                index_id: SchemaIndexId::new(index_id)
                    .ok_or_else(InternalError::store_corruption)?,
            }),
            Self::Relation { relation_id } => Ok(AcceptedConstraintKind::Relation {
                relation_id: RelationId::new(relation_id)
                    .ok_or_else(InternalError::store_corruption)?,
            }),
            Self::Check { expression } => Ok(AcceptedConstraintKind::Check {
                expression: Box::new((*expression).into_expression()?),
            }),
        }
    }
}

impl AcceptedCheckExprV1Wire {
    fn from_expression(expression: &AcceptedCheckExprV1) -> Self {
        match expression {
            AcceptedCheckExprV1::True => Self::True,
            AcceptedCheckExprV1::False => Self::False,
            AcceptedCheckExprV1::Not(inner) => Self::Not(Box::new(Self::from_expression(inner))),
            AcceptedCheckExprV1::And(children) => {
                Self::And(children.iter().map(Self::from_expression).collect())
            }
            AcceptedCheckExprV1::Or(children) => {
                Self::Or(children.iter().map(Self::from_expression).collect())
            }
            AcceptedCheckExprV1::Compare { left, op, right } => Self::Compare {
                left: AcceptedCheckValueExprV1Wire::from_value(left),
                op: AcceptedCheckCompareOpV1Wire::from_op(*op),
                right: AcceptedCheckValueExprV1Wire::from_value(right),
            },
            AcceptedCheckExprV1::IsNull(value) => {
                Self::IsNull(AcceptedCheckValueExprV1Wire::from_value(value))
            }
            AcceptedCheckExprV1::IsNotNull(value) => {
                Self::IsNotNull(AcceptedCheckValueExprV1Wire::from_value(value))
            }
        }
    }

    fn into_expression(self) -> Result<AcceptedCheckExprV1, InternalError> {
        Ok(match self {
            Self::True => AcceptedCheckExprV1::True,
            Self::False => AcceptedCheckExprV1::False,
            Self::Not(inner) => AcceptedCheckExprV1::Not(Box::new((*inner).into_expression()?)),
            Self::And(children) => AcceptedCheckExprV1::And(
                children
                    .into_iter()
                    .map(Self::into_expression)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Or(children) => AcceptedCheckExprV1::Or(
                children
                    .into_iter()
                    .map(Self::into_expression)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            Self::Compare { left, op, right } => AcceptedCheckExprV1::Compare {
                left: left.into_value()?,
                op: op.into_op(),
                right: right.into_value()?,
            },
            Self::IsNull(value) => AcceptedCheckExprV1::IsNull(value.into_value()?),
            Self::IsNotNull(value) => AcceptedCheckExprV1::IsNotNull(value.into_value()?),
        })
    }
}

impl AcceptedCheckValueExprV1Wire {
    fn from_value(value: &AcceptedCheckValueExprV1) -> Self {
        match value {
            AcceptedCheckValueExprV1::Field(field_id) => Self::Field(field_id.get()),
            AcceptedCheckValueExprV1::Literal(literal) => {
                Self::Literal(AcceptedCheckLiteralV1Wire::from_literal(literal))
            }
            AcceptedCheckValueExprV1::CharLength(field_id) => Self::CharLength(field_id.get()),
            AcceptedCheckValueExprV1::OctetLength(field_id) => Self::OctetLength(field_id.get()),
            AcceptedCheckValueExprV1::Cardinality(field_id) => Self::Cardinality(field_id.get()),
        }
    }

    fn into_value(self) -> Result<AcceptedCheckValueExprV1, InternalError> {
        Ok(match self {
            Self::Field(field_id) => AcceptedCheckValueExprV1::Field(FieldId::new(field_id)),
            Self::Literal(literal) => AcceptedCheckValueExprV1::Literal(literal.into_literal()?),
            Self::CharLength(field_id) => {
                AcceptedCheckValueExprV1::CharLength(FieldId::new(field_id))
            }
            Self::OctetLength(field_id) => {
                AcceptedCheckValueExprV1::OctetLength(FieldId::new(field_id))
            }
            Self::Cardinality(field_id) => {
                AcceptedCheckValueExprV1::Cardinality(FieldId::new(field_id))
            }
        })
    }
}

impl AcceptedCheckLiteralV1Wire {
    fn from_literal(literal: &AcceptedCheckLiteralV1) -> Self {
        Self {
            kind: AcceptedFieldKindWire::from_kind(literal.kind()),
            storage_decode: FieldStorageDecodeWire::from_storage_decode(literal.storage_decode()),
            leaf_codec: LeafCodecWire::from_leaf_codec(literal.leaf_codec()),
            payload: literal.payload().to_vec(),
        }
    }

    fn into_literal(self) -> Result<AcceptedCheckLiteralV1, InternalError> {
        Ok(AcceptedCheckLiteralV1::from_accepted_parts(
            self.kind.into_kind()?,
            self.storage_decode.into_storage_decode(),
            self.leaf_codec.into_leaf_codec(),
            self.payload,
        ))
    }
}

impl AcceptedCheckCompareOpV1Wire {
    const fn from_op(op: AcceptedCheckCompareOpV1) -> Self {
        match op {
            AcceptedCheckCompareOpV1::Eq => Self::Eq,
            AcceptedCheckCompareOpV1::Ne => Self::Ne,
            AcceptedCheckCompareOpV1::Lt => Self::Lt,
            AcceptedCheckCompareOpV1::Lte => Self::Lte,
            AcceptedCheckCompareOpV1::Gt => Self::Gt,
            AcceptedCheckCompareOpV1::Gte => Self::Gte,
        }
    }

    const fn into_op(self) -> AcceptedCheckCompareOpV1 {
        match self {
            Self::Eq => AcceptedCheckCompareOpV1::Eq,
            Self::Ne => AcceptedCheckCompareOpV1::Ne,
            Self::Lt => AcceptedCheckCompareOpV1::Lt,
            Self::Lte => AcceptedCheckCompareOpV1::Lte,
            Self::Gt => AcceptedCheckCompareOpV1::Gt,
            Self::Gte => AcceptedCheckCompareOpV1::Gte,
        }
    }
}

impl SchemaRowLayoutWire {
    fn from_layout(layout: &SchemaRowLayout) -> Self {
        Self {
            current_version: layout.current_version().get(),
            history_floor: layout.history_floor().get(),
            field_to_slot: layout
                .field_to_slot()
                .iter()
                .map(|(field_id, slot)| (field_id.get(), slot.get()))
                .collect(),
        }
    }

    fn into_layout(self) -> Result<SchemaRowLayout, InternalError> {
        let current_version = RowLayoutVersion::new(self.current_version)
            .ok_or_else(InternalError::store_corruption)?;
        let history_floor = RowLayoutVersion::new(self.history_floor)
            .ok_or_else(InternalError::store_corruption)?;

        Ok(SchemaRowLayout::new(
            current_version,
            history_floor,
            self.field_to_slot
                .into_iter()
                .map(|(field_id, slot)| (FieldId::new(field_id), SchemaFieldSlot::new(slot)))
                .collect(),
        ))
    }
}

impl PersistedFieldSnapshotWire {
    fn from_field(field: &PersistedFieldSnapshot) -> Self {
        Self {
            id: field.id().get(),
            name: field.name().to_string(),
            slot: field.slot().get(),
            kind: AcceptedFieldKindWire::from_kind(field.kind()),
            nested_leaves: field
                .nested_leaves()
                .iter()
                .map(PersistedNestedLeafSnapshotWire::from_leaf)
                .collect(),
            nullable: field.nullable(),
            introduced_in_layout: field.introduced_in_layout().get(),
            insert_default: SchemaInsertDefaultWire::from_default(field.insert_default()),
            historical_fill: SchemaHistoricalFillWire::from_fill(field.historical_fill()),
            write_policy: SchemaFieldWritePolicyWire::from_policy(field.write_policy()),
            origin: PersistedFieldOriginWire::from_origin(field.origin()),
            storage_decode: FieldStorageDecodeWire::from_storage_decode(field.storage_decode()),
            leaf_codec: LeafCodecWire::from_leaf_codec(field.leaf_codec()),
        }
    }

    fn into_field(self) -> Result<PersistedFieldSnapshot, InternalError> {
        let introduced_in_layout = RowLayoutVersion::new(self.introduced_in_layout)
            .ok_or_else(InternalError::store_corruption)?;

        Ok(PersistedFieldSnapshot::new_with_write_policy_and_origin(
            FieldId::new(self.id),
            self.name,
            SchemaFieldSlot::new(self.slot),
            self.kind.into_kind()?,
            self.nested_leaves
                .into_iter()
                .map(PersistedNestedLeafSnapshotWire::into_leaf)
                .collect::<Result<Vec<_>, _>>()?,
            self.nullable,
            introduced_in_layout,
            self.insert_default.into_default(),
            self.historical_fill.into_fill(),
            self.write_policy.into_policy(),
            self.origin.into_origin(),
            self.storage_decode.into_storage_decode(),
            self.leaf_codec.into_leaf_codec(),
        ))
    }
}

impl PersistedFieldOriginWire {
    const fn from_origin(origin: PersistedFieldOrigin) -> Self {
        match origin {
            PersistedFieldOrigin::Generated => Self::Generated,
            PersistedFieldOrigin::SqlDdl => Self::SqlDdl,
        }
    }

    const fn into_origin(self) -> PersistedFieldOrigin {
        match self {
            Self::Generated => PersistedFieldOrigin::Generated,
            Self::SqlDdl => PersistedFieldOrigin::SqlDdl,
        }
    }
}

impl PersistedNestedLeafSnapshotWire {
    fn from_leaf(leaf: &PersistedNestedLeafSnapshot) -> Self {
        Self {
            path: leaf.path().to_vec(),
            kind: AcceptedFieldKindWire::from_kind(leaf.kind()),
            nullable: leaf.nullable(),
        }
    }

    fn into_leaf(self) -> Result<PersistedNestedLeafSnapshot, InternalError> {
        Ok(PersistedNestedLeafSnapshot::new(
            self.path,
            self.kind.into_kind()?,
            self.nullable,
        ))
    }
}

impl PersistedIndexSnapshotWire {
    fn from_index(index: &PersistedIndexSnapshot) -> Self {
        Self {
            schema_id: index.schema_id().get(),
            ordinal: index.ordinal(),
            physical_generation: index.physical_generation(),
            name: index.name().to_string(),
            store: index.store().to_string(),
            unique: index.unique(),
            origin: PersistedIndexOriginWire::from_origin(index.origin()),
            key: PersistedIndexKeySnapshotWire::from_key(index.key()),
            predicate_sql: index.predicate_sql().map(str::to_string),
        }
    }

    fn into_index(self) -> Result<PersistedIndexSnapshot, InternalError> {
        let schema_id =
            SchemaIndexId::new(self.schema_id).ok_or_else(InternalError::store_corruption)?;
        let physical_generation = self.physical_generation;
        let key = self.key.into_key()?;
        let index = match self.origin.into_origin() {
            PersistedIndexOrigin::Generated => PersistedIndexSnapshot::new(
                schema_id,
                self.ordinal,
                self.name,
                self.store,
                self.unique,
                key,
                self.predicate_sql,
            ),
            PersistedIndexOrigin::SqlDdl => PersistedIndexSnapshot::new_sql_ddl(
                schema_id,
                self.ordinal,
                self.name,
                self.store,
                self.unique,
                key,
                self.predicate_sql,
            ),
        };
        Ok(index.clone_with_schema_identity(schema_id, self.ordinal, physical_generation))
    }
}

impl PersistedIndexOriginWire {
    const fn from_origin(origin: PersistedIndexOrigin) -> Self {
        match origin {
            PersistedIndexOrigin::Generated => Self::Generated,
            PersistedIndexOrigin::SqlDdl => Self::SqlDdl,
        }
    }

    const fn into_origin(self) -> PersistedIndexOrigin {
        match self {
            Self::Generated => PersistedIndexOrigin::Generated,
            Self::SqlDdl => PersistedIndexOrigin::SqlDdl,
        }
    }
}

impl PersistedRelationEdgeSnapshotWire {
    fn from_relation(relation: &PersistedRelationEdgeSnapshot) -> Self {
        Self {
            relation_id: relation.id().get(),
            physical_generation: relation.physical_generation(),
            name: relation.name().to_string(),
            target_path: relation.target_path().to_string(),
            local_field_ids: relation
                .local_field_ids()
                .iter()
                .map(|field_id| field_id.get())
                .collect(),
        }
    }

    fn into_relation(self) -> Result<PersistedRelationEdgeSnapshot, InternalError> {
        let relation_id =
            RelationId::new(self.relation_id).ok_or_else(InternalError::store_corruption)?;
        Ok(PersistedRelationEdgeSnapshot::new(
            relation_id,
            self.name,
            self.target_path,
            self.local_field_ids.into_iter().map(FieldId::new).collect(),
        )
        .clone_with_physical_generation(self.physical_generation))
    }
}

impl PersistedIndexKeySnapshotWire {
    fn from_key(key: &PersistedIndexKeySnapshot) -> Self {
        match key {
            PersistedIndexKeySnapshot::FieldPath(paths) => Self::FieldPath(
                paths
                    .iter()
                    .map(PersistedIndexFieldPathSnapshotWire::from_path)
                    .collect(),
            ),
            PersistedIndexKeySnapshot::Items(items) => Self::Items(
                items
                    .iter()
                    .map(PersistedIndexKeyItemSnapshotWire::from_item)
                    .collect(),
            ),
        }
    }

    fn into_key(self) -> Result<PersistedIndexKeySnapshot, InternalError> {
        match self {
            Self::FieldPath(paths) => Ok(PersistedIndexKeySnapshot::FieldPath(
                paths
                    .into_iter()
                    .map(PersistedIndexFieldPathSnapshotWire::into_path)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Self::Items(items) => Ok(PersistedIndexKeySnapshot::Items(
                items
                    .into_iter()
                    .map(PersistedIndexKeyItemSnapshotWire::into_item)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
        }
    }
}

impl PersistedIndexKeyItemSnapshotWire {
    fn from_item(item: &PersistedIndexKeyItemSnapshot) -> Self {
        match item {
            PersistedIndexKeyItemSnapshot::FieldPath(path) => {
                Self::FieldPath(PersistedIndexFieldPathSnapshotWire::from_path(path))
            }
            PersistedIndexKeyItemSnapshot::Expression(expression) => Self::Expression(Box::new(
                PersistedIndexExpressionSnapshotWire::from_expression(expression),
            )),
        }
    }

    fn into_item(self) -> Result<PersistedIndexKeyItemSnapshot, InternalError> {
        match self {
            Self::FieldPath(path) => {
                Ok(PersistedIndexKeyItemSnapshot::FieldPath(path.into_path()?))
            }
            Self::Expression(expression) => Ok(PersistedIndexKeyItemSnapshot::Expression(
                Box::new((*expression).into_expression()?),
            )),
        }
    }
}

impl PersistedIndexFieldPathSnapshotWire {
    fn from_path(path: &PersistedIndexFieldPathSnapshot) -> Self {
        Self {
            field_id: path.field_id().get(),
            slot: path.slot().get(),
            path: path.path().to_vec(),
            kind: AcceptedFieldKindWire::from_kind(path.kind()),
            nullable: path.nullable(),
        }
    }

    fn into_path(self) -> Result<PersistedIndexFieldPathSnapshot, InternalError> {
        Ok(PersistedIndexFieldPathSnapshot::new(
            FieldId::new(self.field_id),
            SchemaFieldSlot::new(self.slot),
            self.path,
            self.kind.into_kind()?,
            self.nullable,
        ))
    }
}

impl PersistedIndexExpressionSnapshotWire {
    fn from_expression(expression: &PersistedIndexExpressionSnapshot) -> Self {
        Self {
            op: PersistedIndexExpressionOpWire::from_op(expression.op()),
            source: PersistedIndexFieldPathSnapshotWire::from_path(expression.source()),
            input_kind: AcceptedFieldKindWire::from_kind(expression.input_kind()),
            output_kind: AcceptedFieldKindWire::from_kind(expression.output_kind()),
            canonical_text: expression.canonical_text().to_string(),
        }
    }

    fn into_expression(self) -> Result<PersistedIndexExpressionSnapshot, InternalError> {
        Ok(PersistedIndexExpressionSnapshot::new(
            self.op.into_op(),
            self.source.into_path()?,
            self.input_kind.into_kind()?,
            self.output_kind.into_kind()?,
            self.canonical_text,
        ))
    }
}

impl PersistedIndexExpressionOpWire {
    const fn from_op(op: PersistedIndexExpressionOp) -> Self {
        match op {
            PersistedIndexExpressionOp::Lower => Self::Lower,
            PersistedIndexExpressionOp::Upper => Self::Upper,
            PersistedIndexExpressionOp::Trim => Self::Trim,
            PersistedIndexExpressionOp::LowerTrim => Self::LowerTrim,
            PersistedIndexExpressionOp::Date => Self::Date,
            PersistedIndexExpressionOp::Year => Self::Year,
            PersistedIndexExpressionOp::Month => Self::Month,
            PersistedIndexExpressionOp::Day => Self::Day,
        }
    }

    const fn into_op(self) -> PersistedIndexExpressionOp {
        match self {
            Self::Lower => PersistedIndexExpressionOp::Lower,
            Self::Upper => PersistedIndexExpressionOp::Upper,
            Self::Trim => PersistedIndexExpressionOp::Trim,
            Self::LowerTrim => PersistedIndexExpressionOp::LowerTrim,
            Self::Date => PersistedIndexExpressionOp::Date,
            Self::Year => PersistedIndexExpressionOp::Year,
            Self::Month => PersistedIndexExpressionOp::Month,
            Self::Day => PersistedIndexExpressionOp::Day,
        }
    }
}

impl SchemaInsertDefaultWire {
    fn from_default(default: &SchemaInsertDefault) -> Self {
        if let Some(bytes) = default.slot_payload() {
            Self::SlotPayload(bytes.to_vec())
        } else {
            Self::None
        }
    }

    fn into_default(self) -> SchemaInsertDefault {
        match self {
            Self::None => SchemaInsertDefault::None,
            Self::SlotPayload(bytes) => SchemaInsertDefault::SlotPayload(bytes),
        }
    }
}

impl SchemaHistoricalFillWire {
    fn from_fill(fill: &SchemaHistoricalFill) -> Self {
        match fill {
            SchemaHistoricalFill::Reject => Self::Reject,
            SchemaHistoricalFill::Null => Self::Null,
            SchemaHistoricalFill::SlotPayload(bytes) => Self::SlotPayload(bytes.clone()),
        }
    }

    fn into_fill(self) -> SchemaHistoricalFill {
        match self {
            Self::Reject => SchemaHistoricalFill::Reject,
            Self::Null => SchemaHistoricalFill::Null,
            Self::SlotPayload(bytes) => SchemaHistoricalFill::SlotPayload(bytes),
        }
    }
}

impl SchemaFieldWritePolicyWire {
    const fn from_policy(policy: SchemaFieldWritePolicy) -> Self {
        Self {
            insert_generation: match policy.insert_generation() {
                Some(FieldInsertGeneration::Ulid) => Some(FieldInsertGenerationWire::Ulid),
                Some(FieldInsertGeneration::Timestamp) => {
                    Some(FieldInsertGenerationWire::Timestamp)
                }
                None => None,
            },
            write_management: match policy.write_management() {
                Some(FieldWriteManagement::CreatedAt) => Some(FieldWriteManagementWire::CreatedAt),
                Some(FieldWriteManagement::UpdatedAt) => Some(FieldWriteManagementWire::UpdatedAt),
                None => None,
            },
        }
    }

    const fn into_policy(self) -> SchemaFieldWritePolicy {
        SchemaFieldWritePolicy::from_model_policies(
            match self.insert_generation {
                Some(FieldInsertGenerationWire::Ulid) => Some(FieldInsertGeneration::Ulid),
                Some(FieldInsertGenerationWire::Timestamp) => {
                    Some(FieldInsertGeneration::Timestamp)
                }
                None => None,
            },
            match self.write_management {
                Some(FieldWriteManagementWire::CreatedAt) => Some(FieldWriteManagement::CreatedAt),
                Some(FieldWriteManagementWire::UpdatedAt) => Some(FieldWriteManagement::UpdatedAt),
                None => None,
            },
        )
    }
}

impl AcceptedFieldKindWire {
    fn from_kind(kind: &AcceptedFieldKind) -> Self {
        match kind {
            AcceptedFieldKind::Account => Self::Account,
            AcceptedFieldKind::Blob { max_len } => Self::Blob { max_len: *max_len },
            AcceptedFieldKind::Bool => Self::Bool,
            AcceptedFieldKind::Date => Self::Date,
            AcceptedFieldKind::Decimal { scale } => Self::Decimal { scale: *scale },
            AcceptedFieldKind::Duration => Self::Duration,
            AcceptedFieldKind::Enum { type_id } => Self::Enum {
                type_id: type_id.get(),
            },
            AcceptedFieldKind::Float32 => Self::Float32,
            AcceptedFieldKind::Float64 => Self::Float64,
            AcceptedFieldKind::Int8 => Self::Int8,
            AcceptedFieldKind::Int16 => Self::Int16,
            AcceptedFieldKind::Int32 => Self::Int32,
            AcceptedFieldKind::Int64 => Self::Int64,
            AcceptedFieldKind::Int128 => Self::Int128,
            AcceptedFieldKind::IntBig { max_bytes } => Self::IntBig {
                max_bytes: *max_bytes,
            },
            AcceptedFieldKind::Principal => Self::Principal,
            AcceptedFieldKind::Subaccount => Self::Subaccount,
            AcceptedFieldKind::Text { max_len } => Self::Text { max_len: *max_len },
            AcceptedFieldKind::Timestamp => Self::Timestamp,
            AcceptedFieldKind::Nat8 => Self::Nat8,
            AcceptedFieldKind::Nat16 => Self::Nat16,
            AcceptedFieldKind::Nat32 => Self::Nat32,
            AcceptedFieldKind::Nat64 => Self::Nat64,
            AcceptedFieldKind::Nat128 => Self::Nat128,
            AcceptedFieldKind::NatBig { max_bytes } => Self::NatBig {
                max_bytes: *max_bytes,
            },
            AcceptedFieldKind::Ulid => Self::Ulid,
            AcceptedFieldKind::Unit => Self::Unit,
            AcceptedFieldKind::Relation {
                target_path,
                target_entity_name,
                target_entity_tag,
                target_store_path,
                key_kind,
            } => Self::Relation {
                target_path: target_path.clone(),
                target_entity_name: target_entity_name.clone(),
                target_entity_tag: target_entity_tag.value(),
                target_store_path: target_store_path.clone(),
                key_kind: Box::new(Self::from_kind(key_kind)),
            },
            AcceptedFieldKind::List(inner) => Self::List(Box::new(Self::from_kind(inner))),
            AcceptedFieldKind::Set(inner) => Self::Set(Box::new(Self::from_kind(inner))),
            AcceptedFieldKind::Map { key, value } => Self::Map {
                key: Box::new(Self::from_kind(key)),
                value: Box::new(Self::from_kind(value)),
            },
            AcceptedFieldKind::Composite { type_id } => Self::Composite {
                type_id: type_id.get(),
            },
        }
    }

    fn into_kind(self) -> Result<AcceptedFieldKind, InternalError> {
        Ok(match self {
            Self::Account => AcceptedFieldKind::Account,
            Self::Blob { max_len } => AcceptedFieldKind::Blob { max_len },
            Self::Bool => AcceptedFieldKind::Bool,
            Self::Date => AcceptedFieldKind::Date,
            Self::Decimal { scale } => AcceptedFieldKind::Decimal { scale },
            Self::Duration => AcceptedFieldKind::Duration,
            Self::Enum { type_id } => AcceptedFieldKind::Enum {
                type_id: EnumTypeId::new(type_id).ok_or_else(InternalError::store_corruption)?,
            },
            Self::Float32 => AcceptedFieldKind::Float32,
            Self::Float64 => AcceptedFieldKind::Float64,
            Self::Int8 => AcceptedFieldKind::Int8,
            Self::Int16 => AcceptedFieldKind::Int16,
            Self::Int32 => AcceptedFieldKind::Int32,
            Self::Int64 => AcceptedFieldKind::Int64,
            Self::Int128 => AcceptedFieldKind::Int128,
            Self::IntBig { max_bytes } => AcceptedFieldKind::IntBig { max_bytes },
            Self::Principal => AcceptedFieldKind::Principal,
            Self::Subaccount => AcceptedFieldKind::Subaccount,
            Self::Text { max_len } => AcceptedFieldKind::Text { max_len },
            Self::Timestamp => AcceptedFieldKind::Timestamp,
            Self::Nat8 => AcceptedFieldKind::Nat8,
            Self::Nat16 => AcceptedFieldKind::Nat16,
            Self::Nat32 => AcceptedFieldKind::Nat32,
            Self::Nat64 => AcceptedFieldKind::Nat64,
            Self::Nat128 => AcceptedFieldKind::Nat128,
            Self::NatBig { max_bytes } => AcceptedFieldKind::NatBig { max_bytes },
            Self::Ulid => AcceptedFieldKind::Ulid,
            Self::Unit => AcceptedFieldKind::Unit,
            Self::Relation {
                target_path,
                target_entity_name,
                target_entity_tag,
                target_store_path,
                key_kind,
            } => AcceptedFieldKind::Relation {
                target_path,
                target_entity_name,
                target_entity_tag: EntityTag::new(target_entity_tag),
                target_store_path,
                key_kind: Box::new(key_kind.into_kind()?),
            },
            Self::List(inner) => AcceptedFieldKind::List(Box::new(inner.into_kind()?)),
            Self::Set(inner) => AcceptedFieldKind::Set(Box::new(inner.into_kind()?)),
            Self::Map { key, value } => AcceptedFieldKind::Map {
                key: Box::new(key.into_kind()?),
                value: Box::new(value.into_kind()?),
            },
            Self::Composite { type_id } => AcceptedFieldKind::Composite {
                type_id: CompositeTypeId::new(type_id)
                    .ok_or_else(InternalError::store_corruption)?,
            },
        })
    }
}

impl FieldStorageDecodeWire {
    const fn from_storage_decode(storage_decode: FieldStorageDecode) -> Self {
        match storage_decode {
            FieldStorageDecode::ByKind => Self::ByKind,
            FieldStorageDecode::CatalogValue => Self::CatalogValue,
        }
    }

    const fn into_storage_decode(self) -> FieldStorageDecode {
        match self {
            Self::ByKind => FieldStorageDecode::ByKind,
            Self::CatalogValue => FieldStorageDecode::CatalogValue,
        }
    }
}

impl LeafCodecWire {
    const fn from_leaf_codec(leaf_codec: LeafCodec) -> Self {
        match leaf_codec {
            LeafCodec::Scalar(scalar) => Self::Scalar(ScalarCodecWire::from_scalar_codec(scalar)),
            LeafCodec::Structural => Self::Structural,
        }
    }

    const fn into_leaf_codec(self) -> LeafCodec {
        match self {
            Self::Scalar(scalar) => LeafCodec::Scalar(scalar.into_scalar_codec()),
            Self::Structural => LeafCodec::Structural,
        }
    }
}

impl ScalarCodecWire {
    const fn from_scalar_codec(scalar_codec: ScalarCodec) -> Self {
        match scalar_codec {
            ScalarCodec::Blob => Self::Blob,
            ScalarCodec::Bool => Self::Bool,
            ScalarCodec::Date => Self::Date,
            ScalarCodec::Duration => Self::Duration,
            ScalarCodec::Float32 => Self::Float32,
            ScalarCodec::Float64 => Self::Float64,
            ScalarCodec::Int64 => Self::Int64,
            ScalarCodec::Principal => Self::Principal,
            ScalarCodec::Subaccount => Self::Subaccount,
            ScalarCodec::Text => Self::Text,
            ScalarCodec::Timestamp => Self::Timestamp,
            ScalarCodec::Nat64 => Self::Nat64,
            ScalarCodec::Ulid => Self::Ulid,
            ScalarCodec::Unit => Self::Unit,
        }
    }

    const fn into_scalar_codec(self) -> ScalarCodec {
        match self {
            Self::Blob => ScalarCodec::Blob,
            Self::Bool => ScalarCodec::Bool,
            Self::Date => ScalarCodec::Date,
            Self::Duration => ScalarCodec::Duration,
            Self::Float32 => ScalarCodec::Float32,
            Self::Float64 => ScalarCodec::Float64,
            Self::Int64 => ScalarCodec::Int64,
            Self::Principal => ScalarCodec::Principal,
            Self::Subaccount => ScalarCodec::Subaccount,
            Self::Text => ScalarCodec::Text,
            Self::Timestamp => ScalarCodec::Timestamp,
            Self::Nat64 => ScalarCodec::Nat64,
            Self::Ulid => ScalarCodec::Ulid,
            Self::Unit => ScalarCodec::Unit,
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
