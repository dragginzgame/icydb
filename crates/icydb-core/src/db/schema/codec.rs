//! Module: db::schema::codec
//! Responsibility: typed persisted-schema snapshot encoding.
//! Does not own: reconciliation policy, schema proposal construction, or row decoding.
//! Boundary: converts schema-owned snapshot DTOs to/from raw `SchemaStore` payload bytes.

use crate::{
    db::schema::{
        FieldId, PersistedEnumVariant, PersistedFieldKind, PersistedFieldSnapshot,
        PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
        PersistedNestedLeafSnapshot, PersistedRelationStrength, PersistedSchemaSnapshot,
        SchemaFieldDefault, SchemaFieldSlot, SchemaFieldWritePolicy, SchemaRowLayout,
        SchemaVersion, schema_snapshot_index_integrity_detail, schema_snapshot_integrity_detail,
    },
    error::InternalError,
    model::field::{
        FieldInsertGeneration, FieldStorageDecode, FieldWriteManagement, LeafCodec, ScalarCodec,
    },
    types::EntityTag,
};
use candid::{CandidType, Decode, Encode};
use serde::Deserialize;

const SCHEMA_SNAPSHOT_CODEC_VERSION: u32 = 3;

// Candid wire container for one persisted schema snapshot.
//
// The public/internal schema DTOs remain normal Rust types; this wire shape is
// the only place that commits their current durable binary encoding.
#[derive(CandidType, Deserialize)]
struct PersistedSchemaSnapshotWire {
    codec_version: u32,
    version: u32,
    entity_path: String,
    entity_name: String,
    primary_key_field_id: u32,
    row_layout: SchemaRowLayoutWire,
    fields: Vec<PersistedFieldSnapshotWire>,
    indexes: Vec<PersistedIndexSnapshotWire>,
}

// Candid wire container for schema row-layout identity.
#[derive(CandidType, Deserialize)]
struct SchemaRowLayoutWire {
    version: u32,
    field_to_slot: Vec<(u32, u16)>,
}

// Candid wire container for one persisted schema field.
#[derive(CandidType, Deserialize)]
struct PersistedFieldSnapshotWire {
    id: u32,
    name: String,
    slot: u16,
    kind: PersistedFieldKindWire,
    nested_leaves: Vec<PersistedNestedLeafSnapshotWire>,
    nullable: bool,
    default: SchemaFieldDefaultWire,
    write_policy: SchemaFieldWritePolicyWire,
    storage_decode: FieldStorageDecodeWire,
    leaf_codec: LeafCodecWire,
}

// Candid wire container for one nested leaf rooted at a top-level field.
#[derive(CandidType, Deserialize)]
struct PersistedNestedLeafSnapshotWire {
    path: Vec<String>,
    kind: PersistedFieldKindWire,
    nullable: bool,
    storage_decode: FieldStorageDecodeWire,
    leaf_codec: LeafCodecWire,
}

// Candid wire container for one accepted index contract.
#[derive(CandidType, Deserialize)]
struct PersistedIndexSnapshotWire {
    ordinal: u16,
    name: String,
    store: String,
    unique: bool,
    key: PersistedIndexKeySnapshotWire,
    predicate_sql: Option<String>,
}

// Candid wire enum for accepted index key contracts.
#[derive(CandidType, Deserialize)]
enum PersistedIndexKeySnapshotWire {
    FieldPath(Vec<PersistedIndexFieldPathSnapshotWire>),
}

// Candid wire container for one accepted field-path index key item.
#[derive(CandidType, Deserialize)]
struct PersistedIndexFieldPathSnapshotWire {
    field_id: u32,
    slot: u16,
    path: Vec<String>,
    kind: PersistedFieldKindWire,
    nullable: bool,
}

// Candid wire enum for database-level default metadata.
#[derive(CandidType, Deserialize)]
enum SchemaFieldDefaultWire {
    None,
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
enum PersistedFieldKindWire {
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
        path: String,
        variants: Vec<PersistedEnumVariantWire>,
    },
    Float32,
    Float64,
    Int,
    Int128,
    IntBig,
    Principal,
    Subaccount,
    Text {
        max_len: Option<u32>,
    },
    Timestamp,
    Uint,
    Uint128,
    UintBig,
    Ulid,
    Unit,
    Relation {
        target_path: String,
        target_entity_name: String,
        target_entity_tag: u64,
        target_store_path: String,
        key_kind: Box<Self>,
        strength: PersistedRelationStrengthWire,
    },
    List(Box<Self>),
    Set(Box<Self>),
    Map {
        key: Box<Self>,
        value: Box<Self>,
    },
    Structured {
        queryable: bool,
    },
}

// Candid wire container for one enum variant contract.
#[derive(CandidType, Deserialize)]
struct PersistedEnumVariantWire {
    ident: String,
    payload_kind: Option<Box<PersistedFieldKindWire>>,
    payload_storage_decode: FieldStorageDecodeWire,
}

// Candid wire enum for relation strength.
#[derive(CandidType, Deserialize)]
enum PersistedRelationStrengthWire {
    Strong,
    Weak,
}

// Candid wire enum for slot payload decode policy.
#[derive(CandidType, Deserialize)]
enum FieldStorageDecodeWire {
    ByKind,
    Value,
}

// Candid wire enum for leaf payload codecs.
#[derive(CandidType, Deserialize)]
enum LeafCodecWire {
    Scalar(ScalarCodecWire),
    StructuralFallback,
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
    Uint64,
    Ulid,
    Unit,
}

/// Encode one typed persisted-schema snapshot into durable raw bytes.
pub(in crate::db) fn encode_persisted_schema_snapshot(
    snapshot: &PersistedSchemaSnapshot,
) -> Result<Vec<u8>, InternalError> {
    let wire = PersistedSchemaSnapshotWire::from_snapshot(snapshot);

    Encode!(&wire).map_err(|err| {
        InternalError::store_corruption(format!(
            "failed to encode persisted schema snapshot: {err}"
        ))
    })
}

/// Decode one typed persisted-schema snapshot from durable raw bytes.
pub(in crate::db) fn decode_persisted_schema_snapshot(
    bytes: &[u8],
) -> Result<PersistedSchemaSnapshot, InternalError> {
    let wire = Decode!(bytes, PersistedSchemaSnapshotWire).map_err(|err| {
        InternalError::store_corruption(format!(
            "failed to decode persisted schema snapshot: {err}"
        ))
    })?;

    wire.into_snapshot()
}

impl PersistedSchemaSnapshotWire {
    fn from_snapshot(snapshot: &PersistedSchemaSnapshot) -> Self {
        Self {
            codec_version: SCHEMA_SNAPSHOT_CODEC_VERSION,
            version: snapshot.version().get(),
            entity_path: snapshot.entity_path().to_string(),
            entity_name: snapshot.entity_name().to_string(),
            primary_key_field_id: snapshot.primary_key_field_id().get(),
            row_layout: SchemaRowLayoutWire::from_layout(snapshot.row_layout()),
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
        }
    }

    fn into_snapshot(self) -> Result<PersistedSchemaSnapshot, InternalError> {
        if self.codec_version != SCHEMA_SNAPSHOT_CODEC_VERSION {
            return Err(InternalError::store_corruption(format!(
                "unsupported persisted schema snapshot codec version: {}",
                self.codec_version
            )));
        }

        let version = SchemaVersion::new(self.version);
        let row_layout = self.row_layout.into_layout();
        let fields = self
            .fields
            .into_iter()
            .map(PersistedFieldSnapshotWire::into_field)
            .collect::<Result<Vec<_>, _>>()?;
        let primary_key_field_id = FieldId::new(self.primary_key_field_id);
        let indexes = self
            .indexes
            .into_iter()
            .map(PersistedIndexSnapshotWire::into_index)
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(detail) = schema_snapshot_integrity_detail(
            "persisted schema snapshot",
            version,
            primary_key_field_id,
            &row_layout,
            &fields,
        ) {
            return Err(InternalError::store_corruption(detail));
        }

        if let Some(detail) = schema_snapshot_index_integrity_detail(
            "persisted schema snapshot",
            &row_layout,
            &fields,
            &indexes,
        ) {
            return Err(InternalError::store_corruption(detail));
        }

        Ok(PersistedSchemaSnapshot::new_with_indexes(
            version,
            self.entity_path,
            self.entity_name,
            primary_key_field_id,
            row_layout,
            fields,
            indexes,
        ))
    }
}

impl SchemaRowLayoutWire {
    fn from_layout(layout: &SchemaRowLayout) -> Self {
        Self {
            version: layout.version().get(),
            field_to_slot: layout
                .field_to_slot()
                .iter()
                .map(|(field_id, slot)| (field_id.get(), slot.get()))
                .collect(),
        }
    }

    fn into_layout(self) -> SchemaRowLayout {
        SchemaRowLayout::new(
            SchemaVersion::new(self.version),
            self.field_to_slot
                .into_iter()
                .map(|(field_id, slot)| (FieldId::new(field_id), SchemaFieldSlot::new(slot)))
                .collect(),
        )
    }
}

impl PersistedFieldSnapshotWire {
    fn from_field(field: &PersistedFieldSnapshot) -> Self {
        Self {
            id: field.id().get(),
            name: field.name().to_string(),
            slot: field.slot().get(),
            kind: PersistedFieldKindWire::from_kind(field.kind()),
            nested_leaves: field
                .nested_leaves()
                .iter()
                .map(PersistedNestedLeafSnapshotWire::from_leaf)
                .collect(),
            nullable: field.nullable(),
            default: SchemaFieldDefaultWire::from_default(field.default()),
            write_policy: SchemaFieldWritePolicyWire::from_policy(field.write_policy()),
            storage_decode: FieldStorageDecodeWire::from_storage_decode(field.storage_decode()),
            leaf_codec: LeafCodecWire::from_leaf_codec(field.leaf_codec()),
        }
    }

    fn into_field(self) -> Result<PersistedFieldSnapshot, InternalError> {
        Ok(PersistedFieldSnapshot::new_with_write_policy(
            FieldId::new(self.id),
            self.name,
            SchemaFieldSlot::new(self.slot),
            self.kind.into_kind()?,
            self.nested_leaves
                .into_iter()
                .map(PersistedNestedLeafSnapshotWire::into_leaf)
                .collect::<Result<Vec<_>, _>>()?,
            self.nullable,
            self.default.into_default(),
            self.write_policy.into_policy(),
            self.storage_decode.into_storage_decode(),
            self.leaf_codec.into_leaf_codec(),
        ))
    }
}

impl PersistedNestedLeafSnapshotWire {
    fn from_leaf(leaf: &PersistedNestedLeafSnapshot) -> Self {
        Self {
            path: leaf.path().to_vec(),
            kind: PersistedFieldKindWire::from_kind(leaf.kind()),
            nullable: leaf.nullable(),
            storage_decode: FieldStorageDecodeWire::from_storage_decode(leaf.storage_decode()),
            leaf_codec: LeafCodecWire::from_leaf_codec(leaf.leaf_codec()),
        }
    }

    fn into_leaf(self) -> Result<PersistedNestedLeafSnapshot, InternalError> {
        Ok(PersistedNestedLeafSnapshot::new(
            self.path,
            self.kind.into_kind()?,
            self.nullable,
            self.storage_decode.into_storage_decode(),
            self.leaf_codec.into_leaf_codec(),
        ))
    }
}

impl PersistedIndexSnapshotWire {
    fn from_index(index: &PersistedIndexSnapshot) -> Self {
        Self {
            ordinal: index.ordinal(),
            name: index.name().to_string(),
            store: index.store().to_string(),
            unique: index.unique(),
            key: PersistedIndexKeySnapshotWire::from_key(index.key()),
            predicate_sql: index.predicate_sql().map(str::to_string),
        }
    }

    fn into_index(self) -> Result<PersistedIndexSnapshot, InternalError> {
        Ok(PersistedIndexSnapshot::new(
            self.ordinal,
            self.name,
            self.store,
            self.unique,
            self.key.into_key()?,
            self.predicate_sql,
        ))
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
        }
    }
}

impl PersistedIndexFieldPathSnapshotWire {
    fn from_path(path: &PersistedIndexFieldPathSnapshot) -> Self {
        Self {
            field_id: path.field_id().get(),
            slot: path.slot().get(),
            path: path.path().to_vec(),
            kind: PersistedFieldKindWire::from_kind(path.kind()),
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

impl SchemaFieldDefaultWire {
    fn from_default(default: &SchemaFieldDefault) -> Self {
        if let Some(bytes) = default.slot_payload() {
            Self::SlotPayload(bytes.to_vec())
        } else {
            Self::None
        }
    }

    fn into_default(self) -> SchemaFieldDefault {
        match self {
            Self::None => SchemaFieldDefault::None,
            Self::SlotPayload(bytes) => SchemaFieldDefault::SlotPayload(bytes),
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

impl PersistedFieldKindWire {
    fn from_kind(kind: &PersistedFieldKind) -> Self {
        match kind {
            PersistedFieldKind::Account => Self::Account,
            PersistedFieldKind::Blob { max_len } => Self::Blob { max_len: *max_len },
            PersistedFieldKind::Bool => Self::Bool,
            PersistedFieldKind::Date => Self::Date,
            PersistedFieldKind::Decimal { scale } => Self::Decimal { scale: *scale },
            PersistedFieldKind::Duration => Self::Duration,
            PersistedFieldKind::Enum { path, variants } => Self::Enum {
                path: path.clone(),
                variants: variants
                    .iter()
                    .map(PersistedEnumVariantWire::from_variant)
                    .collect(),
            },
            PersistedFieldKind::Float32 => Self::Float32,
            PersistedFieldKind::Float64 => Self::Float64,
            PersistedFieldKind::Int => Self::Int,
            PersistedFieldKind::Int128 => Self::Int128,
            PersistedFieldKind::IntBig => Self::IntBig,
            PersistedFieldKind::Principal => Self::Principal,
            PersistedFieldKind::Subaccount => Self::Subaccount,
            PersistedFieldKind::Text { max_len } => Self::Text { max_len: *max_len },
            PersistedFieldKind::Timestamp => Self::Timestamp,
            PersistedFieldKind::Uint => Self::Uint,
            PersistedFieldKind::Uint128 => Self::Uint128,
            PersistedFieldKind::UintBig => Self::UintBig,
            PersistedFieldKind::Ulid => Self::Ulid,
            PersistedFieldKind::Unit => Self::Unit,
            PersistedFieldKind::Relation {
                target_path,
                target_entity_name,
                target_entity_tag,
                target_store_path,
                key_kind,
                strength,
            } => Self::Relation {
                target_path: target_path.clone(),
                target_entity_name: target_entity_name.clone(),
                target_entity_tag: target_entity_tag.value(),
                target_store_path: target_store_path.clone(),
                key_kind: Box::new(Self::from_kind(key_kind)),
                strength: PersistedRelationStrengthWire::from_strength(*strength),
            },
            PersistedFieldKind::List(inner) => Self::List(Box::new(Self::from_kind(inner))),
            PersistedFieldKind::Set(inner) => Self::Set(Box::new(Self::from_kind(inner))),
            PersistedFieldKind::Map { key, value } => Self::Map {
                key: Box::new(Self::from_kind(key)),
                value: Box::new(Self::from_kind(value)),
            },
            PersistedFieldKind::Structured { queryable } => Self::Structured {
                queryable: *queryable,
            },
        }
    }

    fn into_kind(self) -> Result<PersistedFieldKind, InternalError> {
        Ok(match self {
            Self::Account => PersistedFieldKind::Account,
            Self::Blob { max_len } => PersistedFieldKind::Blob { max_len },
            Self::Bool => PersistedFieldKind::Bool,
            Self::Date => PersistedFieldKind::Date,
            Self::Decimal { scale } => PersistedFieldKind::Decimal { scale },
            Self::Duration => PersistedFieldKind::Duration,
            Self::Enum { path, variants } => PersistedFieldKind::Enum {
                path,
                variants: variants
                    .into_iter()
                    .map(PersistedEnumVariantWire::into_variant)
                    .collect::<Result<Vec<_>, _>>()?,
            },
            Self::Float32 => PersistedFieldKind::Float32,
            Self::Float64 => PersistedFieldKind::Float64,
            Self::Int => PersistedFieldKind::Int,
            Self::Int128 => PersistedFieldKind::Int128,
            Self::IntBig => PersistedFieldKind::IntBig,
            Self::Principal => PersistedFieldKind::Principal,
            Self::Subaccount => PersistedFieldKind::Subaccount,
            Self::Text { max_len } => PersistedFieldKind::Text { max_len },
            Self::Timestamp => PersistedFieldKind::Timestamp,
            Self::Uint => PersistedFieldKind::Uint,
            Self::Uint128 => PersistedFieldKind::Uint128,
            Self::UintBig => PersistedFieldKind::UintBig,
            Self::Ulid => PersistedFieldKind::Ulid,
            Self::Unit => PersistedFieldKind::Unit,
            Self::Relation {
                target_path,
                target_entity_name,
                target_entity_tag,
                target_store_path,
                key_kind,
                strength,
            } => PersistedFieldKind::Relation {
                target_path,
                target_entity_name,
                target_entity_tag: EntityTag::new(target_entity_tag),
                target_store_path,
                key_kind: Box::new(key_kind.into_kind()?),
                strength: strength.into_strength(),
            },
            Self::List(inner) => PersistedFieldKind::List(Box::new(inner.into_kind()?)),
            Self::Set(inner) => PersistedFieldKind::Set(Box::new(inner.into_kind()?)),
            Self::Map { key, value } => PersistedFieldKind::Map {
                key: Box::new(key.into_kind()?),
                value: Box::new(value.into_kind()?),
            },
            Self::Structured { queryable } => PersistedFieldKind::Structured { queryable },
        })
    }
}

impl PersistedEnumVariantWire {
    fn from_variant(variant: &PersistedEnumVariant) -> Self {
        Self {
            ident: variant.ident().to_string(),
            payload_kind: variant
                .payload_kind()
                .map(|kind| Box::new(PersistedFieldKindWire::from_kind(kind))),
            payload_storage_decode: FieldStorageDecodeWire::from_storage_decode(
                variant.payload_storage_decode(),
            ),
        }
    }

    fn into_variant(self) -> Result<PersistedEnumVariant, InternalError> {
        Ok(PersistedEnumVariant::new(
            self.ident,
            self.payload_kind
                .map(|kind| kind.into_kind().map(Box::new))
                .transpose()?,
            self.payload_storage_decode.into_storage_decode(),
        ))
    }
}

impl PersistedRelationStrengthWire {
    const fn from_strength(strength: PersistedRelationStrength) -> Self {
        match strength {
            PersistedRelationStrength::Strong => Self::Strong,
            PersistedRelationStrength::Weak => Self::Weak,
        }
    }

    const fn into_strength(self) -> PersistedRelationStrength {
        match self {
            Self::Strong => PersistedRelationStrength::Strong,
            Self::Weak => PersistedRelationStrength::Weak,
        }
    }
}

impl FieldStorageDecodeWire {
    const fn from_storage_decode(storage_decode: FieldStorageDecode) -> Self {
        match storage_decode {
            FieldStorageDecode::ByKind => Self::ByKind,
            FieldStorageDecode::Value => Self::Value,
        }
    }

    const fn into_storage_decode(self) -> FieldStorageDecode {
        match self {
            Self::ByKind => FieldStorageDecode::ByKind,
            Self::Value => FieldStorageDecode::Value,
        }
    }
}

impl LeafCodecWire {
    const fn from_leaf_codec(leaf_codec: LeafCodec) -> Self {
        match leaf_codec {
            LeafCodec::Scalar(scalar) => Self::Scalar(ScalarCodecWire::from_scalar_codec(scalar)),
            LeafCodec::StructuralFallback => Self::StructuralFallback,
        }
    }

    const fn into_leaf_codec(self) -> LeafCodec {
        match self {
            Self::Scalar(scalar) => LeafCodec::Scalar(scalar.into_scalar_codec()),
            Self::StructuralFallback => LeafCodec::StructuralFallback,
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
            ScalarCodec::Uint64 => Self::Uint64,
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
            Self::Uint64 => ScalarCodec::Uint64,
            Self::Ulid => ScalarCodec::Ulid,
            Self::Unit => ScalarCodec::Unit,
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::schema::{
            FieldId, PersistedFieldKind, PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedSchemaSnapshot,
            SchemaFieldDefault, SchemaFieldSlot, SchemaFieldWritePolicy, SchemaRowLayout,
            SchemaVersion, decode_persisted_schema_snapshot, encode_persisted_schema_snapshot,
        },
        model::field::{
            FieldInsertGeneration, FieldStorageDecode, FieldWriteManagement, LeafCodec, ScalarCodec,
        },
    };

    #[test]
    fn decode_persisted_schema_snapshot_rejects_snapshot_layout_version_mismatch() {
        let snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::new(2),
            "entities::Mismatch".to_string(),
            "Mismatch".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(SchemaVersion::initial(), Vec::new()),
            Vec::new(),
        );
        let encoded = encode_persisted_schema_snapshot(&snapshot)
            .expect("schema snapshot should encode for decode-boundary coverage");

        let err = decode_persisted_schema_snapshot(&encoded)
            .expect_err("decode should reject mismatched snapshot/layout versions");

        assert!(
            err.message()
                .contains("persisted schema snapshot row-layout version mismatch"),
            "schema codec should report the decoded version invariant"
        );
    }

    #[test]
    fn persisted_schema_snapshot_round_trips_field_write_policy() {
        let snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "entities::WritePolicy".to_string(),
            "WritePolicy".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new_with_write_policy(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    SchemaFieldWritePolicy::from_model_policies(
                        Some(FieldInsertGeneration::Ulid),
                        None,
                    ),
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new_with_write_policy(
                    FieldId::new(2),
                    "updated_at".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Timestamp,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    SchemaFieldWritePolicy::from_model_policies(
                        None,
                        Some(FieldWriteManagement::UpdatedAt),
                    ),
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Timestamp),
                ),
            ],
        );
        let encoded = encode_persisted_schema_snapshot(&snapshot)
            .expect("schema snapshot should encode persisted write policy");

        let decoded = decode_persisted_schema_snapshot(&encoded)
            .expect("schema snapshot should decode persisted write policy");

        assert_eq!(
            decoded.fields()[0].write_policy().insert_generation(),
            Some(FieldInsertGeneration::Ulid),
            "insert generation should survive schema snapshot round-trip",
        );
        assert_eq!(
            decoded.fields()[1].write_policy().write_management(),
            Some(FieldWriteManagement::UpdatedAt),
            "managed write policy should survive schema snapshot round-trip",
        );
    }

    #[test]
    fn persisted_schema_snapshot_round_trips_encoded_default_payload() {
        let default_payload = vec![0x01, 0x02, 0x03];
        let snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "entities::DefaultPayload".to_string(),
            "DefaultPayload".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![(FieldId::new(1), SchemaFieldSlot::new(0))],
            ),
            vec![PersistedFieldSnapshot::new_with_write_policy(
                FieldId::new(1),
                "score".to_string(),
                SchemaFieldSlot::new(0),
                PersistedFieldKind::Uint,
                Vec::new(),
                false,
                SchemaFieldDefault::SlotPayload(default_payload.clone()),
                SchemaFieldWritePolicy::none(),
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Uint64),
            )],
        );
        let encoded = encode_persisted_schema_snapshot(&snapshot)
            .expect("schema snapshot should encode persisted default payload");

        let decoded = decode_persisted_schema_snapshot(&encoded)
            .expect("schema snapshot should decode persisted default payload");

        assert_eq!(
            decoded.fields()[0].default().slot_payload(),
            Some(default_payload.as_slice())
        );
    }

    #[test]
    fn persisted_schema_snapshot_round_trips_field_path_indexes() {
        let snapshot = PersistedSchemaSnapshot::new_with_indexes(
            SchemaVersion::initial(),
            "entities::Indexed".to_string(),
            "Indexed".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new_with_write_policy(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    SchemaFieldWritePolicy::none(),
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new_with_write_policy(
                    FieldId::new(2),
                    "email".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Text { max_len: None },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    SchemaFieldWritePolicy::none(),
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Text),
                ),
            ],
            vec![PersistedIndexSnapshot::new(
                7,
                "Indexed|email".to_string(),
                "indexed::email".to_string(),
                true,
                PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(2),
                    SchemaFieldSlot::new(1),
                    vec!["email".to_string()],
                    PersistedFieldKind::Text { max_len: None },
                    false,
                )]),
                Some("email IS NOT NULL".to_string()),
            )],
        );
        let encoded = encode_persisted_schema_snapshot(&snapshot)
            .expect("schema snapshot should encode accepted index contracts");

        let decoded = decode_persisted_schema_snapshot(&encoded)
            .expect("schema snapshot should decode accepted index contracts");

        assert_eq!(decoded.indexes().len(), 1);
        let index = &decoded.indexes()[0];
        assert_eq!(index.ordinal(), 7);
        assert_eq!(index.name(), "Indexed|email");
        assert_eq!(index.store(), "indexed::email");
        assert!(index.unique());
        assert_eq!(index.predicate_sql(), Some("email IS NOT NULL"));
        assert_eq!(index.key().field_paths()[0].field_id(), FieldId::new(2));
        assert_eq!(index.key().field_paths()[0].slot(), SchemaFieldSlot::new(1));
        assert_eq!(index.key().field_paths()[0].path(), &["email".to_string()]);
    }
}
