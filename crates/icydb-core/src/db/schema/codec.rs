//! Module: db::schema::codec
//! Responsibility: typed persisted-schema snapshot encoding.
//! Does not own: reconciliation policy, schema proposal construction, or row decoding.
//! Boundary: converts schema-owned snapshot DTOs to/from raw `SchemaStore` payload bytes.

use crate::{
    db::schema::{
        FieldId, PersistedEnumVariant, PersistedFieldKind, PersistedFieldSnapshot,
        PersistedNestedLeafSnapshot, PersistedRelationStrength, PersistedSchemaSnapshot,
        SchemaFieldDefault, SchemaFieldSlot, SchemaRowLayout, SchemaVersion,
    },
    error::InternalError,
    model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
    types::EntityTag,
};
use candid::{CandidType, Decode, Encode};
use serde::Deserialize;

const SCHEMA_SNAPSHOT_CODEC_VERSION: u32 = 1;

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

// Candid wire enum for database-level default metadata.
#[derive(CandidType, Deserialize)]
enum SchemaFieldDefaultWire {
    None,
}

// Candid wire enum for the complete persisted field-kind shape.
#[derive(CandidType, Deserialize)]
enum PersistedFieldKindWire {
    Account,
    Blob,
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
        }
    }

    fn into_snapshot(self) -> Result<PersistedSchemaSnapshot, InternalError> {
        if self.codec_version != SCHEMA_SNAPSHOT_CODEC_VERSION {
            return Err(InternalError::store_corruption(format!(
                "unsupported persisted schema snapshot codec version: {}",
                self.codec_version
            )));
        }

        Ok(PersistedSchemaSnapshot::new(
            SchemaVersion::new(self.version),
            self.entity_path,
            self.entity_name,
            FieldId::new(self.primary_key_field_id),
            self.row_layout.into_layout(),
            self.fields
                .into_iter()
                .map(PersistedFieldSnapshotWire::into_field)
                .collect::<Result<Vec<_>, _>>()?,
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
            storage_decode: FieldStorageDecodeWire::from_storage_decode(field.storage_decode()),
            leaf_codec: LeafCodecWire::from_leaf_codec(field.leaf_codec()),
        }
    }

    fn into_field(self) -> Result<PersistedFieldSnapshot, InternalError> {
        Ok(PersistedFieldSnapshot::new(
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

impl SchemaFieldDefaultWire {
    const fn from_default(default: SchemaFieldDefault) -> Self {
        match default {
            SchemaFieldDefault::None => Self::None,
        }
    }

    const fn into_default(self) -> SchemaFieldDefault {
        match self {
            Self::None => SchemaFieldDefault::None,
        }
    }
}

impl PersistedFieldKindWire {
    fn from_kind(kind: &PersistedFieldKind) -> Self {
        match kind {
            PersistedFieldKind::Account => Self::Account,
            PersistedFieldKind::Blob => Self::Blob,
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
            Self::Blob => PersistedFieldKind::Blob,
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
