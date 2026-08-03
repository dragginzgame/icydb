//! Direct mappings for accepted field and storage contracts.

use super::{
    MAX_NAME_BYTES, MAX_SLOT_PAYLOAD_BYTES, SnapshotReader, SnapshotWriter,
    mapping::{decode_sequence, direct_unit_enum_codec, encode_sequence},
};
use crate::{
    db::schema::{
        AcceptedFieldKind, FieldInsertGeneration, FieldStorageDecode, FieldWriteManagement,
        LeafCodec, MAX_ACCEPTED_RECURSIVE_DEPTH, PersistedFieldOrigin, PersistedFieldSnapshot,
        PersistedNestedLeafSnapshot, RowLayoutVersion, ScalarCodec, SchemaFieldSlot,
        SchemaFieldWritePolicy, SchemaHistoricalFill, SchemaInsertDefault,
        composite_catalog::CompositeTypeId, enum_catalog::MAX_SCHEMA_STORE_PATH_BYTES,
    },
    error::InternalError,
    types::EntityTag,
    value::EnumTypeId,
};

pub(super) fn encode_field(
    writer: &mut SnapshotWriter,
    field: &PersistedFieldSnapshot,
) -> Result<(), InternalError> {
    writer.push_u32(field.id().get());
    writer.push_bounded_string(field.name(), MAX_NAME_BYTES)?;
    writer.push_u16(field.slot().get());
    encode_kind(writer, field.kind(), 0)?;
    encode_sequence!(
        writer,
        field.nested_leaves(),
        icydb_schema::MAX_FRAGMENT_FIELDS,
        |leaf| {
            encode_nested_leaf(writer, leaf)?;
        }
    );
    writer.push_bool(field.nullable());
    writer.push_u32(field.introduced_in_layout().get());
    encode_insert_default(writer, field.insert_default())?;
    encode_historical_fill(writer, field.historical_fill())?;
    encode_write_policy(writer, field.write_policy());
    encode_field_origin(writer, field.origin());
    encode_storage_decode(writer, field.storage_decode());
    encode_leaf_codec(writer, field.leaf_codec());
    Ok(())
}

pub(super) fn decode_field(
    reader: &mut SnapshotReader<'_>,
) -> Result<PersistedFieldSnapshot, InternalError> {
    let id = crate::db::schema::FieldId::new(reader.read_u32()?);
    let name = reader.read_bounded_string(MAX_NAME_BYTES)?;
    let slot = SchemaFieldSlot::new(reader.read_u16()?);
    let kind = decode_kind(reader, 0)?;
    let nested_leaves = decode_sequence!(reader, icydb_schema::MAX_FRAGMENT_FIELDS, {
        decode_nested_leaf(reader)?
    });
    let nullable = reader.read_bool()?;
    let introduced_in_layout =
        RowLayoutVersion::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
    let insert_default = decode_insert_default(reader)?;
    let historical_fill = decode_historical_fill(reader)?;
    let write_policy = decode_write_policy(reader)?;
    let origin = decode_field_origin(reader)?;
    let storage_decode = decode_storage_decode(reader)?;
    let leaf_codec = decode_leaf_codec(reader)?;
    Ok(PersistedFieldSnapshot::new_with_write_policy_and_origin(
        id,
        name,
        slot,
        kind,
        nested_leaves,
        nullable,
        introduced_in_layout,
        insert_default,
        historical_fill,
        write_policy,
        origin,
        storage_decode,
        leaf_codec,
    ))
}

fn encode_nested_leaf(
    writer: &mut SnapshotWriter,
    leaf: &PersistedNestedLeafSnapshot,
) -> Result<(), InternalError> {
    encode_sequence!(
        writer,
        leaf.path(),
        MAX_ACCEPTED_RECURSIVE_DEPTH,
        |component| {
            writer.push_bounded_string(component, MAX_NAME_BYTES)?;
        }
    );
    encode_kind(writer, leaf.kind(), 0)?;
    writer.push_bool(leaf.nullable());
    Ok(())
}

fn decode_nested_leaf(
    reader: &mut SnapshotReader<'_>,
) -> Result<PersistedNestedLeafSnapshot, InternalError> {
    let path = decode_sequence!(reader, MAX_ACCEPTED_RECURSIVE_DEPTH, {
        reader.read_bounded_string(MAX_NAME_BYTES)?
    });
    let kind = decode_kind(reader, 0)?;
    let nullable = reader.read_bool()?;
    Ok(PersistedNestedLeafSnapshot::new(path, kind, nullable))
}

pub(super) fn encode_kind(
    writer: &mut SnapshotWriter,
    kind: &AcceptedFieldKind,
    depth: usize,
) -> Result<(), InternalError> {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return Err(InternalError::store_unsupported());
    }
    match kind {
        AcceptedFieldKind::Account => writer.push_u8(1),
        AcceptedFieldKind::Blob { max_len } => {
            writer.push_u8(2);
            writer.push_optional_u32(*max_len);
        }
        AcceptedFieldKind::Bool => writer.push_u8(3),
        AcceptedFieldKind::Date => writer.push_u8(4),
        AcceptedFieldKind::Decimal { scale } => {
            writer.push_u8(5);
            writer.push_u32(*scale);
        }
        AcceptedFieldKind::Duration => writer.push_u8(6),
        AcceptedFieldKind::Enum { type_id } => {
            writer.push_u8(7);
            writer.push_u32(type_id.get());
        }
        AcceptedFieldKind::Float32 => writer.push_u8(8),
        AcceptedFieldKind::Float64 => writer.push_u8(9),
        AcceptedFieldKind::Int8 => writer.push_u8(10),
        AcceptedFieldKind::Int16 => writer.push_u8(11),
        AcceptedFieldKind::Int32 => writer.push_u8(12),
        AcceptedFieldKind::Int64 => writer.push_u8(13),
        AcceptedFieldKind::Int128 => writer.push_u8(14),
        AcceptedFieldKind::IntBig { max_bytes } => {
            writer.push_u8(15);
            writer.push_u32(*max_bytes);
        }
        AcceptedFieldKind::Principal => writer.push_u8(16),
        AcceptedFieldKind::Subaccount => writer.push_u8(17),
        AcceptedFieldKind::Text { max_len } => {
            writer.push_u8(18);
            writer.push_optional_u32(*max_len);
        }
        AcceptedFieldKind::Timestamp => writer.push_u8(19),
        AcceptedFieldKind::Nat8 => writer.push_u8(20),
        AcceptedFieldKind::Nat16 => writer.push_u8(21),
        AcceptedFieldKind::Nat32 => writer.push_u8(22),
        AcceptedFieldKind::Nat64 => writer.push_u8(23),
        AcceptedFieldKind::Nat128 => writer.push_u8(24),
        AcceptedFieldKind::NatBig { max_bytes } => {
            writer.push_u8(25);
            writer.push_u32(*max_bytes);
        }
        AcceptedFieldKind::Ulid => writer.push_u8(26),
        AcceptedFieldKind::Unit => writer.push_u8(27),
        AcceptedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
        } => {
            writer.push_u8(28);
            writer.push_bounded_string(target_path, MAX_SCHEMA_STORE_PATH_BYTES)?;
            writer.push_bounded_string(target_entity_name, MAX_NAME_BYTES)?;
            writer.push_u64(target_entity_tag.value());
            writer.push_bounded_string(target_store_path, MAX_SCHEMA_STORE_PATH_BYTES)?;
            encode_kind(writer, key_kind, depth.saturating_add(1))?;
        }
        AcceptedFieldKind::List(inner) => {
            writer.push_u8(29);
            encode_kind(writer, inner, depth.saturating_add(1))?;
        }
        AcceptedFieldKind::Set(inner) => {
            writer.push_u8(30);
            encode_kind(writer, inner, depth.saturating_add(1))?;
        }
        AcceptedFieldKind::Map { key, value } => {
            writer.push_u8(31);
            encode_kind(writer, key, depth.saturating_add(1))?;
            encode_kind(writer, value, depth.saturating_add(1))?;
        }
        AcceptedFieldKind::Composite { type_id } => {
            writer.push_u8(32);
            writer.push_u32(type_id.get());
        }
    }
    Ok(())
}

pub(super) fn decode_kind(
    reader: &mut SnapshotReader<'_>,
    depth: usize,
) -> Result<AcceptedFieldKind, InternalError> {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return Err(InternalError::store_corruption());
    }
    let next_depth = depth.saturating_add(1);
    match reader.read_u8()? {
        1 => Ok(AcceptedFieldKind::Account),
        2 => Ok(AcceptedFieldKind::Blob {
            max_len: reader.read_optional_u32()?,
        }),
        3 => Ok(AcceptedFieldKind::Bool),
        4 => Ok(AcceptedFieldKind::Date),
        5 => Ok(AcceptedFieldKind::Decimal {
            scale: reader.read_u32()?,
        }),
        6 => Ok(AcceptedFieldKind::Duration),
        7 => Ok(AcceptedFieldKind::Enum {
            type_id: EnumTypeId::new(reader.read_u32()?)
                .ok_or_else(InternalError::store_corruption)?,
        }),
        8 => Ok(AcceptedFieldKind::Float32),
        9 => Ok(AcceptedFieldKind::Float64),
        10 => Ok(AcceptedFieldKind::Int8),
        11 => Ok(AcceptedFieldKind::Int16),
        12 => Ok(AcceptedFieldKind::Int32),
        13 => Ok(AcceptedFieldKind::Int64),
        14 => Ok(AcceptedFieldKind::Int128),
        15 => Ok(AcceptedFieldKind::IntBig {
            max_bytes: reader.read_u32()?,
        }),
        16 => Ok(AcceptedFieldKind::Principal),
        17 => Ok(AcceptedFieldKind::Subaccount),
        18 => Ok(AcceptedFieldKind::Text {
            max_len: reader.read_optional_u32()?,
        }),
        19 => Ok(AcceptedFieldKind::Timestamp),
        20 => Ok(AcceptedFieldKind::Nat8),
        21 => Ok(AcceptedFieldKind::Nat16),
        22 => Ok(AcceptedFieldKind::Nat32),
        23 => Ok(AcceptedFieldKind::Nat64),
        24 => Ok(AcceptedFieldKind::Nat128),
        25 => Ok(AcceptedFieldKind::NatBig {
            max_bytes: reader.read_u32()?,
        }),
        26 => Ok(AcceptedFieldKind::Ulid),
        27 => Ok(AcceptedFieldKind::Unit),
        28 => Ok(AcceptedFieldKind::Relation {
            target_path: reader.read_bounded_string(MAX_SCHEMA_STORE_PATH_BYTES)?,
            target_entity_name: reader.read_bounded_string(MAX_NAME_BYTES)?,
            target_entity_tag: EntityTag::new(reader.read_u64()?),
            target_store_path: reader.read_bounded_string(MAX_SCHEMA_STORE_PATH_BYTES)?,
            key_kind: Box::new(decode_kind(reader, next_depth)?),
        }),
        29 => Ok(AcceptedFieldKind::List(Box::new(decode_kind(
            reader, next_depth,
        )?))),
        30 => Ok(AcceptedFieldKind::Set(Box::new(decode_kind(
            reader, next_depth,
        )?))),
        31 => Ok(AcceptedFieldKind::Map {
            key: Box::new(decode_kind(reader, next_depth)?),
            value: Box::new(decode_kind(reader, next_depth)?),
        }),
        32 => Ok(AcceptedFieldKind::Composite {
            type_id: CompositeTypeId::new(reader.read_u32()?)
                .ok_or_else(InternalError::store_corruption)?,
        }),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_insert_default(
    writer: &mut SnapshotWriter,
    value: &SchemaInsertDefault,
) -> Result<(), InternalError> {
    match value {
        SchemaInsertDefault::None => writer.push_u8(0),
        SchemaInsertDefault::SlotPayload(bytes) => {
            writer.push_u8(1);
            writer.push_bounded_len_prefixed_bytes(bytes, MAX_SLOT_PAYLOAD_BYTES)?;
        }
    }
    Ok(())
}

fn decode_insert_default(
    reader: &mut SnapshotReader<'_>,
) -> Result<SchemaInsertDefault, InternalError> {
    match reader.read_u8()? {
        0 => Ok(SchemaInsertDefault::None),
        1 => Ok(SchemaInsertDefault::SlotPayload(
            reader
                .read_bounded_len_prefixed_bytes(MAX_SLOT_PAYLOAD_BYTES)?
                .to_vec(),
        )),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_historical_fill(
    writer: &mut SnapshotWriter,
    value: &SchemaHistoricalFill,
) -> Result<(), InternalError> {
    match value {
        SchemaHistoricalFill::Reject => writer.push_u8(0),
        SchemaHistoricalFill::Null => writer.push_u8(1),
        SchemaHistoricalFill::SlotPayload(bytes) => {
            writer.push_u8(2);
            writer.push_bounded_len_prefixed_bytes(bytes, MAX_SLOT_PAYLOAD_BYTES)?;
        }
    }
    Ok(())
}

fn decode_historical_fill(
    reader: &mut SnapshotReader<'_>,
) -> Result<SchemaHistoricalFill, InternalError> {
    match reader.read_u8()? {
        0 => Ok(SchemaHistoricalFill::Reject),
        1 => Ok(SchemaHistoricalFill::Null),
        2 => Ok(SchemaHistoricalFill::SlotPayload(
            reader
                .read_bounded_len_prefixed_bytes(MAX_SLOT_PAYLOAD_BYTES)?
                .to_vec(),
        )),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_write_policy(writer: &mut SnapshotWriter, value: SchemaFieldWritePolicy) {
    match value.insert_generation() {
        None => writer.push_u8(0),
        Some(FieldInsertGeneration::Identity) => writer.push_u8(1),
        Some(FieldInsertGeneration::Ulid) => writer.push_u8(2),
        Some(FieldInsertGeneration::Timestamp) => writer.push_u8(3),
    }
    match value.write_management() {
        None => writer.push_u8(0),
        Some(FieldWriteManagement::CreatedAt) => writer.push_u8(1),
        Some(FieldWriteManagement::UpdatedAt) => writer.push_u8(2),
    }
}

fn decode_write_policy(
    reader: &mut SnapshotReader<'_>,
) -> Result<SchemaFieldWritePolicy, InternalError> {
    let insert_generation = match reader.read_u8()? {
        0 => None,
        1 => Some(FieldInsertGeneration::Identity),
        2 => Some(FieldInsertGeneration::Ulid),
        3 => Some(FieldInsertGeneration::Timestamp),
        _ => return Err(InternalError::store_corruption()),
    };
    let write_management = match reader.read_u8()? {
        0 => None,
        1 => Some(FieldWriteManagement::CreatedAt),
        2 => Some(FieldWriteManagement::UpdatedAt),
        _ => return Err(InternalError::store_corruption()),
    };
    Ok(SchemaFieldWritePolicy::from_model_policies(
        insert_generation,
        write_management,
    ))
}

direct_unit_enum_codec! {
    encode = encode_field_origin,
    decode = decode_field_origin,
    type = PersistedFieldOrigin,
    writer = SnapshotWriter,
    {
        1 => PersistedFieldOrigin::Generated,
        2 => PersistedFieldOrigin::SqlDdl,
    }
}

direct_unit_enum_codec! {
    encode = encode_storage_decode,
    decode = decode_storage_decode,
    type = FieldStorageDecode,
    writer = SnapshotWriter,
    {
        1 => FieldStorageDecode::ByKind,
        2 => FieldStorageDecode::CatalogValue,
    }
}

fn encode_leaf_codec(writer: &mut SnapshotWriter, value: LeafCodec) {
    match value {
        LeafCodec::Scalar(codec) => {
            writer.push_u8(1);
            encode_scalar_codec(writer, codec);
        }
        LeafCodec::Structural => writer.push_u8(2),
    }
}

fn decode_leaf_codec(reader: &mut SnapshotReader<'_>) -> Result<LeafCodec, InternalError> {
    match reader.read_u8()? {
        1 => decode_scalar_codec(reader).map(LeafCodec::Scalar),
        2 => Ok(LeafCodec::Structural),
        _ => Err(InternalError::store_corruption()),
    }
}

direct_unit_enum_codec! {
    encode = encode_scalar_codec,
    decode = decode_scalar_codec,
    type = ScalarCodec,
    writer = SnapshotWriter,
    {
        1 => ScalarCodec::Blob,
        2 => ScalarCodec::Bool,
        3 => ScalarCodec::Date,
        4 => ScalarCodec::Duration,
        5 => ScalarCodec::Float32,
        6 => ScalarCodec::Float64,
        7 => ScalarCodec::Int64,
        8 => ScalarCodec::Principal,
        9 => ScalarCodec::Subaccount,
        10 => ScalarCodec::Text,
        11 => ScalarCodec::Timestamp,
        12 => ScalarCodec::Nat64,
        13 => ScalarCodec::Ulid,
        14 => ScalarCodec::Unit,
    }
}

pub(super) fn encode_literal_storage(
    writer: &mut SnapshotWriter,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
) {
    encode_storage_decode(writer, storage_decode);
    encode_leaf_codec(writer, leaf_codec);
}

pub(super) fn decode_literal_storage(
    reader: &mut SnapshotReader<'_>,
) -> Result<(FieldStorageDecode, LeafCodec), InternalError> {
    Ok((decode_storage_decode(reader)?, decode_leaf_codec(reader)?))
}
