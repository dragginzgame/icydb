//! Module: db::schema::enum_catalog::codec
//! Responsibility: bounded versioned encoding for accepted enum catalogs.
//! Does not own: schema-store keys, publication roots, or generated proposal merging.
//! Boundary: canonical accepted catalog <-> durable catalog bytes.

use super::{
    AcceptedEnumCatalog, AcceptedEnumType, AcceptedEnumVariant, AcceptedEnumVariantBody,
    AcceptedFieldKind, AcceptedValueContract, EnumOrderingPolicy, EnumTypeId, EnumVariantId,
};
use crate::{
    db::schema::FieldStorageDecode,
    db::schema::{
        MAX_ACCEPTED_RECURSIVE_DEPTH,
        composite_catalog::CompositeTypeId,
        wire::{SchemaWireReader, SchemaWireWriter},
    },
    error::InternalError,
    types::EntityTag,
};
use std::collections::BTreeMap;

const ACCEPTED_ENUM_CATALOG_MAGIC: &[u8; 8] = b"ICYDBENX";
const ACCEPTED_ENUM_CATALOG_CODEC_VERSION: u16 = 1;
const ACCEPTED_ENUM_CATALOG_HEADER_BYTES: usize = 14;
pub(super) const MAX_ACCEPTED_ENUM_CATALOG_BYTES: usize = 512 * 1024;
pub(in crate::db::schema) type CatalogWriter = SchemaWireWriter<MAX_ACCEPTED_ENUM_CATALOG_BYTES>;
pub(in crate::db::schema) type CatalogReader<'a> = SchemaWireReader<'a>;

const ORDERING_EQUALITY_ONLY: u8 = 0;
const VARIANT_BODY_UNIT: u8 = 0;
const VARIANT_BODY_PAYLOAD: u8 = 1;
const STORAGE_DECODE_BY_KIND: u8 = 0;
const STORAGE_DECODE_CATALOG_VALUE: u8 = 1;

const KIND_ACCOUNT: u8 = 0;
const KIND_BLOB: u8 = 1;
const KIND_BOOL: u8 = 2;
const KIND_DATE: u8 = 3;
const KIND_DECIMAL: u8 = 4;
const KIND_DURATION: u8 = 5;
const KIND_ENUM: u8 = 6;
const KIND_FLOAT32: u8 = 7;
const KIND_FLOAT64: u8 = 8;
const KIND_INT8: u8 = 9;
const KIND_INT16: u8 = 10;
const KIND_INT32: u8 = 11;
const KIND_INT64: u8 = 12;
const KIND_INT128: u8 = 13;
const KIND_INT_BIG: u8 = 14;
const KIND_PRINCIPAL: u8 = 15;
const KIND_SUBACCOUNT: u8 = 16;
const KIND_TEXT: u8 = 17;
const KIND_TIMESTAMP: u8 = 18;
const KIND_NAT8: u8 = 19;
const KIND_NAT16: u8 = 20;
const KIND_NAT32: u8 = 21;
const KIND_NAT64: u8 = 22;
const KIND_NAT128: u8 = 23;
const KIND_NAT_BIG: u8 = 24;
const KIND_ULID: u8 = 25;
const KIND_UNIT: u8 = 26;
const KIND_RELATION: u8 = 27;
const KIND_LIST: u8 = 28;
const KIND_SET: u8 = 29;
const KIND_MAP: u8 = 30;
const KIND_COMPOSITE: u8 = 31;
const KIND_U256: u8 = 32;

/// Encode one canonical accepted enum catalog into its current durable codec.
pub(in crate::db::schema) fn encode_accepted_enum_catalog(
    catalog: &AcceptedEnumCatalog,
) -> Result<Vec<u8>, InternalError> {
    if !catalog.validate() {
        return Err(InternalError::store_invariant());
    }

    let mut writer = CatalogWriter::new();
    writer.push_bytes(ACCEPTED_ENUM_CATALOG_MAGIC);
    writer.push_u16(ACCEPTED_ENUM_CATALOG_CODEC_VERSION);
    writer.push_len(catalog.by_id.len())?;
    for (type_id, definition) in &catalog.by_id {
        writer.push_u32(type_id.get());
        writer.push_string(&definition.path)?;
        writer.push_u8(match definition.ordering {
            EnumOrderingPolicy::EqualityOnly => ORDERING_EQUALITY_ONLY,
        });
        writer.push_len(definition.variants_by_id.len())?;
        for (variant_id, variant) in &definition.variants_by_id {
            writer.push_u32(variant_id.get());
            writer.push_string(&variant.name)?;
            match &variant.body {
                AcceptedEnumVariantBody::Unit => writer.push_u8(VARIANT_BODY_UNIT),
                AcceptedEnumVariantBody::Payload { contract } => {
                    writer.push_u8(VARIANT_BODY_PAYLOAD);
                    writer.push_u8(encode_storage_decode(contract.storage_decode()));
                    encode_value_kind(&mut writer, &contract.kind, 0)?;
                }
            }
        }
    }

    writer.finish()
}

/// Decode and validate one current durable accepted enum catalog.
pub(in crate::db::schema) fn decode_accepted_enum_catalog(
    bytes: &[u8],
) -> Result<AcceptedEnumCatalog, InternalError> {
    if bytes.len() < ACCEPTED_ENUM_CATALOG_HEADER_BYTES
        || bytes.len() > MAX_ACCEPTED_ENUM_CATALOG_BYTES
    {
        return Err(InternalError::store_corruption());
    }

    let mut reader = CatalogReader::new(bytes);
    if reader.read_array::<8>()? != *ACCEPTED_ENUM_CATALOG_MAGIC {
        return Err(InternalError::store_corruption());
    }
    if reader.read_u16()? != ACCEPTED_ENUM_CATALOG_CODEC_VERSION {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }

    let type_count = reader.read_count()?;
    let mut by_id = BTreeMap::new();
    let mut id_by_path = BTreeMap::new();
    let mut previous_type_id = None;
    for _ in 0..type_count {
        let type_id =
            EnumTypeId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
        if previous_type_id.is_some_and(|previous| previous >= type_id) {
            return Err(InternalError::store_corruption());
        }
        previous_type_id = Some(type_id);
        let path = reader.read_string()?;
        if path.is_empty() {
            return Err(InternalError::store_corruption());
        }
        let ordering = match reader.read_u8()? {
            ORDERING_EQUALITY_ONLY => EnumOrderingPolicy::EqualityOnly,
            _ => return Err(InternalError::store_corruption()),
        };
        let definition = decode_enum_type(&mut reader, path.clone(), ordering)?;
        if id_by_path.insert(path, type_id).is_some() || by_id.insert(type_id, definition).is_some()
        {
            return Err(InternalError::store_corruption());
        }
    }
    reader.finish()?;

    let catalog = AcceptedEnumCatalog { by_id, id_by_path };
    if !catalog.validate() {
        return Err(InternalError::store_corruption());
    }
    Ok(catalog)
}

fn decode_enum_type(
    reader: &mut CatalogReader<'_>,
    path: String,
    ordering: EnumOrderingPolicy,
) -> Result<AcceptedEnumType, InternalError> {
    let variant_count = reader.read_count()?;
    let mut variants_by_id = BTreeMap::new();
    let mut variant_id_by_name = BTreeMap::new();
    let mut previous_variant_id = None;
    for _ in 0..variant_count {
        let variant_id =
            EnumVariantId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
        if previous_variant_id.is_some_and(|previous| previous >= variant_id) {
            return Err(InternalError::store_corruption());
        }
        previous_variant_id = Some(variant_id);
        let name = reader.read_string()?;
        if name.is_empty() {
            return Err(InternalError::store_corruption());
        }
        let body = decode_variant_body(reader)?;
        let variant = AcceptedEnumVariant {
            name: name.clone(),
            body,
        };
        if variant_id_by_name.insert(name, variant_id).is_some()
            || variants_by_id.insert(variant_id, variant).is_some()
        {
            return Err(InternalError::store_corruption());
        }
    }

    Ok(AcceptedEnumType {
        path,
        variants_by_id,
        variant_id_by_name,
        ordering,
    })
}

fn decode_variant_body(
    reader: &mut CatalogReader<'_>,
) -> Result<AcceptedEnumVariantBody, InternalError> {
    match reader.read_u8()? {
        VARIANT_BODY_UNIT => Ok(AcceptedEnumVariantBody::Unit),
        VARIANT_BODY_PAYLOAD => Ok(AcceptedEnumVariantBody::Payload {
            contract: AcceptedValueContract {
                storage_decode: decode_storage_decode(reader.read_u8()?)?,
                kind: decode_value_kind(reader, 0)?,
            },
        }),
        _ => Err(InternalError::store_corruption()),
    }
}

const fn encode_storage_decode(decode: FieldStorageDecode) -> u8 {
    match decode {
        FieldStorageDecode::ByKind => STORAGE_DECODE_BY_KIND,
        FieldStorageDecode::CatalogValue => STORAGE_DECODE_CATALOG_VALUE,
    }
}

fn decode_storage_decode(tag: u8) -> Result<FieldStorageDecode, InternalError> {
    match tag {
        STORAGE_DECODE_BY_KIND => Ok(FieldStorageDecode::ByKind),
        STORAGE_DECODE_CATALOG_VALUE => Ok(FieldStorageDecode::CatalogValue),
        _ => Err(InternalError::store_corruption()),
    }
}

pub(in crate::db::schema) fn encode_value_kind(
    writer: &mut CatalogWriter,
    kind: &AcceptedFieldKind,
    depth: usize,
) -> Result<(), InternalError> {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return Err(InternalError::store_invariant());
    }
    let nested_depth = depth.saturating_add(1);
    match kind {
        AcceptedFieldKind::Account => writer.push_u8(KIND_ACCOUNT),
        AcceptedFieldKind::Blob { max_len } => {
            writer.push_u8(KIND_BLOB);
            writer.push_optional_u32(*max_len);
        }
        AcceptedFieldKind::Bool => writer.push_u8(KIND_BOOL),
        AcceptedFieldKind::Date => writer.push_u8(KIND_DATE),
        AcceptedFieldKind::Decimal { scale } => {
            writer.push_u8(KIND_DECIMAL);
            writer.push_u32(*scale);
        }
        AcceptedFieldKind::Duration => writer.push_u8(KIND_DURATION),
        AcceptedFieldKind::Enum { type_id } => {
            writer.push_u8(KIND_ENUM);
            writer.push_u32(type_id.get());
        }
        AcceptedFieldKind::Float32 => writer.push_u8(KIND_FLOAT32),
        AcceptedFieldKind::Float64 => writer.push_u8(KIND_FLOAT64),
        AcceptedFieldKind::Int8 => writer.push_u8(KIND_INT8),
        AcceptedFieldKind::Int16 => writer.push_u8(KIND_INT16),
        AcceptedFieldKind::Int32 => writer.push_u8(KIND_INT32),
        AcceptedFieldKind::Int64 => writer.push_u8(KIND_INT64),
        AcceptedFieldKind::Int128 => writer.push_u8(KIND_INT128),
        AcceptedFieldKind::IntBig { max_bytes } => {
            writer.push_u8(KIND_INT_BIG);
            writer.push_u32(*max_bytes);
        }
        AcceptedFieldKind::Principal => writer.push_u8(KIND_PRINCIPAL),
        AcceptedFieldKind::Subaccount => writer.push_u8(KIND_SUBACCOUNT),
        AcceptedFieldKind::Text { max_len } => {
            writer.push_u8(KIND_TEXT);
            writer.push_optional_u32(*max_len);
        }
        AcceptedFieldKind::Timestamp => writer.push_u8(KIND_TIMESTAMP),
        AcceptedFieldKind::Nat8 => writer.push_u8(KIND_NAT8),
        AcceptedFieldKind::Nat16 => writer.push_u8(KIND_NAT16),
        AcceptedFieldKind::Nat32 => writer.push_u8(KIND_NAT32),
        AcceptedFieldKind::Nat64 => writer.push_u8(KIND_NAT64),
        AcceptedFieldKind::Nat128 => writer.push_u8(KIND_NAT128),
        AcceptedFieldKind::NatBig { max_bytes } => {
            writer.push_u8(KIND_NAT_BIG);
            writer.push_u32(*max_bytes);
        }
        AcceptedFieldKind::Ulid => writer.push_u8(KIND_ULID),
        AcceptedFieldKind::Unit => writer.push_u8(KIND_UNIT),
        AcceptedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
        } => {
            writer.push_u8(KIND_RELATION);
            writer.push_string(target_path)?;
            writer.push_string(target_entity_name)?;
            writer.push_u64(target_entity_tag.value());
            writer.push_string(target_store_path)?;
            encode_value_kind(writer, key_kind, nested_depth)?;
        }
        AcceptedFieldKind::List(inner) => {
            writer.push_u8(KIND_LIST);
            encode_value_kind(writer, inner, nested_depth)?;
        }
        AcceptedFieldKind::Set(inner) => {
            writer.push_u8(KIND_SET);
            encode_value_kind(writer, inner, nested_depth)?;
        }
        AcceptedFieldKind::Map { key, value } => {
            writer.push_u8(KIND_MAP);
            encode_value_kind(writer, key, nested_depth)?;
            encode_value_kind(writer, value, nested_depth)?;
        }
        AcceptedFieldKind::Composite { type_id } => {
            writer.push_u8(KIND_COMPOSITE);
            writer.push_u32(type_id.get());
        }
        AcceptedFieldKind::U256 => writer.push_u8(KIND_U256),
    }
    Ok(())
}

pub(in crate::db::schema) fn decode_value_kind(
    reader: &mut CatalogReader<'_>,
    depth: usize,
) -> Result<AcceptedFieldKind, InternalError> {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return Err(InternalError::store_corruption());
    }
    let nested_depth = depth.saturating_add(1);
    Ok(match reader.read_u8()? {
        KIND_ACCOUNT => AcceptedFieldKind::Account,
        KIND_BLOB => AcceptedFieldKind::Blob {
            max_len: reader.read_optional_u32()?,
        },
        KIND_BOOL => AcceptedFieldKind::Bool,
        KIND_DATE => AcceptedFieldKind::Date,
        KIND_DECIMAL => AcceptedFieldKind::Decimal {
            scale: reader.read_u32()?,
        },
        KIND_DURATION => AcceptedFieldKind::Duration,
        KIND_ENUM => AcceptedFieldKind::Enum {
            type_id: EnumTypeId::new(reader.read_u32()?)
                .ok_or_else(InternalError::store_corruption)?,
        },
        KIND_FLOAT32 => AcceptedFieldKind::Float32,
        KIND_FLOAT64 => AcceptedFieldKind::Float64,
        KIND_INT8 => AcceptedFieldKind::Int8,
        KIND_INT16 => AcceptedFieldKind::Int16,
        KIND_INT32 => AcceptedFieldKind::Int32,
        KIND_INT64 => AcceptedFieldKind::Int64,
        KIND_INT128 => AcceptedFieldKind::Int128,
        KIND_INT_BIG => AcceptedFieldKind::IntBig {
            max_bytes: reader.read_u32()?,
        },
        KIND_PRINCIPAL => AcceptedFieldKind::Principal,
        KIND_SUBACCOUNT => AcceptedFieldKind::Subaccount,
        KIND_TEXT => AcceptedFieldKind::Text {
            max_len: reader.read_optional_u32()?,
        },
        KIND_TIMESTAMP => AcceptedFieldKind::Timestamp,
        KIND_NAT8 => AcceptedFieldKind::Nat8,
        KIND_NAT16 => AcceptedFieldKind::Nat16,
        KIND_NAT32 => AcceptedFieldKind::Nat32,
        KIND_NAT64 => AcceptedFieldKind::Nat64,
        KIND_NAT128 => AcceptedFieldKind::Nat128,
        KIND_NAT_BIG => AcceptedFieldKind::NatBig {
            max_bytes: reader.read_u32()?,
        },
        KIND_ULID => AcceptedFieldKind::Ulid,
        KIND_UNIT => AcceptedFieldKind::Unit,
        KIND_RELATION => AcceptedFieldKind::Relation {
            target_path: reader.read_string()?,
            target_entity_name: reader.read_string()?,
            target_entity_tag: EntityTag::new(reader.read_u64()?),
            target_store_path: reader.read_string()?,
            key_kind: Box::new(decode_value_kind(reader, nested_depth)?),
        },
        KIND_LIST => AcceptedFieldKind::List(Box::new(decode_value_kind(reader, nested_depth)?)),
        KIND_SET => AcceptedFieldKind::Set(Box::new(decode_value_kind(reader, nested_depth)?)),
        KIND_MAP => AcceptedFieldKind::Map {
            key: Box::new(decode_value_kind(reader, nested_depth)?),
            value: Box::new(decode_value_kind(reader, nested_depth)?),
        },
        KIND_COMPOSITE => AcceptedFieldKind::Composite {
            type_id: CompositeTypeId::new(reader.read_u32()?)
                .ok_or_else(InternalError::store_corruption)?,
        },
        KIND_U256 => AcceptedFieldKind::U256,
        _ => return Err(InternalError::store_corruption()),
    })
}

#[cfg(test)]
mod tests;
