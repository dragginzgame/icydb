//! Module: db::schema::composite_catalog::codec
//! Responsibility: bounded versioned encoding for accepted composite catalogs.
//! Does not own: accepted-schema bundle framing or generated structural codecs.
//! Boundary: canonical accepted composite catalog <-> durable catalog bytes.

#[cfg(test)]
mod tests;

use super::{
    AcceptedCompositeCatalog, AcceptedCompositeElement, AcceptedCompositeField,
    AcceptedCompositeShape, AcceptedCompositeType, CompositeTypeId,
};
use crate::{
    db::schema::enum_catalog::{
        AcceptedEnumCatalog,
        codec::{CatalogReader, CatalogWriter, decode_value_kind, encode_value_kind},
    },
    error::InternalError,
    model::field::CompositeCodec,
};
use std::collections::BTreeMap;

const ACCEPTED_COMPOSITE_CATALOG_MAGIC: &[u8; 8] = b"ICYDBCMP";
const ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION: u16 = 1;
const ACCEPTED_COMPOSITE_CATALOG_HEADER_BYTES: usize = 14;
const MAX_ACCEPTED_COMPOSITE_CATALOG_BYTES: usize = 512 * 1024;

const CODEC_STRUCTURAL_V1: u8 = 1;
const SHAPE_RECORD: u8 = 1;
const SHAPE_TUPLE: u8 = 2;
const SHAPE_NEWTYPE: u8 = 3;

/// Encode one validated current-form accepted composite catalog.
pub(in crate::db::schema) fn encode_accepted_composite_catalog(
    catalog: &AcceptedCompositeCatalog,
    enum_catalog: &AcceptedEnumCatalog,
) -> Result<Vec<u8>, InternalError> {
    if !catalog.validate(enum_catalog) {
        return Err(InternalError::store_invariant());
    }

    let mut writer = CatalogWriter::new();
    writer.push_bytes(ACCEPTED_COMPOSITE_CATALOG_MAGIC);
    writer.push_u16(ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION);
    writer.push_len(catalog.by_id.len())?;
    for (type_id, definition) in &catalog.by_id {
        writer.push_u32(type_id.get());
        writer.push_string(&definition.path)?;
        writer.push_u8(match definition.codec {
            CompositeCodec::StructuralV1 => CODEC_STRUCTURAL_V1,
        });
        encode_shape(&mut writer, &definition.shape)?;
    }
    let bytes = writer.finish()?;
    if bytes.len() > MAX_ACCEPTED_COMPOSITE_CATALOG_BYTES {
        return Err(InternalError::store_unsupported());
    }
    Ok(bytes)
}

/// Decode one bounded current-form catalog and prove all referenced kinds.
pub(in crate::db::schema) fn decode_accepted_composite_catalog(
    bytes: &[u8],
    enum_catalog: &AcceptedEnumCatalog,
) -> Result<AcceptedCompositeCatalog, InternalError> {
    if bytes.len() < ACCEPTED_COMPOSITE_CATALOG_HEADER_BYTES
        || bytes.len() > MAX_ACCEPTED_COMPOSITE_CATALOG_BYTES
    {
        return Err(InternalError::store_corruption());
    }

    let mut reader = CatalogReader::new(bytes);
    if reader.read_array::<8>()? != *ACCEPTED_COMPOSITE_CATALOG_MAGIC {
        return Err(InternalError::store_corruption());
    }
    if reader.read_u16()? != ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }

    let type_count = reader.read_count()?;
    let mut by_id = BTreeMap::new();
    let mut id_by_path = BTreeMap::new();
    let mut previous_id = None;
    for _ in 0..type_count {
        let type_id =
            CompositeTypeId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
        if previous_id.is_some_and(|previous| previous >= type_id) {
            return Err(InternalError::store_corruption());
        }
        previous_id = Some(type_id);
        let path = reader.read_string()?;
        if path.is_empty() {
            return Err(InternalError::store_corruption());
        }
        let codec = match reader.read_u8()? {
            CODEC_STRUCTURAL_V1 => CompositeCodec::StructuralV1,
            _ => return Err(InternalError::store_corruption()),
        };
        let definition = AcceptedCompositeType {
            path: path.clone(),
            codec,
            shape: decode_shape(&mut reader)?,
        };
        if id_by_path.insert(path, type_id).is_some() || by_id.insert(type_id, definition).is_some()
        {
            return Err(InternalError::store_corruption());
        }
    }
    reader.finish()?;

    let catalog = AcceptedCompositeCatalog { by_id, id_by_path };
    if !catalog.validate(enum_catalog) {
        return Err(InternalError::store_corruption());
    }
    Ok(catalog)
}

fn encode_shape(
    writer: &mut CatalogWriter,
    shape: &AcceptedCompositeShape,
) -> Result<(), InternalError> {
    match shape {
        AcceptedCompositeShape::Record(fields) => {
            writer.push_u8(SHAPE_RECORD);
            writer.push_len(fields.len())?;
            for field in fields {
                writer.push_string(&field.name)?;
                encode_element(writer, &field.contract)?;
            }
        }
        AcceptedCompositeShape::Tuple(elements) => {
            writer.push_u8(SHAPE_TUPLE);
            writer.push_len(elements.len())?;
            for element in elements {
                encode_element(writer, element)?;
            }
        }
        AcceptedCompositeShape::Newtype(inner) => {
            writer.push_u8(SHAPE_NEWTYPE);
            encode_element(writer, inner)?;
        }
    }
    Ok(())
}

fn decode_shape(reader: &mut CatalogReader<'_>) -> Result<AcceptedCompositeShape, InternalError> {
    match reader.read_u8()? {
        SHAPE_RECORD => {
            let field_count = reader.read_count()?;
            let mut fields = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                fields.push(AcceptedCompositeField {
                    name: reader.read_string()?,
                    contract: decode_element(reader)?,
                });
            }
            Ok(AcceptedCompositeShape::Record(fields))
        }
        SHAPE_TUPLE => {
            let element_count = reader.read_count()?;
            let mut elements = Vec::with_capacity(element_count);
            for _ in 0..element_count {
                elements.push(decode_element(reader)?);
            }
            Ok(AcceptedCompositeShape::Tuple(elements))
        }
        SHAPE_NEWTYPE => Ok(AcceptedCompositeShape::Newtype(decode_element(reader)?)),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_element(
    writer: &mut CatalogWriter,
    element: &AcceptedCompositeElement,
) -> Result<(), InternalError> {
    writer.push_u8(u8::from(element.nullable));
    encode_value_kind(writer, &element.kind, 0)
}

fn decode_element(
    reader: &mut CatalogReader<'_>,
) -> Result<AcceptedCompositeElement, InternalError> {
    let nullable = match reader.read_u8()? {
        0 => false,
        1 => true,
        _ => return Err(InternalError::store_corruption()),
    };
    Ok(AcceptedCompositeElement {
        kind: decode_value_kind(reader, 0)?,
        nullable,
    })
}
