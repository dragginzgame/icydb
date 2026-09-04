//! Direct mappings for accepted indexes and relation edges.

use super::{
    MAX_NAME_BYTES, MAX_SQL_TEXT_BYTES, SnapshotReader, SnapshotWriter,
    field::{decode_kind, encode_kind},
    mapping::{decode_sequence, direct_unit_enum_codec, encode_sequence},
};
use crate::{
    db::schema::{
        FieldId, MAX_ACCEPTED_RECURSIVE_DEPTH, PersistedIndexExpressionOp,
        PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
        PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexOrigin,
        PersistedIndexSnapshot, PersistedRelationEdgeSnapshot, PersistedRelationSourceSnapshot,
        RelationId, SchemaFieldSlot, SchemaIndexId, enum_catalog::MAX_SCHEMA_STORE_PATH_BYTES,
    },
    error::InternalError,
};

const RELATION_SOURCE_DIRECT: u8 = 1;

pub(super) fn encode_index(
    writer: &mut SnapshotWriter,
    index: &PersistedIndexSnapshot,
) -> Result<(), InternalError> {
    writer.push_u32(index.schema_id().get());
    writer.push_u16(index.ordinal());
    writer.push_u64(index.physical_generation());
    writer.push_bounded_string(index.name(), MAX_NAME_BYTES)?;
    writer.push_bounded_string(index.store(), MAX_SCHEMA_STORE_PATH_BYTES)?;
    writer.push_bool(index.unique());
    encode_index_origin(writer, index.origin());
    encode_index_key(writer, index.key())?;
    match index.predicate_sql() {
        None => writer.push_u8(0),
        Some(predicate) => {
            writer.push_u8(1);
            writer.push_bounded_string(predicate, MAX_SQL_TEXT_BYTES)?;
        }
    }
    Ok(())
}

pub(super) fn decode_index(
    reader: &mut SnapshotReader<'_>,
) -> Result<PersistedIndexSnapshot, InternalError> {
    let schema_id =
        SchemaIndexId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
    let ordinal = reader.read_u16()?;
    let physical_generation = reader.read_u64()?;
    let name = reader.read_bounded_string(MAX_NAME_BYTES)?;
    let store = reader.read_bounded_string(MAX_SCHEMA_STORE_PATH_BYTES)?;
    let unique = reader.read_bool()?;
    let origin = decode_index_origin(reader)?;
    let key = decode_index_key(reader)?;
    let predicate_sql = match reader.read_u8()? {
        0 => None,
        1 => Some(reader.read_bounded_string(MAX_SQL_TEXT_BYTES)?),
        _ => return Err(InternalError::store_corruption()),
    };
    let index = match origin {
        PersistedIndexOrigin::Generated => {
            PersistedIndexSnapshot::new(schema_id, ordinal, name, store, unique, key, predicate_sql)
        }
        PersistedIndexOrigin::SqlDdl => PersistedIndexSnapshot::new_sql_ddl(
            schema_id,
            ordinal,
            name,
            store,
            unique,
            key,
            predicate_sql,
        ),
    };
    Ok(index.clone_with_schema_identity(schema_id, ordinal, physical_generation))
}

pub(super) fn encode_relation(
    writer: &mut SnapshotWriter,
    relation: &PersistedRelationEdgeSnapshot,
) -> Result<(), InternalError> {
    writer.push_u32(relation.id().get());
    writer.push_u64(relation.physical_generation());
    writer.push_bounded_string(relation.name(), MAX_NAME_BYTES)?;
    writer.push_bounded_string(relation.target_path(), MAX_SCHEMA_STORE_PATH_BYTES)?;
    match relation.source() {
        PersistedRelationSourceSnapshot::Direct { field_ids } => {
            writer.push_u8(RELATION_SOURCE_DIRECT);
            encode_sequence!(
                writer,
                field_ids,
                icydb_schema::MAX_FRAGMENT_FIELDS,
                |field_id| {
                    writer.push_u32(field_id.get());
                }
            );
        }
    }
    Ok(())
}

pub(super) fn decode_relation(
    reader: &mut SnapshotReader<'_>,
) -> Result<PersistedRelationEdgeSnapshot, InternalError> {
    let id = RelationId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
    let physical_generation = reader.read_u64()?;
    let name = reader.read_bounded_string(MAX_NAME_BYTES)?;
    let target_path = reader.read_bounded_string(MAX_SCHEMA_STORE_PATH_BYTES)?;
    let source = match reader.read_u8()? {
        RELATION_SOURCE_DIRECT => PersistedRelationSourceSnapshot::Direct {
            field_ids: decode_sequence!(reader, icydb_schema::MAX_FRAGMENT_FIELDS, {
                FieldId::new(reader.read_u32()?)
            }),
        },
        0 => return Err(InternalError::serialize_incompatible_persisted_format()),
        _ => return Err(InternalError::store_corruption()),
    };
    let PersistedRelationSourceSnapshot::Direct { field_ids } = source;
    Ok(
        PersistedRelationEdgeSnapshot::new_direct(id, name, target_path, field_ids)
            .clone_with_physical_generation(physical_generation),
    )
}

fn encode_index_key(
    writer: &mut SnapshotWriter,
    key: &PersistedIndexKeySnapshot,
) -> Result<(), InternalError> {
    match key {
        PersistedIndexKeySnapshot::FieldPath(paths) => {
            writer.push_u8(1);
            encode_sequence!(writer, paths, icydb_schema::MAX_FRAGMENT_FIELDS, |path| {
                encode_field_path(writer, path)?;
            });
        }
        PersistedIndexKeySnapshot::Items(items) => {
            writer.push_u8(2);
            encode_sequence!(writer, items, icydb_schema::MAX_FRAGMENT_FIELDS, |item| {
                encode_index_item(writer, item)?;
            });
        }
    }
    Ok(())
}

fn decode_index_key(
    reader: &mut SnapshotReader<'_>,
) -> Result<PersistedIndexKeySnapshot, InternalError> {
    match reader.read_u8()? {
        1 => Ok(PersistedIndexKeySnapshot::FieldPath(decode_sequence!(
            reader,
            icydb_schema::MAX_FRAGMENT_FIELDS,
            decode_field_path(reader)?
        ))),
        2 => Ok(PersistedIndexKeySnapshot::Items(decode_sequence!(
            reader,
            icydb_schema::MAX_FRAGMENT_FIELDS,
            decode_index_item(reader)?
        ))),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_index_item(
    writer: &mut SnapshotWriter,
    item: &PersistedIndexKeyItemSnapshot,
) -> Result<(), InternalError> {
    match item {
        PersistedIndexKeyItemSnapshot::FieldPath(path) => {
            writer.push_u8(1);
            encode_field_path(writer, path)?;
        }
        PersistedIndexKeyItemSnapshot::Expression(expression) => {
            writer.push_u8(2);
            encode_index_expression(writer, expression)?;
        }
    }
    Ok(())
}

fn decode_index_item(
    reader: &mut SnapshotReader<'_>,
) -> Result<PersistedIndexKeyItemSnapshot, InternalError> {
    match reader.read_u8()? {
        1 => decode_field_path(reader).map(PersistedIndexKeyItemSnapshot::FieldPath),
        2 => decode_index_expression(reader)
            .map(Box::new)
            .map(PersistedIndexKeyItemSnapshot::Expression),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_field_path(
    writer: &mut SnapshotWriter,
    path: &PersistedIndexFieldPathSnapshot,
) -> Result<(), InternalError> {
    writer.push_u32(path.field_id().get());
    writer.push_u16(path.slot().get());
    encode_sequence!(
        writer,
        path.path(),
        MAX_ACCEPTED_RECURSIVE_DEPTH,
        |component| {
            writer.push_bounded_string(component, MAX_NAME_BYTES)?;
        }
    );
    encode_kind(writer, path.kind(), 0)?;
    writer.push_bool(path.nullable());
    Ok(())
}

fn decode_field_path(
    reader: &mut SnapshotReader<'_>,
) -> Result<PersistedIndexFieldPathSnapshot, InternalError> {
    let field_id = FieldId::new(reader.read_u32()?);
    let slot = SchemaFieldSlot::new(reader.read_u16()?);
    let path = decode_sequence!(reader, MAX_ACCEPTED_RECURSIVE_DEPTH, {
        reader.read_bounded_string(MAX_NAME_BYTES)?
    });
    let kind = decode_kind(reader, 0)?;
    let nullable = reader.read_bool()?;
    Ok(PersistedIndexFieldPathSnapshot::new(
        field_id, slot, path, kind, nullable,
    ))
}

fn encode_index_expression(
    writer: &mut SnapshotWriter,
    expression: &PersistedIndexExpressionSnapshot,
) -> Result<(), InternalError> {
    encode_expression_op(writer, expression.op());
    encode_field_path(writer, expression.source())?;
    encode_kind(writer, expression.input_kind(), 0)?;
    encode_kind(writer, expression.output_kind(), 0)?;
    writer.push_bounded_string(expression.canonical_text(), MAX_SQL_TEXT_BYTES)?;
    Ok(())
}

fn decode_index_expression(
    reader: &mut SnapshotReader<'_>,
) -> Result<PersistedIndexExpressionSnapshot, InternalError> {
    Ok(PersistedIndexExpressionSnapshot::new(
        decode_expression_op(reader)?,
        decode_field_path(reader)?,
        decode_kind(reader, 0)?,
        decode_kind(reader, 0)?,
        reader.read_bounded_string(MAX_SQL_TEXT_BYTES)?,
    ))
}

direct_unit_enum_codec! {
    encode = encode_index_origin,
    decode = decode_index_origin,
    type = PersistedIndexOrigin,
    writer = SnapshotWriter,
    {
        1 => PersistedIndexOrigin::Generated,
        2 => PersistedIndexOrigin::SqlDdl,
    }
}

direct_unit_enum_codec! {
    encode = encode_expression_op,
    decode = decode_expression_op,
    type = PersistedIndexExpressionOp,
    writer = SnapshotWriter,
    {
        1 => PersistedIndexExpressionOp::Lower,
        2 => PersistedIndexExpressionOp::Upper,
        3 => PersistedIndexExpressionOp::Trim,
        4 => PersistedIndexExpressionOp::LowerTrim,
        5 => PersistedIndexExpressionOp::Date,
        6 => PersistedIndexExpressionOp::Year,
        7 => PersistedIndexExpressionOp::Month,
        8 => PersistedIndexExpressionOp::Day,
    }
}
