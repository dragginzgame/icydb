//! Bounded current-format accepted index predicate encoding.

use super::{
    MAX_SCHEMA_SNAPSHOT_BYTES, SnapshotReader, SnapshotWriter,
    field::{decode_kind, decode_literal_storage, encode_kind, encode_literal_storage},
};
use crate::{
    db::{
        predicate::{CoercionId, CompareOp},
        schema::{AcceptedCheckLiteralV1, AcceptedIndexPredicate as P, FieldId},
    },
    error::InternalError,
};

// Match maintained SQL source depth without turning SQL parser interpretation
// into durable authority. Total bytes bound nodes, children and literal backing.
const MAX_DEPTH: usize = crate::db::sql_shared::MAX_SQL_EXPR_DEPTH;
const MAX_ITEMS: usize = MAX_SCHEMA_SNAPSHOT_BYTES as usize;

pub(super) fn encode(
    writer: &mut SnapshotWriter,
    p: &P,
    depth: usize,
) -> Result<(), InternalError> {
    if depth >= MAX_DEPTH {
        return Err(InternalError::store_unsupported());
    }
    match p {
        P::True => writer.push_u8(0),
        P::False => writer.push_u8(1),
        P::Not(inner) => {
            writer.push_u8(2);
            encode(writer, inner, depth + 1)?;
        }
        P::And(children) | P::Or(children) => {
            writer.push_u8(if matches!(p, P::And(_)) { 3 } else { 4 });
            push_count(writer, children.len(), MAX_ITEMS)?;
            for child in children {
                encode(writer, child, depth + 1)?;
            }
        }
        P::Compare {
            field,
            op,
            coercion,
            values,
        } => {
            if values.is_empty()
                || (!matches!(op, CompareOp::In | CompareOp::NotIn) && values.len() != 1)
            {
                return Err(InternalError::store_unsupported());
            }
            writer.push_u8(5);
            encode_field(writer, *field);
            encode_op(writer, *op);
            encode_coercion(writer, *coercion);
            push_count(writer, values.len(), MAX_ITEMS)?;
            for value in values {
                match value {
                    None => writer.push_u8(0),
                    Some(value) => {
                        writer.push_u8(1);
                        encode_literal(writer, value)?;
                    }
                }
            }
        }
        P::CompareFields {
            left,
            op,
            right,
            coercion,
        } => {
            writer.push_u8(6);
            encode_field(writer, *left);
            encode_op(writer, *op);
            encode_field(writer, *right);
            encode_coercion(writer, *coercion);
        }
        P::IsNull(field) | P::IsNotNull(field) => {
            writer.push_u8(if matches!(p, P::IsNull(_)) { 7 } else { 8 });
            encode_field(writer, *field);
        }
    }
    Ok(())
}

pub(super) fn decode(
    reader: &mut SnapshotReader<'_>,
    depth: usize,
    nodes: &mut usize,
) -> Result<P, InternalError> {
    if depth >= MAX_DEPTH || *nodes >= MAX_ITEMS {
        return Err(InternalError::store_corruption());
    }
    *nodes += 1;
    Ok(match reader.read_u8()? {
        0 => P::True,
        1 => P::False,
        2 => P::Not(Box::new(decode(reader, depth + 1, nodes)?)),
        tag @ (3 | 4) => {
            let count = reader.read_bounded_count(MAX_ITEMS)?;
            let mut children = Vec::new();
            for _ in 0..count {
                children.push(decode(reader, depth + 1, nodes)?);
            }
            if tag == 3 {
                P::And(children)
            } else {
                P::Or(children)
            }
        }
        5 => {
            let field = decode_field(reader)?;
            let op = decode_op(reader)?;
            let coercion = decode_coercion(reader)?;
            let count = reader.read_bounded_count(MAX_ITEMS)?;
            if count == 0 || (!matches!(op, CompareOp::In | CompareOp::NotIn) && count != 1) {
                return Err(InternalError::store_corruption());
            }
            let mut values = Vec::new();
            for _ in 0..count {
                values.push(match reader.read_u8()? {
                    0 => None,
                    1 => Some(decode_literal(reader)?),
                    _ => return Err(InternalError::store_corruption()),
                });
            }
            P::Compare {
                field,
                op,
                coercion,
                values,
            }
        }
        6 => P::CompareFields {
            left: decode_field(reader)?,
            op: decode_op(reader)?,
            right: decode_field(reader)?,
            coercion: decode_coercion(reader)?,
        },
        7 => P::IsNull(decode_field(reader)?),
        8 => P::IsNotNull(decode_field(reader)?),
        _ => return Err(InternalError::store_corruption()),
    })
}
fn encode_field(writer: &mut SnapshotWriter, field: FieldId) {
    writer.push_u32(field.get());
}
fn decode_field(reader: &mut SnapshotReader<'_>) -> Result<FieldId, InternalError> {
    Ok(FieldId::new(reader.read_u32()?))
}
fn encode_literal(
    writer: &mut SnapshotWriter,
    literal: &AcceptedCheckLiteralV1,
) -> Result<(), InternalError> {
    if !literal.kind().has_valid_local_shape()
        || literal.leaf_codec()
            != literal
                .kind()
                .leaf_codec_for_storage(literal.storage_decode())
        || literal.payload().is_empty()
    {
        return Err(InternalError::store_unsupported());
    }
    encode_kind(writer, literal.kind(), 0)?;
    encode_literal_storage(writer, literal.storage_decode(), literal.leaf_codec());
    writer.push_bounded_len_prefixed_bytes(literal.payload(), MAX_ITEMS)
}
fn decode_literal(
    reader: &mut SnapshotReader<'_>,
) -> Result<AcceptedCheckLiteralV1, InternalError> {
    let kind = decode_kind(reader, 0)?;
    let (storage, leaf) = decode_literal_storage(reader)?;
    Ok(AcceptedCheckLiteralV1::from_accepted_parts(
        kind,
        storage,
        leaf,
        reader.read_bounded_len_prefixed_bytes(MAX_ITEMS)?.to_vec(),
    ))
}
fn encode_op(writer: &mut SnapshotWriter, op: CompareOp) {
    writer.push_u8(match op {
        CompareOp::Eq => 0,
        CompareOp::Ne => 1,
        CompareOp::Lt => 2,
        CompareOp::Lte => 3,
        CompareOp::Gt => 4,
        CompareOp::Gte => 5,
        CompareOp::In => 6,
        CompareOp::NotIn => 7,
        CompareOp::Contains => 8,
        CompareOp::StartsWith => 9,
        CompareOp::EndsWith => 10,
    });
}
fn decode_op(reader: &mut SnapshotReader<'_>) -> Result<CompareOp, InternalError> {
    Ok(match reader.read_u8()? {
        0 => CompareOp::Eq,
        1 => CompareOp::Ne,
        2 => CompareOp::Lt,
        3 => CompareOp::Lte,
        4 => CompareOp::Gt,
        5 => CompareOp::Gte,
        6 => CompareOp::In,
        7 => CompareOp::NotIn,
        8 => CompareOp::Contains,
        9 => CompareOp::StartsWith,
        10 => CompareOp::EndsWith,
        _ => return Err(InternalError::store_corruption()),
    })
}
fn encode_coercion(writer: &mut SnapshotWriter, coercion: CoercionId) {
    writer.push_u8(match coercion {
        CoercionId::Strict => 0,
        CoercionId::NumericWiden => 1,
        CoercionId::TextCasefold => 2,
        CoercionId::CollectionElement => 3,
    });
}
fn decode_coercion(reader: &mut SnapshotReader<'_>) -> Result<CoercionId, InternalError> {
    Ok(match reader.read_u8()? {
        0 => CoercionId::Strict,
        1 => CoercionId::NumericWiden,
        2 => CoercionId::TextCasefold,
        3 => CoercionId::CollectionElement,
        _ => return Err(InternalError::store_corruption()),
    })
}

fn push_count(
    writer: &mut SnapshotWriter,
    count: usize,
    limit: usize,
) -> Result<(), InternalError> {
    if count > limit {
        return Err(InternalError::store_unsupported());
    }
    writer.push_len(count)
}
