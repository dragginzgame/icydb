//! Direct mappings for accepted constraints and activation state.

use super::{
    SnapshotReader, SnapshotWriter,
    field::{decode_kind, decode_literal_storage, encode_kind, encode_literal_storage},
    mapping::{direct_unit_enum_codec, encode_sequence},
};
use crate::{
    db::schema::{
        AcceptedCheckCompareOpV1, AcceptedCheckExprV1, AcceptedCheckLiteralV1,
        AcceptedCheckValueExprV1, AcceptedConstraintKind, AcceptedConstraintSnapshot,
        AcceptedNamedTypeIdentity, AcceptedRuleOperation, AcceptedRuleTarget,
        AcceptedSchemaFingerprint, ConstraintActivationFingerprint, ConstraintActivationKind,
        ConstraintActivationSnapshot, ConstraintActivationState, ConstraintId, ConstraintOrigin,
        FieldId, RelationId, SchemaIndexId,
        check::{
            MAX_CHECK_EXPR_V1_CHILDREN, MAX_CHECK_EXPR_V1_DEPTH, MAX_CHECK_EXPR_V1_LITERAL_BYTES,
            MAX_CHECK_EXPR_V1_NODES,
        },
        composite_catalog::CompositeTypeId,
        constraint::MAX_ACCEPTED_CONSTRAINT_NAME_BYTES,
    },
    error::InternalError,
    value::EnumTypeId,
};

pub(super) fn encode_constraint(
    writer: &mut SnapshotWriter,
    constraint: &AcceptedConstraintSnapshot,
) -> Result<(), InternalError> {
    writer.push_u32(constraint.id().get());
    writer.push_bounded_string(constraint.name(), MAX_ACCEPTED_CONSTRAINT_NAME_BYTES)?;
    encode_origin(writer, constraint.origin());
    encode_constraint_kind(writer, constraint.kind())
}

pub(super) fn decode_constraint(
    reader: &mut SnapshotReader<'_>,
) -> Result<AcceptedConstraintSnapshot, InternalError> {
    let id = ConstraintId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
    let name = reader.read_bounded_string(MAX_ACCEPTED_CONSTRAINT_NAME_BYTES)?;
    let origin = decode_origin(reader)?;
    let kind = decode_constraint_kind(reader)?;
    Ok(AcceptedConstraintSnapshot::new(id, name, origin, kind))
}

pub(super) fn encode_activation(
    writer: &mut SnapshotWriter,
    activation: &ConstraintActivationSnapshot,
) -> Result<(), InternalError> {
    writer.push_u32(activation.id().get());
    writer.push_bounded_string(activation.name(), MAX_ACCEPTED_CONSTRAINT_NAME_BYTES)?;
    encode_origin(writer, activation.origin());
    encode_activation_kind(writer, activation.kind())?;
    encode_activation_state(writer, activation.state());
    writer.push_bytes(&activation.base_schema_fingerprint().as_bytes());
    writer.push_u64(activation.activation_epoch());
    writer.push_bytes(&activation.fingerprint().as_bytes());
    Ok(())
}

pub(super) fn decode_activation(
    reader: &mut SnapshotReader<'_>,
) -> Result<ConstraintActivationSnapshot, InternalError> {
    let id = ConstraintId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
    let name = reader.read_bounded_string(MAX_ACCEPTED_CONSTRAINT_NAME_BYTES)?;
    let origin = decode_origin(reader)?;
    let kind = decode_activation_kind(reader)?;
    let state = decode_activation_state(reader)?;
    let base_schema_fingerprint = AcceptedSchemaFingerprint::new(reader.read_array()?);
    let activation_epoch = reader.read_u64()?;
    let fingerprint = ConstraintActivationFingerprint::new(reader.read_array()?);
    Ok(ConstraintActivationSnapshot::from_persisted_parts(
        id,
        name,
        origin,
        kind,
        state,
        base_schema_fingerprint,
        activation_epoch,
        fingerprint,
    ))
}

fn encode_constraint_kind(
    writer: &mut SnapshotWriter,
    kind: &AcceptedConstraintKind,
) -> Result<(), InternalError> {
    match kind {
        AcceptedConstraintKind::PrimaryKey => writer.push_u8(1),
        AcceptedConstraintKind::NotNull { field_id } => {
            writer.push_u8(2);
            writer.push_u32(field_id.get());
        }
        AcceptedConstraintKind::Unique { index_id } => {
            writer.push_u8(3);
            writer.push_u32(index_id.get());
        }
        AcceptedConstraintKind::Relation { relation_id } => {
            writer.push_u8(4);
            writer.push_u32(relation_id.get());
        }
        AcceptedConstraintKind::Check { expression } => {
            writer.push_u8(5);
            encode_check_expression(writer, expression, 0)?;
        }
        AcceptedConstraintKind::TargetedRule { target, operation } => {
            writer.push_u8(6);
            encode_rule_target(writer, *target);
            encode_rule_operation(writer, operation)?;
        }
    }
    Ok(())
}

fn decode_constraint_kind(
    reader: &mut SnapshotReader<'_>,
) -> Result<AcceptedConstraintKind, InternalError> {
    match reader.read_u8()? {
        1 => Ok(AcceptedConstraintKind::PrimaryKey),
        2 => Ok(AcceptedConstraintKind::NotNull {
            field_id: FieldId::new(reader.read_u32()?),
        }),
        3 => Ok(AcceptedConstraintKind::Unique {
            index_id: SchemaIndexId::new(reader.read_u32()?)
                .ok_or_else(InternalError::store_corruption)?,
        }),
        4 => Ok(AcceptedConstraintKind::Relation {
            relation_id: RelationId::new(reader.read_u32()?)
                .ok_or_else(InternalError::store_corruption)?,
        }),
        5 => {
            let mut nodes = 0;
            Ok(AcceptedConstraintKind::Check {
                expression: Box::new(decode_check_expression(reader, 0, &mut nodes)?),
            })
        }
        6 => Ok(AcceptedConstraintKind::TargetedRule {
            target: decode_rule_target(reader)?,
            operation: Box::new(decode_rule_operation(reader)?),
        }),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_activation_kind(
    writer: &mut SnapshotWriter,
    kind: &ConstraintActivationKind,
) -> Result<(), InternalError> {
    match kind {
        ConstraintActivationKind::NotNull { field_id } => {
            writer.push_u8(1);
            writer.push_u32(field_id.get());
        }
        ConstraintActivationKind::Unique { index_id } => {
            writer.push_u8(2);
            writer.push_u32(index_id.get());
        }
        ConstraintActivationKind::Relation { relation_id } => {
            writer.push_u8(3);
            writer.push_u32(relation_id.get());
        }
        ConstraintActivationKind::Check { expression } => {
            writer.push_u8(4);
            encode_check_expression(writer, expression, 0)?;
        }
        ConstraintActivationKind::TargetedRule { target, operation } => {
            writer.push_u8(5);
            encode_rule_target(writer, *target);
            encode_rule_operation(writer, operation)?;
        }
    }
    Ok(())
}

fn decode_activation_kind(
    reader: &mut SnapshotReader<'_>,
) -> Result<ConstraintActivationKind, InternalError> {
    match reader.read_u8()? {
        1 => Ok(ConstraintActivationKind::NotNull {
            field_id: FieldId::new(reader.read_u32()?),
        }),
        2 => Ok(ConstraintActivationKind::Unique {
            index_id: SchemaIndexId::new(reader.read_u32()?)
                .ok_or_else(InternalError::store_corruption)?,
        }),
        3 => Ok(ConstraintActivationKind::Relation {
            relation_id: RelationId::new(reader.read_u32()?)
                .ok_or_else(InternalError::store_corruption)?,
        }),
        4 => {
            let mut nodes = 0;
            Ok(ConstraintActivationKind::Check {
                expression: Box::new(decode_check_expression(reader, 0, &mut nodes)?),
            })
        }
        5 => Ok(ConstraintActivationKind::TargetedRule {
            target: decode_rule_target(reader)?,
            operation: Box::new(decode_rule_operation(reader)?),
        }),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_rule_target(writer: &mut SnapshotWriter, target: AcceptedRuleTarget) {
    writer.push_u32(target.root_field_id().get());
    match target.target_type() {
        AcceptedNamedTypeIdentity::Enum(type_id) => {
            writer.push_u8(1);
            writer.push_u32(type_id.get());
        }
        AcceptedNamedTypeIdentity::Composite(type_id) => {
            writer.push_u8(2);
            writer.push_u32(type_id.get());
        }
    }
}

fn decode_rule_target(
    reader: &mut SnapshotReader<'_>,
) -> Result<AcceptedRuleTarget, InternalError> {
    let root_field_id = FieldId::new(reader.read_u32()?);
    let target_type = match reader.read_u8()? {
        1 => AcceptedNamedTypeIdentity::Enum(
            EnumTypeId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?,
        ),
        2 => AcceptedNamedTypeIdentity::Composite(
            CompositeTypeId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?,
        ),
        _ => return Err(InternalError::store_corruption()),
    };
    Ok(AcceptedRuleTarget::new(root_field_id, target_type))
}

fn encode_rule_operation(
    writer: &mut SnapshotWriter,
    operation: &AcceptedRuleOperation,
) -> Result<(), InternalError> {
    match operation {
        AcceptedRuleOperation::LengthRangeInclusive { min, max } => {
            writer.push_u8(1);
            writer.push_u64(*min);
            writer.push_u64(*max);
        }
        AcceptedRuleOperation::MultipleOf { divisor } => {
            writer.push_u8(2);
            encode_literal(writer, divisor)?;
        }
        AcceptedRuleOperation::NumericMaximumInclusive { value } => {
            writer.push_u8(3);
            encode_literal(writer, value)?;
        }
        AcceptedRuleOperation::NumericMinimumInclusive { value } => {
            writer.push_u8(4);
            encode_literal(writer, value)?;
        }
        AcceptedRuleOperation::NumericRangeInclusive { min, max } => {
            writer.push_u8(5);
            encode_literal(writer, min)?;
            encode_literal(writer, max)?;
        }
    }
    Ok(())
}

fn decode_rule_operation(
    reader: &mut SnapshotReader<'_>,
) -> Result<AcceptedRuleOperation, InternalError> {
    match reader.read_u8()? {
        1 => Ok(AcceptedRuleOperation::LengthRangeInclusive {
            min: reader.read_u64()?,
            max: reader.read_u64()?,
        }),
        2 => Ok(AcceptedRuleOperation::MultipleOf {
            divisor: decode_literal(reader)?,
        }),
        3 => Ok(AcceptedRuleOperation::NumericMaximumInclusive {
            value: decode_literal(reader)?,
        }),
        4 => Ok(AcceptedRuleOperation::NumericMinimumInclusive {
            value: decode_literal(reader)?,
        }),
        5 => Ok(AcceptedRuleOperation::NumericRangeInclusive {
            min: decode_literal(reader)?,
            max: decode_literal(reader)?,
        }),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_check_expression(
    writer: &mut SnapshotWriter,
    expression: &AcceptedCheckExprV1,
    depth: u16,
) -> Result<(), InternalError> {
    if depth >= MAX_CHECK_EXPR_V1_DEPTH {
        return Err(InternalError::store_unsupported());
    }
    let next_depth = depth.saturating_add(1);
    match expression {
        AcceptedCheckExprV1::True => writer.push_u8(1),
        AcceptedCheckExprV1::False => writer.push_u8(2),
        AcceptedCheckExprV1::Not(inner) => {
            writer.push_u8(3);
            encode_check_expression(writer, inner, next_depth)?;
        }
        AcceptedCheckExprV1::And(children) => {
            writer.push_u8(4);
            encode_sequence!(writer, children, MAX_CHECK_EXPR_V1_CHILDREN, |child| {
                encode_check_expression(writer, child, next_depth)?;
            });
        }
        AcceptedCheckExprV1::Or(children) => {
            writer.push_u8(5);
            encode_sequence!(writer, children, MAX_CHECK_EXPR_V1_CHILDREN, |child| {
                encode_check_expression(writer, child, next_depth)?;
            });
        }
        AcceptedCheckExprV1::Compare { left, op, right } => {
            writer.push_u8(6);
            encode_check_value(writer, left)?;
            encode_compare_op(writer, *op);
            encode_check_value(writer, right)?;
        }
        AcceptedCheckExprV1::IsNull(value) => {
            writer.push_u8(7);
            encode_check_value(writer, value)?;
        }
        AcceptedCheckExprV1::IsNotNull(value) => {
            writer.push_u8(8);
            encode_check_value(writer, value)?;
        }
    }
    Ok(())
}

pub(super) fn decode_check_expression(
    reader: &mut SnapshotReader<'_>,
    depth: u16,
    nodes: &mut u16,
) -> Result<AcceptedCheckExprV1, InternalError> {
    if depth >= MAX_CHECK_EXPR_V1_DEPTH || *nodes >= MAX_CHECK_EXPR_V1_NODES {
        return Err(InternalError::store_corruption());
    }
    *nodes = nodes.saturating_add(1);
    let next_depth = depth.saturating_add(1);
    match reader.read_u8()? {
        1 => Ok(AcceptedCheckExprV1::True),
        2 => Ok(AcceptedCheckExprV1::False),
        3 => Ok(AcceptedCheckExprV1::Not(Box::new(decode_check_expression(
            reader, next_depth, nodes,
        )?))),
        4 => {
            let count = reader.read_bounded_count(MAX_CHECK_EXPR_V1_CHILDREN)?;
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(decode_check_expression(reader, next_depth, nodes)?);
            }
            Ok(AcceptedCheckExprV1::And(children))
        }
        5 => {
            let count = reader.read_bounded_count(MAX_CHECK_EXPR_V1_CHILDREN)?;
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(decode_check_expression(reader, next_depth, nodes)?);
            }
            Ok(AcceptedCheckExprV1::Or(children))
        }
        6 => Ok(AcceptedCheckExprV1::Compare {
            left: decode_check_value(reader)?,
            op: decode_compare_op(reader)?,
            right: decode_check_value(reader)?,
        }),
        7 => Ok(AcceptedCheckExprV1::IsNull(decode_check_value(reader)?)),
        8 => Ok(AcceptedCheckExprV1::IsNotNull(decode_check_value(reader)?)),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_check_value(
    writer: &mut SnapshotWriter,
    value: &AcceptedCheckValueExprV1,
) -> Result<(), InternalError> {
    match value {
        AcceptedCheckValueExprV1::Field(field_id) => {
            writer.push_u8(1);
            writer.push_u32(field_id.get());
        }
        AcceptedCheckValueExprV1::Literal(literal) => {
            writer.push_u8(2);
            encode_literal(writer, literal)?;
        }
        AcceptedCheckValueExprV1::CharLength(field_id) => {
            writer.push_u8(3);
            writer.push_u32(field_id.get());
        }
        AcceptedCheckValueExprV1::OctetLength(field_id) => {
            writer.push_u8(4);
            writer.push_u32(field_id.get());
        }
        AcceptedCheckValueExprV1::Cardinality(field_id) => {
            writer.push_u8(5);
            writer.push_u32(field_id.get());
        }
    }
    Ok(())
}

fn decode_check_value(
    reader: &mut SnapshotReader<'_>,
) -> Result<AcceptedCheckValueExprV1, InternalError> {
    match reader.read_u8()? {
        1 => Ok(AcceptedCheckValueExprV1::Field(FieldId::new(
            reader.read_u32()?,
        ))),
        2 => decode_literal(reader).map(AcceptedCheckValueExprV1::Literal),
        3 => Ok(AcceptedCheckValueExprV1::CharLength(FieldId::new(
            reader.read_u32()?,
        ))),
        4 => Ok(AcceptedCheckValueExprV1::OctetLength(FieldId::new(
            reader.read_u32()?,
        ))),
        5 => Ok(AcceptedCheckValueExprV1::Cardinality(FieldId::new(
            reader.read_u32()?,
        ))),
        _ => Err(InternalError::store_corruption()),
    }
}

fn encode_literal(
    writer: &mut SnapshotWriter,
    literal: &AcceptedCheckLiteralV1,
) -> Result<(), InternalError> {
    encode_kind(writer, literal.kind(), 0)?;
    encode_literal_storage(writer, literal.storage_decode(), literal.leaf_codec());
    writer.push_bounded_len_prefixed_bytes(literal.payload(), MAX_CHECK_EXPR_V1_LITERAL_BYTES)
}

fn decode_literal(
    reader: &mut SnapshotReader<'_>,
) -> Result<AcceptedCheckLiteralV1, InternalError> {
    let kind = decode_kind(reader, 0)?;
    let (storage_decode, leaf_codec) = decode_literal_storage(reader)?;
    let payload = reader
        .read_bounded_len_prefixed_bytes(MAX_CHECK_EXPR_V1_LITERAL_BYTES)?
        .to_vec();
    Ok(AcceptedCheckLiteralV1::from_accepted_parts(
        kind,
        storage_decode,
        leaf_codec,
        payload,
    ))
}

direct_unit_enum_codec! {
    encode = encode_origin,
    decode = decode_origin,
    type = ConstraintOrigin,
    writer = SnapshotWriter,
    {
        1 => ConstraintOrigin::Generated,
        2 => ConstraintOrigin::SqlDdl,
    }
}

direct_unit_enum_codec! {
    encode = encode_activation_state,
    decode = decode_activation_state,
    type = ConstraintActivationState,
    writer = SnapshotWriter,
    {
        1 => ConstraintActivationState::EnforcingNewWrites,
        2 => ConstraintActivationState::Validating,
    }
}

direct_unit_enum_codec! {
    encode = encode_compare_op,
    decode = decode_compare_op,
    type = AcceptedCheckCompareOpV1,
    writer = SnapshotWriter,
    {
        1 => AcceptedCheckCompareOpV1::Eq,
        2 => AcceptedCheckCompareOpV1::Ne,
        3 => AcceptedCheckCompareOpV1::Lt,
        4 => AcceptedCheckCompareOpV1::Lte,
        5 => AcceptedCheckCompareOpV1::Gt,
        6 => AcceptedCheckCompareOpV1::Gte,
    }
}
