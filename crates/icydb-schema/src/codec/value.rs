use crate::{
    Account, Blob, ConstraintSourceKey, Date, Decimal, Duration, EntitySourceKey, FieldSourceKey,
    FieldType, Float32, Float64, IndexSourceKey, IntBig, MAX_PROPOSAL_LITERAL_BYTES,
    MAX_SCHEMA_FIELD_TYPE_DEPTH, MAX_SCHEMA_NAME_BYTES, MAX_SOURCE_CHECK_INSTRUCTIONS,
    MAX_SOURCE_KEY_BYTES, NatBig, Principal, RelationSourceKey, RuleSourceKey, ScalarLiteral,
    ScalarType, SchemaContractError, SchemaName, SourceCheckExpr, SourceCheckInstruction,
    SourceRuleOperation, Subaccount, Timestamp, TypeSourceKey, Ulid, Unit,
};

use super::wire::{WireReader, WireWriter};

pub(super) fn encode_schema_name(
    writer: &mut WireWriter,
    value: &SchemaName,
) -> Result<(), SchemaContractError> {
    writer.push_string(value.as_str())
}

pub(super) fn decode_schema_name(
    reader: &mut WireReader<'_>,
) -> Result<SchemaName, SchemaContractError> {
    SchemaName::try_new(reader.read_string(MAX_SCHEMA_NAME_BYTES)?)
}

pub(super) fn encode_source_key(
    writer: &mut WireWriter,
    value: &str,
) -> Result<(), SchemaContractError> {
    writer.push_string(value)
}

pub(super) fn decode_field_key(
    reader: &mut WireReader<'_>,
) -> Result<FieldSourceKey, SchemaContractError> {
    FieldSourceKey::try_new(reader.read_string(MAX_SOURCE_KEY_BYTES)?)
}

macro_rules! decode_source_key {
    ($function:ident, $type:ty) => {
        pub(super) fn $function(reader: &mut WireReader<'_>) -> Result<$type, SchemaContractError> {
            <$type>::try_new(reader.read_string(MAX_SOURCE_KEY_BYTES)?)
        }
    };
}

decode_source_key!(decode_entity_key, EntitySourceKey);
decode_source_key!(decode_constraint_key, ConstraintSourceKey);
decode_source_key!(decode_index_key, IndexSourceKey);
decode_source_key!(decode_relation_key, RelationSourceKey);
decode_source_key!(decode_rule_key, RuleSourceKey);

pub(super) fn decode_type_key(
    reader: &mut WireReader<'_>,
) -> Result<TypeSourceKey, SchemaContractError> {
    TypeSourceKey::try_new(reader.read_string(MAX_SOURCE_KEY_BYTES)?)
}

pub(super) fn encode_field_type(
    writer: &mut WireWriter,
    value: &FieldType,
) -> Result<(), SchemaContractError> {
    encode_field_type_at_depth(writer, value, 0)
}

fn encode_field_type_at_depth(
    writer: &mut WireWriter,
    value: &FieldType,
    depth: usize,
) -> Result<(), SchemaContractError> {
    let depth = depth
        .checked_add(1)
        .ok_or(SchemaContractError::FieldTypeDepthExceeded)?;
    if depth > MAX_SCHEMA_FIELD_TYPE_DEPTH {
        return Err(SchemaContractError::FieldTypeDepthExceeded);
    }
    match value {
        FieldType::Scalar(scalar) => {
            writer.push_u8(0)?;
            encode_scalar_type(writer, *scalar)
        }
        FieldType::List(item) => {
            writer.push_u8(1)?;
            encode_field_type_at_depth(writer, item, depth)
        }
        FieldType::Named(key) => {
            writer.push_u8(2)?;
            encode_source_key(writer, key.as_str())
        }
    }
}

pub(super) fn decode_field_type(
    reader: &mut WireReader<'_>,
) -> Result<FieldType, SchemaContractError> {
    decode_field_type_at_depth(reader, 0)
}

fn decode_field_type_at_depth(
    reader: &mut WireReader<'_>,
    depth: usize,
) -> Result<FieldType, SchemaContractError> {
    let depth = depth
        .checked_add(1)
        .ok_or(SchemaContractError::FieldTypeDepthExceeded)?;
    if depth > MAX_SCHEMA_FIELD_TYPE_DEPTH {
        return Err(SchemaContractError::FieldTypeDepthExceeded);
    }
    match reader.read_u8()? {
        0 => Ok(FieldType::Scalar(decode_scalar_type(reader)?)),
        1 => Ok(FieldType::List(Box::new(decode_field_type_at_depth(
            reader, depth,
        )?))),
        2 => Ok(FieldType::Named(decode_type_key(reader)?)),
        _ => Err(SchemaContractError::Decode),
    }
}

pub(super) fn encode_scalar_type(
    writer: &mut WireWriter,
    value: ScalarType,
) -> Result<(), SchemaContractError> {
    match value {
        ScalarType::Blob { max_len } => {
            writer.push_u8(1)?;
            encode_optional_u32(writer, max_len)?;
        }
        ScalarType::Decimal { scale } => {
            writer.push_u8(4)?;
            writer.push_u32(scale)?;
        }
        ScalarType::IntBig { max_bytes } => {
            writer.push_u8(13)?;
            writer.push_u32(max_bytes)?;
        }
        ScalarType::Text { max_len } => {
            writer.push_u8(16)?;
            encode_optional_u32(writer, max_len)?;
        }
        ScalarType::NatBig { max_bytes } => {
            writer.push_u8(23)?;
            writer.push_u32(max_bytes)?;
        }
        scalar => writer.push_u8(match scalar {
            ScalarType::Account => 0,
            ScalarType::Bool => 2,
            ScalarType::Date => 3,
            ScalarType::Duration => 5,
            ScalarType::Float32 => 6,
            ScalarType::Float64 => 7,
            ScalarType::Int8 => 8,
            ScalarType::Int16 => 9,
            ScalarType::Int32 => 10,
            ScalarType::Int64 => 11,
            ScalarType::Int128 => 12,
            ScalarType::Principal => 14,
            ScalarType::Subaccount => 15,
            ScalarType::Timestamp => 17,
            ScalarType::Nat8 => 18,
            ScalarType::Nat16 => 19,
            ScalarType::Nat32 => 20,
            ScalarType::Nat64 => 21,
            ScalarType::Nat128 => 22,
            ScalarType::Ulid => 24,
            ScalarType::Unit => 25,
            ScalarType::Blob { .. }
            | ScalarType::Decimal { .. }
            | ScalarType::IntBig { .. }
            | ScalarType::Text { .. }
            | ScalarType::NatBig { .. } => return Err(SchemaContractError::Encode),
        })?,
    }
    Ok(())
}

pub(super) fn decode_scalar_type(
    reader: &mut WireReader<'_>,
) -> Result<ScalarType, SchemaContractError> {
    let value = match reader.read_u8()? {
        0 => ScalarType::Account,
        1 => ScalarType::Blob {
            max_len: decode_optional_u32(reader)?,
        },
        2 => ScalarType::Bool,
        3 => ScalarType::Date,
        4 => ScalarType::Decimal {
            scale: reader.read_u32()?,
        },
        5 => ScalarType::Duration,
        6 => ScalarType::Float32,
        7 => ScalarType::Float64,
        8 => ScalarType::Int8,
        9 => ScalarType::Int16,
        10 => ScalarType::Int32,
        11 => ScalarType::Int64,
        12 => ScalarType::Int128,
        13 => ScalarType::IntBig {
            max_bytes: reader.read_u32()?,
        },
        14 => ScalarType::Principal,
        15 => ScalarType::Subaccount,
        16 => ScalarType::Text {
            max_len: decode_optional_u32(reader)?,
        },
        17 => ScalarType::Timestamp,
        18 => ScalarType::Nat8,
        19 => ScalarType::Nat16,
        20 => ScalarType::Nat32,
        21 => ScalarType::Nat64,
        22 => ScalarType::Nat128,
        23 => ScalarType::NatBig {
            max_bytes: reader.read_u32()?,
        },
        24 => ScalarType::Ulid,
        25 => ScalarType::Unit,
        _ => return Err(SchemaContractError::Decode),
    };
    value.validate()?;
    Ok(value)
}

fn encode_optional_u32(
    writer: &mut WireWriter,
    value: Option<u32>,
) -> Result<(), SchemaContractError> {
    writer.push_bool(value.is_some())?;
    if let Some(value) = value {
        writer.push_u32(value)?;
    }
    Ok(())
}

fn decode_optional_u32(reader: &mut WireReader<'_>) -> Result<Option<u32>, SchemaContractError> {
    reader.read_bool()?.then(|| reader.read_u32()).transpose()
}

pub(super) fn encode_literal(
    writer: &mut WireWriter,
    value: &ScalarLiteral,
) -> Result<(), SchemaContractError> {
    value.validate()?;
    match value {
        ScalarLiteral::Account(value) => {
            writer.push_u8(0)?;
            encode_principal(writer, value.owner())?;
            writer.push_bool(value.subaccount().is_some())?;
            if let Some(subaccount) = value.subaccount() {
                writer.push_raw(&subaccount.to_bytes())?;
            }
        }
        ScalarLiteral::Blob(value) => {
            writer.push_u8(1)?;
            writer.push_bytes(value.as_bytes())?;
        }
        ScalarLiteral::Bool(value) => {
            writer.push_u8(2)?;
            writer.push_bool(*value)?;
        }
        ScalarLiteral::Date(value) => {
            writer.push_u8(3)?;
            writer.push_i32(value.as_days_since_epoch())?;
        }
        ScalarLiteral::Decimal(value) => {
            writer.push_u8(4)?;
            writer.push_i128(value.mantissa())?;
            writer.push_u32(value.scale())?;
        }
        ScalarLiteral::Duration(value) => {
            writer.push_u8(5)?;
            writer.push_u64(value.as_millis())?;
        }
        ScalarLiteral::EnumUnit { enum_type, variant } => {
            writer.push_u8(6)?;
            encode_source_key(writer, enum_type.as_str())?;
            encode_source_key(writer, variant.as_str())?;
        }
        ScalarLiteral::Float32(value) => {
            writer.push_u8(7)?;
            writer.push_raw(&value.to_be_bytes())?;
        }
        ScalarLiteral::Float64(value) => {
            writer.push_u8(8)?;
            writer.push_raw(&value.to_be_bytes())?;
        }
        ScalarLiteral::Int(value) => {
            writer.push_u8(9)?;
            writer.push_i128(*value)?;
        }
        ScalarLiteral::IntBig(value) => {
            writer.push_u8(10)?;
            let (negative, magnitude) = value.to_sign_and_magnitude_bytes();
            writer.push_bool(negative)?;
            writer.push_bytes(&magnitude)?;
        }
        ScalarLiteral::Nat(value) => {
            writer.push_u8(11)?;
            writer.push_u128(*value)?;
        }
        ScalarLiteral::NatBig(value) => {
            writer.push_u8(12)?;
            writer.push_bytes(&value.to_magnitude_bytes())?;
        }
        ScalarLiteral::Principal(value) => {
            writer.push_u8(13)?;
            encode_principal(writer, *value)?;
        }
        ScalarLiteral::Subaccount(value) => {
            writer.push_u8(14)?;
            writer.push_raw(&value.to_bytes())?;
        }
        ScalarLiteral::Text(value) => {
            writer.push_u8(15)?;
            writer.push_string(value)?;
        }
        ScalarLiteral::Timestamp(value) => {
            writer.push_u8(16)?;
            writer.push_i64(value.as_millis())?;
        }
        ScalarLiteral::Ulid(value) => {
            writer.push_u8(17)?;
            writer.push_raw(&value.to_bytes())?;
        }
        ScalarLiteral::Unit(_) => writer.push_u8(18)?,
    }
    Ok(())
}

pub(super) fn decode_literal(
    reader: &mut WireReader<'_>,
) -> Result<ScalarLiteral, SchemaContractError> {
    let value = match reader.read_u8()? {
        0 => {
            let owner = decode_principal(reader)?;
            let subaccount = reader
                .read_bool()?
                .then(|| reader.read_array().map(Subaccount::from_array))
                .transpose()?;
            ScalarLiteral::Account(Account::from_owner_and_subaccount(owner, subaccount))
        }
        1 => ScalarLiteral::Blob(Blob::from(
            reader.read_bytes(MAX_PROPOSAL_LITERAL_BYTES)?.to_vec(),
        )),
        2 => ScalarLiteral::Bool(reader.read_bool()?),
        3 => ScalarLiteral::Date(
            Date::try_from_days_since_epoch(reader.read_i32()?)
                .ok_or(SchemaContractError::Decode)?,
        ),
        4 => ScalarLiteral::Decimal(
            Decimal::try_from_i128_with_scale(reader.read_i128()?, reader.read_u32()?)
                .ok_or(SchemaContractError::Decode)?,
        ),
        5 => ScalarLiteral::Duration(Duration::from_millis(reader.read_u64()?)),
        6 => ScalarLiteral::EnumUnit {
            enum_type: decode_type_key(reader)?,
            variant: decode_type_key(reader)?,
        },
        7 => ScalarLiteral::Float32(
            Float32::try_from_bytes(&reader.read_array::<4>()?)
                .map_err(|_| SchemaContractError::Decode)?,
        ),
        8 => ScalarLiteral::Float64(
            Float64::try_from_bytes(&reader.read_array::<8>()?)
                .map_err(|_| SchemaContractError::Decode)?,
        ),
        9 => ScalarLiteral::Int(reader.read_i128()?),
        10 => {
            let negative = reader.read_bool()?;
            let magnitude = reader.read_bytes(MAX_PROPOSAL_LITERAL_BYTES)?;
            ScalarLiteral::IntBig(IntBig::from_sign_and_magnitude_bytes(negative, magnitude))
        }
        11 => ScalarLiteral::Nat(reader.read_u128()?),
        12 => ScalarLiteral::NatBig(NatBig::from_magnitude_bytes(
            reader.read_bytes(MAX_PROPOSAL_LITERAL_BYTES)?,
        )),
        13 => ScalarLiteral::Principal(decode_principal(reader)?),
        14 => ScalarLiteral::Subaccount(Subaccount::from_array(reader.read_array()?)),
        15 => ScalarLiteral::Text(reader.read_string(MAX_PROPOSAL_LITERAL_BYTES)?),
        16 => ScalarLiteral::Timestamp(Timestamp::from_millis(reader.read_i64()?)),
        17 => ScalarLiteral::Ulid(
            Ulid::try_from_bytes(&reader.read_array::<16>()?)
                .map_err(|_| SchemaContractError::Decode)?,
        ),
        18 => ScalarLiteral::Unit(Unit),
        _ => return Err(SchemaContractError::Decode),
    };
    value.validate()?;
    Ok(value)
}

fn encode_principal(writer: &mut WireWriter, value: Principal) -> Result<(), SchemaContractError> {
    writer.push_bytes(
        value
            .stored_bytes()
            .map_err(|_| SchemaContractError::Encode)?,
    )
}

fn decode_principal(reader: &mut WireReader<'_>) -> Result<Principal, SchemaContractError> {
    Principal::try_from_bytes(reader.read_bytes(Principal::MAX_LENGTH_IN_BYTES as usize)?)
        .map_err(|_| SchemaContractError::Decode)
}

pub(super) fn encode_expression(
    writer: &mut WireWriter,
    value: &SourceCheckExpr,
) -> Result<(), SchemaContractError> {
    writer.push_len(value.instructions().len())?;
    for instruction in value.instructions() {
        match instruction {
            SourceCheckInstruction::Field(field) => {
                writer.push_u8(0)?;
                encode_source_key(writer, field.as_str())?;
            }
            SourceCheckInstruction::Literal(literal) => {
                writer.push_u8(1)?;
                encode_literal(writer, literal)?;
            }
            SourceCheckInstruction::Equal => writer.push_u8(2)?,
            SourceCheckInstruction::NotEqual => writer.push_u8(3)?,
            SourceCheckInstruction::LessThan => writer.push_u8(4)?,
            SourceCheckInstruction::LessThanOrEqual => writer.push_u8(5)?,
            SourceCheckInstruction::GreaterThan => writer.push_u8(6)?,
            SourceCheckInstruction::GreaterThanOrEqual => writer.push_u8(7)?,
            SourceCheckInstruction::And => writer.push_u8(8)?,
            SourceCheckInstruction::Or => writer.push_u8(9)?,
            SourceCheckInstruction::Not => writer.push_u8(10)?,
            SourceCheckInstruction::IsNull => writer.push_u8(11)?,
            SourceCheckInstruction::IsNotNull => writer.push_u8(12)?,
            SourceCheckInstruction::Length => writer.push_u8(13)?,
        }
    }
    Ok(())
}

pub(super) fn decode_expression(
    reader: &mut WireReader<'_>,
) -> Result<SourceCheckExpr, SchemaContractError> {
    let len = reader.read_count("source check instructions", MAX_SOURCE_CHECK_INSTRUCTIONS)?;
    let mut instructions = Vec::new();
    instructions
        .try_reserve_exact(len)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..len {
        instructions.push(match reader.read_u8()? {
            0 => SourceCheckInstruction::Field(decode_field_key(reader)?),
            1 => SourceCheckInstruction::Literal(decode_literal(reader)?),
            2 => SourceCheckInstruction::Equal,
            3 => SourceCheckInstruction::NotEqual,
            4 => SourceCheckInstruction::LessThan,
            5 => SourceCheckInstruction::LessThanOrEqual,
            6 => SourceCheckInstruction::GreaterThan,
            7 => SourceCheckInstruction::GreaterThanOrEqual,
            8 => SourceCheckInstruction::And,
            9 => SourceCheckInstruction::Or,
            10 => SourceCheckInstruction::Not,
            11 => SourceCheckInstruction::IsNull,
            12 => SourceCheckInstruction::IsNotNull,
            13 => SourceCheckInstruction::Length,
            _ => return Err(SchemaContractError::Decode),
        });
    }
    SourceCheckExpr::try_new(instructions)
}

pub(super) fn encode_rule_operation(
    writer: &mut WireWriter,
    value: &SourceRuleOperation,
) -> Result<(), SchemaContractError> {
    value.validate()?;
    match value {
        SourceRuleOperation::LengthRangeInclusive { min, max } => {
            writer.push_u8(0)?;
            writer.push_u64(*min)?;
            writer.push_u64(*max)?;
        }
        SourceRuleOperation::MultipleOf { divisor } => {
            writer.push_u8(1)?;
            encode_literal(writer, divisor)?;
        }
        SourceRuleOperation::NumericMaximumInclusive { value } => {
            writer.push_u8(2)?;
            encode_literal(writer, value)?;
        }
        SourceRuleOperation::NumericMinimumInclusive { value } => {
            writer.push_u8(3)?;
            encode_literal(writer, value)?;
        }
        SourceRuleOperation::NumericRangeInclusive { min, max } => {
            writer.push_u8(4)?;
            encode_literal(writer, min)?;
            encode_literal(writer, max)?;
        }
    }
    Ok(())
}

pub(super) fn decode_rule_operation(
    reader: &mut WireReader<'_>,
) -> Result<SourceRuleOperation, SchemaContractError> {
    let value = match reader.read_u8()? {
        0 => SourceRuleOperation::LengthRangeInclusive {
            min: reader.read_u64()?,
            max: reader.read_u64()?,
        },
        1 => SourceRuleOperation::MultipleOf {
            divisor: decode_literal(reader)?,
        },
        2 => SourceRuleOperation::NumericMaximumInclusive {
            value: decode_literal(reader)?,
        },
        3 => SourceRuleOperation::NumericMinimumInclusive {
            value: decode_literal(reader)?,
        },
        4 => SourceRuleOperation::NumericRangeInclusive {
            min: decode_literal(reader)?,
            max: decode_literal(reader)?,
        },
        _ => return Err(SchemaContractError::Decode),
    };
    value.validate()?;
    Ok(value)
}
