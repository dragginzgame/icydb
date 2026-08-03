use crate::{
    DeclaredEntityVersion, EntityMigration, MAX_SCHEMA_MIGRATION_ENTITIES,
    MAX_SCHEMA_MIGRATION_PLAN_BYTES, MAX_SCHEMA_MIGRATION_RENAMES, MAX_SCHEMA_MIGRATION_TRANSFORMS,
    SchemaContractError, SchemaMigrationPlan, SchemaMigrationRename, SchemaMigrationTransform,
};

use super::{
    value::{
        decode_constraint_key, decode_entity_key, decode_field_key, decode_literal,
        decode_relation_key, decode_rule_key, decode_scalar_type, decode_type_key, encode_literal,
        encode_scalar_type, encode_source_key,
    },
    wire::{WireReader, WireWriter},
};

pub(super) fn encode_migration_payload(
    writer: &mut WireWriter,
    plan: &SchemaMigrationPlan,
) -> Result<(), SchemaContractError> {
    writer.push_u16(plan.program_version())?;
    encode_transitions(writer, plan.transitions())
}

pub(super) fn decode_migration_payload(
    reader: &mut WireReader<'_>,
) -> Result<SchemaMigrationPlan, SchemaContractError> {
    let program_version = reader.read_u16()?;
    if program_version != crate::migration::MIGRATION_PROGRAM_VERSION_CURRENT {
        return Err(SchemaContractError::UnsupportedMigrationProgramVersion {
            found: program_version,
            supported: crate::migration::MIGRATION_PROGRAM_VERSION_CURRENT,
        });
    }
    SchemaMigrationPlan::try_new(decode_transitions(reader)?)
}

pub(crate) fn encode_migration_transitions_for_digest(
    transitions: &[EntityMigration],
) -> Result<Vec<u8>, SchemaContractError> {
    let mut writer = WireWriter::new(MAX_SCHEMA_MIGRATION_PLAN_BYTES);
    encode_transitions(&mut writer, transitions)?;
    let bytes = writer.finish();
    let encoded_len = bytes
        .len()
        .checked_add(7)
        .ok_or(SchemaContractError::Encode)?;
    if encoded_len > MAX_SCHEMA_MIGRATION_PLAN_BYTES {
        return Err(SchemaContractError::EncodedTooLarge {
            len: encoded_len,
            max: MAX_SCHEMA_MIGRATION_PLAN_BYTES,
        });
    }
    Ok(bytes)
}

fn encode_transitions(
    writer: &mut WireWriter,
    transitions: &[EntityMigration],
) -> Result<(), SchemaContractError> {
    writer.push_len(transitions.len())?;
    for transition in transitions {
        encode_source_key(writer, transition.entity().as_str())?;
        writer.push_u32(transition.from().get())?;
        writer.push_bool(transition.from_name().is_some())?;
        if let Some(from_name) = transition.from_name() {
            encode_source_key(writer, from_name.as_str())?;
        }
        writer.push_len(transition.renames().len())?;
        for rename in transition.renames() {
            encode_rename(writer, rename)?;
        }
        writer.push_len(transition.transforms().len())?;
        for transform in transition.transforms() {
            encode_transform(writer, transform)?;
        }
    }
    Ok(())
}

fn decode_transitions(
    reader: &mut WireReader<'_>,
) -> Result<Vec<EntityMigration>, SchemaContractError> {
    let len = reader.read_count(
        "migration entity transitions",
        MAX_SCHEMA_MIGRATION_ENTITIES,
    )?;
    let mut transitions = Vec::new();
    transitions
        .try_reserve_exact(len)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..len {
        let entity = decode_entity_key(reader)?;
        let from = DeclaredEntityVersion::try_new(reader.read_u32()?)?;
        let from_name = reader
            .read_bool()?
            .then(|| decode_entity_key(reader))
            .transpose()?;

        let rename_count = reader.read_count("migration renames", MAX_SCHEMA_MIGRATION_RENAMES)?;
        let mut renames = Vec::new();
        renames
            .try_reserve_exact(rename_count)
            .map_err(|_| SchemaContractError::Decode)?;
        for _ in 0..rename_count {
            renames.push(decode_rename(reader)?);
        }

        let transform_count =
            reader.read_count("migration transforms", MAX_SCHEMA_MIGRATION_TRANSFORMS)?;
        let mut transforms = Vec::new();
        transforms
            .try_reserve_exact(transform_count)
            .map_err(|_| SchemaContractError::Decode)?;
        for _ in 0..transform_count {
            transforms.push(decode_transform(reader)?);
        }
        transitions.push(EntityMigration::try_new(
            entity, from, from_name, renames, transforms,
        )?);
    }
    Ok(transitions)
}

fn encode_rename(
    writer: &mut WireWriter,
    rename: &SchemaMigrationRename,
) -> Result<(), SchemaContractError> {
    match rename {
        SchemaMigrationRename::Field { from, to } => {
            writer.push_u8(0)?;
            encode_source_key(writer, from.as_str())?;
            encode_source_key(writer, to.as_str())?;
        }
        SchemaMigrationRename::NamedType { from, to } => {
            writer.push_u8(1)?;
            encode_source_key(writer, from.as_str())?;
            encode_source_key(writer, to.as_str())?;
        }
        SchemaMigrationRename::EnumVariant {
            named_type,
            from,
            to,
        } => {
            writer.push_u8(2)?;
            encode_source_key(writer, named_type.as_str())?;
            encode_source_key(writer, from.as_str())?;
            encode_source_key(writer, to.as_str())?;
        }
        SchemaMigrationRename::RecordField {
            named_type,
            from,
            to,
        } => {
            writer.push_u8(3)?;
            encode_source_key(writer, named_type.as_str())?;
            encode_source_key(writer, from.as_str())?;
            encode_source_key(writer, to.as_str())?;
        }
        SchemaMigrationRename::Relation { from, to } => {
            writer.push_u8(4)?;
            encode_source_key(writer, from.as_str())?;
            encode_source_key(writer, to.as_str())?;
        }
        SchemaMigrationRename::Constraint { from, to } => {
            writer.push_u8(5)?;
            encode_source_key(writer, from.as_str())?;
            encode_source_key(writer, to.as_str())?;
        }
        SchemaMigrationRename::Rule {
            named_type,
            from,
            to,
        } => {
            writer.push_u8(6)?;
            encode_source_key(writer, named_type.as_str())?;
            encode_source_key(writer, from.as_str())?;
            encode_source_key(writer, to.as_str())?;
        }
    }
    Ok(())
}

fn decode_rename(
    reader: &mut WireReader<'_>,
) -> Result<SchemaMigrationRename, SchemaContractError> {
    match reader.read_u8()? {
        0 => Ok(SchemaMigrationRename::Field {
            from: decode_field_key(reader)?,
            to: decode_field_key(reader)?,
        }),
        1 => Ok(SchemaMigrationRename::NamedType {
            from: decode_type_key(reader)?,
            to: decode_type_key(reader)?,
        }),
        2 => Ok(SchemaMigrationRename::EnumVariant {
            named_type: decode_type_key(reader)?,
            from: decode_type_key(reader)?,
            to: decode_type_key(reader)?,
        }),
        3 => Ok(SchemaMigrationRename::RecordField {
            named_type: decode_type_key(reader)?,
            from: decode_field_key(reader)?,
            to: decode_field_key(reader)?,
        }),
        4 => Ok(SchemaMigrationRename::Relation {
            from: decode_relation_key(reader)?,
            to: decode_relation_key(reader)?,
        }),
        5 => Ok(SchemaMigrationRename::Constraint {
            from: decode_constraint_key(reader)?,
            to: decode_constraint_key(reader)?,
        }),
        6 => Ok(SchemaMigrationRename::Rule {
            named_type: decode_type_key(reader)?,
            from: decode_rule_key(reader)?,
            to: decode_rule_key(reader)?,
        }),
        _ => Err(SchemaContractError::Decode),
    }
}

fn encode_transform(
    writer: &mut WireWriter,
    transform: &SchemaMigrationTransform,
) -> Result<(), SchemaContractError> {
    match transform {
        SchemaMigrationTransform::Fill { to, literal } => {
            writer.push_u8(0)?;
            encode_source_key(writer, to.as_str())?;
            encode_literal(writer, literal)?;
        }
        SchemaMigrationTransform::Copy { from, to } => {
            writer.push_u8(1)?;
            encode_source_key(writer, from.as_str())?;
            encode_source_key(writer, to.as_str())?;
        }
        SchemaMigrationTransform::CheckedCast { from, to, target } => {
            writer.push_u8(2)?;
            encode_source_key(writer, from.as_str())?;
            encode_source_key(writer, to.as_str())?;
            encode_scalar_type(writer, *target)?;
        }
        SchemaMigrationTransform::Coalesce { from, to, literal } => {
            writer.push_u8(3)?;
            encode_source_key(writer, from.as_str())?;
            encode_source_key(writer, to.as_str())?;
            encode_literal(writer, literal)?;
        }
    }
    Ok(())
}

fn decode_transform(
    reader: &mut WireReader<'_>,
) -> Result<SchemaMigrationTransform, SchemaContractError> {
    match reader.read_u8()? {
        0 => Ok(SchemaMigrationTransform::Fill {
            to: decode_field_key(reader)?,
            literal: decode_literal(reader)?,
        }),
        1 => Ok(SchemaMigrationTransform::Copy {
            from: decode_field_key(reader)?,
            to: decode_field_key(reader)?,
        }),
        2 => Ok(SchemaMigrationTransform::CheckedCast {
            from: decode_field_key(reader)?,
            to: decode_field_key(reader)?,
            target: decode_scalar_type(reader)?,
        }),
        3 => Ok(SchemaMigrationTransform::Coalesce {
            from: decode_field_key(reader)?,
            to: decode_field_key(reader)?,
            literal: decode_literal(reader)?,
        }),
        _ => Err(SchemaContractError::Decode),
    }
}
