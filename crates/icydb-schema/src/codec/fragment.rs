use crate::{
    ConstraintFragment, ConstraintFragmentKind, DeclaredEntityVersion, EntityFragment,
    EnumTypeFragment, EnumVariantFragment, FieldFragment, FieldInsertPolicy, FieldManagementPolicy,
    IndexFragment, IndexKeyFragment, MAX_FRAGMENT_CONSTRAINTS, MAX_FRAGMENT_ENTITIES,
    MAX_FRAGMENT_FIELDS, MAX_FRAGMENT_INDEXES, MAX_FRAGMENT_RELATIONS, MAX_FRAGMENT_TYPES,
    MAX_SCHEMA_FRAGMENT_BYTES, NamedTypeFragment, RecordFieldFragment, RecordTypeFragment,
    RelationDeleteAction, RelationFragment, SchemaContractError, SchemaFragment,
    TargetedRuleFragment, TupleElementFragment,
};

use super::{
    value::{
        decode_entity_key, decode_expression, decode_field_key, decode_field_type, decode_rule_key,
        decode_rule_operation, decode_schema_name, decode_type_key, encode_expression,
        encode_field_type, encode_rule_operation, encode_schema_name, encode_source_key,
    },
    wire::{WireReader, WireWriter},
};

pub(super) fn encode_fragment_payload(
    writer: &mut WireWriter,
    fragment: &SchemaFragment,
) -> Result<(), SchemaContractError> {
    writer.push_len(fragment.entities().len())?;
    for entity in fragment.entities() {
        encode_entity(writer, entity)?;
    }
    writer.push_len(fragment.types().len())?;
    for r#type in fragment.types() {
        encode_named_type(writer, r#type)?;
    }
    Ok(())
}

pub(super) fn decode_fragment_payload(
    reader: &mut WireReader<'_>,
) -> Result<SchemaFragment, SchemaContractError> {
    let entity_count = reader.read_count("fragment entities", MAX_FRAGMENT_ENTITIES)?;
    let mut entities = Vec::new();
    entities
        .try_reserve_exact(entity_count)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..entity_count {
        entities.push(decode_entity(reader)?);
    }

    let type_count = reader.read_count("fragment types", MAX_FRAGMENT_TYPES)?;
    let mut types = Vec::new();
    types
        .try_reserve_exact(type_count)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..type_count {
        types.push(decode_named_type(reader)?);
    }
    SchemaFragment::try_new(entities, types)
}

pub(super) fn encode_entity(
    writer: &mut WireWriter,
    entity: &EntityFragment,
) -> Result<(), SchemaContractError> {
    encode_schema_name(writer, entity.name())?;
    writer.push_u32(entity.version().get())?;

    writer.push_len(entity.fields().len())?;
    for field in entity.fields() {
        encode_field(writer, field)?;
    }
    encode_field_keys(writer, entity.primary_key())?;

    writer.push_len(entity.indexes().len())?;
    for index in entity.indexes() {
        encode_index(writer, index)?;
    }
    writer.push_len(entity.relations().len())?;
    for relation in entity.relations() {
        encode_relation(writer, relation)?;
    }
    writer.push_len(entity.constraints().len())?;
    for constraint in entity.constraints() {
        encode_constraint(writer, constraint)?;
    }
    Ok(())
}

fn decode_entity(reader: &mut WireReader<'_>) -> Result<EntityFragment, SchemaContractError> {
    let name = decode_schema_name(reader)?;
    let version = DeclaredEntityVersion::try_new(reader.read_u32()?)?;
    let fields = decode_fields(reader)?;
    let primary_key = decode_field_keys(reader, "primary key fields", MAX_FRAGMENT_FIELDS)?;
    let indexes = decode_indexes(reader)?;
    let relations = decode_relations(reader)?;
    let constraints = decode_constraints(reader)?;
    EntityFragment::try_new(
        name,
        version,
        fields,
        primary_key,
        indexes,
        relations,
        constraints,
    )
}

pub(super) fn encode_field(
    writer: &mut WireWriter,
    field: &FieldFragment,
) -> Result<(), SchemaContractError> {
    encode_schema_name(writer, field.name())?;
    encode_field_type(writer, field.field_type())?;
    writer.push_bool(field.nullable())?;
    match field.insert_policy() {
        FieldInsertPolicy::Required => writer.push_u8(0)?,
        FieldInsertPolicy::Nullable => writer.push_u8(1)?,
        FieldInsertPolicy::Default(literal) => {
            writer.push_u8(2)?;
            super::value::encode_literal(writer, literal)?;
        }
        FieldInsertPolicy::Generated => writer.push_u8(3)?,
    }
    match field.management() {
        None => writer.push_u8(0)?,
        Some(FieldManagementPolicy::CreatedAt) => writer.push_u8(1)?,
        Some(FieldManagementPolicy::UpdatedAt) => writer.push_u8(2)?,
    }
    Ok(())
}

fn decode_fields(reader: &mut WireReader<'_>) -> Result<Vec<FieldFragment>, SchemaContractError> {
    let len = reader.read_count("entity fields", MAX_FRAGMENT_FIELDS)?;
    let mut fields = Vec::new();
    fields
        .try_reserve_exact(len)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..len {
        let name = decode_schema_name(reader)?;
        let field_type = decode_field_type(reader)?;
        let nullable = reader.read_bool()?;
        let insert_policy = match reader.read_u8()? {
            0 => FieldInsertPolicy::Required,
            1 => FieldInsertPolicy::Nullable,
            2 => FieldInsertPolicy::Default(super::value::decode_literal(reader)?),
            3 => FieldInsertPolicy::Generated,
            _ => return Err(SchemaContractError::Decode),
        };
        let management = match reader.read_u8()? {
            0 => None,
            1 => Some(FieldManagementPolicy::CreatedAt),
            2 => Some(FieldManagementPolicy::UpdatedAt),
            _ => return Err(SchemaContractError::Decode),
        };
        fields.push(FieldFragment::new(
            name,
            field_type,
            nullable,
            insert_policy,
            management,
        ));
    }
    Ok(fields)
}

fn encode_field_keys(
    writer: &mut WireWriter,
    fields: &[crate::FieldSourceKey],
) -> Result<(), SchemaContractError> {
    writer.push_len(fields.len())?;
    for field in fields {
        encode_source_key(writer, field.as_str())?;
    }
    Ok(())
}

fn decode_field_keys(
    reader: &mut WireReader<'_>,
    kind: &'static str,
    max: usize,
) -> Result<Vec<crate::FieldSourceKey>, SchemaContractError> {
    let len = reader.read_count(kind, max)?;
    let mut fields = Vec::new();
    fields
        .try_reserve_exact(len)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..len {
        fields.push(decode_field_key(reader)?);
    }
    Ok(fields)
}

fn encode_index(writer: &mut WireWriter, index: &IndexFragment) -> Result<(), SchemaContractError> {
    encode_schema_name(writer, index.name())?;
    writer.push_len(index.key().len())?;
    for component in index.key() {
        let tag = match component {
            IndexKeyFragment::Field(_) => 0,
            IndexKeyFragment::Lower(_) => 1,
            IndexKeyFragment::Upper(_) => 2,
            IndexKeyFragment::Trim(_) => 3,
            IndexKeyFragment::LowerTrim(_) => 4,
            IndexKeyFragment::Date(_) => 5,
            IndexKeyFragment::Year(_) => 6,
            IndexKeyFragment::Month(_) => 7,
            IndexKeyFragment::Day(_) => 8,
        };
        writer.push_u8(tag)?;
        encode_source_key(writer, component.field().as_str())?;
    }
    writer.push_bool(index.unique())?;
    writer.push_bool(index.predicate().is_some())?;
    if let Some(predicate) = index.predicate() {
        encode_expression(writer, predicate)?;
    }
    Ok(())
}

fn decode_indexes(reader: &mut WireReader<'_>) -> Result<Vec<IndexFragment>, SchemaContractError> {
    let len = reader.read_count("entity indexes", MAX_FRAGMENT_INDEXES)?;
    let mut indexes = Vec::new();
    indexes
        .try_reserve_exact(len)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..len {
        let name = decode_schema_name(reader)?;
        let key_len = reader.read_count("index key components", MAX_SCHEMA_FRAGMENT_BYTES)?;
        let mut key = Vec::new();
        key.try_reserve_exact(key_len)
            .map_err(|_| SchemaContractError::Decode)?;
        for _ in 0..key_len {
            let tag = reader.read_u8()?;
            let field = decode_field_key(reader)?;
            key.push(match tag {
                0 => IndexKeyFragment::Field(field),
                1 => IndexKeyFragment::Lower(field),
                2 => IndexKeyFragment::Upper(field),
                3 => IndexKeyFragment::Trim(field),
                4 => IndexKeyFragment::LowerTrim(field),
                5 => IndexKeyFragment::Date(field),
                6 => IndexKeyFragment::Year(field),
                7 => IndexKeyFragment::Month(field),
                8 => IndexKeyFragment::Day(field),
                _ => return Err(SchemaContractError::Decode),
            });
        }
        let unique = reader.read_bool()?;
        let predicate = reader
            .read_bool()?
            .then(|| decode_expression(reader))
            .transpose()?;
        indexes.push(IndexFragment::try_new(name, key, unique, predicate)?);
    }
    Ok(indexes)
}

fn encode_relation(
    writer: &mut WireWriter,
    relation: &RelationFragment,
) -> Result<(), SchemaContractError> {
    encode_schema_name(writer, relation.name())?;
    encode_field_keys(writer, relation.local_fields())?;
    encode_source_key(writer, relation.target_entity().as_str())?;
    encode_field_keys(writer, relation.target_fields())?;
    match relation.on_delete() {
        RelationDeleteAction::Restrict => writer.push_u8(0)?,
    }
    Ok(())
}

fn decode_relations(
    reader: &mut WireReader<'_>,
) -> Result<Vec<RelationFragment>, SchemaContractError> {
    let len = reader.read_count("entity relations", MAX_FRAGMENT_RELATIONS)?;
    let mut relations = Vec::new();
    relations
        .try_reserve_exact(len)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..len {
        let name = decode_schema_name(reader)?;
        let local_fields = decode_field_keys(reader, "relation local fields", MAX_FRAGMENT_FIELDS)?;
        let target_entity = decode_entity_key(reader)?;
        let target_fields =
            decode_field_keys(reader, "relation target fields", MAX_FRAGMENT_FIELDS)?;
        let on_delete = match reader.read_u8()? {
            0 => RelationDeleteAction::Restrict,
            _ => return Err(SchemaContractError::Decode),
        };
        relations.push(RelationFragment::try_new(
            name,
            local_fields,
            target_entity,
            target_fields,
            on_delete,
        )?);
    }
    Ok(relations)
}

fn encode_constraint(
    writer: &mut WireWriter,
    constraint: &ConstraintFragment,
) -> Result<(), SchemaContractError> {
    match constraint.kind() {
        ConstraintFragmentKind::Check(expression) => {
            writer.push_u8(0)?;
            encode_schema_name(writer, constraint.name())?;
            encode_expression(writer, expression)?;
        }
        ConstraintFragmentKind::TargetedRule(rule) => {
            writer.push_u8(1)?;
            encode_source_key(writer, rule.root().as_str())?;
            encode_source_key(writer, rule.target_type().as_str())?;
            encode_source_key(writer, rule.rule().as_str())?;
            encode_rule_operation(writer, rule.operation())?;
        }
    }
    Ok(())
}

fn decode_constraints(
    reader: &mut WireReader<'_>,
) -> Result<Vec<ConstraintFragment>, SchemaContractError> {
    let len = reader.read_count("entity constraints", MAX_FRAGMENT_CONSTRAINTS)?;
    let mut constraints = Vec::new();
    constraints
        .try_reserve_exact(len)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..len {
        constraints.push(match reader.read_u8()? {
            0 => ConstraintFragment::check(decode_schema_name(reader)?, decode_expression(reader)?),
            1 => {
                let root = decode_field_key(reader)?;
                let target_type = decode_type_key(reader)?;
                let rule = decode_rule_key(reader)?;
                let rule_name = crate::SchemaName::try_new(rule.as_str())?;
                ConstraintFragment::targeted_rule(TargetedRuleFragment::new(
                    root,
                    target_type,
                    rule_name,
                    decode_rule_operation(reader)?,
                ))
            }
            _ => return Err(SchemaContractError::Decode),
        });
    }
    Ok(constraints)
}

pub(super) fn encode_named_type(
    writer: &mut WireWriter,
    value: &NamedTypeFragment,
) -> Result<(), SchemaContractError> {
    match value {
        NamedTypeFragment::Record(record) => {
            writer.push_u8(0)?;
            encode_schema_name(writer, record.name())?;
            writer.push_len(record.fields().len())?;
            for field in record.fields() {
                encode_schema_name(writer, field.name())?;
                encode_field_type(writer, field.field_type())?;
                writer.push_bool(field.nullable())?;
            }
        }
        NamedTypeFragment::Enum(r#enum) => {
            writer.push_u8(1)?;
            encode_schema_name(writer, r#enum.name())?;
            writer.push_len(r#enum.variants().len())?;
            for variant in r#enum.variants() {
                encode_schema_name(writer, variant.name())?;
                writer.push_bool(variant.payload().is_some())?;
                if let Some(payload) = variant.payload() {
                    encode_field_type(writer, payload)?;
                }
            }
        }
        NamedTypeFragment::Newtype { name, inner, .. } => {
            writer.push_u8(2)?;
            encode_schema_name(writer, name)?;
            encode_field_type(writer, inner)?;
        }
        NamedTypeFragment::List { name, item, .. } => {
            writer.push_u8(3)?;
            encode_schema_name(writer, name)?;
            encode_field_type(writer, item)?;
        }
        NamedTypeFragment::Set { name, item, .. } => {
            writer.push_u8(4)?;
            encode_schema_name(writer, name)?;
            encode_field_type(writer, item)?;
        }
        NamedTypeFragment::Map {
            name, key, value, ..
        } => {
            writer.push_u8(5)?;
            encode_schema_name(writer, name)?;
            encode_field_type(writer, key)?;
            encode_field_type(writer, value)?;
        }
        NamedTypeFragment::Tuple { name, members, .. } => {
            writer.push_u8(6)?;
            encode_schema_name(writer, name)?;
            writer.push_len(members.len())?;
            for member in members {
                encode_field_type(writer, member.field_type())?;
                writer.push_bool(member.nullable())?;
            }
        }
    }
    Ok(())
}

fn decode_named_type(
    reader: &mut WireReader<'_>,
) -> Result<NamedTypeFragment, SchemaContractError> {
    match reader.read_u8()? {
        0 => {
            let name = decode_schema_name(reader)?;
            let len = reader.read_count("record fields", MAX_FRAGMENT_FIELDS)?;
            let mut fields = Vec::new();
            fields
                .try_reserve_exact(len)
                .map_err(|_| SchemaContractError::Decode)?;
            for _ in 0..len {
                fields.push(RecordFieldFragment::new(
                    decode_schema_name(reader)?,
                    decode_field_type(reader)?,
                    reader.read_bool()?,
                ));
            }
            Ok(NamedTypeFragment::Record(RecordTypeFragment::try_new(
                name, fields,
            )?))
        }
        1 => {
            let name = decode_schema_name(reader)?;
            let len = reader.read_count("enum variants", MAX_FRAGMENT_FIELDS)?;
            let mut variants = Vec::new();
            variants
                .try_reserve_exact(len)
                .map_err(|_| SchemaContractError::Decode)?;
            for _ in 0..len {
                let variant_name = decode_schema_name(reader)?;
                variants.push(if reader.read_bool()? {
                    EnumVariantFragment::with_payload(variant_name, decode_field_type(reader)?)
                } else {
                    EnumVariantFragment::new(variant_name)
                });
            }
            Ok(NamedTypeFragment::Enum(EnumTypeFragment::try_new(
                name, variants,
            )?))
        }
        2 => Ok(NamedTypeFragment::newtype(
            decode_schema_name(reader)?,
            decode_field_type(reader)?,
        )),
        3 => Ok(NamedTypeFragment::list(
            decode_schema_name(reader)?,
            decode_field_type(reader)?,
        )),
        4 => Ok(NamedTypeFragment::set(
            decode_schema_name(reader)?,
            decode_field_type(reader)?,
        )),
        5 => Ok(NamedTypeFragment::map(
            decode_schema_name(reader)?,
            decode_field_type(reader)?,
            decode_field_type(reader)?,
        )),
        6 => {
            let name = decode_schema_name(reader)?;
            let len = reader.read_count("tuple members", MAX_FRAGMENT_FIELDS)?;
            let mut members = Vec::new();
            members
                .try_reserve_exact(len)
                .map_err(|_| SchemaContractError::Decode)?;
            for _ in 0..len {
                members.push(TupleElementFragment::new(
                    decode_field_type(reader)?,
                    reader.read_bool()?,
                ));
            }
            Ok(NamedTypeFragment::tuple(name, members))
        }
        _ => Err(SchemaContractError::Decode),
    }
}
