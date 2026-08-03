use crate::{
    EntityStoreAssignment, ExpectedAcceptedHead, ExpectedSchemaFingerprint, MAX_SCHEMA_ASSIGNMENTS,
    MAX_SCHEMA_CAPABILITIES, MAX_SCHEMA_PROPOSAL_FRAGMENTS, MAX_SCHEMA_REMOVALS,
    MAX_SCHEMA_SUBMISSION_KEY_BYTES, ProposalContractVersion, SchemaCapability,
    SchemaContractError, SchemaProposal, SchemaRemoval, SchemaSubmissionKey,
    TargetDatabaseIdentity, TargetStoreIdentity,
};

use super::{
    fragment::{decode_fragment_payload, encode_fragment_payload},
    migration::{decode_migration_payload, encode_migration_payload},
    value::{
        decode_constraint_key, decode_entity_key, decode_field_key, decode_index_key,
        decode_relation_key, decode_type_key, encode_source_key,
    },
    wire::{WireReader, WireWriter},
};

pub(super) fn encode_proposal_payload(
    writer: &mut WireWriter,
    proposal: &SchemaProposal,
) -> Result<(), SchemaContractError> {
    writer.push_u16(proposal.version().get())?;
    writer.push_len(proposal.capabilities().len())?;
    for capability in proposal.capabilities() {
        writer.push_u16(capability.get())?;
    }
    writer.push_raw(&proposal.target_database().to_bytes())?;
    writer.push_string(proposal.submission_key().as_str())?;
    match proposal.expected_head() {
        ExpectedAcceptedHead::Empty => writer.push_u8(0)?,
        ExpectedAcceptedHead::Exact {
            revision,
            fingerprint,
        } => {
            writer.push_u8(1)?;
            writer.push_u64(*revision)?;
            writer.push_raw(&fingerprint.to_bytes())?;
        }
    }
    writer.push_len(proposal.fragments().len())?;
    for fragment in proposal.fragments() {
        encode_fragment_payload(writer, fragment)?;
    }
    writer.push_len(proposal.assignments().len())?;
    for assignment in proposal.assignments() {
        encode_source_key(writer, assignment.entity().as_str())?;
        writer.push_raw(&assignment.store().to_bytes())?;
    }
    writer.push_len(proposal.removals().len())?;
    for removal in proposal.removals() {
        encode_removal(writer, removal)?;
    }
    writer.push_bool(proposal.migration().is_some())?;
    if let Some(migration) = proposal.migration() {
        encode_migration_payload(writer, migration)?;
    }
    Ok(())
}

pub(super) fn decode_proposal_payload(
    reader: &mut WireReader<'_>,
) -> Result<SchemaProposal, SchemaContractError> {
    let version = reader.read_u16()?;
    if version != ProposalContractVersion::CURRENT.get() {
        return Err(SchemaContractError::UnsupportedVersion {
            found: version,
            supported: ProposalContractVersion::CURRENT.get(),
        });
    }

    let capability_count = reader.read_count("proposal capabilities", MAX_SCHEMA_CAPABILITIES)?;
    let mut capabilities = Vec::new();
    capabilities
        .try_reserve_exact(capability_count)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..capability_count {
        capabilities.push(SchemaCapability::from_raw(reader.read_u16()?));
    }

    let target_database = TargetDatabaseIdentity::from_bytes(reader.read_array()?);
    let submission_key =
        SchemaSubmissionKey::try_new(reader.read_string(MAX_SCHEMA_SUBMISSION_KEY_BYTES)?)?;
    let expected_head = match reader.read_u8()? {
        0 => ExpectedAcceptedHead::Empty,
        1 => ExpectedAcceptedHead::Exact {
            revision: reader.read_u64()?,
            fingerprint: ExpectedSchemaFingerprint::from_bytes(reader.read_array()?),
        },
        _ => return Err(SchemaContractError::Decode),
    };

    let fragment_count = reader.read_count("proposal fragments", MAX_SCHEMA_PROPOSAL_FRAGMENTS)?;
    let mut fragments = Vec::new();
    fragments
        .try_reserve_exact(fragment_count)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..fragment_count {
        fragments.push(decode_fragment_payload(reader)?);
    }

    let assignment_count = reader.read_count("proposal assignments", MAX_SCHEMA_ASSIGNMENTS)?;
    let mut assignments = Vec::new();
    assignments
        .try_reserve_exact(assignment_count)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..assignment_count {
        assignments.push(EntityStoreAssignment::new(
            decode_entity_key(reader)?,
            TargetStoreIdentity::from_bytes(reader.read_array()?),
        ));
    }

    let removal_count = reader.read_count("proposal removals", MAX_SCHEMA_REMOVALS)?;
    let mut removals = Vec::new();
    removals
        .try_reserve_exact(removal_count)
        .map_err(|_| SchemaContractError::Decode)?;
    for _ in 0..removal_count {
        removals.push(decode_removal(reader)?);
    }

    let migration = reader
        .read_bool()?
        .then(|| decode_migration_payload(reader))
        .transpose()?;
    SchemaProposal::try_compose(
        capabilities,
        target_database,
        submission_key,
        expected_head,
        fragments,
        assignments,
        removals,
        migration,
    )
}

fn encode_removal(
    writer: &mut WireWriter,
    removal: &SchemaRemoval,
) -> Result<(), SchemaContractError> {
    match removal {
        SchemaRemoval::Entity(entity) => {
            writer.push_u8(0)?;
            encode_source_key(writer, entity.as_str())?;
        }
        SchemaRemoval::Field { entity, field } => {
            writer.push_u8(1)?;
            encode_source_key(writer, entity.as_str())?;
            encode_source_key(writer, field.as_str())?;
        }
        SchemaRemoval::Type(r#type) => {
            writer.push_u8(2)?;
            encode_source_key(writer, r#type.as_str())?;
        }
        SchemaRemoval::Constraint { entity, constraint } => {
            writer.push_u8(3)?;
            encode_source_key(writer, entity.as_str())?;
            encode_source_key(writer, constraint.as_str())?;
        }
        SchemaRemoval::Index { entity, index } => {
            writer.push_u8(4)?;
            encode_source_key(writer, entity.as_str())?;
            encode_source_key(writer, index.as_str())?;
        }
        SchemaRemoval::Relation { entity, relation } => {
            writer.push_u8(5)?;
            encode_source_key(writer, entity.as_str())?;
            encode_source_key(writer, relation.as_str())?;
        }
    }
    Ok(())
}

fn decode_removal(reader: &mut WireReader<'_>) -> Result<SchemaRemoval, SchemaContractError> {
    match reader.read_u8()? {
        0 => Ok(SchemaRemoval::Entity(decode_entity_key(reader)?)),
        1 => Ok(SchemaRemoval::Field {
            entity: decode_entity_key(reader)?,
            field: decode_field_key(reader)?,
        }),
        2 => Ok(SchemaRemoval::Type(decode_type_key(reader)?)),
        3 => Ok(SchemaRemoval::Constraint {
            entity: decode_entity_key(reader)?,
            constraint: decode_constraint_key(reader)?,
        }),
        4 => Ok(SchemaRemoval::Index {
            entity: decode_entity_key(reader)?,
            index: decode_index_key(reader)?,
        }),
        5 => Ok(SchemaRemoval::Relation {
            entity: decode_entity_key(reader)?,
            relation: decode_relation_key(reader)?,
        }),
        _ => Err(SchemaContractError::Decode),
    }
}
