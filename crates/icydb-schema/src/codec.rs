//! Bounded current-form schema authoring transport.
//!
//! These bytes are internal build/application artifacts, not Candid endpoint
//! payloads. Each top-level form has one tagged V1 representation. Pre-1.0
//! obsolete representations are rejected rather than translated.

mod fragment;
mod migration;
mod proposal;
mod value;
mod wire;

use crate::{
    EntityFragment, EntitySourceKey, FieldFragment, FieldSourceKey, MAX_SCHEMA_FRAGMENT_BYTES,
    MAX_SCHEMA_MIGRATION_PLAN_BYTES, MAX_SCHEMA_PROPOSAL_BYTES, NamedTypeFragment,
    SchemaContractError, SchemaFragment, SchemaMigrationPlan, SchemaProposal,
};

use self::{
    fragment::{
        decode_fragment_payload, encode_entity, encode_field, encode_fragment_payload,
        encode_named_type,
    },
    migration::{decode_migration_payload, encode_migration_payload},
    proposal::{decode_proposal_payload, encode_proposal_payload},
    wire::{WireReader, WireWriter},
};

const FRAGMENT_HEADER: &[u8; 5] = b"ICYF\x01";
const PROPOSAL_HEADER: &[u8; 5] = b"ICYP\x01";
const MIGRATION_HEADER: &[u8; 5] = b"ICYM\x01";
const SOURCE_MEANING_HEADER: &[u8; 5] = b"ICYS\x01";

/// Encode one canonical schema fragment.
///
/// # Errors
///
/// Returns a typed contract or size error when the fragment is malformed or
/// exceeds the frozen transport bound.
pub fn encode_schema_fragment(fragment: &SchemaFragment) -> Result<Vec<u8>, SchemaContractError> {
    fragment.validate()?;
    let mut writer = WireWriter::new(MAX_SCHEMA_FRAGMENT_BYTES);
    writer.push_raw(FRAGMENT_HEADER)?;
    encode_fragment_payload(&mut writer, fragment)?;
    Ok(writer.finish())
}

/// Decode one bounded current-form schema fragment.
///
/// # Errors
///
/// Returns a typed size, decoding, canonicalization, or nested-contract error.
pub fn decode_schema_fragment(bytes: &[u8]) -> Result<SchemaFragment, SchemaContractError> {
    ensure_input_bound(bytes, MAX_SCHEMA_FRAGMENT_BYTES)?;
    let mut reader = WireReader::new(bytes);
    reader.expect_raw(FRAGMENT_HEADER)?;
    let fragment = decode_fragment_payload(&mut reader)?;
    reader.finish()?;
    fragment.validate()?;
    ensure_canonical(bytes, encode_schema_fragment(&fragment)?)?;
    Ok(fragment)
}

/// Encode one canonical database-scoped schema proposal.
///
/// # Errors
///
/// Returns a typed contract or size error when the proposal is malformed or
/// exceeds the frozen transport bound.
pub fn encode_schema_proposal(proposal: &SchemaProposal) -> Result<Vec<u8>, SchemaContractError> {
    proposal.validate_current()?;
    let mut writer = WireWriter::new(MAX_SCHEMA_PROPOSAL_BYTES);
    writer.push_raw(PROPOSAL_HEADER)?;
    encode_proposal_payload(&mut writer, proposal)?;
    Ok(writer.finish())
}

/// Decode one bounded current-form database-scoped proposal.
///
/// # Errors
///
/// Returns a typed size, decoding, version, canonicalization, or nested
/// contract error. Obsolete forms are never translated.
pub fn decode_schema_proposal(bytes: &[u8]) -> Result<SchemaProposal, SchemaContractError> {
    ensure_input_bound(bytes, MAX_SCHEMA_PROPOSAL_BYTES)?;
    let mut reader = WireReader::new(bytes);
    reader.expect_raw(PROPOSAL_HEADER)?;
    let proposal = decode_proposal_payload(&mut reader)?;
    reader.finish()?;
    proposal.validate_current()?;
    ensure_canonical(bytes, encode_schema_proposal(&proposal)?)?;
    Ok(proposal)
}

/// Encode one bounded canonical migration plan.
///
/// # Errors
///
/// Returns a typed validation, encoding, or size error.
pub fn encode_schema_migration_plan(
    plan: &SchemaMigrationPlan,
) -> Result<Vec<u8>, SchemaContractError> {
    plan.validate()?;
    let mut writer = WireWriter::new(MAX_SCHEMA_MIGRATION_PLAN_BYTES);
    writer.push_raw(MIGRATION_HEADER)?;
    encode_migration_payload(&mut writer, plan)?;
    Ok(writer.finish())
}

/// Decode one bounded canonical migration plan.
///
/// # Errors
///
/// Returns a typed size, decoding, version, validation, or canonicalization
/// error. Obsolete forms are never translated.
pub fn decode_schema_migration_plan(
    bytes: &[u8],
) -> Result<SchemaMigrationPlan, SchemaContractError> {
    ensure_input_bound(bytes, MAX_SCHEMA_MIGRATION_PLAN_BYTES)?;
    let mut reader = WireReader::new(bytes);
    reader.expect_raw(MIGRATION_HEADER)?;
    let plan = decode_migration_payload(&mut reader)?;
    reader.finish()?;
    plan.validate()?;
    ensure_canonical(bytes, encode_schema_migration_plan(&plan)?)?;
    Ok(plan)
}

pub(crate) use migration::encode_migration_transitions_for_digest;

pub(crate) fn encode_entity_source_meaning(
    entity: &EntityFragment,
    relation_targets: &[(EntitySourceKey, Vec<(FieldSourceKey, FieldFragment)>)],
    reachable_types: &[NamedTypeFragment],
) -> Result<Vec<u8>, SchemaContractError> {
    let mut writer = WireWriter::new(MAX_SCHEMA_PROPOSAL_BYTES);
    writer.push_raw(SOURCE_MEANING_HEADER)?;
    encode_entity(&mut writer, entity)?;
    writer.push_len(relation_targets.len())?;
    for (entity, fields) in relation_targets {
        value::encode_source_key(&mut writer, entity.as_str())?;
        writer.push_len(fields.len())?;
        for (_, field) in fields {
            encode_field(&mut writer, field)?;
        }
    }
    writer.push_len(reachable_types.len())?;
    for definition in reachable_types {
        encode_named_type(&mut writer, definition)?;
    }
    Ok(writer.finish())
}

const fn ensure_input_bound(bytes: &[u8], max: usize) -> Result<(), SchemaContractError> {
    if bytes.len() > max {
        return Err(SchemaContractError::EncodedTooLarge {
            len: bytes.len(),
            max,
        });
    }
    Ok(())
}

fn ensure_canonical(bytes: &[u8], canonical: Vec<u8>) -> Result<(), SchemaContractError> {
    if canonical == bytes {
        Ok(())
    } else {
        Err(SchemaContractError::NonCanonical)
    }
}
