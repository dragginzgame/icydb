//! Bounded current-form proposal transport.

use crate::{
    MAX_SCHEMA_FRAGMENT_BYTES, MAX_SCHEMA_PROPOSAL_BYTES, SchemaContractError, SchemaFragment,
    SchemaProposal,
};

/// Encode one canonical schema fragment.
///
/// # Errors
///
/// Returns a typed contract or size error when the fragment is malformed or
/// exceeds the frozen transport bound.
pub fn encode_schema_fragment(fragment: &SchemaFragment) -> Result<Vec<u8>, SchemaContractError> {
    fragment.validate()?;
    let bytes = candid::encode_one(fragment).map_err(|_| SchemaContractError::Encode)?;
    enforce_encoded_bound(bytes, MAX_SCHEMA_FRAGMENT_BYTES)
}

/// Decode one bounded current-form schema fragment.
///
/// # Errors
///
/// Returns a typed size, decoding, canonicalization, or nested-contract error.
pub fn decode_schema_fragment(bytes: &[u8]) -> Result<SchemaFragment, SchemaContractError> {
    ensure_input_bound(bytes, MAX_SCHEMA_FRAGMENT_BYTES)?;
    let fragment =
        candid::decode_one::<SchemaFragment>(bytes).map_err(|_| SchemaContractError::Decode)?;
    fragment.validate()?;
    let canonical = encode_schema_fragment(&fragment)?;
    if canonical != bytes {
        return Err(SchemaContractError::NonCanonical);
    }
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
    let bytes = candid::encode_one(proposal).map_err(|_| SchemaContractError::Encode)?;
    enforce_encoded_bound(bytes, MAX_SCHEMA_PROPOSAL_BYTES)
}

/// Decode one bounded current-form database-scoped proposal.
///
/// # Errors
///
/// Returns a typed size, decoding, version, canonicalization, or nested
/// contract error. Obsolete forms are never translated.
pub fn decode_schema_proposal(bytes: &[u8]) -> Result<SchemaProposal, SchemaContractError> {
    ensure_input_bound(bytes, MAX_SCHEMA_PROPOSAL_BYTES)?;
    let proposal =
        candid::decode_one::<SchemaProposal>(bytes).map_err(|_| SchemaContractError::Decode)?;
    proposal.validate_current()?;
    let canonical = encode_schema_proposal(&proposal)?;
    if canonical != bytes {
        return Err(SchemaContractError::NonCanonical);
    }
    Ok(proposal)
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

fn enforce_encoded_bound(bytes: Vec<u8>, max: usize) -> Result<Vec<u8>, SchemaContractError> {
    ensure_input_bound(&bytes, max)?;
    Ok(bytes)
}
