//! Canonical, bounded IC Wire schema authoring artifacts.
//!
//! Framing and schema validation belong to IcyDB; serialization and allocation
//! admission belong to IC Wire. These identities are a pre-1.0 hard cut: earlier
//! authoring artifacts and their derived digests must not be reinterpreted.

use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};

use crate::{
    Blob, EntityFragment, EntityMigration, EntitySourceKey, FieldFragment, FieldSourceKey,
    MAX_PROPOSAL_LITERAL_BYTES, MAX_SCHEMA_FRAGMENT_BYTES, MAX_SCHEMA_MIGRATION_PLAN_BYTES,
    MAX_SCHEMA_PROPOSAL_BYTES, NamedTypeFragment, SchemaContractError, SchemaFragment,
    SchemaMigrationPlan, SchemaProposal,
};

const FRAGMENT_HEADER: [u8; 5] = *b"ICWF\x01";
const PROPOSAL_HEADER: [u8; 5] = *b"ICWP\x01";
const MIGRATION_HEADER: [u8; 5] = *b"ICWM\x01";
const SOURCE_MEANING_HEADER: [u8; 5] = *b"ICWS\x01";

/// Encode one canonical schema fragment.
///
/// # Errors
///
/// Returns a typed validation, size or encoding error.
pub fn encode_schema_fragment(fragment: &SchemaFragment) -> Result<Vec<u8>, SchemaContractError> {
    fragment.validate()?;
    encode(FRAGMENT_HEADER, fragment, MAX_SCHEMA_FRAGMENT_BYTES)
}

/// Decode and validate one complete canonical schema fragment.
///
/// # Errors
///
/// Returns a typed size, decoding, canonicalization or schema-contract error.
pub fn decode_schema_fragment(bytes: &[u8]) -> Result<SchemaFragment, SchemaContractError> {
    let fragment: SchemaFragment = decode(FRAGMENT_HEADER, bytes, MAX_SCHEMA_FRAGMENT_BYTES)?;
    fragment.validate()?;
    ensure_canonical(bytes, encode_schema_fragment(&fragment)?)?;
    Ok(fragment)
}

/// Encode one canonical database-scoped schema proposal.
///
/// # Errors
///
/// Returns a typed validation, size or encoding error.
pub fn encode_schema_proposal(proposal: &SchemaProposal) -> Result<Vec<u8>, SchemaContractError> {
    proposal.validate_current()?;
    encode(PROPOSAL_HEADER, proposal, MAX_SCHEMA_PROPOSAL_BYTES)
}

/// Decode and validate one complete canonical schema proposal.
///
/// # Errors
///
/// Returns a typed size, decoding, version, canonicalization or contract error.
pub fn decode_schema_proposal(bytes: &[u8]) -> Result<SchemaProposal, SchemaContractError> {
    let proposal: SchemaProposal = decode(PROPOSAL_HEADER, bytes, MAX_SCHEMA_PROPOSAL_BYTES)?;
    proposal.validate_current()?;
    ensure_canonical(bytes, encode_schema_proposal(&proposal)?)?;
    Ok(proposal)
}

/// Encode one canonical coordinated migration plan.
///
/// # Errors
///
/// Returns a typed validation, size or encoding error.
pub fn encode_schema_migration_plan(
    plan: &SchemaMigrationPlan,
) -> Result<Vec<u8>, SchemaContractError> {
    plan.validate()?;
    encode(MIGRATION_HEADER, plan, MAX_SCHEMA_MIGRATION_PLAN_BYTES)
}

/// Decode and validate one complete canonical migration plan.
///
/// # Errors
///
/// Returns a typed size, decoding, version, canonicalization or contract error.
pub fn decode_schema_migration_plan(
    bytes: &[u8],
) -> Result<SchemaMigrationPlan, SchemaContractError> {
    let plan: SchemaMigrationPlan =
        decode(MIGRATION_HEADER, bytes, MAX_SCHEMA_MIGRATION_PLAN_BYTES)?;
    plan.validate()?;
    ensure_canonical(bytes, encode_schema_migration_plan(&plan)?)?;
    Ok(plan)
}

pub(crate) fn encode_migration_transitions_for_digest(
    transitions: &[EntityMigration],
) -> Result<Vec<u8>, SchemaContractError> {
    // Include the current artifact identity in digest material as well as the
    // separate digest domain, so layouts cannot share an authority accidentally.
    encode(
        MIGRATION_HEADER,
        &(
            crate::migration::MIGRATION_PROGRAM_VERSION_CURRENT,
            transitions,
        ),
        MAX_SCHEMA_MIGRATION_PLAN_BYTES,
    )
}

pub(crate) fn encode_entity_source_meaning(
    entity: &EntityFragment,
    relation_targets: &[(EntitySourceKey, Vec<(FieldSourceKey, FieldFragment)>)],
    reachable_types: &[NamedTypeFragment],
) -> Result<Vec<u8>, SchemaContractError> {
    encode(
        SOURCE_MEANING_HEADER,
        &(entity, relation_targets, reachable_types),
        MAX_SCHEMA_PROPOSAL_BYTES,
    )
}

fn encode<T: Serialize + ?Sized>(
    header: [u8; 5],
    value: &T,
    max: usize,
) -> Result<Vec<u8>, SchemaContractError> {
    let framed = (header, value);
    let len = ic_wire::native::encoded_len(&framed, crate::wire::sizing_limits())
        .map_err(|_| SchemaContractError::Encode)?;
    ensure_input_bound(len, max)?;
    ic_wire::native::encode(&framed, crate::wire::limits(max))
        .map_err(|_| SchemaContractError::Encode)
}

fn decode<T: DeserializeOwned>(
    header: [u8; 5],
    bytes: &[u8],
    max: usize,
) -> Result<T, SchemaContractError> {
    ensure_input_bound(bytes.len(), max)?;
    if !bytes.starts_with(&header) {
        return Err(SchemaContractError::Decode);
    }
    let (_, value): ([u8; 5], T) = ic_wire::native::decode(bytes, crate::wire::limits(max))
        .map_err(|_| SchemaContractError::Decode)?;
    Ok(value)
}

const fn ensure_input_bound(len: usize, max: usize) -> Result<(), SchemaContractError> {
    if len > max {
        Err(SchemaContractError::EncodedTooLarge { len, max })
    } else {
        Ok(())
    }
}

fn ensure_canonical(bytes: &[u8], canonical: Vec<u8>) -> Result<(), SchemaContractError> {
    if canonical == bytes {
        Ok(())
    } else {
        Err(SchemaContractError::NonCanonical)
    }
}

// These adapters retain the existing domain Vec/String/Blob APIs. Wire owns
// length admission and fallible allocation; IcyDB selects each field's bound.
pub(crate) fn deserialize_list<'de, D, T, const N: usize>(
    deserializer: D,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    ic_wire::List::<T, N>::deserialize(deserializer).map(ic_wire::List::into_vec)
}

pub(crate) fn deserialize_literal_text<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    if deserializer.is_human_readable() {
        let value = String::deserialize(deserializer)?;
        if value.len() > MAX_PROPOSAL_LITERAL_BYTES {
            return Err(serde::de::Error::custom("proposal text limit"));
        }
        Ok(value)
    } else {
        ic_wire::Text::<MAX_PROPOSAL_LITERAL_BYTES>::deserialize(deserializer)
            .map(ic_wire::Text::into_string)
    }
}

pub(crate) fn deserialize_literal_blob<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Blob, D::Error> {
    if deserializer.is_human_readable() {
        let value = Blob::deserialize(deserializer)?;
        if value.len() > MAX_PROPOSAL_LITERAL_BYTES {
            return Err(serde::de::Error::custom("proposal blob limit"));
        }
        Ok(value)
    } else {
        ic_wire::Bytes::<MAX_PROPOSAL_LITERAL_BYTES>::deserialize(deserializer)
            .map(|bytes| Blob::from(bytes.into_vec()))
    }
}

// Bound artifact key bytes through Wire, then reuse the key's existing Serde
// constructor. Public keys retain their string hook for RPC/schema reflection.
pub(crate) fn deserialize_key<'de, D, T, const N: usize>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    if d.is_human_readable() {
        return T::deserialize(d);
    }
    let text = ic_wire::Text::<N>::deserialize(d)?.into_string();
    T::deserialize(serde::de::value::StringDeserializer::<D::Error>::new(text))
}

pub(crate) fn deserialize_optional_key<'de, D, T, const N: usize>(
    d: D,
) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    if d.is_human_readable() {
        return Option::<T>::deserialize(d);
    }
    Option::<ic_wire::Text<N>>::deserialize(d)?
        .map(|text| {
            T::deserialize(serde::de::value::StringDeserializer::<D::Error>::new(
                text.into_string(),
            ))
        })
        .transpose()
}

pub(crate) fn deserialize_keys<'de, D, T, const N: usize>(d: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    if d.is_human_readable() {
        return deserialize_list::<D, T, N>(d);
    }
    let keys = ic_wire::List::<ic_wire::Text<{ crate::MAX_SOURCE_KEY_BYTES }>, N>::deserialize(d)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(keys.as_slice().len())
        .map_err(serde::de::Error::custom)?;
    for key in keys.into_vec() {
        values.push(T::deserialize(serde::de::value::StringDeserializer::<
            D::Error,
        >::new(key.into_string()))?);
    }
    Ok(values)
}

// Decimal's canonical text has a finite intrinsic bound. Do not let a generic
// String visitor allocate an artifact-sized body before numeric admission.
pub(crate) fn deserialize_literal_decimal<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<crate::Decimal, D::Error> {
    const MAX_BYTES: usize = 1
        + crate::decimal::DECIMAL_DIGIT_BUFFER_LEN
        + 1
        + crate::decimal::MAX_SUPPORTED_SCALE as usize;
    let text = if deserializer.is_human_readable() {
        let text = String::deserialize(deserializer)?;
        if text.len() > MAX_BYTES {
            return Err(serde::de::Error::custom("decimal text limit"));
        }
        text
    } else {
        ic_wire::Text::<MAX_BYTES>::deserialize(deserializer)?.into_string()
    };
    text.parse().map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifacts_refuse_noncanonical_wire_and_truncation() {
        let fragment = SchemaFragment::try_new(Vec::new(), Vec::new()).unwrap();
        let canonical = encode_schema_fragment(&fragment).unwrap();
        assert_eq!(&canonical[..5], b"ICWF\x01");
        let mut alternate = canonical.clone();
        alternate.splice(5..6, [0x80, 0]);
        assert_eq!(
            decode_schema_fragment(&alternate),
            Err(SchemaContractError::Decode)
        );
        for end in 0..canonical.len() {
            assert!(decode_schema_fragment(&canonical[..end]).is_err());
        }
    }

    #[test]
    fn artifact_decimal_admission_is_bounded_before_numeric_parsing() {
        // ScalarLiteral's Decimal variant carries text, matching the shared
        // atom's Serde form, but storage artifacts admit only its finite domain.
        let wide = crate::wire::encode_one((4_u32, "0".repeat(1_024))).unwrap();
        assert!(crate::wire::decode_one::<crate::ScalarLiteral>(&wide).is_err());
        let invalid = crate::ScalarLiteral::Decimal(crate::Decimal::new_unchecked(
            1,
            crate::decimal::MAX_SUPPORTED_SCALE + 1,
        ));
        assert_eq!(invalid.validate(), Err(SchemaContractError::InvalidLiteral));
    }

    #[test]
    fn artifact_relation_keys_bound_each_name_and_the_complete_list() {
        for count in [crate::MAX_FRAGMENT_FIELDS, crate::MAX_FRAGMENT_FIELDS + 1] {
            let bytes = crate::wire::encode_one((0_u32, vec!["id"; count])).unwrap();
            assert_eq!(
                crate::wire::decode_one::<crate::RelationSourceFragment>(&bytes).is_ok(),
                count == crate::MAX_FRAGMENT_FIELDS
            );
        }
        let bytes =
            crate::wire::encode_one((0_u32, vec!["x".repeat(crate::MAX_SOURCE_KEY_BYTES + 1)]))
                .unwrap();
        assert!(crate::wire::decode_one::<crate::RelationSourceFragment>(&bytes).is_err());
    }

    #[test]
    fn artifacts_admit_only_bounded_collections_and_valid_scalar_tags() {
        let excessive = vec![(); crate::MAX_FRAGMENT_ENTITIES + 1];
        let bytes =
            crate::wire::encode_one((FRAGMENT_HEADER, (&excessive, Vec::<u8>::new()))).unwrap();
        assert_eq!(
            decode_schema_fragment(&bytes),
            Err(SchemaContractError::Decode)
        );
        let invalid = crate::wire::encode_one(u32::MAX).unwrap();
        assert!(
            ic_wire::native::decode::<crate::ScalarLiteral>(&invalid, crate::wire::limits(64))
                .is_err()
        );
        let wide = crate::wire::encode_one("x".repeat(crate::MAX_SOURCE_KEY_BYTES + 1)).unwrap();
        assert!(crate::wire::decode_one::<crate::SchemaName>(&wide).is_err());
    }
}
