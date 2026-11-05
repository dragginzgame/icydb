//! Module: naming
//! Responsibility: canonical schema naming and engine-reserved authoring namespace.
//! Does not own: accepted identifier allocation or persisted catalog validation.
//! Boundary: gives proposal generation and accepted-runtime identity one exact
//! bounded index-name construction contract.

use crate::MAX_SCHEMA_NAME_BYTES;
use sha2::{Digest, Sha256};

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Whether a name belongs to the engine-owned constraint/index namespace.
///
/// Authoring admission rejects this exact, case-sensitive prefix. Persisted
/// catalog decoding must still accept engine-generated names in this namespace.
#[must_use]
pub fn is_engine_reserved_schema_name(name: &str) -> bool {
    name.starts_with("__icydb_")
}

/// Derive a bounded readable name from an index's complete canonical definition.
///
/// Terms must retain field-path/expression distinctions and their declared order.
/// `predicate` is the author's canonical predicate encoding, not raw SQL or a
/// debug rendering. Absence differs from an explicitly present predicate.
/// Labels are presentation only: length-framed exact inputs feed a domain-tagged
/// SHA-256 digest. The 128-bit suffix is not an injectivity guarantee; every
/// intake must still reject duplicate names and duplicate definitions.
#[must_use]
pub fn canonical_index_name(
    entity: &str,
    terms: &[&str],
    unique: bool,
    predicate: Option<&[u8]>,
) -> String {
    let mut hash = Sha256::new();
    hash.update(b"icydb:index-name:v1\0");
    hash.update([u8::from(unique)]);
    hash_name_part(&mut hash, entity.as_bytes());
    hash.update((terms.len() as u64).to_be_bytes());
    for term in terms {
        hash_name_part(&mut hash, term.as_bytes());
    }
    hash.update([u8::from(predicate.is_some())]);
    if let Some(predicate) = predicate {
        hash_name_part(&mut hash, predicate);
    }
    let digest = hash.finalize();
    let prefix = if unique { "uniq" } else { "idx" };
    let mut name = format!(
        "{prefix}_{}__{}",
        canonical_index_name_slug(entity),
        terms
            .iter()
            .map(|term| canonical_index_name_slug(term))
            .collect::<Vec<_>>()
            .join("_")
    );
    // Slugs are ASCII. Reserve one delimiter and 32 hexadecimal suffix bytes.
    name.truncate(MAX_SCHEMA_NAME_BYTES - 33);
    name.push('_');
    for byte in &digest[..16] {
        name.push(char::from(HEX[(byte >> 4) as usize]));
        name.push(char::from(HEX[(byte & 15) as usize]));
    }
    name
}

fn hash_name_part(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

/// Normalize one entity or key label for the readable portion of an index name.
///
/// This is deliberately lossy presentation. Exact inputs, rather than this slug,
/// feed the suffix in `canonical_index_name`; a slug must never be an identity.
#[must_use]
pub fn canonical_index_name_slug(value: &str) -> String {
    let separated = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();

    to_snake_case(separated.as_str())
}

fn to_snake_case(value: &str) -> String {
    let mut snake_case = String::new();
    let mut characters = value.trim().chars().peekable();
    let mut previous = None;
    let mut index = 0;

    while let Some(character) = characters.next() {
        let next = characters.peek().copied();

        if character.is_uppercase() {
            let previous_is_lower_or_digit = previous.is_some_and(|candidate: char| {
                candidate.is_lowercase() || candidate.is_ascii_digit()
            });
            let next_is_lower = next.is_some_and(char::is_lowercase);

            if index != 0
                && !snake_case.ends_with('_')
                && (previous_is_lower_or_digit || next_is_lower)
            {
                snake_case.push('_');
            }

            snake_case.extend(character.to_lowercase());
        } else if character == ' ' || character == '_' {
            if !snake_case.ends_with('_') {
                snake_case.push('_');
            }
        } else if character.is_alphanumeric() {
            snake_case.push(character);
        }

        previous = Some(character);
        index += 1;
    }

    snake_case.trim_matches('_').to_string()
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
