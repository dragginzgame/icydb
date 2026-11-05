//! Shared replay JSON and artifact-size mechanics; records retain semantic admission.

use crate::{SqlGeneratorError, SqlGeneratorErrorKind, replay::canonical_json_bytes};
use serde::{Serialize, de::DeserializeOwned};

pub(crate) fn encode_bounded_replay<T: Serialize>(
    record: &T,
    maximum_bytes: u32,
    kind: &str,
) -> Result<Vec<u8>, SqlGeneratorError> {
    let bytes = canonical_json_bytes(record)?;
    let byte_count = u32::try_from(bytes.len()).map_err(|_| {
        SqlGeneratorError::new(
            SqlGeneratorErrorKind::Budget,
            format!("canonical {kind} replay exceeds u32 byte accounting"),
        )
    })?;
    if byte_count > maximum_bytes {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::Budget,
            format!(
                "canonical {kind} replay has {byte_count} bytes, exceeding its {maximum_bytes}-byte budget"
            ),
        ));
    }
    Ok(bytes)
}

pub(crate) fn decode_replay_json<T: DeserializeOwned>(
    bytes: &[u8],
    kind: &str,
) -> Result<T, SqlGeneratorError> {
    serde_json::from_slice(bytes).map_err(|source| {
        SqlGeneratorError::with_json_source(
            SqlGeneratorErrorKind::CanonicalReplay,
            format!("failed to decode canonical {kind} replay"),
            source,
        )
    })
}

#[cfg(test)]
mod tests;
