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
mod tests {
    use super::{decode_replay_json, encode_bounded_replay};
    use crate::SqlGeneratorErrorKind;
    use std::error::Error as _;

    #[test]
    fn canonical_artifacts_accept_the_exact_byte_budget() {
        let value = serde_json::json!({"b": 1, "a": "x"});
        let expected = br#"{"a":"x","b":1}"#;
        let byte_budget = u32::try_from(expected.len()).unwrap();
        for kind in ["SELECT", "mutation"] {
            let bytes = encode_bounded_replay(&value, byte_budget, kind).unwrap();
            assert_eq!(bytes, expected);
            assert_eq!(
                decode_replay_json::<serde_json::Value>(&bytes, kind).unwrap(),
                value
            );
            assert_eq!(
                encode_bounded_replay(&value, byte_budget - 1, kind)
                    .unwrap_err()
                    .kind(),
                SqlGeneratorErrorKind::Budget
            );
            let malformed = decode_replay_json::<serde_json::Value>(b"{", kind).unwrap_err();
            assert_eq!(malformed.kind(), SqlGeneratorErrorKind::CanonicalReplay);
            assert!(malformed.source().is_some());
        }
    }
}
