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
