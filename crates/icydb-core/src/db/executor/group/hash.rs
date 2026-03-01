//! Module: executor::group::hash
//! Responsibility: stable hash derivation for canonical grouped/distinct keys.
//! Does not own: key canonicalization policy or grouping equality checks.
//! Boundary: hash utilities consumed by grouped key materialization.

use crate::{
    error::InternalError,
    value::{Value, hash_value},
};

///
/// StableHash
///
/// StableHash is the canonical fixed-width hash identifier used by grouping
/// and distinct key materialization paths.
///

pub(in crate::db) type StableHash = u64;

/// Derive one stable 64-bit hash from the canonical value hash digest.
#[must_use]
pub(in crate::db) const fn stable_hash_from_digest(digest: [u8; 16]) -> StableHash {
    u64::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ])
}

/// Hash one value with the stable grouping/distinct hashing contract.
pub(in crate::db) fn stable_hash_value(value: &Value) -> Result<StableHash, InternalError> {
    let digest = hash_value(value)?;
    Ok(stable_hash_from_digest(digest))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{types::Decimal, value::Value};

    #[test]
    fn stable_hash_uses_digest_prefix_contract() {
        let digest = [
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xF0,
            0x0A, 0x0B,
        ];
        assert_eq!(
            stable_hash_from_digest(digest),
            0x1122_3344_5566_7788,
            "stable hash must use the canonical leading 64 bits of the value digest",
        );
    }

    #[test]
    fn stable_hash_is_deterministic_for_same_value() {
        let value = Value::Decimal(Decimal::new(12300, 4));
        let left = stable_hash_value(&value).expect("stable hash");
        let right = stable_hash_value(&value).expect("stable hash");
        assert_eq!(left, right);
    }

    #[test]
    fn stable_hash_respects_canonical_map_order() {
        let left = Value::Map(vec![
            (Value::Text("z".to_string()), Value::Uint(9)),
            (Value::Text("a".to_string()), Value::Uint(1)),
        ]);
        let right = Value::Map(vec![
            (Value::Text("a".to_string()), Value::Uint(1)),
            (Value::Text("z".to_string()), Value::Uint(9)),
        ]);
        assert_eq!(
            stable_hash_value(&left).expect("stable hash"),
            stable_hash_value(&right).expect("stable hash"),
            "stable hash must not depend on non-canonical map insertion order",
        );
    }

    #[test]
    fn stable_hash_contract_vectors_are_frozen_for_upgrade_stability() {
        let vectors = vec![
            ("null", Value::Null, 0x07d3_310a_0679_d482),
            ("uint_42", Value::Uint(42), 0x8c99_03a0_7f2c_731c),
            ("int_neg7", Value::Int(-7), 0x7470_6cc5_9093_df80),
            (
                "text_alpha",
                Value::Text("alpha".to_string()),
                0x6ec7_96a5_45c2_ad82,
            ),
            (
                "decimal_1",
                Value::Decimal(Decimal::new(10, 1)),
                0x7d42_1e3f_fffc_9100,
            ),
            (
                "map_a1_z9",
                Value::Map(vec![
                    (Value::Text("a".to_string()), Value::Uint(1)),
                    (Value::Text("z".to_string()), Value::Uint(9)),
                ]),
                0xea0e_28c9_f878_6d85,
            ),
        ];
        for (label, value, expected_hash) in vectors {
            let actual_hash = stable_hash_value(&value).expect("stable hash");
            assert_eq!(
                actual_hash, expected_hash,
                "stable hash vector drift for {label}; seed/version/encoding contract changed",
            );
        }
    }
}
