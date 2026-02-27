use crate::{error::InternalError, types::Repr, value::Value};
use canic_utils::hash::Xxh3;

/// Value-hash format version byte used by canonical digest encoding.
pub(crate) const VALUE_HASH_VERSION: u8 = 1;

/// Stable XXH3 seed used by canonical value hashing across upgrades.
pub(crate) const VALUE_HASH_SEED: u64 = 0;

fn feed_i32(h: &mut Xxh3, x: i32) {
    h.update(&x.to_be_bytes());
}
fn feed_i64(h: &mut Xxh3, x: i64) {
    h.update(&x.to_be_bytes());
}
fn feed_i128(h: &mut Xxh3, x: i128) {
    h.update(&x.to_be_bytes());
}
fn feed_u8(h: &mut Xxh3, x: u8) {
    h.update(&[x]);
}
fn feed_u32(h: &mut Xxh3, x: u32) {
    h.update(&x.to_be_bytes());
}
fn feed_u64(h: &mut Xxh3, x: u64) {
    h.update(&x.to_be_bytes());
}
fn feed_u128(h: &mut Xxh3, x: u128) {
    h.update(&x.to_be_bytes());
}
fn feed_bytes(h: &mut Xxh3, b: &[u8]) {
    h.update(b);
}

#[cfg(test)]
thread_local! {
    static TEST_HASH_OVERRIDE: std::cell::Cell<Option<[u8; 16]>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
#[expect(clippy::redundant_closure_for_method_calls)]
fn test_hash_override() -> Option<[u8; 16]> {
    TEST_HASH_OVERRIDE.with(|cell| cell.get())
}

// Execute one closure with a thread-local test hash override and always restore
// the previous override state, even if the closure panics.
#[cfg(test)]
pub(in crate::db) fn with_test_hash_override<T>(
    override_hash: [u8; 16],
    f: impl FnOnce() -> T + std::panic::UnwindSafe,
) -> T {
    let previous = TEST_HASH_OVERRIDE.with(|cell| cell.replace(Some(override_hash)));
    let result = std::panic::catch_unwind(f);
    TEST_HASH_OVERRIDE.with(|cell| cell.set(previous));
    match result {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

// Hash map entries under canonical key order to keep fingerprints deterministic
// even when callers construct `Value::Map` directly in non-canonical order.
#[expect(clippy::cast_possible_truncation)]
fn write_map_entries_to_hasher(
    entries: &[(Value, Value)],
    h: &mut Xxh3,
) -> Result<(), InternalError> {
    let mut ordered = entries.iter().collect::<Vec<_>>();
    ordered.sort_by(|(left_key, left_value), (right_key, right_value)| {
        Value::canonical_cmp_key(left_key, right_key)
            .then_with(|| Value::canonical_cmp(left_value, right_value))
    });

    feed_u32(h, ordered.len() as u32);
    for (key, value) in ordered {
        feed_u8(h, 0xFD);
        write_to_hasher(key, h)?;
        feed_u8(h, 0xFE);
        write_to_hasher(value, h)?;
    }

    Ok(())
}

#[expect(clippy::cast_possible_truncation)]
#[expect(clippy::too_many_lines)]
fn write_to_hasher(value: &Value, h: &mut Xxh3) -> Result<(), InternalError> {
    feed_u8(h, value.canonical_tag().to_u8());

    match value {
        Value::Account(a) => {
            let bytes = a
                .to_bytes()
                .map_err(|err| InternalError::serialize_unsupported(err.to_string()))?;
            feed_bytes(h, &bytes);
        }
        Value::Blob(v) => {
            feed_u8(h, 0x01);
            feed_u32(h, v.len() as u32);
            feed_bytes(h, v);
        }
        Value::Bool(b) => {
            feed_u8(h, u8::from(*b));
        }
        Value::Date(d) => feed_i32(h, d.get()),
        Value::Decimal(d) => {
            let normalized = d.normalize();

            // encode (sign, scale, mantissa) deterministically:
            feed_u8(h, u8::from(normalized.is_sign_negative()));
            feed_u32(h, normalized.scale());
            feed_bytes(h, &normalized.mantissa().to_be_bytes());
        }
        Value::Duration(t) => {
            feed_u64(h, t.repr());
        }
        Value::Enum(v) => {
            match &v.path {
                Some(path) => {
                    feed_u8(h, 0x01); // path present
                    feed_u32(h, path.len() as u32);
                    feed_bytes(h, path.as_bytes());
                }
                None => feed_u8(h, 0x00), // path absent -> loose match
            }

            feed_u32(h, v.variant.len() as u32);
            feed_bytes(h, v.variant.as_bytes());

            match &v.payload {
                Some(payload) => {
                    feed_u8(h, 0x01); // payload present
                    write_to_hasher(payload, h)?; // include nested value
                }
                None => feed_u8(h, 0x00),
            }
        }
        Value::Float32(v) => {
            feed_bytes(h, &v.to_be_bytes());
        }
        Value::Float64(v) => {
            feed_bytes(h, &v.to_be_bytes());
        }
        Value::Int(i) => {
            feed_i64(h, *i);
        }
        Value::Int128(i) => {
            feed_i128(h, i.get());
        }
        Value::IntBig(v) => {
            let bytes = v.to_leb128();
            feed_u32(h, bytes.len() as u32);
            feed_bytes(h, &bytes);
        }
        Value::List(xs) => {
            feed_u32(h, xs.len() as u32);
            for x in xs {
                feed_u8(h, 0xFF);
                write_to_hasher(x, h)?; // recurse, no sub-hash
            }
        }
        Value::Map(entries) => {
            // Map entries must hash under canonical key order.
            // Fingerprint stability depends on deterministic iteration order.
            write_map_entries_to_hasher(entries.as_slice(), h)?;
        }
        Value::Principal(p) => {
            let raw = p
                .to_bytes()
                .map_err(|err| InternalError::serialize_unsupported(err.to_string()))?;
            feed_u32(h, raw.len() as u32);
            feed_bytes(h, &raw);
        }
        Value::Subaccount(s) => {
            feed_bytes(h, &s.to_bytes());
        }
        Value::Text(s) => {
            // If you need case/Unicode insensitivity, normalize; else skip (much faster)
            // let norm = normalize_nfkc_casefold(s);
            // feed_u32( h, norm.len() as u32);
            // feed_bytes( h, norm.as_bytes());
            feed_u32(h, s.len() as u32);
            feed_bytes(h, s.as_bytes());
        }
        Value::Timestamp(t) => {
            feed_u64(h, t.repr());
        }
        Value::Uint(u) => {
            feed_u64(h, *u);
        }
        Value::Uint128(u) => {
            feed_u128(h, u.get());
        }
        Value::UintBig(v) => {
            let bytes = v.to_leb128();
            feed_u32(h, bytes.len() as u32);
            feed_bytes(h, &bytes);
        }
        Value::Ulid(u) => {
            feed_bytes(h, &u.to_bytes());
        }
        Value::Null | Value::Unit => {
            // No additional payload beyond canonical tag.
        }
    }

    Ok(())
}

/// Stable hash used for canonical value fingerprinting across db layers.
pub(crate) fn hash_value(value: &Value) -> Result<[u8; 16], InternalError> {
    #[cfg(test)]
    if let Some(override_hash) = test_hash_override() {
        return Ok(override_hash);
    }

    let mut h = Xxh3::with_seed(VALUE_HASH_SEED);
    feed_u8(&mut h, VALUE_HASH_VERSION); // version

    write_to_hasher(value, &mut h)?;
    Ok(h.digest128().to_be_bytes())
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        types::{Decimal, Float32 as F32, Float64 as F64},
        value::{Value, ValueEnum},
    };

    #[test]
    fn hash_contract_seed_and_version_are_frozen() {
        assert_eq!(VALUE_HASH_SEED, 0);
        assert_eq!(VALUE_HASH_VERSION, 1);
    }

    #[test]
    fn hash_digest_contract_vectors_are_frozen_for_upgrade_stability() {
        let vectors = vec![
            (
                "null",
                Value::Null,
                0x07d3_310a_0679_d482_1974_aae7_68bf_e723,
            ),
            (
                "empty_list",
                Value::List(Vec::new()),
                0xc961_6022_4099_6188_601a_2e1d_a3d4_577d,
            ),
            (
                "empty_map",
                Value::Map(Vec::new()),
                0xfe53_d0af_a864_e41f_7540_c693_50b9_b8d7,
            ),
            (
                "uint_42",
                Value::Uint(42),
                0x8c99_03a0_7f2c_731c_2e7a_9cd6_52cb_010f,
            ),
            (
                "int_neg7",
                Value::Int(-7),
                0x7470_6cc5_9093_df80_0d3b_e517_da6b_0104,
            ),
            (
                "text_alpha",
                Value::Text("alpha".to_string()),
                0x6ec7_96a5_45c2_ad82_58ff_9d4a_4ea8_1c2b,
            ),
            (
                "decimal_1",
                Value::Decimal(Decimal::new(10, 1)),
                0x7d42_1e3f_fffc_9100_0be6_fa20_26b6_0b82,
            ),
            (
                "map_a1_z9",
                Value::Map(vec![
                    (Value::Text("a".to_string()), Value::Uint(1)),
                    (Value::Text("z".to_string()), Value::Uint(9)),
                ]),
                0xea0e_28c9_f878_6d85_c240_88c8_f0d7_9d81,
            ),
            (
                "nested_list_map",
                Value::List(vec![
                    Value::Map(vec![
                        (Value::Text("z".to_string()), Value::Uint(9)),
                        (Value::Text("a".to_string()), Value::Uint(1)),
                    ]),
                    Value::Decimal(Decimal::new(25, 0)),
                ]),
                0x65e9_3e05_85b6_72cc_de03_6d05_eaa9_8c57,
            ),
        ];
        for (label, value, expected_digest) in vectors {
            let actual_digest = hash_value(&value).expect("hash value");
            assert_eq!(
                u128::from_be_bytes(actual_digest),
                expected_digest,
                "value-hash digest vector drift for {label}; canonical encoding/seed/version contract changed",
            );
        }
    }

    fn v_f64(x: f64) -> Value {
        Value::Float64(F64::try_new(x).expect("finite f64"))
    }
    fn v_f32(x: f32) -> Value {
        Value::Float32(F32::try_new(x).expect("finite f32"))
    }
    fn v_i(x: i64) -> Value {
        Value::Int(x)
    }
    fn v_txt(s: &str) -> Value {
        Value::Text(s.to_string())
    }

    #[test]
    fn hash_is_deterministic_for_int() {
        let v = Value::Int(42);
        let a = hash_value(&v).expect("hash value");
        let b = hash_value(&v).expect("hash value");
        assert_eq!(a, b, "hash should be deterministic for same value");
    }

    #[test]
    fn different_variants_produce_different_hashes() {
        let a = hash_value(&Value::Int(5)).expect("hash value");
        let b = hash_value(&Value::Uint(5)).expect("hash value");
        assert_ne!(
            a, b,
            "Int(5) and Uint(5) must hash differently (different tag)"
        );
    }

    #[test]
    fn enum_hash_tracks_path_presence() {
        let strict = Value::Enum(ValueEnum::new("A", Some("MyEnum")));
        let loose = Value::Enum(ValueEnum::new("A", None));
        assert_ne!(
            hash_value(&strict).expect("hash value"),
            hash_value(&loose).expect("hash value"),
            "Enum hashes must differ when path is present vs absent"
        );
    }

    #[test]
    fn enum_hash_includes_payload() {
        let base = ValueEnum::new("A", Some("MyEnum"));
        let with_one = Value::Enum(base.clone().with_payload(Value::Uint(1)));
        let with_two = Value::Enum(base.with_payload(Value::Uint(2)));

        assert_ne!(
            hash_value(&with_one).expect("hash value"),
            hash_value(&with_two).expect("hash value"),
            "Enum payload must influence hash/fingerprint"
        );
    }

    #[test]
    fn float32_and_float64_hash_differ() {
        let a = hash_value(&v_f32(1.0)).expect("hash value");
        let b = hash_value(&v_f64(1.0)).expect("hash value");
        assert_ne!(
            a, b,
            "Float32 and Float64 must hash differently (different tag)"
        );
    }

    #[test]
    fn decimal_hash_normalizes_equivalent_values() {
        let one = Value::Decimal(Decimal::new(1, 0));
        let one_point_zero = Value::Decimal(Decimal::new(10, 1));

        assert_eq!(
            one, one_point_zero,
            "decimal values should be semantically equal"
        );
        assert_eq!(
            hash_value(&one).expect("hash value"),
            hash_value(&one_point_zero).expect("hash value"),
            "equivalent decimals must hash to the same fingerprint"
        );
    }

    #[test]
    fn text_is_length_and_content_sensitive() {
        let a = hash_value(&v_txt("foo")).expect("hash value");
        let b = hash_value(&v_txt("bar")).expect("hash value");
        assert_ne!(a, b, "different strings should hash differently");

        let c = hash_value(&v_txt("foo")).expect("hash value");
        assert_eq!(a, c, "same string should hash the same");
    }

    #[test]
    fn list_hash_is_order_sensitive() {
        let l1 = Value::from_slice(&[v_i(1), v_i(2)]);
        let l2 = Value::from_slice(&[v_i(2), v_i(1)]);
        assert_ne!(
            hash_value(&l1).expect("hash value"),
            hash_value(&l2).expect("hash value"),
            "list order should affect hash"
        );
    }

    #[test]
    fn list_hash_is_length_sensitive() {
        let l1 = Value::from_slice(&[v_i(1)]);
        let l2 = Value::from_slice(&[v_i(1), v_i(1)]);
        assert_ne!(
            hash_value(&l1).expect("hash value"),
            hash_value(&l2).expect("hash value"),
            "list length should affect hash"
        );
    }

    #[test]
    fn list_blob_boundaries_are_length_framed() {
        let left = Value::List(vec![
            Value::Blob(vec![0x10, 0xFF, 0x02, 0x11]),
            Value::Blob(vec![0x12]),
        ]);
        let right = Value::List(vec![
            Value::Blob(vec![0x10]),
            Value::Blob(vec![0x11, 0xFF, 0x02, 0x12]),
        ]);

        assert_ne!(
            hash_value(&left).expect("hash value"),
            hash_value(&right).expect("hash value"),
            "blob boundaries must be length-framed to avoid collisions"
        );
    }

    #[test]
    fn map_hash_is_order_independent_for_non_canonical_construction_order() {
        let left = Value::Map(vec![
            (Value::Text("z".to_string()), Value::Uint(9)),
            (Value::Text("a".to_string()), Value::Uint(1)),
        ]);
        let right = Value::Map(vec![
            (Value::Text("a".to_string()), Value::Uint(1)),
            (Value::Text("z".to_string()), Value::Uint(9)),
        ]);

        assert_eq!(
            hash_value(&left).expect("hash value"),
            hash_value(&right).expect("hash value"),
            "map hash must be deterministic regardless of insertion order"
        );
    }
}
