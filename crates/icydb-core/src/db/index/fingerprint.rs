use crate::{
    error::{ErrorClass, ErrorOrigin, InternalError},
    value::Value,
};
use canic_utils::hash::Xxh3;

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
#[allow(clippy::redundant_closure_for_method_calls)]
fn test_hash_override() -> Option<[u8; 16]> {
    TEST_HASH_OVERRIDE.with(|cell| cell.get())
}

#[allow(clippy::cast_possible_truncation)]
#[allow(clippy::too_many_lines)]
fn write_to_hasher(value: &Value, h: &mut Xxh3) -> Result<(), InternalError> {
    feed_u8(h, value.canonical_tag().to_u8());

    match value {
        Value::Account(a) => {
            let bytes = a.to_bytes().map_err(|err| {
                InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Serialize,
                    err.to_string(),
                )
            })?;
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
            feed_u64(h, t.get());
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
        Value::E8s(v) => {
            feed_u64(h, v.get());
        }
        Value::E18s(v) => {
            feed_bytes(h, &v.to_be_bytes());
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
            feed_u32(h, entries.len() as u32);
            for (key, value) in entries {
                feed_u8(h, 0xFD);
                write_to_hasher(key, h)?;
                feed_u8(h, 0xFE);
                write_to_hasher(value, h)?;
            }
        }
        Value::Principal(p) => {
            let raw = p.to_bytes().map_err(|err| {
                InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Serialize,
                    err.to_string(),
                )
            })?;
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
            feed_u64(h, t.get());
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
            // NOTE: Non-indexable values intentionally contribute no hash input.
        }
    }

    Ok(())
}

/// Stable hash used for index/storage fingerprints.
pub fn hash_value(value: &Value) -> Result<[u8; 16], InternalError> {
    const VERSION: u8 = 1;

    #[cfg(test)]
    if let Some(override_hash) = test_hash_override() {
        return Ok(override_hash);
    }

    let mut h = Xxh3::with_seed(0);
    feed_u8(&mut h, VERSION); // version

    write_to_hasher(value, &mut h)?;
    Ok(h.digest128().to_be_bytes())
}

/// Index fingerprint semantics:
///
/// - Only indexable values produce fingerprints.
/// - `Value::Null` does not produce fingerprints and
///   therefore do not participate in indexing.
/// - For unique indexes, uniqueness is enforced only over indexable values.
///   Multiple rows with non-indexable values are permitted.
///
/// This behavior matches SQL-style UNIQUE constraints with NULL values.
///
/// Stable 128-bit hash used for index keys; returns `None` for non-indexable values.
pub fn to_index_fingerprint(value: &Value) -> Result<Option<[u8; 16]>, InternalError> {
    if matches!(value, Value::Null) {
        // Intentionally skipped: non-indexable values do not participate in indexes.
        return Ok(None);
    }

    Ok(Some(hash_value(value)?))
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
}
