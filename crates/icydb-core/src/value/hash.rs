//! Module: value::hash
//! Responsibility: module-local ownership and contracts for value::hash.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#[cfg(test)]
mod tests;

use crate::{error::InternalError, traits::Repr, value::Value};
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
pub(crate) fn with_test_hash_override<T>(
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
        Value::canonical_cmp_map_entry(left_key, left_value, right_key, right_value)
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
            match v.path() {
                Some(path) => {
                    feed_u8(h, 0x01); // path present
                    feed_u32(h, path.len() as u32);
                    feed_bytes(h, path.as_bytes());
                }
                None => feed_u8(h, 0x00), // path absent -> loose match
            }

            feed_u32(h, v.variant().len() as u32);
            feed_bytes(h, v.variant().as_bytes());

            match v.payload() {
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
            feed_i64(h, t.repr());
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
