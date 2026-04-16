//! Module: value::hash
//! Defines the stable value hashing contract used by fingerprinting,
//! diagnostics, and persisted comparison helpers.

#[cfg(test)]
mod tests;

use crate::{
    error::InternalError,
    traits::Repr,
    value::{Value, ValueTag},
};
use icydb_utils::Xxh3;

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

///
/// ValueHashWriter
///
/// ValueHashWriter incrementally encodes canonical value-hash bytes so hot
/// runtime loops can hash virtual list shapes without first materializing an
/// owned `Value::List`.
///

pub(crate) struct ValueHashWriter {
    hasher: Xxh3,
}

impl ValueHashWriter {
    /// Build one canonical value-hash writer with the stable hash version header.
    #[must_use]
    pub(crate) fn new() -> Self {
        let mut hasher = Xxh3::with_seed(VALUE_HASH_SEED);
        feed_u8(&mut hasher, VALUE_HASH_VERSION);

        Self { hasher }
    }

    /// Write one virtual list prefix so callers can hash grouped key slots
    /// without building an owned `Value::List`.
    #[expect(clippy::cast_possible_truncation)]
    pub(crate) fn write_list_prefix(&mut self, len: usize) {
        feed_u8(&mut self.hasher, ValueTag::List.to_u8());
        feed_u32(&mut self.hasher, len as u32);
    }

    /// Write one virtual list element using the canonical list-item framing.
    pub(crate) fn write_list_value(&mut self, value: &Value) -> Result<(), InternalError> {
        feed_u8(&mut self.hasher, 0xFF);
        write_to_hasher(value, &mut self.hasher)
    }

    /// Write one canonical value payload into this hash stream.
    pub(crate) fn write_value(&mut self, value: &Value) -> Result<(), InternalError> {
        write_to_hasher(value, &mut self.hasher)
    }

    /// Finish this hash stream and return the canonical 128-bit digest bytes.
    #[must_use]
    pub(crate) fn finish(self) -> [u8; 16] {
        #[cfg(test)]
        if let Some(override_hash) = test_hash_override() {
            return override_hash;
        }

        self.hasher.digest128().to_be_bytes()
    }
}

// Hash one identity-canonical value under the same one-element list framing used
// by grouped single-field keys without routing through the fully generic value
// writer. This keeps the common grouped `COUNT(*)` fast path off the broader
// `Value` hashing dispatch when the grouped slot already arrives canonical.
pub(crate) fn hash_single_list_identity_canonical_value(
    value: &Value,
) -> Result<Option<[u8; 16]>, InternalError> {
    #[cfg(test)]
    if let Some(override_hash) = test_hash_override() {
        return Ok(Some(override_hash));
    }

    let mut hasher = Xxh3::with_seed(VALUE_HASH_SEED);
    feed_u8(&mut hasher, VALUE_HASH_VERSION);
    feed_u8(&mut hasher, ValueTag::List.to_u8());
    feed_u32(&mut hasher, 1);
    feed_u8(&mut hasher, 0xFF);
    feed_u8(&mut hasher, value.canonical_tag().to_u8());

    match value {
        Value::Account(account) => {
            let bytes = account
                .to_stored_bytes()
                .map_err(|err| InternalError::serialize_unsupported(err.to_string()))?;
            feed_bytes(&mut hasher, &bytes);
        }
        Value::Blob(bytes) => {
            feed_u8(&mut hasher, 0x01);
            feed_u32(&mut hasher, bytes.len() as u32);
            feed_bytes(&mut hasher, bytes);
        }
        Value::Bool(value) => feed_u8(&mut hasher, u8::from(*value)),
        Value::Date(value) => feed_i32(&mut hasher, value.as_days_since_epoch()),
        Value::Duration(value) => feed_u64(&mut hasher, value.repr()),
        Value::Float32(value) => feed_bytes(&mut hasher, &value.to_be_bytes()),
        Value::Float64(value) => feed_bytes(&mut hasher, &value.to_be_bytes()),
        Value::Int(value) => feed_i64(&mut hasher, *value),
        Value::Int128(value) => feed_i128(&mut hasher, value.get()),
        Value::IntBig(value) => {
            let bytes = value.to_leb128();
            feed_u32(&mut hasher, bytes.len() as u32);
            feed_bytes(&mut hasher, &bytes);
        }
        Value::Principal(value) => {
            let raw = value
                .stored_bytes()
                .map_err(|err| InternalError::serialize_unsupported(err.to_string()))?;
            feed_u32(&mut hasher, raw.len() as u32);
            feed_bytes(&mut hasher, raw);
        }
        Value::Subaccount(value) => feed_bytes(&mut hasher, &value.to_bytes()),
        Value::Text(value) => {
            feed_u32(&mut hasher, value.len() as u32);
            feed_bytes(&mut hasher, value.as_bytes());
        }
        Value::Timestamp(value) => feed_i64(&mut hasher, value.repr()),
        Value::Uint(value) => feed_u64(&mut hasher, *value),
        Value::Uint128(value) => feed_u128(&mut hasher, value.get()),
        Value::UintBig(value) => {
            let bytes = value.to_leb128();
            feed_u32(&mut hasher, bytes.len() as u32);
            feed_bytes(&mut hasher, &bytes);
        }
        Value::Ulid(value) => feed_bytes(&mut hasher, &value.to_bytes()),
        _ => return Ok(None),
    }

    Ok(Some(hasher.digest128().to_be_bytes()))
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
    let ordered = Value::ordered_map_entries(entries);

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
                .to_stored_bytes()
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
        Value::Date(d) => feed_i32(h, d.as_days_since_epoch()),
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
                .stored_bytes()
                .map_err(|err| InternalError::serialize_unsupported(err.to_string()))?;
            feed_u32(h, raw.len() as u32);
            feed_bytes(h, raw);
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

    let mut writer = ValueHashWriter::new();
    writer.write_value(value)?;

    Ok(writer.finish())
}
