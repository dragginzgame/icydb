//! Module: predicate::encoding
//! Responsibility: shared predicate/value/coercion encoding primitives.
//! Does not own: predicate normalization policy or hash consumer boundaries.
//! Boundary: consumed by normalization sort-key generation and fingerprint hashing.

use crate::{
    db::predicate::{CoercionId, CoercionSpec, Predicate},
    value::{Value, ValueEnum, hash_value},
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const SORT_PRED_TRUE: u8 = 0x00;
const SORT_PRED_FALSE: u8 = 0x01;
const SORT_PRED_AND: u8 = 0x02;
const SORT_PRED_OR: u8 = 0x03;
const SORT_PRED_NOT: u8 = 0x04;
const SORT_PRED_COMPARE: u8 = 0x05;
const SORT_PRED_IS_NULL: u8 = 0x06;
const SORT_PRED_IS_MISSING: u8 = 0x07;
const SORT_PRED_IS_EMPTY: u8 = 0x08;
const SORT_PRED_IS_NOT_EMPTY: u8 = 0x09;
const SORT_PRED_TEXT_CONTAINS: u8 = 0x0D;
const SORT_PRED_TEXT_CONTAINS_CI: u8 = 0x0E;

///
/// Encode a predicate into deterministic sort-key bytes.
///
#[must_use]
pub(in crate::db::predicate) fn encode_predicate_sort_key(predicate: &Predicate) -> Vec<u8> {
    let mut out = Vec::new();
    encode_predicate_sort_key_into(&mut out, predicate);
    out
}

// Encode predicate keys with length-prefixed segments to avoid collisions.
fn encode_predicate_sort_key_into(out: &mut Vec<u8>, predicate: &Predicate) {
    match predicate {
        Predicate::True => out.push(SORT_PRED_TRUE),
        Predicate::False => out.push(SORT_PRED_FALSE),
        Predicate::And(children) => {
            out.push(SORT_PRED_AND);
            push_len_u64(out, children.len());
            for child in children {
                push_framed(out, |buf| encode_predicate_sort_key_into(buf, child));
            }
        }
        Predicate::Or(children) => {
            out.push(SORT_PRED_OR);
            push_len_u64(out, children.len());
            for child in children {
                push_framed(out, |buf| encode_predicate_sort_key_into(buf, child));
            }
        }
        Predicate::Not(inner) => {
            out.push(SORT_PRED_NOT);
            push_framed(out, |buf| encode_predicate_sort_key_into(buf, inner));
        }
        Predicate::Compare(cmp) => {
            out.push(SORT_PRED_COMPARE);
            push_str_u64(out, &cmp.field);
            out.push(cmp.op.tag());
            push_framed(out, |buf| encode_value_sort_key_into(buf, &cmp.value));
            push_framed(out, |buf| encode_coercion_sort_key_into(buf, &cmp.coercion));
        }
        Predicate::IsNull { field } => {
            out.push(SORT_PRED_IS_NULL);
            push_str_u64(out, field);
        }
        Predicate::IsMissing { field } => {
            out.push(SORT_PRED_IS_MISSING);
            push_str_u64(out, field);
        }
        Predicate::IsEmpty { field } => {
            out.push(SORT_PRED_IS_EMPTY);
            push_str_u64(out, field);
        }
        Predicate::IsNotEmpty { field } => {
            out.push(SORT_PRED_IS_NOT_EMPTY);
            push_str_u64(out, field);
        }
        Predicate::TextContains { field, value } => {
            out.push(SORT_PRED_TEXT_CONTAINS);
            push_str_u64(out, field);
            push_framed(out, |buf| encode_value_sort_key_into(buf, value));
        }
        Predicate::TextContainsCi { field, value } => {
            out.push(SORT_PRED_TEXT_CONTAINS_CI);
            push_str_u64(out, field);
            push_framed(out, |buf| encode_value_sort_key_into(buf, value));
        }
    }
}

fn encode_value_sort_key_into(out: &mut Vec<u8>, value: &Value) {
    out.push(value.canonical_tag().to_u8());

    match value {
        Value::Account(v) => {
            push_bytes_u64(out, v.owner.as_slice());
            match v.subaccount {
                Some(sub) => {
                    out.push(1);
                    push_bytes_u64(out, &sub.to_bytes());
                }
                None => out.push(0),
            }
        }
        Value::Blob(v) => push_bytes_u64(out, v),
        Value::Bool(v) => out.push(u8::from(*v)),
        Value::Date(v) => out.extend_from_slice(&v.get().to_be_bytes()),
        Value::Decimal(v) => {
            let normalized = v.normalize();
            out.push(u8::from(normalized.is_sign_negative()));
            out.extend_from_slice(&normalized.scale().to_be_bytes());
            out.extend_from_slice(&normalized.mantissa().to_be_bytes());
        }
        Value::Duration(v) => out.extend_from_slice(&v.as_millis().to_be_bytes()),
        Value::Enum(v) => encode_enum_sort_key_into(out, v),
        Value::Float32(v) => out.extend_from_slice(&v.to_be_bytes()),
        Value::Float64(v) => out.extend_from_slice(&v.to_be_bytes()),
        Value::Int(v) => out.extend_from_slice(&v.to_be_bytes()),
        Value::Int128(v) => out.extend_from_slice(&v.get().to_be_bytes()),
        Value::IntBig(v) => push_bytes_u64(out, &v.to_leb128()),
        Value::List(items) => {
            push_len_u64(out, items.len());
            for item in items {
                push_framed(out, |buf| encode_value_sort_key_into(buf, item));
            }
        }
        Value::Map(entries) => {
            // Normalize map entry order at encode time so callers that build
            // `Value::Map` directly in non-canonical order cannot perturb
            // predicate sort-key determinism.
            let mut ordered = entries.iter().collect::<Vec<_>>();
            ordered.sort_by(|(left_key, left_value), (right_key, right_value)| {
                Value::canonical_cmp_key(left_key, right_key)
                    .then_with(|| Value::canonical_cmp(left_value, right_value))
            });

            push_len_u64(out, ordered.len());
            for (key, value) in ordered {
                push_framed(out, |buf| encode_value_sort_key_into(buf, key));
                push_framed(out, |buf| encode_value_sort_key_into(buf, value));
            }
        }
        Value::Null | Value::Unit => {}
        Value::Principal(v) => push_bytes_u64(out, v.as_slice()),
        Value::Subaccount(v) => push_bytes_u64(out, &v.to_bytes()),
        Value::Text(v) => push_str_u64(out, v),
        Value::Timestamp(v) => out.extend_from_slice(&v.as_millis().to_be_bytes()),
        Value::Uint(v) => out.extend_from_slice(&v.to_be_bytes()),
        Value::Uint128(v) => out.extend_from_slice(&v.get().to_be_bytes()),
        Value::UintBig(v) => push_bytes_u64(out, &v.to_leb128()),
        Value::Ulid(v) => out.extend_from_slice(&v.to_bytes()),
    }
}

fn encode_enum_sort_key_into(out: &mut Vec<u8>, value: &ValueEnum) {
    match &value.path {
        Some(path) => {
            out.push(1);
            push_str_u64(out, path);
        }
        None => out.push(0),
    }
    push_str_u64(out, &value.variant);
    match &value.payload {
        Some(payload) => {
            out.push(1);
            push_framed(out, |buf| encode_value_sort_key_into(buf, payload));
        }
        None => out.push(0),
    }
}

fn encode_coercion_sort_key_into(out: &mut Vec<u8>, spec: &CoercionSpec) {
    out.push(coercion_id_sort_tag(spec.id));
    push_len_u64(out, spec.params.len());
    for (key, value) in &spec.params {
        push_str_u64(out, key);
        push_str_u64(out, value);
    }
}

const fn coercion_id_sort_tag(id: CoercionId) -> u8 {
    match id {
        CoercionId::Strict => 0,
        CoercionId::NumericWiden => 1,
        CoercionId::TextCasefold => 3,
        CoercionId::CollectionElement => 4,
    }
}

///
/// Hash one predicate using the fingerprint stream encoding.
///
pub(in crate::db::predicate) fn hash_predicate_fingerprint(
    hasher: &mut Sha256,
    predicate: &Predicate,
) {
    match predicate {
        Predicate::True => write_tag_u8(hasher, 0x21),
        Predicate::False => write_tag_u8(hasher, 0x22),
        Predicate::And(children) => {
            write_tag_u8(hasher, 0x23);
            write_len_u32(hasher, children.len());
            for child in children {
                hash_predicate_fingerprint(hasher, child);
            }
        }
        Predicate::Or(children) => {
            write_tag_u8(hasher, 0x24);
            write_len_u32(hasher, children.len());
            for child in children {
                hash_predicate_fingerprint(hasher, child);
            }
        }
        Predicate::Not(inner) => {
            write_tag_u8(hasher, 0x25);
            hash_predicate_fingerprint(hasher, inner);
        }
        Predicate::Compare(compare) => {
            write_tag_u8(hasher, 0x26);
            write_str_u32(hasher, &compare.field);
            write_tag_u8(hasher, compare.op.tag());
            hash_value_fingerprint(hasher, &compare.value);
            hash_coercion_fingerprint(hasher, compare.coercion.id, &compare.coercion.params);
        }
        Predicate::IsNull { field } => {
            write_tag_u8(hasher, 0x27);
            write_str_u32(hasher, field);
        }
        Predicate::IsMissing { field } => {
            write_tag_u8(hasher, 0x28);
            write_str_u32(hasher, field);
        }
        Predicate::IsEmpty { field } => {
            write_tag_u8(hasher, 0x29);
            write_str_u32(hasher, field);
        }
        Predicate::IsNotEmpty { field } => {
            write_tag_u8(hasher, 0x2a);
            write_str_u32(hasher, field);
        }
        Predicate::TextContains { field, value } => {
            write_tag_u8(hasher, 0x2e);
            write_str_u32(hasher, field);
            hash_value_fingerprint(hasher, value);
        }
        Predicate::TextContainsCi { field, value } => {
            write_tag_u8(hasher, 0x2f);
            write_str_u32(hasher, field);
            hash_value_fingerprint(hasher, value);
        }
    }
}

///
/// Hash one coercion descriptor using the fingerprint stream encoding.
///
pub(in crate::db::predicate) fn hash_coercion_fingerprint(
    hasher: &mut Sha256,
    id: CoercionId,
    params: &BTreeMap<String, String>,
) {
    write_tag_u8(hasher, id.plan_hash_tag());
    write_len_u32(hasher, params.len());
    for (key, value) in params {
        write_str_u32(hasher, key);
        write_str_u32(hasher, value);
    }
}

fn hash_value_fingerprint(hasher: &mut Sha256, value: &Value) {
    match hash_value(value) {
        Ok(digest) => hasher.update(digest),
        Err(err) => {
            write_tag_u8(hasher, 0xEE);
            write_str_u32(hasher, &err.display_with_class());
        }
    }
}

fn push_len_u64(out: &mut Vec<u8>, len: usize) {
    // Sort keys are diagnostics-only; overflow saturates for determinism.
    let len = u64::try_from(len).unwrap_or(u64::MAX);
    out.extend_from_slice(&len.to_be_bytes());
}

// Write one nested deterministic payload as [len:u64be][payload] without
// allocating an intermediate buffer.
fn push_framed(out: &mut Vec<u8>, encode: impl FnOnce(&mut Vec<u8>)) {
    let len_pos = out.len();
    out.extend_from_slice(&0u64.to_be_bytes());
    let payload_start = out.len();

    encode(out);

    let payload_len = out.len().saturating_sub(payload_start);
    let payload_len = u64::try_from(payload_len).unwrap_or(u64::MAX);
    out[len_pos..len_pos + std::mem::size_of::<u64>()].copy_from_slice(&payload_len.to_be_bytes());
}

fn push_bytes_u64(out: &mut Vec<u8>, bytes: &[u8]) {
    push_len_u64(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn push_str_u64(out: &mut Vec<u8>, s: &str) {
    push_bytes_u64(out, s.as_bytes());
}

fn write_tag_u8(hasher: &mut Sha256, tag: u8) {
    hasher.update([tag]);
}

fn write_len_u32(hasher: &mut Sha256, len: usize) {
    let len = u32::try_from(len).unwrap_or(u32::MAX);
    hasher.update(len.to_be_bytes());
}

fn write_str_u32(hasher: &mut Sha256, value: &str) {
    write_len_u32(hasher, value.len());
    hasher.update(value.as_bytes());
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::predicate::{ComparePredicate, Predicate, encoding::encode_predicate_sort_key},
        value::Value,
    };

    #[test]
    fn predicate_sort_key_normalizes_map_entry_order() {
        let map_a = Value::Map(vec![
            (Value::Text("z".to_string()), Value::Int(9)),
            (Value::Text("a".to_string()), Value::Int(1)),
        ]);
        let map_b = Value::Map(vec![
            (Value::Text("a".to_string()), Value::Int(1)),
            (Value::Text("z".to_string()), Value::Int(9)),
        ]);
        let predicate_a = Predicate::Compare(ComparePredicate::eq("payload".to_string(), map_a));
        let predicate_b = Predicate::Compare(ComparePredicate::eq("payload".to_string(), map_b));

        assert_eq!(
            encode_predicate_sort_key(&predicate_a),
            encode_predicate_sort_key(&predicate_b)
        );
    }
}
