//! Module: predicate::encoding
//! Responsibility: shared predicate/value/coercion encoding primitives.
//! Does not own: predicate normalization policy or hash consumer boundaries.
//! Boundary: consumed by normalization sort-key generation and fingerprint hashing.

use crate::{
    db::{
        access::canonical::canonicalize_value_set,
        numeric::coerce_numeric_decimal,
        predicate::{CoercionId, CoercionSpec, CompareOp, Predicate},
    },
    value::{Value, ValueEnum},
};

const SORT_PRED_TRUE: u8 = 0x00;
const SORT_PRED_FALSE: u8 = 0x01;
const SORT_PRED_AND: u8 = 0x02;
const SORT_PRED_OR: u8 = 0x03;
const SORT_PRED_NOT: u8 = 0x04;
const SORT_PRED_COMPARE: u8 = 0x05;
const SORT_PRED_COMPARE_FIELDS: u8 = 0x0B;
const SORT_PRED_IS_NULL: u8 = 0x06;
const SORT_PRED_IS_MISSING: u8 = 0x07;
const SORT_PRED_IS_EMPTY: u8 = 0x08;
const SORT_PRED_IS_NOT_EMPTY: u8 = 0x09;
const SORT_PRED_IS_NOT_NULL: u8 = 0x0A;
const SORT_PRED_TEXT_CONTAINS: u8 = 0x0D;
const SORT_PRED_TEXT_CONTAINS_CI: u8 = 0x0E;

///
/// Encode a predicate into deterministic sort-key bytes.
///
#[must_use]
pub(in crate::db::predicate) fn encode_predicate_sort_key(predicate: &Predicate) -> Vec<u8> {
    let mut out = Vec::new();
    encode_predicate_sort_key_into(&mut out, predicate, false);
    out
}

///
/// Encode an already-normalized predicate into deterministic sort-key bytes.
///
/// This boundary is only for planner-owned normalized predicates. It reuses the
/// same canonical framing as `encode_predicate_sort_key(...)` while skipping
/// repeated list sort/dedup work for `IN` / `NOT IN` literals that were already
/// canonicalized during schema-aware normalization.
///
#[must_use]
pub(in crate::db::predicate) fn encode_normalized_predicate_sort_key(
    predicate: &Predicate,
) -> Vec<u8> {
    let mut out = Vec::new();
    encode_predicate_sort_key_into(&mut out, predicate, true);
    out
}

// Encode predicate keys with length-prefixed segments to avoid collisions.
fn encode_predicate_sort_key_into(
    out: &mut Vec<u8>,
    predicate: &Predicate,
    compare_lists_already_canonical: bool,
) {
    match predicate {
        Predicate::True => out.push(SORT_PRED_TRUE),
        Predicate::False => out.push(SORT_PRED_FALSE),
        Predicate::And(children) => {
            out.push(SORT_PRED_AND);
            push_len_u64(out, children.len());
            for child in children {
                push_predicate_sort_key_framed(out, child, compare_lists_already_canonical);
            }
        }
        Predicate::Or(children) => {
            out.push(SORT_PRED_OR);
            push_len_u64(out, children.len());
            for child in children {
                push_predicate_sort_key_framed(out, child, compare_lists_already_canonical);
            }
        }
        Predicate::Not(inner) => {
            out.push(SORT_PRED_NOT);
            push_predicate_sort_key_framed(out, inner, compare_lists_already_canonical);
        }
        Predicate::Compare(cmp) => {
            out.push(SORT_PRED_COMPARE);
            push_str_u64(out, &cmp.field);
            out.push(cmp.op.tag());
            push_compare_value_sort_key_framed(
                out,
                cmp.op,
                cmp.coercion.id,
                &cmp.value,
                compare_lists_already_canonical,
            );
            push_coercion_sort_key_framed(out, &cmp.coercion);
        }
        Predicate::CompareFields(cmp) => {
            out.push(SORT_PRED_COMPARE_FIELDS);
            push_str_u64(out, &cmp.left_field);
            out.push(cmp.op.tag());
            push_str_u64(out, &cmp.right_field);
            push_coercion_sort_key_framed(out, &cmp.coercion);
        }
        Predicate::IsNull { field } => {
            out.push(SORT_PRED_IS_NULL);
            push_str_u64(out, field);
        }
        Predicate::IsNotNull { field } => {
            out.push(SORT_PRED_IS_NOT_NULL);
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
            push_value_sort_key_framed(out, value);
        }
        Predicate::TextContainsCi { field, value } => {
            out.push(SORT_PRED_TEXT_CONTAINS_CI);
            push_str_u64(out, field);
            push_value_sort_key_framed(out, value);
        }
    }
}

fn encode_value_sort_key_into(out: &mut Vec<u8>, value: &Value) {
    out.push(value.canonical_tag().to_u8());

    match value {
        Value::Account(v) => {
            push_bytes_u64(out, v.owner().as_slice());
            match v.subaccount() {
                Some(sub) => {
                    out.push(1);
                    push_bytes_u64(out, &sub.to_bytes());
                }
                None => out.push(0),
            }
        }
        Value::Blob(v) => push_bytes_u64(out, v),
        Value::Bool(v) => out.push(u8::from(*v)),
        Value::Date(v) => out.extend_from_slice(&v.as_days_since_epoch().to_be_bytes()),
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
                push_value_sort_key_framed(out, item);
            }
        }
        Value::Map(entries) => {
            // Normalize map entry order at encode time so callers that build
            // `Value::Map` directly in non-canonical order cannot perturb
            // predicate sort-key determinism.
            let ordered = Value::ordered_map_entries(entries);

            push_len_u64(out, ordered.len());
            for (key, value) in ordered {
                push_value_sort_key_framed(out, key);
                push_value_sort_key_framed(out, value);
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

fn encode_compare_value_sort_key_into(
    out: &mut Vec<u8>,
    op: CompareOp,
    coercion: CoercionId,
    value: &Value,
    compare_lists_already_canonical: bool,
) {
    if matches!(op, CompareOp::In | CompareOp::NotIn)
        && let Value::List(items) = value
    {
        out.push(value.canonical_tag().to_u8());
        if compare_lists_already_canonical {
            push_len_u64(out, items.len());
            for item in items {
                match coercion {
                    CoercionId::Strict | CoercionId::CollectionElement => {
                        push_value_sort_key_framed(out, item);
                    }
                    CoercionId::NumericWiden | CoercionId::TextCasefold => {
                        let canonical = canonicalize_compare_literal_for_coercion(coercion, item);
                        push_value_sort_key_framed(out, &canonical);
                    }
                }
            }
        } else {
            let ordered = canonicalize_compare_literal_list_for_coercion(coercion, items);

            push_len_u64(out, ordered.len());
            for item in &ordered {
                push_value_sort_key_framed(out, item);
            }
        }
        return;
    }

    let canonical = canonicalize_compare_literal_for_coercion(coercion, value);
    encode_value_sort_key_into(out, &canonical);
}

fn canonicalize_compare_literal_for_coercion(coercion: CoercionId, value: &Value) -> Value {
    match coercion {
        CoercionId::Strict | CoercionId::CollectionElement => value.clone(),
        CoercionId::NumericWiden => {
            if let Some(decimal) = coerce_numeric_decimal(value) {
                return Value::Decimal(decimal);
            }

            value.clone()
        }
        CoercionId::TextCasefold => {
            if let Value::Text(text) = value {
                return Value::Text(casefold_for_identity(text));
            }

            value.clone()
        }
    }
}

fn casefold_for_identity(input: &str) -> String {
    if input.is_ascii() {
        return input.to_ascii_lowercase();
    }

    input.to_lowercase()
}

fn encode_enum_sort_key_into(out: &mut Vec<u8>, value: &ValueEnum) {
    match value.path() {
        Some(path) => {
            out.push(1);
            push_str_u64(out, path);
        }
        None => out.push(0),
    }
    push_str_u64(out, value.variant());
    match value.payload() {
        Some(payload) => {
            out.push(1);
            push_value_sort_key_framed(out, payload);
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

// Canonicalize compare-list literals once so sort-key and fingerprint encoding
// share the same deterministic IN/NOT IN normalization boundary.
fn canonicalize_compare_literal_list_for_coercion(
    coercion: CoercionId,
    items: &[Value],
) -> Vec<Value> {
    let mut ordered = items
        .iter()
        .map(|item| canonicalize_compare_literal_for_coercion(coercion, item))
        .collect::<Vec<_>>();
    canonicalize_value_set(&mut ordered);

    ordered
}

fn push_len_u64(out: &mut Vec<u8>, len: usize) {
    // Sort keys are diagnostics-only; overflow saturates for determinism.
    let len = u64::try_from(len).unwrap_or(u64::MAX);
    out.extend_from_slice(&len.to_be_bytes());
}

// Reserve one `[len:u64be]` header and return the payload start offset.
fn begin_framed(out: &mut Vec<u8>) -> (usize, usize) {
    let len_pos = out.len();
    out.extend_from_slice(&0u64.to_be_bytes());
    let payload_start = out.len();

    (len_pos, payload_start)
}

// Finalize one `[len:u64be][payload]` frame after the payload bytes have been written.
fn finish_framed(out: &mut [u8], len_pos: usize, payload_start: usize) {
    let payload_len = out.len().saturating_sub(payload_start);
    let payload_len = u64::try_from(payload_len).unwrap_or(u64::MAX);
    out[len_pos..len_pos + std::mem::size_of::<u64>()].copy_from_slice(&payload_len.to_be_bytes());
}

fn push_predicate_sort_key_framed(
    out: &mut Vec<u8>,
    predicate: &Predicate,
    compare_lists_already_canonical: bool,
) {
    let (len_pos, payload_start) = begin_framed(out);
    encode_predicate_sort_key_into(out, predicate, compare_lists_already_canonical);
    finish_framed(out, len_pos, payload_start);
}

fn push_value_sort_key_framed(out: &mut Vec<u8>, value: &Value) {
    let (len_pos, payload_start) = begin_framed(out);
    encode_value_sort_key_into(out, value);
    finish_framed(out, len_pos, payload_start);
}

fn push_compare_value_sort_key_framed(
    out: &mut Vec<u8>,
    op: CompareOp,
    coercion: CoercionId,
    value: &Value,
    compare_lists_already_canonical: bool,
) {
    let (len_pos, payload_start) = begin_framed(out);
    encode_compare_value_sort_key_into(out, op, coercion, value, compare_lists_already_canonical);
    finish_framed(out, len_pos, payload_start);
}

fn push_coercion_sort_key_framed(out: &mut Vec<u8>, spec: &CoercionSpec) {
    let (len_pos, payload_start) = begin_framed(out);
    encode_coercion_sort_key_into(out, spec);
    finish_framed(out, len_pos, payload_start);
}

fn push_bytes_u64(out: &mut Vec<u8>, bytes: &[u8]) {
    push_len_u64(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn push_str_u64(out: &mut Vec<u8>, s: &str) {
    push_bytes_u64(out, s.as_bytes());
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, encoding::encode_predicate_sort_key,
        },
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

    #[test]
    fn predicate_sort_key_normalizes_duplicate_map_keys_by_value_order() {
        let map_a = Value::Map(vec![
            (Value::Text("a".to_string()), Value::Int(2)),
            (Value::Text("a".to_string()), Value::Int(1)),
        ]);
        let map_b = Value::Map(vec![
            (Value::Text("a".to_string()), Value::Int(1)),
            (Value::Text("a".to_string()), Value::Int(2)),
        ]);
        let predicate_a = Predicate::Compare(ComparePredicate::eq("payload".to_string(), map_a));
        let predicate_b = Predicate::Compare(ComparePredicate::eq("payload".to_string(), map_b));

        assert_eq!(
            encode_predicate_sort_key(&predicate_a),
            encode_predicate_sort_key(&predicate_b)
        );
    }

    #[test]
    fn predicate_sort_key_normalizes_in_list_literal_order() {
        let predicate_a = Predicate::Compare(ComparePredicate::in_(
            "rank".to_string(),
            vec![Value::Uint(3), Value::Uint(1), Value::Uint(2)],
        ));
        let predicate_b = Predicate::Compare(ComparePredicate::in_(
            "rank".to_string(),
            vec![Value::Uint(1), Value::Uint(2), Value::Uint(3)],
        ));

        assert_eq!(
            encode_predicate_sort_key(&predicate_a),
            encode_predicate_sort_key(&predicate_b)
        );
    }

    #[test]
    fn predicate_sort_key_normalizes_in_list_duplicate_literals() {
        let predicate_a = Predicate::Compare(ComparePredicate::in_(
            "rank".to_string(),
            vec![
                Value::Uint(3),
                Value::Uint(1),
                Value::Uint(3),
                Value::Uint(2),
            ],
        ));
        let predicate_b = Predicate::Compare(ComparePredicate::in_(
            "rank".to_string(),
            vec![Value::Uint(1), Value::Uint(2), Value::Uint(3)],
        ));

        assert_eq!(
            encode_predicate_sort_key(&predicate_a),
            encode_predicate_sort_key(&predicate_b)
        );
    }

    #[test]
    fn predicate_sort_key_numeric_widen_treats_equivalent_literal_subtypes_as_identical() {
        let predicate_int = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Int(1),
            CoercionId::NumericWiden,
        ));
        let predicate_decimal = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Decimal(crate::types::Decimal::new(10, 1)),
            CoercionId::NumericWiden,
        ));

        assert_eq!(
            encode_predicate_sort_key(&predicate_int),
            encode_predicate_sort_key(&predicate_decimal)
        );
    }

    #[test]
    fn predicate_sort_key_strict_keeps_numeric_literal_subtypes_distinct() {
        let predicate_int = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Int(1),
            CoercionId::Strict,
        ));
        let predicate_decimal = Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::Eq,
            Value::Decimal(crate::types::Decimal::new(10, 1)),
            CoercionId::Strict,
        ));

        assert_ne!(
            encode_predicate_sort_key(&predicate_int),
            encode_predicate_sort_key(&predicate_decimal)
        );
    }

    #[test]
    fn predicate_sort_key_text_casefold_treats_case_only_literals_as_identical() {
        let predicate_lower = Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("ada".to_string()),
            CoercionId::TextCasefold,
        ));
        let predicate_upper = Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("ADA".to_string()),
            CoercionId::TextCasefold,
        ));

        assert_eq!(
            encode_predicate_sort_key(&predicate_lower),
            encode_predicate_sort_key(&predicate_upper)
        );
    }

    #[test]
    fn predicate_sort_key_strict_keeps_text_case_variants_distinct() {
        let predicate_lower = Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("ada".to_string()),
            CoercionId::Strict,
        ));
        let predicate_upper = Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("ADA".to_string()),
            CoercionId::Strict,
        ));

        assert_ne!(
            encode_predicate_sort_key(&predicate_lower),
            encode_predicate_sort_key(&predicate_upper)
        );
    }

    #[test]
    fn predicate_sort_key_text_casefold_normalizes_in_list_case_variants() {
        let predicate_mixed = Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::In,
            Value::List(vec![
                Value::Text("ADA".to_string()),
                Value::Text("ada".to_string()),
                Value::Text("Bob".to_string()),
            ]),
            CoercionId::TextCasefold,
        ));
        let predicate_canonical = Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::In,
            Value::List(vec![
                Value::Text("ada".to_string()),
                Value::Text("bob".to_string()),
            ]),
            CoercionId::TextCasefold,
        ));

        assert_eq!(
            encode_predicate_sort_key(&predicate_mixed),
            encode_predicate_sort_key(&predicate_canonical)
        );
    }
}
