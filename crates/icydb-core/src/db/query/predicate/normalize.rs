use crate::{
    db::query::predicate::{
        ast::{ComparePredicate, Predicate},
        coercion::{CoercionId, CoercionSpec},
    },
    value::{Value, ValueEnum},
};

///
/// Normalize a predicate into a canonical, deterministic form.
///
/// Normalization guarantees:
/// - Logical equivalence is preserved
/// - Nested AND / OR nodes are flattened
/// - Neutral elements are removed (True / False)
/// - Double negation is eliminated
/// - Child predicates are deterministically ordered
///
/// Note: this pass does not normalize literal values (numeric width, collation).
/// Ordering uses the structural `Value` representation.
///
/// This is used to ensure:
/// - stable planner output
/// - consistent caching / equality checks
/// - predictable test behavior
///
#[must_use]
pub(crate) fn normalize(predicate: &Predicate) -> Predicate {
    match predicate {
        Predicate::True => Predicate::True,
        Predicate::False => Predicate::False,

        Predicate::And(children) => normalize_and(children),
        Predicate::Or(children) => normalize_or(children),
        Predicate::Not(inner) => normalize_not(inner),

        Predicate::Compare(cmp) => Predicate::Compare(normalize_compare(cmp)),

        Predicate::IsNull { field } => Predicate::IsNull {
            field: field.clone(),
        },
        Predicate::IsMissing { field } => Predicate::IsMissing {
            field: field.clone(),
        },
        Predicate::IsEmpty { field } => Predicate::IsEmpty {
            field: field.clone(),
        },
        Predicate::IsNotEmpty { field } => Predicate::IsNotEmpty {
            field: field.clone(),
        },
        Predicate::TextContains { field, value } => Predicate::TextContains {
            field: field.clone(),
            value: value.clone(),
        },
        Predicate::TextContainsCi { field, value } => Predicate::TextContainsCi {
            field: field.clone(),
            value: value.clone(),
        },
    }
}

///
/// Normalize a comparison predicate by cloning its components.
///
/// This function exists primarily for symmetry and future-proofing
/// (e.g. if comparison-level rewrites are introduced later).
///
fn normalize_compare(cmp: &ComparePredicate) -> ComparePredicate {
    ComparePredicate {
        field: cmp.field.clone(),
        op: cmp.op,
        value: cmp.value.clone(),
        coercion: cmp.coercion.clone(),
    }
}

///
/// Normalize a NOT expression.
///
/// Eliminates double negation:
///     NOT (NOT x)  →  x
///
fn normalize_not(inner: &Predicate) -> Predicate {
    let normalized = normalize(inner);

    if let Predicate::Not(double) = normalized {
        return normalize(&double);
    }

    Predicate::Not(Box::new(normalized))
}

///
/// Normalize an AND expression.
///
/// Rules:
/// - AND(True, x)        → x
/// - AND(False, x)       → False
/// - AND(AND(a, b), c)   → AND(a, b, c)
/// - AND()               → True
///
/// Children are sorted deterministically.
///
fn normalize_and(children: &[Predicate]) -> Predicate {
    let mut out = Vec::new();

    for child in children {
        let normalized = normalize(child);

        match normalized {
            Predicate::True => {}
            Predicate::False => return Predicate::False,
            Predicate::And(grandchildren) => out.extend(grandchildren),
            other => out.push(other),
        }
    }

    if out.is_empty() {
        return Predicate::True;
    }

    out.sort_by_cached_key(sort_key);
    Predicate::And(out)
}

///
/// Normalize an OR expression.
///
/// Rules:
/// - OR(False, x)       → x
/// - OR(True, x)        → True
/// - OR(OR(a, b), c)    → OR(a, b, c)
/// - OR()               → False
///
/// Children are sorted deterministically.
///
fn normalize_or(children: &[Predicate]) -> Predicate {
    let mut out = Vec::new();

    for child in children {
        let normalized = normalize(child);

        match normalized {
            Predicate::False => {}
            Predicate::True => return Predicate::True,
            Predicate::Or(grandchildren) => out.extend(grandchildren),
            other => out.push(other),
        }
    }

    if out.is_empty() {
        return Predicate::False;
    }

    out.sort_by_cached_key(sort_key);
    Predicate::Or(out)
}

///
/// Generate a deterministic, length-prefixed key for a predicate.
///
/// This key is used **only for sorting**, not for display.
/// Ordering ensures:
/// - planner determinism
/// - stable normalization
/// - predictable equality
///
fn sort_key(predicate: &Predicate) -> Vec<u8> {
    let mut out = Vec::new();
    encode_predicate_key(&mut out, predicate);
    out
}

const PRED_TRUE: u8 = 0x00;
const PRED_FALSE: u8 = 0x01;
const PRED_AND: u8 = 0x02;
const PRED_OR: u8 = 0x03;
const PRED_NOT: u8 = 0x04;
const PRED_COMPARE: u8 = 0x05;
const PRED_IS_NULL: u8 = 0x06;
const PRED_IS_MISSING: u8 = 0x07;
const PRED_IS_EMPTY: u8 = 0x08;
const PRED_IS_NOT_EMPTY: u8 = 0x09;
const PRED_TEXT_CONTAINS: u8 = 0x0D;
const PRED_TEXT_CONTAINS_CI: u8 = 0x0E;

// Encode predicate keys with length-prefixed segments to avoid collisions.
fn encode_predicate_key(out: &mut Vec<u8>, predicate: &Predicate) {
    match predicate {
        Predicate::True => out.push(PRED_TRUE),
        Predicate::False => out.push(PRED_FALSE),
        Predicate::And(children) => {
            out.push(PRED_AND);
            push_len(out, children.len());
            for child in children {
                push_predicate(out, child);
            }
        }
        Predicate::Or(children) => {
            out.push(PRED_OR);
            push_len(out, children.len());
            for child in children {
                push_predicate(out, child);
            }
        }
        Predicate::Not(inner) => {
            out.push(PRED_NOT);
            push_predicate(out, inner);
        }
        Predicate::Compare(cmp) => {
            out.push(PRED_COMPARE);
            push_str(out, &cmp.field);
            out.push(cmp.op.tag());
            push_value(out, &cmp.value);
            push_coercion(out, &cmp.coercion);
        }
        Predicate::IsNull { field } => {
            out.push(PRED_IS_NULL);
            push_str(out, field);
        }
        Predicate::IsMissing { field } => {
            out.push(PRED_IS_MISSING);
            push_str(out, field);
        }
        Predicate::IsEmpty { field } => {
            out.push(PRED_IS_EMPTY);
            push_str(out, field);
        }
        Predicate::IsNotEmpty { field } => {
            out.push(PRED_IS_NOT_EMPTY);
            push_str(out, field);
        }
        Predicate::TextContains { field, value } => {
            out.push(PRED_TEXT_CONTAINS);
            push_str(out, field);
            push_value(out, value);
        }
        Predicate::TextContainsCi { field, value } => {
            out.push(PRED_TEXT_CONTAINS_CI);
            push_str(out, field);
            push_value(out, value);
        }
    }
}

fn encode_value_key(out: &mut Vec<u8>, value: &Value) {
    out.push(value.canonical_tag().to_u8());

    match value {
        Value::Account(v) => {
            push_bytes(out, v.owner.as_slice());
            match v.subaccount {
                Some(sub) => {
                    out.push(1);
                    push_bytes(out, &sub.to_bytes());
                }
                None => out.push(0),
            }
        }
        Value::Blob(v) => {
            push_bytes(out, v);
        }
        Value::Bool(v) => {
            out.push(u8::from(*v));
        }
        Value::Date(v) => {
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::Decimal(v) => {
            out.push(u8::from(v.is_sign_negative()));
            out.extend_from_slice(&v.scale().to_be_bytes());
            out.extend_from_slice(&v.mantissa().to_be_bytes());
        }
        Value::Duration(v) => {
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::Enum(v) => {
            push_enum(out, v);
        }
        Value::E8s(v) => {
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::E18s(v) => {
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::Float32(v) => {
            out.extend_from_slice(&v.to_be_bytes());
        }
        Value::Float64(v) => {
            out.extend_from_slice(&v.to_be_bytes());
        }
        Value::Int(v) => {
            out.extend_from_slice(&v.to_be_bytes());
        }
        Value::Int128(v) => {
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::IntBig(v) => {
            push_bytes(out, &v.to_leb128());
        }
        Value::List(items) => {
            push_len(out, items.len());
            for item in items {
                push_value(out, item);
            }
        }
        Value::Map(entries) => {
            push_len(out, entries.len());
            for (key, value) in entries {
                push_value(out, key);
                push_value(out, value);
            }
        }
        Value::Null | Value::Unit => {}
        Value::Principal(v) => {
            push_bytes(out, v.as_slice());
        }
        Value::Subaccount(v) => {
            push_bytes(out, &v.to_bytes());
        }
        Value::Text(v) => {
            push_str(out, v);
        }
        Value::Timestamp(v) => {
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::Uint(v) => {
            out.extend_from_slice(&v.to_be_bytes());
        }
        Value::Uint128(v) => {
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::UintBig(v) => {
            push_bytes(out, &v.to_leb128());
        }
        Value::Ulid(v) => {
            out.extend_from_slice(&v.to_bytes());
        }
    }
}

fn push_predicate(out: &mut Vec<u8>, predicate: &Predicate) {
    push_framed(out, |buf| encode_predicate_key(buf, predicate));
}

fn push_value(out: &mut Vec<u8>, value: &Value) {
    push_framed(out, |buf| encode_value_key(buf, value));
}

fn push_enum(out: &mut Vec<u8>, value: &ValueEnum) {
    match &value.path {
        Some(path) => {
            out.push(1);
            push_str(out, path);
        }
        None => out.push(0),
    }
    push_str(out, &value.variant);
    match &value.payload {
        Some(payload) => {
            out.push(1);
            push_value(out, payload);
        }
        None => out.push(0),
    }
}

fn push_coercion(out: &mut Vec<u8>, spec: &CoercionSpec) {
    out.push(coercion_id_tag(spec.id));
    push_len(out, spec.params.len());
    for (key, value) in &spec.params {
        push_str(out, key);
        push_str(out, value);
    }
}

const fn coercion_id_tag(id: CoercionId) -> u8 {
    match id {
        CoercionId::Strict => 0,
        CoercionId::NumericWiden => 1,
        CoercionId::TextCasefold => 3,
        CoercionId::CollectionElement => 4,
    }
}

fn push_len(out: &mut Vec<u8>, len: usize) {
    // NOTE: Sort keys are diagnostics-only; overflow saturates for determinism.
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

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_len(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn push_str(out: &mut Vec<u8>, s: &str) {
    push_bytes(out, s.as_bytes());
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::query::predicate::CompareOp;

    #[test]
    fn sort_key_distinguishes_list_text_with_delimiters() {
        let left = Predicate::Compare(ComparePredicate {
            field: "field".to_string(),
            op: CompareOp::Eq,
            value: Value::List(vec![Value::Text("a,b".to_string())]),
            coercion: CoercionSpec::default(),
        });
        let right = Predicate::Compare(ComparePredicate {
            field: "field".to_string(),
            op: CompareOp::Eq,
            value: Value::List(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
            ]),
            coercion: CoercionSpec::default(),
        });

        assert_ne!(sort_key(&left), sort_key(&right));
    }
}
