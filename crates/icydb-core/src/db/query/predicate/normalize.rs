use crate::{
    db::query::predicate::{
        ast::{CompareOp, ComparePredicate, Predicate},
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
pub fn normalize(predicate: &Predicate) -> Predicate {
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

        Predicate::MapContainsKey {
            field,
            key,
            coercion,
        } => Predicate::MapContainsKey {
            field: field.clone(),
            key: key.clone(),
            coercion: coercion.clone(),
        },

        Predicate::MapContainsValue {
            field,
            value,
            coercion,
        } => Predicate::MapContainsValue {
            field: field.clone(),
            value: value.clone(),
            coercion: coercion.clone(),
        },

        Predicate::MapContainsEntry {
            field,
            key,
            value,
            coercion,
        } => Predicate::MapContainsEntry {
            field: field.clone(),
            key: key.clone(),
            value: value.clone(),
            coercion: coercion.clone(),
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
const PRED_MAP_CONTAINS_KEY: u8 = 0x0A;
const PRED_MAP_CONTAINS_VALUE: u8 = 0x0B;
const PRED_MAP_CONTAINS_ENTRY: u8 = 0x0C;

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
            out.push(compare_op_tag(cmp.op));
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
        Predicate::MapContainsKey {
            field,
            key,
            coercion,
        } => {
            out.push(PRED_MAP_CONTAINS_KEY);
            push_str(out, field);
            push_value(out, key);
            push_coercion(out, coercion);
        }
        Predicate::MapContainsValue {
            field,
            value,
            coercion,
        } => {
            out.push(PRED_MAP_CONTAINS_VALUE);
            push_str(out, field);
            push_value(out, value);
            push_coercion(out, coercion);
        }
        Predicate::MapContainsEntry {
            field,
            key,
            value,
            coercion,
        } => {
            out.push(PRED_MAP_CONTAINS_ENTRY);
            push_str(out, field);
            push_value(out, key);
            push_value(out, value);
            push_coercion(out, coercion);
        }
    }
}

const VALUE_ACCOUNT: u8 = 1;
const VALUE_BLOB: u8 = 2;
const VALUE_BOOL: u8 = 3;
const VALUE_DATE: u8 = 4;
const VALUE_DECIMAL: u8 = 5;
const VALUE_DURATION: u8 = 6;
const VALUE_ENUM: u8 = 7;
const VALUE_E8S: u8 = 8;
const VALUE_E18S: u8 = 9;
const VALUE_FLOAT32: u8 = 10;
const VALUE_FLOAT64: u8 = 11;
const VALUE_INT: u8 = 12;
const VALUE_INT128: u8 = 13;
const VALUE_INT_BIG: u8 = 14;
const VALUE_LIST: u8 = 15;
const VALUE_NONE: u8 = 16;
const VALUE_PRINCIPAL: u8 = 17;
const VALUE_SUBACCOUNT: u8 = 18;
const VALUE_TEXT: u8 = 19;
const VALUE_TIMESTAMP: u8 = 20;
const VALUE_UINT: u8 = 21;
const VALUE_UINT128: u8 = 22;
const VALUE_UINT_BIG: u8 = 23;
const VALUE_ULID: u8 = 24;
const VALUE_UNIT: u8 = 25;
const VALUE_UNSUPPORTED: u8 = 26;

#[expect(clippy::too_many_lines)]
fn encode_value_key(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Account(v) => {
            out.push(VALUE_ACCOUNT);
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
            out.push(VALUE_BLOB);
            push_bytes(out, v);
        }
        Value::Bool(v) => {
            out.push(VALUE_BOOL);
            out.push(u8::from(*v));
        }
        Value::Date(v) => {
            out.push(VALUE_DATE);
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::Decimal(v) => {
            out.push(VALUE_DECIMAL);
            out.push(u8::from(v.is_sign_negative()));
            out.extend_from_slice(&v.scale().to_be_bytes());
            out.extend_from_slice(&v.mantissa().to_be_bytes());
        }
        Value::Duration(v) => {
            out.push(VALUE_DURATION);
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::Enum(v) => {
            out.push(VALUE_ENUM);
            push_enum(out, v);
        }
        Value::E8s(v) => {
            out.push(VALUE_E8S);
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::E18s(v) => {
            out.push(VALUE_E18S);
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::Float32(v) => {
            out.push(VALUE_FLOAT32);
            out.extend_from_slice(&v.to_be_bytes());
        }
        Value::Float64(v) => {
            out.push(VALUE_FLOAT64);
            out.extend_from_slice(&v.to_be_bytes());
        }
        Value::Int(v) => {
            out.push(VALUE_INT);
            out.extend_from_slice(&v.to_be_bytes());
        }
        Value::Int128(v) => {
            out.push(VALUE_INT128);
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::IntBig(v) => {
            out.push(VALUE_INT_BIG);
            push_bytes(out, &v.to_leb128());
        }
        Value::List(items) => {
            out.push(VALUE_LIST);
            push_len(out, items.len());
            for item in items {
                push_value(out, item);
            }
        }
        Value::None => out.push(VALUE_NONE),
        Value::Principal(v) => {
            out.push(VALUE_PRINCIPAL);
            push_bytes(out, v.as_slice());
        }
        Value::Subaccount(v) => {
            out.push(VALUE_SUBACCOUNT);
            push_bytes(out, &v.to_bytes());
        }
        Value::Text(v) => {
            out.push(VALUE_TEXT);
            push_str(out, v);
        }
        Value::Timestamp(v) => {
            out.push(VALUE_TIMESTAMP);
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::Uint(v) => {
            out.push(VALUE_UINT);
            out.extend_from_slice(&v.to_be_bytes());
        }
        Value::Uint128(v) => {
            out.push(VALUE_UINT128);
            out.extend_from_slice(&v.get().to_be_bytes());
        }
        Value::UintBig(v) => {
            out.push(VALUE_UINT_BIG);
            push_bytes(out, &v.to_leb128());
        }
        Value::Ulid(v) => {
            out.push(VALUE_ULID);
            out.extend_from_slice(&v.to_bytes());
        }
        Value::Unit => out.push(VALUE_UNIT),
        Value::Unsupported => out.push(VALUE_UNSUPPORTED),
    }
}

fn push_predicate(out: &mut Vec<u8>, predicate: &Predicate) {
    let mut buf = Vec::new();
    encode_predicate_key(&mut buf, predicate);
    push_bytes(out, &buf);
}

fn push_value(out: &mut Vec<u8>, value: &Value) {
    let mut buf = Vec::new();
    encode_value_key(&mut buf, value);
    push_bytes(out, &buf);
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

const fn compare_op_tag(op: CompareOp) -> u8 {
    match op {
        CompareOp::Eq => 0,
        CompareOp::Ne => 1,
        CompareOp::Lt => 2,
        CompareOp::Lte => 3,
        CompareOp::Gt => 4,
        CompareOp::Gte => 5,
        CompareOp::In => 6,
        CompareOp::NotIn => 7,
        CompareOp::AnyIn => 8,
        CompareOp::AllIn => 9,
        CompareOp::Contains => 10,
        CompareOp::StartsWith => 11,
        CompareOp::EndsWith => 12,
    }
}

const fn coercion_id_tag(id: CoercionId) -> u8 {
    match id {
        CoercionId::Strict => 0,
        CoercionId::NumericWiden => 1,
        CoercionId::IdentifierText => 2,
        CoercionId::TextCasefold => 3,
        CoercionId::CollectionElement => 4,
    }
}

fn push_len(out: &mut Vec<u8>, len: usize) {
    let len = u64::try_from(len).unwrap_or(u64::MAX);
    out.extend_from_slice(&len.to_be_bytes());
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
