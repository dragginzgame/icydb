use crate::{
    db::predicate::{
        CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate, SchemaInfo, ValidateError,
    },
    model::field::FieldKind,
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
pub(in crate::db) fn normalize(predicate: &Predicate) -> Predicate {
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
/// Normalize enum literals in predicates against schema enum metadata.
///
/// Contract:
/// - strict enum literals (`path = Some`) must match the schema enum path
/// - loose enum literals (`path = None`) are resolved once at filter construction
/// - predicate semantics stay strict at runtime (`Eq` is unchanged)
///
pub(in crate::db) fn normalize_enum_literals(
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> Result<Predicate, ValidateError> {
    match predicate {
        Predicate::True => Ok(Predicate::True),
        Predicate::False => Ok(Predicate::False),
        Predicate::And(children) => {
            let mut normalized = Vec::with_capacity(children.len());
            for child in children {
                normalized.push(normalize_enum_literals(schema, child)?);
            }

            Ok(Predicate::And(normalized))
        }
        Predicate::Or(children) => {
            let mut normalized = Vec::with_capacity(children.len());
            for child in children {
                normalized.push(normalize_enum_literals(schema, child)?);
            }

            Ok(Predicate::Or(normalized))
        }
        Predicate::Not(inner) => Ok(Predicate::Not(Box::new(normalize_enum_literals(
            schema, inner,
        )?))),
        Predicate::Compare(cmp) => Ok(Predicate::Compare(normalize_compare_with_schema(
            schema, cmp,
        )?)),
        Predicate::IsNull { field } => Ok(Predicate::IsNull {
            field: field.clone(),
        }),
        Predicate::IsMissing { field } => Ok(Predicate::IsMissing {
            field: field.clone(),
        }),
        Predicate::IsEmpty { field } => Ok(Predicate::IsEmpty {
            field: field.clone(),
        }),
        Predicate::IsNotEmpty { field } => Ok(Predicate::IsNotEmpty {
            field: field.clone(),
        }),
        Predicate::TextContains { field, value } => Ok(Predicate::TextContains {
            field: field.clone(),
            value: value.clone(),
        }),
        Predicate::TextContainsCi { field, value } => Ok(Predicate::TextContainsCi {
            field: field.clone(),
            value: value.clone(),
        }),
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

fn normalize_compare_with_schema(
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Result<ComparePredicate, ValidateError> {
    let Some(field_kind) = schema.field_kind(&cmp.field) else {
        return Ok(cmp.clone());
    };

    let value = normalize_compare_value_for_kind(&cmp.field, cmp.op, &cmp.value, field_kind)?;

    Ok(ComparePredicate {
        field: cmp.field.clone(),
        op: cmp.op,
        value,
        coercion: cmp.coercion.clone(),
    })
}

fn normalize_compare_value_for_kind(
    field: &str,
    op: CompareOp,
    value: &Value,
    field_kind: &FieldKind,
) -> Result<Value, ValidateError> {
    match op {
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };

            let mut normalized = Vec::with_capacity(values.len());
            for item in values {
                normalized.push(normalize_value_for_kind(field, item, field_kind)?);
            }

            Ok(Value::List(normalized))
        }
        CompareOp::Contains => {
            let element_kind = match field_kind {
                FieldKind::List(inner) | FieldKind::Set(inner) => *inner,
                _ => return Ok(value.clone()),
            };

            normalize_value_for_kind(field, value, element_kind)
        }
        _ => normalize_value_for_kind(field, value, field_kind),
    }
}

fn normalize_value_for_kind(
    field: &str,
    value: &Value,
    expected_kind: &FieldKind,
) -> Result<Value, ValidateError> {
    match expected_kind {
        FieldKind::Enum { path } => normalize_enum_value(field, value, path),
        FieldKind::Relation { key_kind, .. } => normalize_value_for_kind(field, value, key_kind),
        FieldKind::List(inner) => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };

            let mut normalized = Vec::with_capacity(values.len());
            for item in values {
                normalized.push(normalize_value_for_kind(field, item, inner)?);
            }

            Ok(Value::List(normalized))
        }
        FieldKind::Set(inner) => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };

            let mut normalized = Vec::with_capacity(values.len());
            for item in values {
                normalized.push(normalize_value_for_kind(field, item, inner)?);
            }

            // Canonicalize set literals to match persisted set encoding:
            // deterministic order + deduplicated members.
            normalized.sort_by(Value::canonical_cmp);
            normalized.dedup();

            Ok(Value::List(normalized))
        }
        FieldKind::Map {
            key,
            value: map_value,
        } => {
            let Value::Map(entries) = value else {
                return Ok(value.clone());
            };

            let mut normalized = Vec::with_capacity(entries.len());
            for (entry_key, entry_value) in entries {
                let key = normalize_value_for_kind(field, entry_key, key)?;
                let value = normalize_value_for_kind(field, entry_value, map_value)?;
                normalized.push((key, value));
            }

            Ok(Value::Map(normalized))
        }
        FieldKind::Account
        | FieldKind::Blob
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Text
        | FieldKind::Timestamp
        | FieldKind::Uint
        | FieldKind::Uint128
        | FieldKind::UintBig
        | FieldKind::Ulid
        | FieldKind::Unit
        | FieldKind::Structured { .. } => Ok(value.clone()),
    }
}

fn normalize_enum_value(
    field: &str,
    value: &Value,
    expected_path: &str,
) -> Result<Value, ValidateError> {
    let Value::Enum(enum_value) = value else {
        return Ok(value.clone());
    };

    if let Some(path) = enum_value.path.as_deref() {
        if path != expected_path {
            return Err(ValidateError::invalid_literal(
                field,
                "enum path does not match field enum type",
            ));
        }

        return Ok(value.clone());
    }

    let mut normalized = enum_value.clone();
    normalized.path = Some(expected_path.to_string());
    Ok(Value::Enum(normalized))
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
            let normalized = v.normalize();
            out.push(u8::from(normalized.is_sign_negative()));
            out.extend_from_slice(&normalized.scale().to_be_bytes());
            out.extend_from_slice(&normalized.mantissa().to_be_bytes());
        }
        Value::Duration(v) => {
            out.extend_from_slice(&v.as_millis().to_be_bytes());
        }
        Value::Enum(v) => {
            push_enum(out, v);
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
            out.extend_from_slice(&v.as_millis().to_be_bytes());
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
