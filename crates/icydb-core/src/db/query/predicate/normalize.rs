use crate::value::Value;

use super::{
    ast::{ComparePredicate, Predicate},
    coercion::CoercionSpec,
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
/// TODO: value-level normalization/fingerprinting belongs in the value/executor layer.
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

    out.sort_by_key(sort_key);
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

    out.sort_by_key(sort_key);
    Predicate::Or(out)
}

///
/// Generate a deterministic string key for a predicate.
///
/// This key is used **only for sorting**, not for display.
/// Ordering ensures:
/// - planner determinism
/// - stable normalization
/// - predictable equality
///
fn sort_key(predicate: &Predicate) -> String {
    match predicate {
        Predicate::True => "00:true".to_string(),
        Predicate::False => "01:false".to_string(),

        Predicate::And(children) => {
            let mut key = String::from("02:and[");
            for child in children {
                key.push_str(&sort_key(child));
                key.push(';');
            }
            key.push(']');
            key
        }

        Predicate::Or(children) => {
            let mut key = String::from("03:or[");
            for child in children {
                key.push_str(&sort_key(child));
                key.push(';');
            }
            key.push(']');
            key
        }

        Predicate::Not(inner) => format!("04:not({})", sort_key(inner)),

        Predicate::Compare(cmp) => {
            let ComparePredicate {
                field,
                op,
                value,
                coercion,
            } = cmp;
            format!(
                "05:cmp:{field}:{op:?}:{}:{}",
                value_key(value),
                coercion_key(coercion)
            )
        }

        Predicate::IsNull { field } => format!("06:is_null:{field}"),
        Predicate::IsMissing { field } => format!("07:is_missing:{field}"),
        Predicate::IsEmpty { field } => format!("08:is_empty:{field}"),
        Predicate::IsNotEmpty { field } => format!("09:is_not_empty:{field}"),

        Predicate::MapContainsKey {
            field,
            key,
            coercion,
        } => format!(
            "10:map_contains_key:{field}:{}:{}",
            value_key(key),
            coercion_key(coercion)
        ),

        Predicate::MapContainsValue {
            field,
            value,
            coercion,
        } => format!(
            "11:map_contains_value:{field}:{}:{}",
            value_key(value),
            coercion_key(coercion)
        ),

        Predicate::MapContainsEntry {
            field,
            key,
            value,
            coercion,
        } => format!(
            "12:map_contains_entry:{field}:{}:{}:{}",
            value_key(key),
            value_key(value),
            coercion_key(coercion)
        ),
    }
}

///
/// Generate a deterministic key for a coercion specification.
///
/// Includes coercion id and any parameters.
///
fn coercion_key(spec: &CoercionSpec) -> String {
    let mut out = format!("{:?}", spec.id);

    if !spec.params.is_empty() {
        out.push('{');
        for (key, value) in &spec.params {
            out.push_str(key);
            out.push('=');
            out.push_str(value);
            out.push(';');
        }
        out.push('}');
    }

    out
}

///
/// Generate a deterministic string key for a value.
///
/// This is used only for predicate sorting, not serialization.
///
fn value_key(value: &Value) -> String {
    match value {
        Value::Account(v) => format!("account:{v}"),
        Value::Blob(v) => format!("blob:{}", hex_bytes(v)),
        Value::Bool(v) => format!("bool:{v}"),
        Value::Date(v) => format!("date:{v}"),
        Value::Decimal(v) => format!("decimal:{v}"),
        Value::Duration(v) => format!("duration:{v}"),
        Value::Enum(v) => enum_key(v),
        Value::E8s(v) => format!("e8s:{v}"),
        Value::E18s(v) => format!("e18s:{v}"),
        Value::Float32(v) => format!("float32:{v}"),
        Value::Float64(v) => format!("float64:{v}"),
        Value::Int(v) => format!("int:{v}"),
        Value::Int128(v) => format!("int128:{v}"),
        Value::IntBig(v) => format!("int_big:{v}"),
        Value::List(items) => {
            let mut out = String::from("list[");
            for item in items {
                out.push_str(&value_key(item));
                out.push(',');
            }
            out.push(']');
            out
        }
        Value::None => "null".to_string(),
        Value::Principal(v) => format!("principal:{v}"),
        Value::Subaccount(v) => format!("subaccount:{v}"),
        Value::Text(v) => format!("text:{v}"),
        Value::Timestamp(v) => format!("timestamp:{v}"),
        Value::Uint(v) => format!("uint:{v}"),
        Value::Uint128(v) => format!("uint128:{v}"),
        Value::UintBig(v) => format!("uint_big:{v}"),
        Value::Ulid(v) => format!("ulid:{v}"),
        Value::Unit => "unit".to_string(),
        Value::Unsupported => "unsupported".to_string(),
    }
}

///
/// Deterministic key for enum values, including optional payload.
///
fn enum_key(value: &crate::value::ValueEnum) -> String {
    let mut out = String::from("enum:");

    if let Some(path) = &value.path {
        out.push_str(path);
        out.push(':');
    }

    out.push_str(&value.variant);

    if let Some(payload) = &value.payload {
        out.push(':');
        out.push_str(&value_key(payload));
    }

    out
}

///
/// Render a byte slice as lowercase hex.
///
/// Used for stable blob ordering.
///
fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }

    out
}
