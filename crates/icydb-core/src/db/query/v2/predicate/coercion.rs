use crate::{
    types::{Account, Principal, Ulid},
    value::{TextMode, Value, ValueFamily},
};
use std::{cmp::Ordering, collections::BTreeMap, mem::discriminant, str::FromStr};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CoercionId {
    Strict,
    NumericWiden,
    IdentifierText,
    TextCasefold,
    CollectionElement,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoercionSpec {
    pub id: CoercionId,
    pub params: BTreeMap<String, String>,
}

impl CoercionSpec {
    #[must_use]
    pub const fn new(id: CoercionId) -> Self {
        Self {
            id,
            params: BTreeMap::new(),
        }
    }
}

impl Default for CoercionSpec {
    fn default() -> Self {
        Self::new(CoercionId::Strict)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoercionFamily {
    Any,
    Family(ValueFamily),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoercionRule {
    pub left: CoercionFamily,
    pub right: CoercionFamily,
    pub id: CoercionId,
}

pub const COERCION_TABLE: &[CoercionRule] = &[
    CoercionRule {
        left: CoercionFamily::Any,
        right: CoercionFamily::Any,
        id: CoercionId::Strict,
    },
    CoercionRule {
        left: CoercionFamily::Family(ValueFamily::Numeric),
        right: CoercionFamily::Family(ValueFamily::Numeric),
        id: CoercionId::NumericWiden,
    },
    CoercionRule {
        left: CoercionFamily::Family(ValueFamily::Identifier),
        right: CoercionFamily::Family(ValueFamily::Textual),
        id: CoercionId::IdentifierText,
    },
    CoercionRule {
        left: CoercionFamily::Family(ValueFamily::Textual),
        right: CoercionFamily::Family(ValueFamily::Identifier),
        id: CoercionId::IdentifierText,
    },
    CoercionRule {
        left: CoercionFamily::Family(ValueFamily::Textual),
        right: CoercionFamily::Family(ValueFamily::Textual),
        id: CoercionId::TextCasefold,
    },
    CoercionRule {
        left: CoercionFamily::Family(ValueFamily::Identifier),
        right: CoercionFamily::Family(ValueFamily::Identifier),
        id: CoercionId::TextCasefold,
    },
    CoercionRule {
        left: CoercionFamily::Family(ValueFamily::Identifier),
        right: CoercionFamily::Family(ValueFamily::Textual),
        id: CoercionId::TextCasefold,
    },
    CoercionRule {
        left: CoercionFamily::Family(ValueFamily::Textual),
        right: CoercionFamily::Family(ValueFamily::Identifier),
        id: CoercionId::TextCasefold,
    },
    CoercionRule {
        left: CoercionFamily::Any,
        right: CoercionFamily::Any,
        id: CoercionId::CollectionElement,
    },
];

#[must_use]
pub fn supports_coercion(left: ValueFamily, right: ValueFamily, id: CoercionId) -> bool {
    COERCION_TABLE.iter().any(|rule| {
        rule.id == id && family_matches(rule.left, left) && family_matches(rule.right, right)
    })
}

fn family_matches(rule: CoercionFamily, value: ValueFamily) -> bool {
    match rule {
        CoercionFamily::Any => true,
        CoercionFamily::Family(expected) => expected == value,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextOp {
    Eq,
    Contains,
    StartsWith,
    EndsWith,
}

#[must_use]
pub fn compare_eq(left: &Value, right: &Value, coercion: &CoercionSpec) -> Option<bool> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => {
            same_variant(left, right).then_some(left == right)
        }
        CoercionId::NumericWiden => left.cmp_numeric(right).map(|ord| ord == Ordering::Equal),
        CoercionId::IdentifierText => {
            let (l, r) = coerce_identifier_text(left, right)?;
            Some(l == r)
        }
        CoercionId::TextCasefold => compare_casefold(left, right),
    }
}

#[must_use]
pub fn compare_order(left: &Value, right: &Value, coercion: &CoercionSpec) -> Option<Ordering> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => {
            if !same_variant(left, right) {
                return None;
            }
            strict_ordering(left, right)
        }
        CoercionId::NumericWiden => left.cmp_numeric(right),
        CoercionId::IdentifierText => {
            let (l, r) = coerce_identifier_text(left, right)?;
            strict_ordering(&l, &r)
        }
        CoercionId::TextCasefold => {
            let left = casefold_value(left)?;
            let right = casefold_value(right)?;
            Some(left.cmp(&right))
        }
    }
}

#[must_use]
pub fn compare_text(
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
    op: TextOp,
) -> Option<bool> {
    let mode = match coercion.id {
        CoercionId::Strict => TextMode::Cs,
        CoercionId::TextCasefold => TextMode::Ci,
        _ => return None,
    };

    match op {
        TextOp::Eq => left.text_eq(right, mode),
        TextOp::Contains => left.text_contains(right, mode),
        TextOp::StartsWith => left.text_starts_with(right, mode),
        TextOp::EndsWith => left.text_ends_with(right, mode),
    }
}

fn same_variant(left: &Value, right: &Value) -> bool {
    discriminant(left) == discriminant(right)
}

fn strict_ordering(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Account(a), Value::Account(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
        (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
        (Value::Decimal(a), Value::Decimal(b)) => a.partial_cmp(b),
        (Value::Duration(a), Value::Duration(b)) => a.partial_cmp(b),
        (Value::E8s(a), Value::E8s(b)) => a.partial_cmp(b),
        (Value::E18s(a), Value::E18s(b)) => a.partial_cmp(b),
        (Value::Enum(a), Value::Enum(b)) => a.partial_cmp(b),
        (Value::Float32(a), Value::Float32(b)) => a.partial_cmp(b),
        (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
        (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
        (Value::Int128(a), Value::Int128(b)) => a.partial_cmp(b),
        (Value::IntBig(a), Value::IntBig(b)) => a.partial_cmp(b),
        (Value::Principal(a), Value::Principal(b)) => a.partial_cmp(b),
        (Value::Subaccount(a), Value::Subaccount(b)) => a.partial_cmp(b),
        (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
        (Value::Uint(a), Value::Uint(b)) => a.partial_cmp(b),
        (Value::Uint128(a), Value::Uint128(b)) => a.partial_cmp(b),
        (Value::UintBig(a), Value::UintBig(b)) => a.partial_cmp(b),
        (Value::Ulid(a), Value::Ulid(b)) => a.partial_cmp(b),
        (Value::Unit, Value::Unit) => Some(Ordering::Equal),
        _ => None,
    }
}

fn coerce_identifier_text(left: &Value, right: &Value) -> Option<(Value, Value)> {
    match (left, right) {
        (Value::Ulid(_) | Value::Principal(_) | Value::Account(_), Value::Text(_)) => {
            let parsed = parse_identifier_text(left, right)?;
            Some((left.clone(), parsed))
        }
        (Value::Text(_), Value::Ulid(_) | Value::Principal(_) | Value::Account(_)) => {
            let parsed = parse_identifier_text(right, left)?;
            Some((parsed, right.clone()))
        }
        _ => None,
    }
}

fn parse_identifier_text(identifier: &Value, text: &Value) -> Option<Value> {
    let Value::Text(raw) = text else {
        return None;
    };

    match identifier {
        Value::Ulid(_) => Ulid::from_str(raw).ok().map(Value::Ulid),
        Value::Principal(_) => Principal::from_str(raw).ok().map(Value::Principal),
        Value::Account(_) => Account::from_str(raw).ok().map(Value::Account),
        _ => None,
    }
}

fn compare_casefold(left: &Value, right: &Value) -> Option<bool> {
    let left = casefold_value(left)?;
    let right = casefold_value(right)?;
    Some(left == right)
}

fn casefold_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(casefold(text)),
        Value::Ulid(ulid) => Some(casefold(&ulid.to_string())),
        Value::Principal(principal) => Some(casefold(&principal.to_string())),
        Value::Account(account) => Some(casefold(&account.to_string())),
        _ => None,
    }
}

fn casefold(input: &str) -> String {
    if input.is_ascii() {
        return input.to_ascii_lowercase();
    }

    // Unicode fallback; matches Value::text_* casefold behavior.
    input.to_lowercase()
}
