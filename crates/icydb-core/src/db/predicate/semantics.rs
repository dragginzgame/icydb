use crate::value::{CoercionFamily, TextMode, Value};
use std::{cmp::Ordering, collections::BTreeMap, mem::discriminant};

///
/// CoercionId
///
/// Identifier for an explicit comparison coercion policy.
///
/// Coercions express *how* values may be compared, not whether a comparison
/// is valid for a given field. Validation and planning enforce legality.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CoercionId {
    Strict,
    NumericWiden,
    TextCasefold,
    CollectionElement,
}

impl CoercionId {
    /// Stable tag used by plan hash encodings (fingerprint/continuation).
    #[must_use]
    pub const fn plan_hash_tag(self) -> u8 {
        match self {
            Self::Strict => 0x01,
            Self::NumericWiden => 0x02,
            Self::TextCasefold => 0x04,
            Self::CollectionElement => 0x05,
        }
    }
}

///
/// CoercionSpec
///
/// Fully-specified coercion policy for predicate comparisons.
///

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

///
/// CoercionRuleFamily
///
/// Rule-side matcher for coercion routing families.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoercionRuleFamily {
    Any,
    Family(CoercionFamily),
}

///
/// CoercionRule
///
/// Declarative coercion routing rule between value families.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CoercionRule {
    pub left: CoercionRuleFamily,
    pub right: CoercionRuleFamily,
    pub id: CoercionId,
}

pub(crate) const COERCION_TABLE: &[CoercionRule] = &[
    CoercionRule {
        left: CoercionRuleFamily::Any,
        right: CoercionRuleFamily::Any,
        id: CoercionId::Strict,
    },
    CoercionRule {
        left: CoercionRuleFamily::Family(CoercionFamily::Numeric),
        right: CoercionRuleFamily::Family(CoercionFamily::Numeric),
        id: CoercionId::NumericWiden,
    },
    CoercionRule {
        left: CoercionRuleFamily::Family(CoercionFamily::Textual),
        right: CoercionRuleFamily::Family(CoercionFamily::Textual),
        id: CoercionId::TextCasefold,
    },
    CoercionRule {
        left: CoercionRuleFamily::Any,
        right: CoercionRuleFamily::Any,
        id: CoercionId::CollectionElement,
    },
];

/// Returns whether a coercion rule exists for the provided routing families.
#[must_use]
pub(in crate::db) fn supports_coercion(
    left: CoercionFamily,
    right: CoercionFamily,
    id: CoercionId,
) -> bool {
    COERCION_TABLE.iter().any(|rule| {
        rule.id == id && family_matches(rule.left, left) && family_matches(rule.right, right)
    })
}

fn family_matches(rule: CoercionRuleFamily, value: CoercionFamily) -> bool {
    match rule {
        CoercionRuleFamily::Any => true,
        CoercionRuleFamily::Family(expected) => expected == value,
    }
}

///
/// TextOp
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum TextOp {
    StartsWith,
    EndsWith,
}

/// Perform equality comparison under an explicit coercion policy.
#[must_use]
pub(in crate::db) fn compare_eq(
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => {
            same_variant(left, right).then_some(left == right)
        }
        CoercionId::NumericWiden => {
            if !left.supports_numeric_coercion() || !right.supports_numeric_coercion() {
                return None;
            }

            left.cmp_numeric(right).map(|ord| ord == Ordering::Equal)
        }
        CoercionId::TextCasefold => compare_casefold(left, right),
    }
}

/// Perform ordering comparison under an explicit coercion policy.
#[must_use]
pub(in crate::db) fn compare_order(
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
) -> Option<Ordering> {
    match coercion.id {
        CoercionId::Strict | CoercionId::CollectionElement => {
            if !same_variant(left, right) {
                return None;
            }
            Value::strict_order_cmp(left, right)
        }
        CoercionId::NumericWiden => {
            if !left.supports_numeric_coercion() || !right.supports_numeric_coercion() {
                return None;
            }

            left.cmp_numeric(right)
        }
        CoercionId::TextCasefold => {
            let left = casefold_value(left)?;
            let right = casefold_value(right)?;
            Some(left.cmp(&right))
        }
    }
}

/// Perform strict same-variant ordering using predicate comparison semantics.
#[must_use]
pub(in crate::db) fn strict_value_order(left: &Value, right: &Value) -> Option<Ordering> {
    compare_order(left, right, &CoercionSpec::new(CoercionId::Strict))
}

/// Canonical total ordering for database predicate semantics.
#[must_use]
pub(in crate::db) fn canonical_cmp(left: &Value, right: &Value) -> Ordering {
    if let Some(ordering) = Value::strict_order_cmp(left, right) {
        return ordering;
    }

    left.canonical_rank().cmp(&right.canonical_rank())
}

/// Perform text-specific comparison operations.
#[must_use]
pub(in crate::db) fn compare_text(
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
    op: TextOp,
) -> Option<bool> {
    if !matches!(left, Value::Text(_)) || !matches!(right, Value::Text(_)) {
        return None;
    }

    let mode = match coercion.id {
        CoercionId::Strict => TextMode::Cs,
        CoercionId::TextCasefold => TextMode::Ci,
        _ => return None,
    };

    match op {
        TextOp::StartsWith => left.text_starts_with(right, mode),
        TextOp::EndsWith => left.text_ends_with(right, mode),
    }
}

fn same_variant(left: &Value, right: &Value) -> bool {
    discriminant(left) == discriminant(right)
}

fn compare_casefold(left: &Value, right: &Value) -> Option<bool> {
    let left = casefold_value(left)?;
    let right = casefold_value(right)?;
    Some(left == right)
}

fn casefold_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(casefold(text)),
        _ => None,
    }
}

fn casefold(input: &str) -> String {
    if input.is_ascii() {
        return input.to_ascii_lowercase();
    }

    input.to_lowercase()
}
