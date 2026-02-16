use crate::value::{CoercionFamily, TextMode, Value};
use std::{cmp::Ordering, collections::BTreeMap, mem::discriminant};

///
/// Predicate coercion and comparison semantics
///
/// Defines which runtime value comparisons are permitted under
/// explicit coercion policies, and how those comparisons behave.
/// This module is schema-agnostic and planner-agnostic; it operates
/// purely on runtime `Value`s and declared coercion intent.
///

///
/// CoercionId
///
/// Identifier for an explicit coercion policy.
///
/// Coercions express *how* values may be compared, not whether
/// a comparison is semantically valid for a given field.
/// Validation and planning enforce legality separately.
///
/// CollectionElement is used when comparing a scalar literal
/// against individual elements of a collection field.
/// It must never be used for scalar-vs-scalar comparisons.
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
/// Fully-specified coercion policy.
///
/// Carries a coercion identifier plus optional parameters.
/// Parameters are currently unused but reserved for future
/// extensions without changing the predicate AST.
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
/// This exists only to express "any" versus an exact family in the coercion table.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoercionRuleFamily {
    Any,
    Family(CoercionFamily),
}

///
/// CoercionRule
///
/// Declarative table defining which coercions are supported
/// between value families.
///
/// This table is intentionally conservative; absence of a rule
/// means the coercion is not permitted.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoercionRule {
    pub left: CoercionRuleFamily,
    pub right: CoercionRuleFamily,
    pub id: CoercionId,
}

// CoercionFamily is a routing category only.
// Capability checks (numeric coercion eligibility, etc.) are registry-driven
// and must be applied before consulting this table.
pub const COERCION_TABLE: &[CoercionRule] = &[
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
pub fn supports_coercion(left: CoercionFamily, right: CoercionFamily, id: CoercionId) -> bool {
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
pub enum TextOp {
    StartsWith,
    EndsWith,
}

/// Perform equality comparison under an explicit coercion.
///
/// Returns `None` if the comparison is not defined for the
/// given values and coercion.
#[must_use]
pub fn compare_eq(left: &Value, right: &Value, coercion: &CoercionSpec) -> Option<bool> {
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

/// Perform ordering comparison under an explicit coercion.
///
/// Returns `None` if ordering is undefined for the given
/// values or coercion.
#[must_use]
pub fn compare_order(left: &Value, right: &Value, coercion: &CoercionSpec) -> Option<Ordering> {
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

/// Canonical total ordering for database semantics.
///
/// This is the only ordering used for:
/// - ORDER BY
/// - range planning
/// - key comparisons
#[must_use]
pub(crate) fn canonical_cmp(left: &Value, right: &Value) -> Ordering {
    if let Some(ordering) = Value::strict_order_cmp(left, right) {
        return ordering;
    }

    left.canonical_rank().cmp(&right.canonical_rank())
}

/// Perform text-specific comparison operations.
///
/// Only strict and casefold coercions are supported.
/// Other coercions return `None`.
#[must_use]
pub fn compare_text(
    left: &Value,
    right: &Value,
    coercion: &CoercionSpec,
    op: TextOp,
) -> Option<bool> {
    if !matches!(left, Value::Text(_)) || !matches!(right, Value::Text(_)) {
        // CONTRACT: text coercions never apply to non-text values.
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

/// Convert a value to its casefolded textual representation,
/// if supported.
fn casefold_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(casefold(text)),
        // CONTRACT: identifiers and structured values never casefold.
        _ => {
            // NOTE: Non-text values do not casefold.
            None
        }
    }
}

fn casefold(input: &str) -> String {
    if input.is_ascii() {
        return input.to_ascii_lowercase();
    }

    // Unicode fallback; matches Value::text_* casefold behavior.
    input.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::canonical_cmp;
    use crate::{types::Account, value::Value};
    use std::cmp::Ordering;

    #[test]
    fn canonical_cmp_orders_accounts() {
        let left = Value::Account(Account::dummy(1));
        let right = Value::Account(Account::dummy(2));

        assert_eq!(canonical_cmp(&left, &right), Ordering::Less);
        assert_eq!(canonical_cmp(&right, &left), Ordering::Greater);
    }

    #[test]
    fn canonical_cmp_is_total_for_mixed_variants() {
        let left = Value::Account(Account::dummy(1));
        let right = Value::Text("x".to_string());

        assert_ne!(canonical_cmp(&left, &right), Ordering::Equal);
        assert_eq!(
            canonical_cmp(&left, &right),
            canonical_cmp(&right, &left).reverse()
        );
    }
}
