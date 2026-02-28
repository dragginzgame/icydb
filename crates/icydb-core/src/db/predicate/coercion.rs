//! Module: predicate::coercion
//! Responsibility: coercion identifiers/specs and family support matching.
//! Does not own: predicate AST evaluation or schema literal validation.
//! Boundary: consumed by predicate schema/semantics/runtime layers.

use crate::value::CoercionFamily;
use std::collections::BTreeMap;

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
