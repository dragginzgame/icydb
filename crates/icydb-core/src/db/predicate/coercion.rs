//! Module: predicate::coercion
//! Responsibility: coercion identifiers/specs and family support matching.
//! Does not own: predicate AST evaluation or schema literal validation.
//! Boundary: consumed by predicate schema/semantics/runtime layers.

use crate::value::CoercionFamily;
use std::fmt;

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

#[derive(Clone, Eq, PartialEq)]
pub struct CoercionSpec {
    pub(crate) id: CoercionId,
    pub(crate) params: Vec<(String, String)>,
}

impl CoercionSpec {
    #[must_use]
    pub const fn new(id: CoercionId) -> Self {
        Self {
            id,
            params: Vec::new(),
        }
    }

    /// Return the canonical coercion identifier.
    #[must_use]
    pub const fn id(&self) -> CoercionId {
        self.id
    }

    /// Borrow any attached coercion parameters.
    #[must_use]
    pub const fn params(&self) -> &[(String, String)] {
        self.params.as_slice()
    }
}

impl fmt::Debug for CoercionSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoercionSpec")
            .field("id", &self.id)
            .field("params", &CoercionParamsDebug(&self.params))
            .finish()
    }
}

impl Default for CoercionSpec {
    fn default() -> Self {
        Self::new(CoercionId::Strict)
    }
}

// Keep verbose predicate diagnostics stable even though coercion params no
// longer retain tree-map machinery internally.
struct CoercionParamsDebug<'a>(&'a [(String, String)]);

impl fmt::Debug for CoercionParamsDebug<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_map();
        for (key, value) in self.0 {
            debug.entry(key, value);
        }

        debug.finish()
    }
}

/// Returns whether a coercion rule exists for the provided routing families.
#[must_use]
pub(in crate::db) fn supports_coercion(
    left: CoercionFamily,
    right: CoercionFamily,
    id: CoercionId,
) -> bool {
    match id {
        CoercionId::Strict | CoercionId::CollectionElement => true,
        CoercionId::NumericWiden => {
            left == CoercionFamily::Numeric && right == CoercionFamily::Numeric
        }
        CoercionId::TextCasefold => {
            left == CoercionFamily::Textual && right == CoercionFamily::Textual
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::predicate::{CoercionId, coercion::supports_coercion},
        value::CoercionFamily,
    };

    #[test]
    fn supports_coercion_matches_canonical_family_matrix() {
        assert!(supports_coercion(
            CoercionFamily::Numeric,
            CoercionFamily::Textual,
            CoercionId::Strict,
        ));
        assert!(supports_coercion(
            CoercionFamily::Textual,
            CoercionFamily::Numeric,
            CoercionId::CollectionElement,
        ));

        assert!(supports_coercion(
            CoercionFamily::Numeric,
            CoercionFamily::Numeric,
            CoercionId::NumericWiden,
        ));
        assert!(!supports_coercion(
            CoercionFamily::Numeric,
            CoercionFamily::Textual,
            CoercionId::NumericWiden,
        ));

        assert!(supports_coercion(
            CoercionFamily::Textual,
            CoercionFamily::Textual,
            CoercionId::TextCasefold,
        ));
        assert!(!supports_coercion(
            CoercionFamily::Textual,
            CoercionFamily::Numeric,
            CoercionId::TextCasefold,
        ));
    }
}
