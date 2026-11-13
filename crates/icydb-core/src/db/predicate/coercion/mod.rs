//! Module: predicate::coercion
//! Responsibility: coercion identifiers/specs and family support matching.
//! Does not own: predicate AST evaluation or schema literal validation.
//! Boundary: consumed by predicate schema/semantics/runtime layers.

use crate::value::CoercionFamily;

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
    /// Compare operands without widening or text folding.
    Strict,
    /// Admit compatible numeric representations through widening.
    NumericWiden,
    /// Apply the canonical case-insensitive text fold.
    TextCasefold,
    /// Apply element-level coercion to collection operands.
    CollectionElement,
}

///
/// CoercionSpec
///
/// Fully-specified coercion policy for predicate comparisons.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoercionSpec {
    pub(crate) id: CoercionId,
}

impl CoercionSpec {
    #[must_use]
    pub const fn new(id: CoercionId) -> Self {
        Self { id }
    }

    /// Return the canonical coercion identifier.
    #[must_use]
    pub const fn id(&self) -> CoercionId {
        self.id
    }
}

impl Default for CoercionSpec {
    fn default() -> Self {
        Self::new(CoercionId::Strict)
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
mod tests;

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_copy!(CoercionId);
crate::retained::retained_fields!(CoercionSpec {
Self{id} => [id],
});
