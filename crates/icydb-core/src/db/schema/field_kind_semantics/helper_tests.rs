//! Test-only constructors and adapters for the parent module.

use super::*;

impl AcceptedFieldKindSemantics {
    /// Return true when the field kind or relation key is signed numeric.
    #[must_use]
    pub(in crate::db) const fn is_signed_numeric(self) -> bool {
        match self.category.scalar_kind() {
            Some(kind) => scalar_kind_is_signed_numeric(kind),
            None => false,
        }
    }

    /// Return true when the field kind or relation key is unsigned numeric.
    #[must_use]
    pub(in crate::db) const fn is_unsigned_numeric(self) -> bool {
        match self.category.scalar_kind() {
            Some(kind) => scalar_kind_is_unsigned_numeric(kind),
            None => false,
        }
    }
}

pub(super) const fn scalar_kind_is_signed_numeric(kind: ScalarKind) -> bool {
    matches!(kind, ScalarKind::Int | ScalarKind::Int128)
}

pub(super) const fn scalar_kind_is_unsigned_numeric(kind: ScalarKind) -> bool {
    matches!(kind, ScalarKind::Nat | ScalarKind::Nat128)
}
