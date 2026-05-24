#[macro_use]
mod macros;

///
/// ScalarKind
///
/// Canonical scalar kind used for shared capability metadata.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ScalarKind {
    Account,
    Blob,
    Bool,
    Date,
    Decimal,
    Duration,
    Enum,
    Float32,
    Float64,
    Int,
    Int128,
    IntBig,
    Principal,
    Subaccount,
    Text,
    Timestamp,
    Nat,
    Nat128,
    NatBig,
    Ulid,
    Unit,
}

impl ScalarKind {
    /// Return the full metadata descriptor for one scalar kind.
    #[must_use]
    pub const fn metadata(self) -> ScalarMetadata {
        scalar_kind_registry!(metadata_from_registry, self)
    }

    /// Return coercion routing family for this scalar kind.
    #[must_use]
    pub const fn coercion_family(self) -> ScalarCoercionFamily {
        self.metadata().family
    }

    /// Return whether this scalar participates in numeric-valued classification.
    #[must_use]
    pub const fn is_numeric_value(self) -> bool {
        self.metadata().is_numeric_value
    }

    /// Return whether this scalar supports numeric widening coercion.
    #[must_use]
    pub const fn supports_numeric_coercion(self) -> bool {
        self.metadata().supports_numeric_coercion
    }

    /// Return whether this scalar supports arithmetic trait derivation.
    #[must_use]
    pub const fn supports_arithmetic(self) -> bool {
        self.metadata().supports_arithmetic
    }

    /// Return whether this scalar supports equality predicates.
    #[must_use]
    pub const fn supports_equality(self) -> bool {
        self.metadata().supports_equality
    }

    /// Return whether this scalar supports ordering predicates.
    #[must_use]
    pub const fn supports_ordering(self) -> bool {
        self.metadata().supports_ordering
    }

    /// Return whether this scalar is keyable at query/schema level.
    #[must_use]
    pub const fn is_keyable(self) -> bool {
        self.metadata().is_keyable
    }

    /// Return whether this scalar can be encoded as a storage key.
    #[must_use]
    pub const fn is_storage_key_encodable(self) -> bool {
        self.metadata().is_storage_key_encodable
    }
}

///
/// ScalarMetadata
///
/// Capability metadata shared across schema/core layers.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[expect(clippy::struct_excessive_bools)]
pub struct ScalarMetadata {
    family: ScalarCoercionFamily,
    is_numeric_value: bool,
    supports_numeric_coercion: bool,
    supports_arithmetic: bool,
    supports_equality: bool,
    supports_ordering: bool,
    is_keyable: bool,
    is_storage_key_encodable: bool,
}

impl ScalarMetadata {
    /// Return coercion routing family for this scalar metadata entry.
    #[must_use]
    pub const fn family(self) -> ScalarCoercionFamily {
        self.family
    }

    /// Return whether this scalar participates in numeric-valued classification.
    #[must_use]
    pub const fn is_numeric_value(self) -> bool {
        self.is_numeric_value
    }

    /// Return whether this scalar supports numeric widening coercion.
    #[must_use]
    pub const fn supports_numeric_coercion(self) -> bool {
        self.supports_numeric_coercion
    }

    /// Return whether this scalar supports arithmetic trait derivation.
    #[must_use]
    pub const fn supports_arithmetic(self) -> bool {
        self.supports_arithmetic
    }

    /// Return whether this scalar supports equality predicates.
    #[must_use]
    pub const fn supports_equality(self) -> bool {
        self.supports_equality
    }

    /// Return whether this scalar supports ordering predicates.
    #[must_use]
    pub const fn supports_ordering(self) -> bool {
        self.supports_ordering
    }

    /// Return whether this scalar is keyable at query/schema level.
    #[must_use]
    pub const fn is_keyable(self) -> bool {
        self.is_keyable
    }

    /// Return whether this scalar can be encoded as a storage key.
    #[must_use]
    pub const fn is_storage_key_encodable(self) -> bool {
        self.is_storage_key_encodable
    }
}

///
/// ScalarCoercionFamily
///
/// Coarse scalar routing family used by query coercion and validation.
///

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ScalarCoercionFamily {
    Numeric,
    Textual,
    Identifier,
    Enum,
    Blob,
    Bool,
    Unit,
}

/// Ordered list of all scalar kinds in registry order.
pub const ALL_SCALAR_KINDS: [ScalarKind; 21] = scalar_kind_registry!(all_kinds_from_registry);

#[cfg(test)]
mod tests {
    use super::{ALL_SCALAR_KINDS, ScalarKind};
    use std::collections::HashSet;

    const EXPECTED_SCALAR_KINDS: [ScalarKind; 21] = [
        ScalarKind::Account,
        ScalarKind::Blob,
        ScalarKind::Bool,
        ScalarKind::Date,
        ScalarKind::Decimal,
        ScalarKind::Duration,
        ScalarKind::Enum,
        ScalarKind::Float32,
        ScalarKind::Float64,
        ScalarKind::Int,
        ScalarKind::Int128,
        ScalarKind::IntBig,
        ScalarKind::Principal,
        ScalarKind::Subaccount,
        ScalarKind::Text,
        ScalarKind::Timestamp,
        ScalarKind::Nat,
        ScalarKind::Nat128,
        ScalarKind::NatBig,
        ScalarKind::Ulid,
        ScalarKind::Unit,
    ];

    #[test]
    fn all_scalar_kinds_has_expected_length_and_order() {
        assert_eq!(ALL_SCALAR_KINDS, EXPECTED_SCALAR_KINDS);
    }

    #[test]
    fn all_scalar_kinds_is_unique() {
        let unique = ALL_SCALAR_KINDS.into_iter().collect::<HashSet<_>>();

        assert_eq!(unique.len(), ALL_SCALAR_KINDS.len());
    }

    #[test]
    fn all_scalar_kind_variants_are_audited() {
        for kind in EXPECTED_SCALAR_KINDS {
            assert_variant_is_known(kind);
        }
    }

    #[test]
    fn all_scalar_kinds_metadata_is_available() {
        for kind in ALL_SCALAR_KINDS {
            let metadata = kind.metadata();

            assert_eq!(kind.coercion_family(), metadata.family());
            assert_eq!(kind.is_numeric_value(), metadata.is_numeric_value());
            assert_eq!(
                kind.supports_numeric_coercion(),
                metadata.supports_numeric_coercion(),
            );
            assert_eq!(kind.supports_arithmetic(), metadata.supports_arithmetic());
            assert_eq!(kind.supports_equality(), metadata.supports_equality());
            assert_eq!(kind.supports_ordering(), metadata.supports_ordering());
            assert_eq!(kind.is_keyable(), metadata.is_keyable());
            assert_eq!(
                kind.is_storage_key_encodable(),
                metadata.is_storage_key_encodable(),
            );
        }
    }

    fn assert_variant_is_known(kind: ScalarKind) {
        match kind {
            ScalarKind::Account
            | ScalarKind::Blob
            | ScalarKind::Bool
            | ScalarKind::Date
            | ScalarKind::Decimal
            | ScalarKind::Duration
            | ScalarKind::Enum
            | ScalarKind::Float32
            | ScalarKind::Float64
            | ScalarKind::Int
            | ScalarKind::Int128
            | ScalarKind::IntBig
            | ScalarKind::Principal
            | ScalarKind::Subaccount
            | ScalarKind::Text
            | ScalarKind::Timestamp
            | ScalarKind::Nat
            | ScalarKind::Nat128
            | ScalarKind::NatBig
            | ScalarKind::Ulid
            | ScalarKind::Unit => {}
        }
    }
}
