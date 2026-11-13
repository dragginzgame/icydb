//! Canonical scalar-kind identities and intrinsic capability metadata.
//!
//! Responsibility: canonical scalar-kind identities and intrinsic capability
//! facts used by the public proposal contract and consumed by runtime policy.
//! Does not own: query coercion decisions, storage encoding, or accepted-schema
//! validation.
//! Boundary: exposes compact scalar-kind capability descriptors and registry order.

use serde::{Deserialize, Serialize};

///
/// ScalarKind
///
/// Canonical scalar kind used for shared capability metadata.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum ScalarKind {
    /// Account identifier.
    Account,
    /// Arbitrary binary bytes.
    Blob,
    /// Boolean value.
    Bool,
    /// Calendar date at day precision.
    Date,
    /// Fixed-point decimal value.
    Decimal,
    /// Duration value.
    Duration,
    /// Named enumeration value.
    Enum,
    /// 32-bit floating-point value.
    Float32,
    /// 64-bit floating-point value.
    Float64,
    /// Signed integer value.
    Int,
    /// 128-bit signed integer value.
    Int128,
    /// Internet Computer principal identifier.
    Principal,
    /// Account subaccount identifier.
    Subaccount,
    /// Unicode text.
    Text,
    /// Timestamp value.
    Timestamp,
    /// Unsigned integer value.
    Nat,
    /// 128-bit unsigned integer value.
    Nat128,
    /// 256-bit unsigned integer value.
    U256,
    /// ULID identifier.
    Ulid,
    /// Unit value with no payload.
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
    pub const fn is_primary_key_component_encodable(self) -> bool {
        self.metadata().is_primary_key_component_encodable
    }
}

///
/// ScalarMetadata
///
/// Capability metadata shared across schema/core layers.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[expect(clippy::struct_excessive_bools)]
pub struct ScalarMetadata {
    family: ScalarCoercionFamily,
    is_numeric_value: bool,
    supports_numeric_coercion: bool,
    supports_arithmetic: bool,
    supports_equality: bool,
    supports_ordering: bool,
    is_keyable: bool,
    is_primary_key_component_encodable: bool,
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
    pub const fn is_primary_key_component_encodable(self) -> bool {
        self.is_primary_key_component_encodable
    }
}

///
/// ScalarCoercionFamily
///
/// Coarse intrinsic family used by downstream coercion and validation policy.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ScalarCoercionFamily {
    /// Numeric scalar coercions.
    Numeric,
    /// Text scalar coercions.
    Textual,
    /// Identifier scalar coercions.
    Identifier,
    /// Named enumeration value.
    Enum,
    /// Arbitrary binary bytes.
    Blob,
    /// Boolean value.
    Bool,
    /// Unit value with no payload.
    Unit,
}

/// Ordered list of all scalar kinds in registry order.
pub const ALL_SCALAR_KINDS: [ScalarKind; 20] = scalar_kind_registry!(all_kinds_from_registry);

#[cfg(test)]
mod tests;
