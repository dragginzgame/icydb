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
    E8s,
    E18s,
    Float32,
    Float64,
    Int,
    Int128,
    IntBig,
    Principal,
    Subaccount,
    Text,
    Timestamp,
    Uint,
    Uint128,
    UintBig,
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
#[allow(clippy::struct_excessive_bools)]
pub struct ScalarMetadata {
    pub family: ScalarCoercionFamily,
    pub is_numeric_value: bool,
    pub supports_numeric_coercion: bool,
    pub supports_arithmetic: bool,
    pub supports_equality: bool,
    pub supports_ordering: bool,
    pub is_keyable: bool,
    pub is_storage_key_encodable: bool,
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
pub const ALL_SCALAR_KINDS: [ScalarKind; 23] = scalar_kind_registry!(all_kinds_from_registry);
