//! Module: model::field
//! Responsibility: module-local ownership and contracts for model::field.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{traits::FieldValueKind, types::EntityTag};

///
/// FieldStorageDecode
///
/// FieldStorageDecode captures how one persisted field payload must be
/// interpreted at structural decode boundaries.
/// Semantic `FieldKind` alone is not always authoritative for persisted decode:
/// some fields intentionally store raw `Value` payloads even when their planner
/// shape is narrower.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldStorageDecode {
    /// Decode the persisted field payload according to semantic `FieldKind`.
    ByKind,
    /// Decode the persisted field payload directly into `Value`.
    Value,
}

///
/// EnumVariantModel
///
/// EnumVariantModel carries structural decode metadata for one generated enum
/// variant payload.
/// Runtime structural decode uses this to stay on the field-kind contract for
/// enum payloads instead of falling back to generic untyped CBOR decoding.
///

#[derive(Clone, Copy, Debug)]
pub struct EnumVariantModel {
    /// Stable schema variant tag.
    pub(crate) ident: &'static str,
    /// Declared payload kind when this variant carries data.
    pub(crate) payload_kind: Option<&'static FieldKind>,
    /// Persisted payload decode contract for the carried data.
    pub(crate) payload_storage_decode: FieldStorageDecode,
}

impl EnumVariantModel {
    /// Build one enum variant structural decode descriptor.
    #[must_use]
    pub const fn new(
        ident: &'static str,
        payload_kind: Option<&'static FieldKind>,
        payload_storage_decode: FieldStorageDecode,
    ) -> Self {
        Self {
            ident,
            payload_kind,
            payload_storage_decode,
        }
    }

    /// Return the stable schema variant tag.
    #[must_use]
    pub const fn ident(&self) -> &'static str {
        self.ident
    }

    /// Return the declared payload kind when this variant carries data.
    #[must_use]
    pub const fn payload_kind(&self) -> Option<&'static FieldKind> {
        self.payload_kind
    }

    /// Return the persisted payload decode contract for this variant.
    #[must_use]
    pub const fn payload_storage_decode(&self) -> FieldStorageDecode {
        self.payload_storage_decode
    }
}

///
/// FieldModel
///
/// Runtime field metadata surfaced by macro-generated `EntityModel` values.
///
/// This is the smallest unit consumed by predicate validation, planning,
/// and executor-side plan checks.
///

#[derive(Debug)]
pub struct FieldModel {
    /// Field name as used in predicates and indexing.
    pub(crate) name: &'static str,
    /// Runtime type shape (no schema-layer graph nodes).
    pub(crate) kind: FieldKind,
    /// Persisted field decode contract used by structural runtime decoders.
    pub(crate) storage_decode: FieldStorageDecode,
}

impl FieldModel {
    /// Build one runtime field descriptor.
    #[must_use]
    pub const fn new(name: &'static str, kind: FieldKind) -> Self {
        Self::new_with_storage_decode(name, kind, FieldStorageDecode::ByKind)
    }

    /// Build one runtime field descriptor with an explicit persisted decode contract.
    #[must_use]
    pub const fn new_with_storage_decode(
        name: &'static str,
        kind: FieldKind,
        storage_decode: FieldStorageDecode,
    ) -> Self {
        Self {
            name,
            kind,
            storage_decode,
        }
    }

    /// Return the stable field name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Return the runtime type-kind descriptor.
    #[must_use]
    pub const fn kind(&self) -> FieldKind {
        self.kind
    }

    /// Return the persisted field decode contract.
    #[must_use]
    pub const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }
}

///
/// RelationStrength
///
/// Explicit relation intent for save-time referential integrity.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationStrength {
    Strong,
    Weak,
}

///
/// FieldKind
///
/// Minimal runtime type surface needed by planning, validation, and execution.
///
/// This is aligned with `Value` variants and intentionally lossy: it encodes
/// only the shape required for predicate compatibility and index planning.
///

#[derive(Clone, Copy, Debug)]
pub enum FieldKind {
    // Scalar primitives
    Account,
    Blob,
    Bool,
    Date,
    Decimal {
        /// Required schema-declared fractional scale for decimal fields.
        scale: u32,
    },
    Duration,
    Enum {
        /// Fully-qualified enum type path used for strict filter normalization.
        path: &'static str,
        /// Declared per-variant payload decode metadata.
        variants: &'static [EnumVariantModel],
    },
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

    /// Typed relation; `key_kind` reflects the referenced key type.
    /// `strength` encodes strong vs. weak relation intent.
    Relation {
        /// Fully-qualified Rust type path for diagnostics.
        target_path: &'static str,
        /// Stable external name used in storage keys.
        target_entity_name: &'static str,
        /// Stable runtime identity used on hot execution paths.
        target_entity_tag: EntityTag,
        /// Data store path where the target entity is persisted.
        target_store_path: &'static str,
        key_kind: &'static Self,
        strength: RelationStrength,
    },

    // Collections
    List(&'static Self),
    Set(&'static Self),
    /// Deterministic, unordered key/value collection.
    ///
    /// Map fields are persistable and patchable, but not queryable or indexable.
    Map {
        key: &'static Self,
        value: &'static Self,
    },

    /// Structured (non-atomic) value.
    /// Queryability here controls whether predicates may target this field,
    /// not whether it may be stored or updated.
    Structured {
        queryable: bool,
    },
}

impl FieldKind {
    #[must_use]
    pub const fn value_kind(&self) -> FieldValueKind {
        match self {
            Self::Account
            | Self::Blob
            | Self::Bool
            | Self::Date
            | Self::Duration
            | Self::Enum { .. }
            | Self::Float32
            | Self::Float64
            | Self::Int
            | Self::Int128
            | Self::IntBig
            | Self::Principal
            | Self::Subaccount
            | Self::Text
            | Self::Timestamp
            | Self::Uint
            | Self::Uint128
            | Self::UintBig
            | Self::Ulid
            | Self::Unit
            | Self::Decimal { .. }
            | Self::Relation { .. } => FieldValueKind::Atomic,
            Self::List(_) | Self::Set(_) => FieldValueKind::Structured { queryable: true },
            Self::Map { .. } => FieldValueKind::Structured { queryable: false },
            Self::Structured { queryable } => FieldValueKind::Structured {
                queryable: *queryable,
            },
        }
    }

    /// Returns `true` if this field shape is permitted in
    /// persisted or query-visible schemas under the current
    /// determinism policy.
    ///
    /// This shape-level check is structural only; query-time policy
    /// enforcement (for example, map predicate fencing) is applied at
    /// query construction and validation boundaries.
    #[must_use]
    pub const fn is_deterministic_collection_shape(&self) -> bool {
        match self {
            Self::Relation { key_kind, .. } => key_kind.is_deterministic_collection_shape(),

            Self::List(inner) | Self::Set(inner) => inner.is_deterministic_collection_shape(),

            Self::Map { key, value } => {
                key.is_deterministic_collection_shape() && value.is_deterministic_collection_shape()
            }

            _ => true,
        }
    }
}
