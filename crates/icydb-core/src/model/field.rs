use crate::traits::FieldValueKind;

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
    pub name: &'static str,
    /// Runtime type shape (no schema-layer graph nodes).
    pub kind: FieldKind,
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
    Decimal,
    Duration,
    Enum {
        /// Fully-qualified enum type path used for strict filter normalization.
        path: &'static str,
    },
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

    /// Typed relation; `key_kind` reflects the referenced key type.
    /// `strength` encodes strong vs. weak relation intent.
    Relation {
        /// Fully-qualified Rust type path for diagnostics.
        target_path: &'static str,
        /// Stable external name used in storage keys.
        target_entity_name: &'static str,
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
            | Self::Decimal
            | Self::Duration
            | Self::Enum { .. }
            | Self::E8s
            | Self::E18s
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
