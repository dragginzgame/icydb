///
/// EntityFieldModel
///
/// Runtime field metadata surfaced by macro-generated `EntityModel` values.
///
/// This is the smallest unit consumed by predicate validation, planning,
/// and executor-side plan checks.
///

pub struct EntityFieldModel {
    /// Field name as used in predicates and indexing.
    pub name: &'static str,
    /// Runtime type shape (no schema-layer graph nodes).
    pub kind: EntityFieldKind,
}

///
/// EntityFieldKind
///
/// Minimal runtime type surface needed by planning, validation, and execution.
///
/// This is aligned with `Value` variants and intentionally lossy: it encodes
/// only the shape required for predicate compatibility and index planning.

pub enum EntityFieldKind {
    // Scalar primitives
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

    /// Typed entity reference; `key_kind` reflects the referenced key type.
    Ref {
        target_path: &'static str,
        key_kind: &'static Self,
    },

    // Collections
    List(&'static Self),
    Set(&'static Self),
    Map {
        key: &'static Self,
        value: &'static Self,
    },

    /// Marker for fields that are not filterable or indexable.
    Unsupported,
}
