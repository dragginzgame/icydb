///
/// EntityFieldModel
/// Runtime field metadata used by planning and validation.
///

pub struct EntityFieldModel {
    /// Field name as used in predicates and indexing.
    pub name: &'static str,
    /// Runtime type shape (no schema-layer nodes).
    pub kind: EntityFieldKind,
}

///
/// EntityFieldKind
///
/// Minimal type surface needed by the v2 planner/validator.
/// Aligned with `Value` variants; this is a lossy projection of schema types.
///

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

    // Collections
    List(Box<Self>),
    Set(Box<Self>),
    Map {
        key: Box<Self>,
        value: Box<Self>,
    },

    /// Marker for fields that are not filterable or indexable.
    Unsupported,
}
