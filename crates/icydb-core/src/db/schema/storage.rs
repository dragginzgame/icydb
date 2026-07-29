//! Module: db::schema::storage
//! Responsibility: accepted field persistence and write-policy vocabulary.
//! Does not own: authored/generated field descriptors or physical row encoding.
//! Boundary: accepted schema snapshots select these contracts; data/runtime
//! consumers execute them.

///
/// FieldStorageDecode
///
/// Accepted persisted-field payload interpretation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum FieldStorageDecode {
    /// Decode the persisted payload according to its accepted field kind.
    ByKind,
    /// Decode the canonical recursive value envelope, then apply its accepted
    /// catalog contract.
    CatalogValue,
}

///
/// ScalarCodec
///
/// Canonical binary leaf encoding selected by accepted schema.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ScalarCodec {
    Blob,
    Bool,
    Date,
    Duration,
    Float32,
    Float64,
    Int64,
    Principal,
    Subaccount,
    Text,
    Timestamp,
    Nat64,
    Ulid,
    Unit,
}

///
/// LeafCodec
///
/// Accepted row-slot codec class.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum LeafCodec {
    /// Dedicated scalar codec selected by the accepted field kind.
    Scalar(ScalarCodec),
    /// Recursive structural field codec.
    Structural,
}

///
/// CompositeCodec
///
/// Canonical persisted grammar for an accepted composite type.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CompositeCodec {
    /// Current structural record, tuple, and newtype encoding.
    StructuralV1,
}

///
/// FieldInsertGeneration
///
/// Accepted insert-time field generation contract.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum FieldInsertGeneration {
    /// Allocate the next database-owned unsigned identity.
    Identity,
    /// Generate a fresh ULID.
    Ulid,
    /// Generate the operation timestamp.
    Timestamp,
}

///
/// FieldWriteManagement
///
/// Accepted database-owned audit-field policy.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum FieldWriteManagement {
    /// Fill only when the row is inserted.
    CreatedAt,
    /// Fill on insert and refresh on every update.
    UpdatedAt,
}
