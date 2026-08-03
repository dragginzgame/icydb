//! Module: db::schema::accepted_field_kind
//! Responsibility: catalog-resolved recursive field-kind contracts.
//! Does not own: generated enum/composite proposals or catalog definition storage.
//! Boundary: accepted snapshots and runtime contracts persist store-local catalog IDs only.

use crate::{
    db::schema::composite_catalog::CompositeTypeId,
    db::schema::{FieldStorageDecode, LeafCodec, MAX_ACCEPTED_RECURSIVE_DEPTH, ScalarCodec},
    types::EntityTag,
    value::EnumTypeId,
};

/// Canonical field-kind shape stored by accepted schema snapshots.
/// Enum and composite references carry store-local catalog IDs and never embed definitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedFieldKind {
    Account,
    Blob {
        max_len: Option<u32>,
    },
    Bool,
    Date,
    Decimal {
        scale: u32,
    },
    Duration,
    Enum {
        type_id: EnumTypeId,
    },
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    IntBig {
        max_bytes: u32,
    },
    Principal,
    Subaccount,
    Text {
        max_len: Option<u32>,
    },
    Timestamp,
    Nat8,
    Nat16,
    Nat32,
    Nat64,
    Nat128,
    NatBig {
        max_bytes: u32,
    },
    Ulid,
    Unit,
    Relation {
        target_path: String,
        target_entity_name: String,
        target_entity_tag: EntityTag,
        target_store_path: String,
        key_kind: Box<Self>,
    },
    List(Box<Self>),
    Set(Box<Self>),
    Map {
        key: Box<Self>,
        value: Box<Self>,
    },
    Composite {
        type_id: CompositeTypeId,
    },
}

impl AcceptedFieldKind {
    /// Return whether this accepted kind is locally well formed without
    /// consulting enum or composite catalogs.
    ///
    /// Persisted decoding uses this check before a snapshot becomes runtime
    /// authority. Catalog-reference existence remains a bundle-level concern.
    #[must_use]
    pub(in crate::db::schema) fn has_valid_local_shape(&self) -> bool {
        self.has_valid_local_shape_at(0)
    }

    fn has_valid_local_shape_at(&self, depth: usize) -> bool {
        if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
            return false;
        }

        let next_depth = depth.saturating_add(1);
        match self {
            Self::Decimal { scale } => *scale <= icydb_schema::Decimal::max_supported_scale(),
            Self::IntBig { max_bytes } | Self::NatBig { max_bytes } => *max_bytes != 0,
            Self::Relation { key_kind, .. } | Self::List(key_kind) | Self::Set(key_kind) => {
                key_kind.has_valid_local_shape_at(next_depth)
            }
            Self::Map { key, value } => {
                key.has_valid_local_shape_at(next_depth)
                    && value.has_valid_local_shape_at(next_depth)
            }
            Self::Account
            | Self::Blob { .. }
            | Self::Bool
            | Self::Composite { .. }
            | Self::Date
            | Self::Duration
            | Self::Enum { .. }
            | Self::Float32
            | Self::Float64
            | Self::Int8
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::Int128
            | Self::Principal
            | Self::Subaccount
            | Self::Text { .. }
            | Self::Timestamp
            | Self::Nat8
            | Self::Nat16
            | Self::Nat32
            | Self::Nat64
            | Self::Nat128
            | Self::Ulid
            | Self::Unit => true,
        }
    }

    /// Resolve the canonical leaf codec for this accepted kind and storage
    /// contract. Catalog-decoded values always retain the structural envelope;
    /// direct scalar storage uses the same leaf codec throughout schema
    /// admission and accepted-check literal binding.
    #[must_use]
    pub(in crate::db) const fn leaf_codec_for_storage(
        &self,
        storage_decode: FieldStorageDecode,
    ) -> LeafCodec {
        if matches!(storage_decode, FieldStorageDecode::CatalogValue) {
            return LeafCodec::Structural;
        }

        match self {
            Self::Blob { .. } => LeafCodec::Scalar(ScalarCodec::Blob),
            Self::Bool => LeafCodec::Scalar(ScalarCodec::Bool),
            Self::Date => LeafCodec::Scalar(ScalarCodec::Date),
            Self::Duration => LeafCodec::Scalar(ScalarCodec::Duration),
            Self::Float32 => LeafCodec::Scalar(ScalarCodec::Float32),
            Self::Float64 => LeafCodec::Scalar(ScalarCodec::Float64),
            Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64 => {
                LeafCodec::Scalar(ScalarCodec::Int64)
            }
            Self::Principal => LeafCodec::Scalar(ScalarCodec::Principal),
            Self::Subaccount => LeafCodec::Scalar(ScalarCodec::Subaccount),
            Self::Text { .. } => LeafCodec::Scalar(ScalarCodec::Text),
            Self::Timestamp => LeafCodec::Scalar(ScalarCodec::Timestamp),
            Self::Nat8 | Self::Nat16 | Self::Nat32 | Self::Nat64 => {
                LeafCodec::Scalar(ScalarCodec::Nat64)
            }
            Self::Relation { key_kind, .. } => key_kind.leaf_codec_for_storage(storage_decode),
            Self::Ulid => LeafCodec::Scalar(ScalarCodec::Ulid),
            Self::Unit => LeafCodec::Scalar(ScalarCodec::Unit),
            Self::Account
            | Self::Composite { .. }
            | Self::Decimal { .. }
            | Self::Enum { .. }
            | Self::Int128
            | Self::IntBig { .. }
            | Self::List(_)
            | Self::Map { .. }
            | Self::Nat128
            | Self::NatBig { .. }
            | Self::Set(_) => LeafCodec::Structural,
        }
    }

    /// Build one catalog-reference kind for metadata-only unit tests.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn test_composite() -> Self {
        Self::Composite {
            type_id: CompositeTypeId::new(1).expect("test composite type ID is non-zero"),
        }
    }

    /// Return whether this accepted kind contains catalog enum identity.
    #[must_use]
    pub(in crate::db) fn contains_enum(&self) -> bool {
        match self {
            Self::Enum { .. } => true,
            Self::Relation { key_kind, .. } | Self::List(key_kind) | Self::Set(key_kind) => {
                key_kind.contains_enum()
            }
            Self::Map { key, value } => key.contains_enum() || value.contains_enum(),
            Self::Composite { .. }
            | Self::Account
            | Self::Blob { .. }
            | Self::Bool
            | Self::Date
            | Self::Decimal { .. }
            | Self::Duration
            | Self::Float32
            | Self::Float64
            | Self::Int8
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::Int128
            | Self::IntBig { .. }
            | Self::Principal
            | Self::Subaccount
            | Self::Text { .. }
            | Self::Timestamp
            | Self::Nat8
            | Self::Nat16
            | Self::Nat32
            | Self::Nat64
            | Self::Nat128
            | Self::NatBig { .. }
            | Self::Ulid
            | Self::Unit => false,
        }
    }

    /// Return whether this accepted kind contains relation identity.
    #[must_use]
    pub(in crate::db) fn contains_relation(&self) -> bool {
        match self {
            Self::Relation { .. } => true,
            Self::List(inner) | Self::Set(inner) => inner.contains_relation(),
            Self::Account
            | Self::Blob { .. }
            | Self::Bool
            | Self::Composite { .. }
            | Self::Date
            | Self::Decimal { .. }
            | Self::Duration
            | Self::Enum { .. }
            | Self::Float32
            | Self::Float64
            | Self::Int8
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::Int128
            | Self::IntBig { .. }
            | Self::Map { .. }
            | Self::Principal
            | Self::Subaccount
            | Self::Text { .. }
            | Self::Timestamp
            | Self::Nat8
            | Self::Nat16
            | Self::Nat32
            | Self::Nat64
            | Self::Nat128
            | Self::NatBig { .. }
            | Self::Ulid
            | Self::Unit => false,
        }
    }

    /// Return whether this kind requires the recursive canonical value wire.
    #[must_use]
    pub(in crate::db) fn requires_canonical_value_wire(&self) -> bool {
        match self {
            Self::Enum { .. } | Self::Composite { .. } => true,
            Self::Relation { key_kind, .. } | Self::List(key_kind) | Self::Set(key_kind) => {
                key_kind.requires_canonical_value_wire()
            }
            Self::Map { key, value } => {
                key.requires_canonical_value_wire() || value.requires_canonical_value_wire()
            }
            Self::Account
            | Self::Blob { .. }
            | Self::Bool
            | Self::Date
            | Self::Decimal { .. }
            | Self::Duration
            | Self::Float32
            | Self::Float64
            | Self::Int8
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::Int128
            | Self::IntBig { .. }
            | Self::Principal
            | Self::Subaccount
            | Self::Text { .. }
            | Self::Timestamp
            | Self::Nat8
            | Self::Nat16
            | Self::Nat32
            | Self::Nat64
            | Self::Nat128
            | Self::NatBig { .. }
            | Self::Ulid
            | Self::Unit => false,
        }
    }
}
