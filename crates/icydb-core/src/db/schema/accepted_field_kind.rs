//! Module: db::schema::accepted_field_kind
//! Responsibility: catalog-resolved recursive field-kind contracts.
//! Does not own: generated enum/composite proposals or catalog definition storage.
//! Boundary: accepted snapshots and runtime contracts persist store-local catalog IDs only.

#[cfg(test)]
use crate::model::field::FieldKind;
use crate::{
    db::schema::composite_catalog::CompositeTypeId,
    model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
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
            | Self::Relation { .. }
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

    #[cfg(test)]
    pub(in crate::db) fn from_model_kind(kind: FieldKind) -> Self {
        match kind {
            FieldKind::Account => Self::Account,
            FieldKind::Blob { max_len } => Self::Blob { max_len },
            FieldKind::Bool => Self::Bool,
            FieldKind::Date => Self::Date,
            FieldKind::Decimal { scale } => Self::Decimal { scale },
            FieldKind::Duration => Self::Duration,
            FieldKind::Enum { .. } => Self::Enum {
                type_id: EnumTypeId::new(1).expect("test enum type ID is non-zero"),
            },
            FieldKind::Float32 => Self::Float32,
            FieldKind::Float64 => Self::Float64,
            FieldKind::Int8 => Self::Int8,
            FieldKind::Int16 => Self::Int16,
            FieldKind::Int32 => Self::Int32,
            FieldKind::Int64 => Self::Int64,
            FieldKind::Int128 => Self::Int128,
            FieldKind::IntBig { max_bytes } => Self::IntBig { max_bytes },
            FieldKind::Principal => Self::Principal,
            FieldKind::Subaccount => Self::Subaccount,
            FieldKind::Text { max_len } => Self::Text { max_len },
            FieldKind::Timestamp => Self::Timestamp,
            FieldKind::Nat8 => Self::Nat8,
            FieldKind::Nat16 => Self::Nat16,
            FieldKind::Nat32 => Self::Nat32,
            FieldKind::Nat64 => Self::Nat64,
            FieldKind::Nat128 => Self::Nat128,
            FieldKind::NatBig { max_bytes } => Self::NatBig { max_bytes },
            FieldKind::Ulid => Self::Ulid,
            FieldKind::Unit => Self::Unit,
            FieldKind::Relation {
                target_path,
                target_entity_name,
                target_entity_tag,
                target_store_path,
                key_kind,
            } => Self::Relation {
                target_path: target_path.to_string(),
                target_entity_name: target_entity_name.to_string(),
                target_entity_tag,
                target_store_path: target_store_path.to_string(),
                key_kind: Box::new(Self::from_model_kind(*key_kind)),
            },
            FieldKind::List(inner) => Self::List(Box::new(Self::from_model_kind(*inner))),
            FieldKind::Set(inner) => Self::Set(Box::new(Self::from_model_kind(*inner))),
            FieldKind::Map { key, value } => Self::Map {
                key: Box::new(Self::from_model_kind(*key)),
                value: Box::new(Self::from_model_kind(*value)),
            },
            FieldKind::Composite { .. } => Self::Composite {
                type_id: CompositeTypeId::new(1).expect("test composite type ID is non-zero"),
            },
        }
    }

    /// Return whether this accepted kind contains catalog enum identity.
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
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
    #[cfg(any(test, feature = "sql"))]
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
