//! Module: db::schema::accepted_field_kind
//! Responsibility: catalog-resolved recursive field-kind contracts.
//! Does not own: generated enum proposals or catalog definition storage.
//! Boundary: accepted snapshots and runtime contracts persist enum IDs only.

use crate::{
    model::field::{FieldKind, RelationStrength},
    types::EntityTag,
    value::EnumTypeId,
};

/// Canonical field-kind shape stored by accepted schema snapshots.
/// Enum references carry store-local catalog IDs and never embed definitions.
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
        strength: AcceptedRelationStrength,
    },
    List(Box<Self>),
    Set(Box<Self>),
    Map {
        key: Box<Self>,
        value: Box<Self>,
    },
    Structured {
        queryable: bool,
    },
}

impl AcceptedFieldKind {
    #[cfg(test)]
    pub(in crate::db) fn from_model_kind(kind: FieldKind) -> Self {
        let catalog = crate::db::schema::enum_catalog::build_initial_accepted_enum_catalog_from_kinds_for_tests(&[kind])
            .expect("test field kind enum catalog should build");
        crate::db::schema::enum_catalog::resolve_model_field_kind(&catalog, kind)
            .expect("test field kind should resolve through its enum catalog")
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
            | Self::Unit
            | Self::Structured { .. } => false,
        }
    }

    /// Return whether this kind requires the recursive canonical value wire.
    #[must_use]
    pub(in crate::db) fn requires_canonical_value_wire(&self) -> bool {
        match self {
            Self::Enum { .. } | Self::Structured { .. } => true,
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

    /// Compare generated decoder shape after catalog publication has already
    /// proven the exact enum path and variant contract.
    #[must_use]
    pub(in crate::db) fn matches_generated_storage_shape(&self, generated: FieldKind) -> bool {
        match (self, generated) {
            (Self::Account, FieldKind::Account)
            | (Self::Bool, FieldKind::Bool)
            | (Self::Date, FieldKind::Date)
            | (Self::Duration, FieldKind::Duration)
            | (Self::Enum { .. }, FieldKind::Enum { .. })
            | (Self::Float32, FieldKind::Float32)
            | (Self::Float64, FieldKind::Float64)
            | (Self::Int8, FieldKind::Int8)
            | (Self::Int16, FieldKind::Int16)
            | (Self::Int32, FieldKind::Int32)
            | (Self::Int64, FieldKind::Int64)
            | (Self::Int128, FieldKind::Int128)
            | (Self::Principal, FieldKind::Principal)
            | (Self::Subaccount, FieldKind::Subaccount)
            | (Self::Timestamp, FieldKind::Timestamp)
            | (Self::Nat8, FieldKind::Nat8)
            | (Self::Nat16, FieldKind::Nat16)
            | (Self::Nat32, FieldKind::Nat32)
            | (Self::Nat64, FieldKind::Nat64)
            | (Self::Nat128, FieldKind::Nat128)
            | (Self::Ulid, FieldKind::Ulid)
            | (Self::Unit, FieldKind::Unit) => true,
            (Self::Blob { max_len: left }, FieldKind::Blob { max_len: right })
            | (Self::Text { max_len: left }, FieldKind::Text { max_len: right }) => *left == right,
            (Self::Decimal { scale: left }, FieldKind::Decimal { scale: right }) => *left == right,
            (Self::IntBig { max_bytes: left }, FieldKind::IntBig { max_bytes: right })
            | (Self::NatBig { max_bytes: left }, FieldKind::NatBig { max_bytes: right }) => {
                *left == right
            }
            (Self::Structured { queryable: left }, FieldKind::Structured { queryable: right }) => {
                *left == right
            }
            (Self::List(left), FieldKind::List(right))
            | (Self::Set(left), FieldKind::Set(right)) => {
                left.matches_generated_storage_shape(*right)
            }
            (
                Self::Map {
                    key: left_key,
                    value: left_value,
                },
                FieldKind::Map {
                    key: right_key,
                    value: right_value,
                },
            ) => {
                left_key.matches_generated_storage_shape(*right_key)
                    && left_value.matches_generated_storage_shape(*right_value)
            }
            (
                Self::Relation {
                    target_path: left_path,
                    target_entity_name: left_name,
                    target_entity_tag: left_tag,
                    target_store_path: left_store,
                    key_kind: left_key,
                    strength: left_strength,
                },
                FieldKind::Relation {
                    target_path: right_path,
                    target_entity_name: right_name,
                    target_entity_tag: right_tag,
                    target_store_path: right_store,
                    key_kind: right_key,
                    strength: right_strength,
                },
            ) => {
                left_path == right_path
                    && left_name == right_name
                    && *left_tag == right_tag
                    && left_store == right_store
                    && left_key.matches_generated_storage_shape(*right_key)
                    && matches!(
                        (left_strength, right_strength),
                        (AcceptedRelationStrength::Strong, RelationStrength::Strong)
                            | (AcceptedRelationStrength::Weak, RelationStrength::Weak)
                    )
            }
            _ => false,
        }
    }
}

/// Accepted relation strength independent of generated metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedRelationStrength {
    Strong,
    Weak,
}
