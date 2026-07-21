//! Module: db::schema::accepted_field_kind
//! Responsibility: catalog-resolved recursive field-kind contracts.
//! Does not own: generated enum proposals or catalog definition storage.
//! Boundary: accepted snapshots and runtime contracts persist enum IDs only.

use crate::{
    db::schema::{
        composite_catalog::{AcceptedCompositeCatalog, CompositeTypeId},
        enum_catalog::AcceptedEnumCatalog,
    },
    model::field::FieldKind,
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
        let (enum_catalog, composite_catalog) =
            crate::db::schema::build_initial_accepted_catalogs_from_kinds_for_tests(&[kind])
                .expect("test field kind catalogs should build");
        crate::db::schema::enum_catalog::resolve_model_field_kind_with_composite_catalog(
            &enum_catalog,
            &composite_catalog,
            kind,
        )
        .expect("test field kind should resolve through its type catalogs")
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

    /// Compare generated decoder shape after catalog publication has already
    /// proven the exact enum path and variant contract.
    #[must_use]
    pub(in crate::db) fn matches_generated_storage_shape(
        &self,
        generated: FieldKind,
        enum_catalog: &AcceptedEnumCatalog,
        composite_catalog: &AcceptedCompositeCatalog,
    ) -> bool {
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
            (Self::Composite { type_id }, FieldKind::Composite { path, codec, shape }) => {
                composite_catalog.matches_generated_composite(
                    enum_catalog,
                    *type_id,
                    path,
                    codec,
                    shape,
                )
            }
            (Self::List(left), FieldKind::List(right))
            | (Self::Set(left), FieldKind::Set(right)) => {
                left.matches_generated_storage_shape(*right, enum_catalog, composite_catalog)
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
                left_key.matches_generated_storage_shape(
                    *right_key,
                    enum_catalog,
                    composite_catalog,
                ) && left_value.matches_generated_storage_shape(
                    *right_value,
                    enum_catalog,
                    composite_catalog,
                )
            }
            (accepted @ Self::Relation { .. }, generated @ FieldKind::Relation { .. }) => accepted
                .relation_matches_generated_storage_shape(
                    generated,
                    enum_catalog,
                    composite_catalog,
                ),
            _ => false,
        }
    }

    fn relation_matches_generated_storage_shape(
        &self,
        generated: FieldKind,
        enum_catalog: &AcceptedEnumCatalog,
        composite_catalog: &AcceptedCompositeCatalog,
    ) -> bool {
        let (
            Self::Relation {
                target_path: accepted_path,
                target_entity_name: accepted_name,
                target_entity_tag: accepted_tag,
                target_store_path: accepted_store,
                key_kind: accepted_key,
            },
            FieldKind::Relation {
                target_path: generated_path,
                target_entity_name: generated_name,
                target_entity_tag: generated_tag,
                target_store_path: generated_store,
                key_kind: generated_key,
            },
        ) = (self, generated)
        else {
            return false;
        };
        accepted_path == generated_path
            && accepted_name == generated_name
            && *accepted_tag == generated_tag
            && accepted_store == generated_store
            && accepted_key.matches_generated_storage_shape(
                *generated_key,
                enum_catalog,
                composite_catalog,
            )
    }
}
