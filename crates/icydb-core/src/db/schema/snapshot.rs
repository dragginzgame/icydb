//! Module: db::schema::snapshot
//! Responsibility: owned persisted-schema snapshot shapes.
//! Does not own: startup reconciliation, stable-memory storage, or generated model metadata.
//! Boundary: schema-owned DTOs that can become the `__icydb_schema` payload.

use crate::{
    db::schema::{FieldId, SchemaFieldSlot, SchemaRowLayout, SchemaVersion},
    model::field::{
        FieldDatabaseDefault, FieldKind, FieldStorageDecode, LeafCodec, RelationStrength,
    },
    types::EntityTag,
};

///
/// AcceptedSchemaSnapshot
///
/// Schema snapshot accepted by startup reconciliation.
/// This wrapper marks the boundary between a decoded persisted payload and a
/// schema authority that has been checked against the compiled proposal.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedSchemaSnapshot {
    snapshot: PersistedSchemaSnapshot,
}

impl AcceptedSchemaSnapshot {
    /// Wrap one persisted snapshot after reconciliation accepts it.
    #[must_use]
    pub(in crate::db) const fn new(snapshot: PersistedSchemaSnapshot) -> Self {
        Self { snapshot }
    }

    /// Borrow the accepted persisted snapshot payload.
    #[must_use]
    pub(in crate::db) const fn persisted_snapshot(&self) -> &PersistedSchemaSnapshot {
        &self.snapshot
    }

    /// Borrow the accepted entity path.
    #[must_use]
    pub(in crate::db) const fn entity_path(&self) -> &str {
        self.snapshot.entity_path()
    }

    /// Borrow the accepted entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.snapshot.entity_name()
    }

    /// Borrow the accepted primary-key field snapshot, when present.
    #[must_use]
    pub(in crate::db) fn primary_key_field(&self) -> Option<&PersistedFieldSnapshot> {
        let primary_key_field_id = self.snapshot.primary_key_field_id();

        self.snapshot
            .fields()
            .iter()
            .find(|field| field.id() == primary_key_field_id)
    }

    /// Borrow the accepted primary-key field name, when present.
    #[must_use]
    pub(in crate::db) fn primary_key_field_name(&self) -> Option<&str> {
        self.primary_key_field().map(PersistedFieldSnapshot::name)
    }

    /// Borrow one accepted field snapshot by its persisted field name.
    #[must_use]
    pub(in crate::db) fn field_by_name(&self, name: &str) -> Option<&PersistedFieldSnapshot> {
        self.snapshot
            .fields()
            .iter()
            .find(|field| field.name() == name)
    }
}

///
/// PersistedSchemaSnapshot
///
/// Owned schema snapshot for one live entity schema.
/// This is the shape intended for the future `__icydb_schema` payload; it is
/// separate from generated `EntityModel` so startup reconciliation can compare
/// stored authority with the compiled proposal.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedSchemaSnapshot {
    version: SchemaVersion,
    entity_path: String,
    entity_name: String,
    primary_key_field_id: FieldId,
    row_layout: SchemaRowLayout,
    fields: Vec<PersistedFieldSnapshot>,
}

impl PersistedSchemaSnapshot {
    /// Build one persisted schema snapshot from already-validated pieces.
    #[must_use]
    pub(in crate::db) const fn new(
        version: SchemaVersion,
        entity_path: String,
        entity_name: String,
        primary_key_field_id: FieldId,
        row_layout: SchemaRowLayout,
        fields: Vec<PersistedFieldSnapshot>,
    ) -> Self {
        Self {
            version,
            entity_path,
            entity_name,
            primary_key_field_id,
            row_layout,
            fields,
        }
    }

    /// Return the schema version for this snapshot.
    #[must_use]
    pub(in crate::db) const fn version(&self) -> SchemaVersion {
        self.version
    }

    /// Borrow the stored entity path.
    #[must_use]
    pub(in crate::db) const fn entity_path(&self) -> &str {
        self.entity_path.as_str()
    }

    /// Borrow the stored entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Return the stored primary-key field identity.
    #[must_use]
    pub(in crate::db) const fn primary_key_field_id(&self) -> FieldId {
        self.primary_key_field_id
    }

    /// Borrow the live row-layout mapping for this snapshot.
    #[must_use]
    pub(in crate::db) const fn row_layout(&self) -> &SchemaRowLayout {
        &self.row_layout
    }

    /// Borrow persisted field entries in row-layout order.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &[PersistedFieldSnapshot] {
        self.fields.as_slice()
    }
}

///
/// PersistedFieldSnapshot
///
/// Owned schema snapshot for one live field.
/// It carries durable identity, current slot placement, type/storage metadata,
/// and the database-level default contract used by future reconciliation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedFieldSnapshot {
    id: FieldId,
    name: String,
    slot: SchemaFieldSlot,
    kind: PersistedFieldKind,
    nested_leaves: Vec<PersistedNestedLeafSnapshot>,
    nullable: bool,
    default: SchemaFieldDefault,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

impl PersistedFieldSnapshot {
    /// Build one persisted field snapshot from already-validated pieces.
    #[expect(
        clippy::too_many_arguments,
        reason = "schema snapshot construction keeps every persisted field contract explicit"
    )]
    #[must_use]
    pub(in crate::db) const fn new(
        id: FieldId,
        name: String,
        slot: SchemaFieldSlot,
        kind: PersistedFieldKind,
        nested_leaves: Vec<PersistedNestedLeafSnapshot>,
        nullable: bool,
        default: SchemaFieldDefault,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
    ) -> Self {
        Self {
            id,
            name,
            slot,
            kind,
            nested_leaves,
            nullable,
            default,
            storage_decode,
            leaf_codec,
        }
    }

    /// Return the durable field identity.
    #[must_use]
    pub(in crate::db) const fn id(&self) -> FieldId {
        self.id
    }

    /// Borrow the stored field name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return the stored physical slot for this field.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> SchemaFieldSlot {
        self.slot
    }

    /// Borrow the owned persisted field kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &PersistedFieldKind {
        &self.kind
    }

    /// Borrow persisted nested leaf descriptors rooted at this top-level field.
    #[must_use]
    pub(in crate::db) const fn nested_leaves(&self) -> &[PersistedNestedLeafSnapshot] {
        self.nested_leaves.as_slice()
    }

    /// Return whether this field permits explicit persisted `NULL`.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the database-level default contract for this field.
    #[must_use]
    pub(in crate::db) const fn default(&self) -> SchemaFieldDefault {
        self.default
    }

    /// Return the stored payload decode contract.
    #[must_use]
    pub(in crate::db) const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }

    /// Return the stored leaf codec contract.
    #[must_use]
    pub(in crate::db) const fn leaf_codec(&self) -> LeafCodec {
        self.leaf_codec
    }
}

///
/// PersistedNestedLeafSnapshot
///
/// Accepted schema metadata for one queryable nested leaf rooted at a
/// top-level field. The path is relative to the owning persisted field, so
/// nested leaves can describe field-path planning facts without claiming their
/// own physical row slots.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedNestedLeafSnapshot {
    path: Vec<String>,
    kind: PersistedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
}

impl PersistedNestedLeafSnapshot {
    /// Build one nested leaf snapshot from already-validated pieces.
    #[must_use]
    pub(in crate::db) const fn new(
        path: Vec<String>,
        kind: PersistedFieldKind,
        nullable: bool,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
    ) -> Self {
        Self {
            path,
            kind,
            nullable,
            storage_decode,
            leaf_codec,
        }
    }

    /// Borrow the path relative to the owning top-level field.
    #[must_use]
    pub(in crate::db) const fn path(&self) -> &[String] {
        self.path.as_slice()
    }

    /// Borrow the persisted field kind for this nested leaf.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &PersistedFieldKind {
        &self.kind
    }

    /// Return whether this nested leaf permits explicit persisted `NULL`.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the nested leaf payload decode contract.
    #[must_use]
    pub(in crate::db) const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }

    /// Return the nested leaf payload codec contract.
    #[must_use]
    pub(in crate::db) const fn leaf_codec(&self) -> LeafCodec {
        self.leaf_codec
    }
}

///
/// SchemaFieldDefault
///
/// Database-level default contract for one persisted field.
/// This intentionally starts with only `None` so 0.146 does not accidentally
/// infer database defaults from Rust struct construction defaults.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaFieldDefault {
    None,
}

impl SchemaFieldDefault {
    /// Convert runtime model default metadata into persisted schema shape.
    #[must_use]
    pub(in crate::db) const fn from_model_default(default: FieldDatabaseDefault) -> Self {
        match default {
            FieldDatabaseDefault::None => Self::None,
        }
    }
}

///
/// PersistedFieldKind
///
/// Owned field-kind representation for persisted schema snapshots.
/// It mirrors the runtime `FieldKind` shape but owns strings and nested field
/// kinds so the live schema can outlive generated static metadata.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum PersistedFieldKind {
    Account,
    Blob,
    Bool,
    Date,
    Decimal {
        scale: u32,
    },
    Duration,
    Enum {
        path: String,
        variants: Vec<PersistedEnumVariant>,
    },
    Float32,
    Float64,
    Int,
    Int128,
    IntBig,
    Principal,
    Subaccount,
    Text {
        max_len: Option<u32>,
    },
    Timestamp,
    Uint,
    Uint128,
    UintBig,
    Ulid,
    Unit,
    Relation {
        target_path: String,
        target_entity_name: String,
        target_entity_tag: EntityTag,
        target_store_path: String,
        key_kind: Box<Self>,
        strength: PersistedRelationStrength,
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

impl PersistedFieldKind {
    /// Convert generated runtime field kind metadata into owned persisted shape.
    #[must_use]
    pub(in crate::db) fn from_model_kind(kind: FieldKind) -> Self {
        match kind {
            FieldKind::Account => Self::Account,
            FieldKind::Blob => Self::Blob,
            FieldKind::Bool => Self::Bool,
            FieldKind::Date => Self::Date,
            FieldKind::Decimal { scale } => Self::Decimal { scale },
            FieldKind::Duration => Self::Duration,
            FieldKind::Enum { path, variants } => Self::Enum {
                path: path.to_string(),
                variants: variants
                    .iter()
                    .map(|variant| PersistedEnumVariant {
                        ident: variant.ident().to_string(),
                        payload_kind: variant
                            .payload_kind()
                            .map(|payload| Box::new(Self::from_model_kind(*payload))),
                        payload_storage_decode: variant.payload_storage_decode(),
                    })
                    .collect(),
            },
            FieldKind::Float32 => Self::Float32,
            FieldKind::Float64 => Self::Float64,
            FieldKind::Int => Self::Int,
            FieldKind::Int128 => Self::Int128,
            FieldKind::IntBig => Self::IntBig,
            FieldKind::Principal => Self::Principal,
            FieldKind::Subaccount => Self::Subaccount,
            FieldKind::Text { max_len } => Self::Text { max_len },
            FieldKind::Timestamp => Self::Timestamp,
            FieldKind::Uint => Self::Uint,
            FieldKind::Uint128 => Self::Uint128,
            FieldKind::UintBig => Self::UintBig,
            FieldKind::Ulid => Self::Ulid,
            FieldKind::Unit => Self::Unit,
            FieldKind::Relation {
                target_path,
                target_entity_name,
                target_entity_tag,
                target_store_path,
                key_kind,
                strength,
            } => Self::Relation {
                target_path: target_path.to_string(),
                target_entity_name: target_entity_name.to_string(),
                target_entity_tag,
                target_store_path: target_store_path.to_string(),
                key_kind: Box::new(Self::from_model_kind(*key_kind)),
                strength: PersistedRelationStrength::from_model_strength(strength),
            },
            FieldKind::List(inner) => Self::List(Box::new(Self::from_model_kind(*inner))),
            FieldKind::Set(inner) => Self::Set(Box::new(Self::from_model_kind(*inner))),
            FieldKind::Map { key, value } => Self::Map {
                key: Box::new(Self::from_model_kind(*key)),
                value: Box::new(Self::from_model_kind(*value)),
            },
            FieldKind::Structured { queryable } => Self::Structured { queryable },
        }
    }
}

///
/// PersistedEnumVariant
///
/// Owned persisted-schema representation of one enum variant.
/// The payload metadata is stored recursively so generated enum payload
/// metadata does not remain the live-schema authority after reconciliation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedEnumVariant {
    ident: String,
    payload_kind: Option<Box<PersistedFieldKind>>,
    payload_storage_decode: FieldStorageDecode,
}

impl PersistedEnumVariant {
    /// Build one persisted enum variant from trusted schema metadata.
    #[must_use]
    pub(in crate::db) const fn new(
        ident: String,
        payload_kind: Option<Box<PersistedFieldKind>>,
        payload_storage_decode: FieldStorageDecode,
    ) -> Self {
        Self {
            ident,
            payload_kind,
            payload_storage_decode,
        }
    }

    /// Borrow the stable enum variant identifier.
    #[must_use]
    pub(in crate::db) const fn ident(&self) -> &str {
        self.ident.as_str()
    }

    /// Borrow the persisted payload kind, when this variant stores data.
    #[must_use]
    pub(in crate::db) fn payload_kind(&self) -> Option<&PersistedFieldKind> {
        match self.payload_kind.as_ref() {
            Some(kind) => Some(kind.as_ref()),
            None => None,
        }
    }

    /// Return the payload storage-decode contract.
    #[must_use]
    pub(in crate::db) const fn payload_storage_decode(&self) -> FieldStorageDecode {
        self.payload_storage_decode
    }
}

///
/// PersistedRelationStrength
///
/// Owned relation-strength representation for persisted schema snapshots.
/// It mirrors generated relation metadata without requiring live schema code to
/// depend on generated `RelationStrength` values after reconciliation.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PersistedRelationStrength {
    Strong,
    Weak,
}

impl PersistedRelationStrength {
    /// Convert generated relation strength into persisted-schema shape.
    #[must_use]
    const fn from_model_strength(strength: RelationStrength) -> Self {
        match strength {
            RelationStrength::Strong => Self::Strong,
            RelationStrength::Weak => Self::Weak,
        }
    }
}
