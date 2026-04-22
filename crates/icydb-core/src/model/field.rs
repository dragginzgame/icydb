//! Module: model::field
//! Responsibility: runtime field metadata and storage-decode contracts.
//! Does not own: planner-wide query semantics or row-container orchestration.
//! Boundary: field-level runtime schema surface used by storage and planning layers.

use crate::{traits::FieldValueKind, types::EntityTag, value::Value};

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
/// ScalarCodec
///
/// ScalarCodec identifies the canonical binary leaf encoding used for one
/// scalar persisted field payload.
/// These codecs are fixed-width or span-bounded by the surrounding row slot
/// container; they do not perform map/array/value dispatch.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScalarCodec {
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
    Uint64,
    Ulid,
    Unit,
}

///
/// LeafCodec
///
/// LeafCodec declares whether one persisted field payload uses a dedicated
/// scalar codec or falls back to structural leaf decoding.
/// The row container consults this metadata before deciding whether a slot can
/// stay on the scalar fast path.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeafCodec {
    Scalar(ScalarCodec),
    StructuralFallback,
}

///
/// EnumVariantModel
///
/// EnumVariantModel carries structural decode metadata for one generated enum
/// variant payload.
/// Runtime structural decode uses this to stay on the field-kind contract for
/// enum payloads instead of falling back to generic untyped structural decode.
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
    /// Whether the field may persist an explicit `NULL` payload.
    pub(crate) nullable: bool,
    /// Persisted field decode contract used by structural runtime decoders.
    pub(crate) storage_decode: FieldStorageDecode,
    /// Leaf payload codec used by slot readers and writers.
    pub(crate) leaf_codec: LeafCodec,
    /// Insert-time generation contract admitted on reduced SQL write lanes.
    pub(crate) insert_generation: Option<FieldInsertGeneration>,
    /// Auto-managed write contract emitted for derive-owned system fields.
    pub(crate) write_management: Option<FieldWriteManagement>,
}

///
/// FieldInsertGeneration
///
/// FieldInsertGeneration declares whether one runtime field may be synthesized
/// by the reduced SQL insert boundary when the user omits that field.
/// This stays separate from typed-Rust `Default` behavior so write-time
/// generation remains an explicit schema contract.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldInsertGeneration {
    /// Generate one fresh `Ulid` value at insert time.
    Ulid,
    /// Generate one current wall-clock `Timestamp` value at insert time.
    Timestamp,
}

///
/// FieldWriteManagement
///
/// FieldWriteManagement declares whether one runtime field is owned by the
/// write boundary during insert or update synthesis.
/// This keeps auto-managed system fields explicit in schema/runtime metadata
/// instead of relying on literal field names in write paths.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldWriteManagement {
    /// Fill only on insert when the row is first created.
    CreatedAt,
    /// Refresh on insert and every update.
    UpdatedAt,
}

impl FieldModel {
    /// Build one generated runtime field descriptor.
    ///
    /// This constructor exists for derive/codegen output and trusted test
    /// fixtures. Runtime planning and execution treat `FieldModel` values as
    /// build-time-validated metadata.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated(name: &'static str, kind: FieldKind) -> Self {
        Self::generated_with_storage_decode_and_nullability(
            name,
            kind,
            FieldStorageDecode::ByKind,
            false,
        )
    }

    /// Build one runtime field descriptor with an explicit persisted decode contract.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_storage_decode(
        name: &'static str,
        kind: FieldKind,
        storage_decode: FieldStorageDecode,
    ) -> Self {
        Self::generated_with_storage_decode_and_nullability(name, kind, storage_decode, false)
    }

    /// Build one runtime field descriptor with an explicit decode contract and nullability.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_storage_decode_and_nullability(
        name: &'static str,
        kind: FieldKind,
        storage_decode: FieldStorageDecode,
        nullable: bool,
    ) -> Self {
        Self::generated_with_storage_decode_nullability_and_write_policies(
            name,
            kind,
            storage_decode,
            nullable,
            None,
            None,
        )
    }

    /// Build one runtime field descriptor with an explicit decode contract, nullability,
    /// and insert-time generation contract.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_storage_decode_nullability_and_insert_generation(
        name: &'static str,
        kind: FieldKind,
        storage_decode: FieldStorageDecode,
        nullable: bool,
        insert_generation: Option<FieldInsertGeneration>,
    ) -> Self {
        Self::generated_with_storage_decode_nullability_and_write_policies(
            name,
            kind,
            storage_decode,
            nullable,
            insert_generation,
            None,
        )
    }

    /// Build one runtime field descriptor with explicit insert-generation and
    /// write-management policies.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_storage_decode_nullability_and_write_policies(
        name: &'static str,
        kind: FieldKind,
        storage_decode: FieldStorageDecode,
        nullable: bool,
        insert_generation: Option<FieldInsertGeneration>,
        write_management: Option<FieldWriteManagement>,
    ) -> Self {
        Self {
            name,
            kind,
            nullable,
            storage_decode,
            leaf_codec: leaf_codec_for(kind, storage_decode),
            insert_generation,
            write_management,
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

    /// Return whether the persisted field contract permits explicit `NULL`.
    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the persisted field decode contract.
    #[must_use]
    pub const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }

    /// Return the persisted leaf payload codec.
    #[must_use]
    pub const fn leaf_codec(&self) -> LeafCodec {
        self.leaf_codec
    }

    /// Return the reduced-SQL insert-time generation contract for this field.
    #[must_use]
    pub const fn insert_generation(&self) -> Option<FieldInsertGeneration> {
        self.insert_generation
    }

    /// Return the write-boundary management contract for this field.
    #[must_use]
    pub const fn write_management(&self) -> Option<FieldWriteManagement> {
        self.write_management
    }
}

// Resolve the canonical leaf codec from semantic field kind plus storage
// contract. Fields that intentionally persist as `Value` or that still require
// recursive payload decoding remain on the shared structural fallback.
const fn leaf_codec_for(kind: FieldKind, storage_decode: FieldStorageDecode) -> LeafCodec {
    if matches!(storage_decode, FieldStorageDecode::Value) {
        return LeafCodec::StructuralFallback;
    }

    match kind {
        FieldKind::Blob => LeafCodec::Scalar(ScalarCodec::Blob),
        FieldKind::Bool => LeafCodec::Scalar(ScalarCodec::Bool),
        FieldKind::Date => LeafCodec::Scalar(ScalarCodec::Date),
        FieldKind::Duration => LeafCodec::Scalar(ScalarCodec::Duration),
        FieldKind::Float32 => LeafCodec::Scalar(ScalarCodec::Float32),
        FieldKind::Float64 => LeafCodec::Scalar(ScalarCodec::Float64),
        FieldKind::Int => LeafCodec::Scalar(ScalarCodec::Int64),
        FieldKind::Principal => LeafCodec::Scalar(ScalarCodec::Principal),
        FieldKind::Subaccount => LeafCodec::Scalar(ScalarCodec::Subaccount),
        FieldKind::Text => LeafCodec::Scalar(ScalarCodec::Text),
        FieldKind::Timestamp => LeafCodec::Scalar(ScalarCodec::Timestamp),
        FieldKind::Uint => LeafCodec::Scalar(ScalarCodec::Uint64),
        FieldKind::Ulid => LeafCodec::Scalar(ScalarCodec::Ulid),
        FieldKind::Unit => LeafCodec::Scalar(ScalarCodec::Unit),
        FieldKind::Relation { key_kind, .. } => leaf_codec_for(*key_kind, storage_decode),
        FieldKind::Account
        | FieldKind::Decimal { .. }
        | FieldKind::Enum { .. }
        | FieldKind::Int128
        | FieldKind::IntBig
        | FieldKind::List(_)
        | FieldKind::Map { .. }
        | FieldKind::Set(_)
        | FieldKind::Structured { .. }
        | FieldKind::Uint128
        | FieldKind::UintBig => LeafCodec::StructuralFallback,
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

    /// Return true when this planner-frozen grouped field kind can stay on the
    /// borrowed grouped-key probe path without owned canonical materialization.
    #[must_use]
    pub(crate) fn supports_group_probe(&self) -> bool {
        match self {
            Self::Enum { variants, .. } => variants.iter().all(|variant| {
                variant
                    .payload_kind()
                    .is_none_or(Self::supports_group_probe)
            }),
            Self::Relation { key_kind, .. } => key_kind.supports_group_probe(),
            Self::List(_)
            | Self::Set(_)
            | Self::Map { .. }
            | Self::Structured { .. }
            | Self::Unit => false,
            Self::Account
            | Self::Blob
            | Self::Bool
            | Self::Date
            | Self::Decimal { .. }
            | Self::Duration
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
            | Self::Ulid => true,
        }
    }

    /// Match one runtime value against this field kind contract.
    ///
    /// This is the shared recursive field-kind acceptance boundary used by
    /// persisted-row encoding, mutation-save validation, and aggregate field
    /// extraction.
    #[must_use]
    pub(crate) fn accepts_value(&self, value: &Value) -> bool {
        match (self, value) {
            (Self::Account, Value::Account(_))
            | (Self::Blob, Value::Blob(_))
            | (Self::Bool, Value::Bool(_))
            | (Self::Date, Value::Date(_))
            | (Self::Decimal { .. }, Value::Decimal(_))
            | (Self::Duration, Value::Duration(_))
            | (Self::Enum { .. }, Value::Enum(_))
            | (Self::Float32, Value::Float32(_))
            | (Self::Float64, Value::Float64(_))
            | (Self::Int, Value::Int(_))
            | (Self::Int128, Value::Int128(_))
            | (Self::IntBig, Value::IntBig(_))
            | (Self::Principal, Value::Principal(_))
            | (Self::Subaccount, Value::Subaccount(_))
            | (Self::Text, Value::Text(_))
            | (Self::Timestamp, Value::Timestamp(_))
            | (Self::Uint, Value::Uint(_))
            | (Self::Uint128, Value::Uint128(_))
            | (Self::UintBig, Value::UintBig(_))
            | (Self::Ulid, Value::Ulid(_))
            | (Self::Unit, Value::Unit)
            | (Self::Structured { .. }, Value::List(_) | Value::Map(_)) => true,
            (Self::Relation { key_kind, .. }, value) => key_kind.accepts_value(value),
            (Self::List(inner) | Self::Set(inner), Value::List(items)) => {
                items.iter().all(|item| inner.accepts_value(item))
            }
            (Self::Map { key, value }, Value::Map(entries)) => {
                if Value::validate_map_entries(entries.as_slice()).is_err() {
                    return false;
                }

                entries.iter().all(|(entry_key, entry_value)| {
                    key.accepts_value(entry_key) && value.accepts_value(entry_value)
                })
            }
            _ => false,
        }
    }
}
