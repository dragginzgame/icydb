//! Module: model::field
//! Responsibility: runtime field metadata and storage-decode contracts.
//! Does not own: planner-wide query semantics or row-container orchestration.
//! Boundary: field-level runtime schema surface used by storage and planning layers.

use crate::{
    traits::RuntimeValueKind,
    types::{Decimal, EntityTag},
    value::Value,
};
use std::{borrow::Cow, cmp::Ordering};

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

    /// Validate one runtime value against this field's persisted storage contract.
    ///
    /// This is the model-owned compatibility gate used before row bytes are
    /// emitted. It intentionally checks storage compatibility, not query
    /// predicate compatibility, so `FieldStorageDecode::Value` can accept
    /// open-ended structured payloads while still enforcing outer collection
    /// shape, decimal scale, and deterministic set/map ordering.
    pub(crate) fn validate_runtime_value_for_storage(&self, value: &Value) -> Result<(), String> {
        if matches!(value, Value::Null) {
            if self.nullable() {
                return Ok(());
            }

            return Err("required field cannot store null".into());
        }

        let accepts = match self.storage_decode() {
            FieldStorageDecode::Value => {
                value_storage_kind_accepts_runtime_value(self.kind(), value)
            }
            FieldStorageDecode::ByKind => {
                by_kind_storage_kind_accepts_runtime_value(self.kind(), value)
            }
        };
        if !accepts {
            return Err(format!(
                "field kind {:?} does not accept runtime value {value:?}",
                self.kind()
            ));
        }

        ensure_decimal_scale_matches(self.kind(), value)?;
        ensure_text_max_len_matches(self.kind(), value)?;
        ensure_value_is_deterministic_for_storage(self.kind(), value)
    }

    // Normalize decimal payloads to this field's fixed scale before storage
    // encoding. Validation still runs after this step, so malformed shapes and
    // deterministic collection rules remain owned by the normal field contract.
    pub(crate) fn normalize_runtime_value_for_storage<'a>(
        &self,
        value: &'a Value,
    ) -> Result<Cow<'a, Value>, String> {
        normalize_decimal_scale_for_storage(self.kind(), value)
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
        FieldKind::Text { .. } => LeafCodec::Scalar(ScalarCodec::Text),
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
    Text {
        /// Optional schema-declared maximum Unicode scalar count for text fields.
        max_len: Option<u32>,
    },
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
    pub const fn value_kind(&self) -> RuntimeValueKind {
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
            | Self::Text { .. }
            | Self::Timestamp
            | Self::Uint
            | Self::Uint128
            | Self::UintBig
            | Self::Ulid
            | Self::Unit
            | Self::Decimal { .. }
            | Self::Relation { .. } => RuntimeValueKind::Atomic,
            Self::List(_) | Self::Set(_) => RuntimeValueKind::Structured { queryable: true },
            Self::Map { .. } => RuntimeValueKind::Structured { queryable: false },
            Self::Structured { queryable } => RuntimeValueKind::Structured {
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
            | Self::Text { .. }
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
            | (Self::Text { .. }, Value::Text(_))
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

// `FieldStorageDecode::ByKind` follows the same literal compatibility rule as
// the schema predicate layer without routing the storage model through
// `db::schema`. Structured field kinds are intentionally not accepted here;
// fields that persist open-ended structured payloads use
// `FieldStorageDecode::Value` instead.
fn by_kind_storage_kind_accepts_runtime_value(kind: FieldKind, value: &Value) -> bool {
    match (kind, value) {
        (FieldKind::Relation { key_kind, .. }, value) => {
            by_kind_storage_kind_accepts_runtime_value(*key_kind, value)
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => items
            .iter()
            .all(|item| by_kind_storage_kind_accepts_runtime_value(*inner, item)),
        (
            FieldKind::Map {
                key,
                value: value_kind,
            },
            Value::Map(entries),
        ) => {
            if Value::validate_map_entries(entries.as_slice()).is_err() {
                return false;
            }

            entries.iter().all(|(entry_key, entry_value)| {
                by_kind_storage_kind_accepts_runtime_value(*key, entry_key)
                    && by_kind_storage_kind_accepts_runtime_value(*value_kind, entry_value)
            })
        }
        (FieldKind::Structured { .. }, _) => false,
        _ => kind.accepts_value(value),
    }
}

// `FieldStorageDecode::Value` fields persist an opaque runtime `Value` envelope,
// so `FieldKind::Structured` must stay open-ended while outer collection/map
// shapes still enforce the recursive structure the model owns.
fn value_storage_kind_accepts_runtime_value(kind: FieldKind, value: &Value) -> bool {
    match (kind, value) {
        (FieldKind::Structured { .. }, _) => true,
        (FieldKind::Relation { key_kind, .. }, value) => {
            value_storage_kind_accepts_runtime_value(*key_kind, value)
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => items
            .iter()
            .all(|item| value_storage_kind_accepts_runtime_value(*inner, item)),
        (
            FieldKind::Map {
                key,
                value: value_kind,
            },
            Value::Map(entries),
        ) => {
            if Value::validate_map_entries(entries.as_slice()).is_err() {
                return false;
            }

            entries.iter().all(|(entry_key, entry_value)| {
                value_storage_kind_accepts_runtime_value(*key, entry_key)
                    && value_storage_kind_accepts_runtime_value(*value_kind, entry_value)
            })
        }
        _ => kind.accepts_value(value),
    }
}

// Enforce fixed decimal scales through nested collection/map shapes before a
// field-level runtime value is persisted.
fn ensure_decimal_scale_matches(kind: FieldKind, value: &Value) -> Result<(), String> {
    if matches!(value, Value::Null) {
        return Ok(());
    }

    match (kind, value) {
        (FieldKind::Decimal { scale }, Value::Decimal(decimal)) => {
            if decimal.scale() != scale {
                return Err(format!(
                    "decimal scale mismatch: expected {scale}, found {}",
                    decimal.scale()
                ));
            }

            Ok(())
        }
        (FieldKind::Relation { key_kind, .. }, value) => {
            ensure_decimal_scale_matches(*key_kind, value)
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => {
            for item in items {
                ensure_decimal_scale_matches(*inner, item)?;
            }

            Ok(())
        }
        (
            FieldKind::Map {
                key,
                value: map_value,
            },
            Value::Map(entries),
        ) => {
            for (entry_key, entry_value) in entries {
                ensure_decimal_scale_matches(*key, entry_key)?;
                ensure_decimal_scale_matches(*map_value, entry_value)?;
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

// Normalize fixed-scale decimal values through nested collection/map shapes
// before the field-level payload is encoded. This is write-side canonicalization;
// callers that validate already persisted bytes still use the exact scale check.
fn normalize_decimal_scale_for_storage(
    kind: FieldKind,
    value: &Value,
) -> Result<Cow<'_, Value>, String> {
    if matches!(value, Value::Null) {
        return Ok(Cow::Borrowed(value));
    }

    match (kind, value) {
        (FieldKind::Decimal { scale }, Value::Decimal(decimal)) => {
            let normalized = decimal_with_storage_scale(*decimal, scale).ok_or_else(|| {
                format!(
                    "decimal scale mismatch: expected {scale}, found {}",
                    decimal.scale()
                )
            })?;

            if normalized.scale() == decimal.scale() {
                Ok(Cow::Borrowed(value))
            } else {
                Ok(Cow::Owned(Value::Decimal(normalized)))
            }
        }
        (FieldKind::Relation { key_kind, .. }, value) => {
            normalize_decimal_scale_for_storage(*key_kind, value)
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => {
            normalize_decimal_list_items(*inner, items.as_slice()).map(|items| {
                items.map_or_else(
                    || Cow::Borrowed(value),
                    |items| Cow::Owned(Value::List(items)),
                )
            })
        }
        (
            FieldKind::Map {
                key,
                value: map_value,
            },
            Value::Map(entries),
        ) => normalize_decimal_map_entries(*key, *map_value, entries.as_slice()).map(|entries| {
            entries.map_or_else(
                || Cow::Borrowed(value),
                |entries| Cow::Owned(Value::Map(entries)),
            )
        }),
        _ => Ok(Cow::Borrowed(value)),
    }
}

// Convert one decimal into the exact storage scale. Lower-scale values are
// padded without changing their numeric value; higher-scale values use the same
// round-half-away-from-zero policy as SQL/fluent decimal rounding.
fn decimal_with_storage_scale(decimal: Decimal, scale: u32) -> Option<Decimal> {
    match decimal.scale().cmp(&scale) {
        Ordering::Equal => Some(decimal),
        Ordering::Less => decimal
            .scale_to_integer(scale)
            .map(|mantissa| Decimal::from_i128_with_scale(mantissa, scale)),
        Ordering::Greater => Some(decimal.round_dp(scale)),
    }
}

// Normalize decimal items while preserving the original list allocation when
// every item is already canonical for its nested field kind.
fn normalize_decimal_list_items(
    kind: FieldKind,
    items: &[Value],
) -> Result<Option<Vec<Value>>, String> {
    let mut normalized_items = None;

    for (index, item) in items.iter().enumerate() {
        let normalized = normalize_decimal_scale_for_storage(kind, item)?;
        if let Cow::Owned(value) = normalized {
            let items = normalized_items.get_or_insert_with(|| items.to_vec());
            items[index] = value;
        }
    }

    Ok(normalized_items)
}

// Normalize decimal keys and values while preserving the original map
// allocation when every entry is already canonical for its nested field kind.
fn normalize_decimal_map_entries(
    key_kind: FieldKind,
    value_kind: FieldKind,
    entries: &[(Value, Value)],
) -> Result<Option<Vec<(Value, Value)>>, String> {
    let mut normalized_entries = None;

    for (index, (entry_key, entry_value)) in entries.iter().enumerate() {
        let normalized_key = normalize_decimal_scale_for_storage(key_kind, entry_key)?;
        let normalized_value = normalize_decimal_scale_for_storage(value_kind, entry_value)?;

        if matches!(normalized_key, Cow::Owned(_)) || matches!(normalized_value, Cow::Owned(_)) {
            let entries = normalized_entries.get_or_insert_with(|| entries.to_vec());
            if let Cow::Owned(value) = normalized_key {
                entries[index].0 = value;
            }
            if let Cow::Owned(value) = normalized_value {
                entries[index].1 = value;
            }
        }
    }

    Ok(normalized_entries)
}

// Enforce bounded text length through nested collection/map shapes before a
// field-level runtime value is persisted.
fn ensure_text_max_len_matches(kind: FieldKind, value: &Value) -> Result<(), String> {
    if matches!(value, Value::Null) {
        return Ok(());
    }

    match (kind, value) {
        (FieldKind::Text { max_len: Some(max) }, Value::Text(text)) => {
            let len = text.chars().count();
            if len > max as usize {
                return Err(format!(
                    "text length exceeds max_len: expected at most {max}, found {len}"
                ));
            }

            Ok(())
        }
        (FieldKind::Relation { key_kind, .. }, value) => {
            ensure_text_max_len_matches(*key_kind, value)
        }
        (FieldKind::List(inner) | FieldKind::Set(inner), Value::List(items)) => {
            for item in items {
                ensure_text_max_len_matches(*inner, item)?;
            }

            Ok(())
        }
        (
            FieldKind::Map {
                key,
                value: map_value,
            },
            Value::Map(entries),
        ) => {
            for (entry_key, entry_value) in entries {
                ensure_text_max_len_matches(*key, entry_key)?;
                ensure_text_max_len_matches(*map_value, entry_value)?;
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

// Enforce the canonical persisted ordering rules for set/map shapes before one
// field-level runtime value becomes row bytes.
fn ensure_value_is_deterministic_for_storage(kind: FieldKind, value: &Value) -> Result<(), String> {
    match (kind, value) {
        (FieldKind::Set(_), Value::List(items)) => {
            for pair in items.windows(2) {
                let [left, right] = pair else {
                    continue;
                };
                if Value::canonical_cmp(left, right) != Ordering::Less {
                    return Err("set payload must already be canonical and deduplicated".into());
                }
            }

            Ok(())
        }
        (FieldKind::Map { .. }, Value::Map(entries)) => {
            Value::validate_map_entries(entries.as_slice()).map_err(|err| err.to_string())?;

            if !Value::map_entries_are_strictly_canonical(entries.as_slice()) {
                return Err("map payload must already be canonical and deduplicated".into());
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        model::field::{FieldKind, FieldModel},
        value::Value,
    };

    static BOUNDED_TEXT: FieldKind = FieldKind::Text { max_len: Some(3) };

    #[test]
    fn text_max_len_accepts_unbounded_text() {
        let field = FieldModel::generated("name", FieldKind::Text { max_len: None });

        assert!(
            field
                .validate_runtime_value_for_storage(&Value::Text("Ada Lovelace".into()))
                .is_ok()
        );
    }

    #[test]
    fn text_max_len_counts_unicode_scalars_not_bytes() {
        let field = FieldModel::generated("name", BOUNDED_TEXT);

        assert!(
            field
                .validate_runtime_value_for_storage(&Value::Text("ééé".into()))
                .is_ok()
        );
        assert!(
            field
                .validate_runtime_value_for_storage(&Value::Text("éééé".into()))
                .is_err()
        );
    }

    #[test]
    fn text_max_len_recurses_through_collections() {
        static TEXT_LIST: FieldKind = FieldKind::List(&BOUNDED_TEXT);
        static TEXT_MAP: FieldKind = FieldKind::Map {
            key: &BOUNDED_TEXT,
            value: &BOUNDED_TEXT,
        };

        let list_field = FieldModel::generated("names", TEXT_LIST);
        let map_field = FieldModel::generated("labels", TEXT_MAP);

        assert!(
            list_field
                .validate_runtime_value_for_storage(&Value::List(vec![
                    Value::Text("Ada".into()),
                    Value::Text("Bob".into()),
                ]))
                .is_ok()
        );
        assert!(
            list_field
                .validate_runtime_value_for_storage(&Value::List(vec![Value::Text("Grace".into())]))
                .is_err()
        );
        assert!(
            map_field
                .validate_runtime_value_for_storage(&Value::Map(vec![(
                    Value::Text("key".into()),
                    Value::Text("val".into()),
                )]))
                .is_ok()
        );
        assert!(
            map_field
                .validate_runtime_value_for_storage(&Value::Map(vec![(
                    Value::Text("long".into()),
                    Value::Text("val".into()),
                )]))
                .is_err()
        );
    }
}
