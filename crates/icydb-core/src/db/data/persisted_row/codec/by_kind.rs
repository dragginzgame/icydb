use crate::{
    db::data::{
        by_kind::{decode as by_kind_decode, encode as by_kind_encode},
        persisted_row::codec::{
            decode_required_with_strategy, decode_with_strategy, encode_with_strategy,
            require_decoded, strategy::StorageStrategy, traversal,
        },
        storage::{decode as storage_decode, encode as storage_encode},
        storage_key::{self, decode as storage_key_decode, encode as storage_key_encode},
    },
    error::InternalError,
    model::field::FieldKind,
    traits::PersistedByKindCodec,
    types::{
        Account, Blob, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128,
        Principal, Subaccount, Timestamp, Ulid, Unit,
    },
    value::{StorageKey, Value},
};
use std::collections::{BTreeMap, BTreeSet};

/// Encode one persisted slot payload using the stricter schema-owned `ByKind`
/// storage contract.
pub fn encode_persisted_slot_payload_by_kind<T>(
    value: &T,
    kind: FieldKind,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedByKindCodec,
{
    encode_with_strategy(
        StorageStrategy::ByKind(kind),
        Some(value),
        field_name,
        |_, value, field_name| value.encode_persisted_slot_payload_by_kind(kind, field_name),
    )
}

// Decode a storage-key by-kind payload once so individual leaf codecs only
// describe the expected variant or conversion shape.
fn decode_optional_storage_key(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
) -> Result<Option<StorageKey>, InternalError> {
    storage_key_decode::optional_field(bytes, kind)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
}

// Decode a storage-key variant with one caller-provided extractor. This keeps
// mismatch errors identical while removing repeated null/variant scaffolding.
fn decode_storage_key_as<T>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
    label: impl std::fmt::Display,
    extract: impl FnOnce(StorageKey) -> Option<T>,
) -> Result<Option<T>, InternalError> {
    let Some(key) = decode_optional_storage_key(bytes, kind, field_name)? else {
        return Ok(None);
    };

    extract(key).map(Some).ok_or_else(|| {
        InternalError::persisted_row_field_decode_failed(
            field_name,
            format!("field kind {kind:?} did not decode as {label}"),
        )
    })
}

// Decode a storage-key variant that still needs a fallible primitive narrowing
// conversion, preserving the original mismatch message for both failure modes.
fn decode_storage_key_and_convert<T, Raw>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
    label: &'static str,
    extract: impl FnOnce(StorageKey) -> Option<Raw>,
    convert: impl FnOnce(Raw) -> Result<T, ()>,
) -> Result<Option<T>, InternalError> {
    let Some(value) = decode_storage_key_as(bytes, kind, field_name, label, extract)? else {
        return Ok(None);
    };

    convert(value).map(Some).map_err(|()| {
        InternalError::persisted_row_field_decode_failed(
            field_name,
            format!("field kind {kind:?} did not decode as {label}"),
        )
    })
}

/// Decode one persisted slot payload using the stricter schema-owned `ByKind`
/// storage contract.
pub fn decode_persisted_slot_payload_by_kind<T>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: PersistedByKindCodec,
{
    decode_required_with_strategy(
        StorageStrategy::ByKind(kind),
        bytes,
        field_name,
        "unexpected null for non-nullable field",
        |_, bytes, field_name| {
            T::decode_persisted_option_slot_payload_by_kind(bytes, kind, field_name)
        },
    )
}

/// Decode one optional persisted slot payload preserving the explicit null
/// sentinel under the stricter schema-owned `ByKind` storage contract.
pub fn decode_persisted_option_slot_payload_by_kind<T>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
) -> Result<Option<T>, InternalError>
where
    T: PersistedByKindCodec,
{
    decode_with_strategy(
        StorageStrategy::ByKind(kind),
        bytes,
        field_name,
        |_, bytes, field_name| {
            T::decode_persisted_option_slot_payload_by_kind(bytes, kind, field_name)
        },
    )
}

macro_rules! impl_persisted_by_kind_direct_leaf {
    ($($ty:ty => { encode: $encode:expr, decode: $decode:expr }),* $(,)?) => {
        $(
            impl PersistedByKindCodec for $ty {
                fn encode_persisted_slot_payload_by_kind(
                    &self,
                    kind: FieldKind,
                    field_name: &'static str,
                ) -> Result<Vec<u8>, InternalError> {
                    ($encode)(self, kind, field_name)
                }

                fn decode_persisted_option_slot_payload_by_kind(
                    bytes: &[u8],
                    kind: FieldKind,
                    field_name: &'static str,
                ) -> Result<Option<Self>, InternalError> {
                    ($decode)(bytes, kind, field_name)
                }
            }
        )*
    };
}

macro_rules! impl_persisted_by_kind_scalar_leaf {
    ($($ty:ty => { encode: $encode:path, decode: $decode:path }),* $(,)?) => {
        impl_persisted_by_kind_direct_leaf!(
            $(
                $ty => {
                    encode: |value: &$ty, kind: FieldKind, field_name: &'static str| {
                        $encode(*value, kind, field_name)
                    },
                    decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                        $decode(bytes, kind)
                            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
                    }
                }
            ),*
        );
    };
}

macro_rules! impl_persisted_by_kind_ref_leaf {
    ($($ty:ty => { encode: $encode:path, decode: $decode:path }),* $(,)?) => {
        impl_persisted_by_kind_direct_leaf!(
            $(
                $ty => {
                    encode: |value: &$ty, kind: FieldKind, field_name: &'static str| {
                        $encode(value, kind, field_name)
                    },
                    decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                        $decode(bytes, kind)
                            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
                    }
                }
            ),*
        );
    };
}

macro_rules! impl_persisted_by_kind_value_leaf {
    ($($ty:ty => { encode: $encode:path, decode: $decode:path }),* $(,)?) => {
        impl_persisted_by_kind_direct_leaf!(
            $(
                $ty => {
                    encode: |value: &$ty, kind: FieldKind, field_name: &'static str| {
                        $encode(*value, kind, field_name)
                    },
                    decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                        $decode(bytes, kind)
                            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
                    }
                }
            ),*
        );
    };
}

macro_rules! impl_persisted_by_kind_storage_int_leaf {
    ($($ty:ty),* $(,)?) => {
        impl_persisted_by_kind_direct_leaf!(
            $(
                $ty => {
                    encode: |value: &$ty, kind: FieldKind, field_name: &'static str| {
                        storage_key_encode::field(StorageKey::Int(i64::from(*value)), kind, field_name)
                    },
                    decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                        decode_storage_key_and_convert(
                            bytes,
                            kind,
                            field_name,
                            "storage int",
                            |key| match key {
                                StorageKey::Int(value) => Some(value),
                                _ => None,
                            },
                            |value| <$ty>::try_from(value).map_err(|_| ()),
                        )
                    }
                }
            ),*
        );
    };
}

macro_rules! impl_persisted_by_kind_storage_uint_leaf {
    ($($ty:ty),* $(,)?) => {
        impl_persisted_by_kind_direct_leaf!(
            $(
                $ty => {
                    encode: |value: &$ty, kind: FieldKind, field_name: &'static str| {
                        storage_key_encode::field(StorageKey::Uint(u64::from(*value)), kind, field_name)
                    },
                    decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                        decode_storage_key_and_convert(
                            bytes,
                            kind,
                            field_name,
                            "storage uint",
                            |key| match key {
                                StorageKey::Uint(value) => Some(value),
                                _ => None,
                            },
                            |value| <$ty>::try_from(value).map_err(|_| ()),
                        )
                    }
                }
            ),*
        );
    };
}

macro_rules! impl_persisted_by_kind_storage_leaf {
    ($($ty:ty => { variant: $variant:ident, label: $label:literal }),* $(,)?) => {
        impl_persisted_by_kind_direct_leaf!(
            $(
                $ty => {
                    encode: |value: &$ty, kind: FieldKind, field_name: &'static str| {
                        storage_key_encode::field(StorageKey::$variant(*value), kind, field_name)
                    },
                    decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                        decode_storage_key_as(bytes, kind, field_name, $label, |key| match key {
                            StorageKey::$variant(value) => Some(value),
                            _ => None,
                        })
                    }
                }
            ),*
        );
    };
}

macro_rules! impl_persisted_by_kind_storage_unit_leaf {
    ($ty:ty) => {
        impl_persisted_by_kind_direct_leaf!(
            $ty => {
                encode: |_: &$ty, kind: FieldKind, field_name: &'static str| {
                    storage_key_encode::field(StorageKey::Unit, kind, field_name)
                },
                decode: |bytes: &[u8], kind: FieldKind, field_name: &'static str| {
                    decode_storage_key_as(bytes, kind, field_name, "storage unit", |key| {
                        matches!(key, StorageKey::Unit).then_some(Unit)
                    })
                }
            }
        );
    };
}

impl_persisted_by_kind_scalar_leaf!(
    bool => { encode: by_kind_encode::bool, decode: by_kind_decode::bool },
    Float32 => { encode: by_kind_encode::float32, decode: by_kind_decode::float32 },
    Float64 => { encode: by_kind_encode::float64, decode: by_kind_decode::float64 },
    Int128 => { encode: by_kind_encode::int128, decode: by_kind_decode::int128 },
    Nat128 => { encode: by_kind_encode::nat128, decode: by_kind_decode::nat128 }
);

impl_persisted_by_kind_ref_leaf!(
    String => { encode: by_kind_encode::text, decode: by_kind_decode::text },
    Blob => { encode: by_kind_encode::blob, decode: by_kind_decode::blob },
    Int => { encode: by_kind_encode::int_big, decode: by_kind_decode::int_big },
    Nat => { encode: by_kind_encode::uint_big, decode: by_kind_decode::uint_big }
);

impl_persisted_by_kind_value_leaf!(
    Date => { encode: by_kind_encode::date, decode: by_kind_decode::date },
    Decimal => { encode: by_kind_encode::decimal, decode: by_kind_decode::decimal },
    Duration => { encode: by_kind_encode::duration, decode: by_kind_decode::duration }
);

impl_persisted_by_kind_storage_int_leaf!(i8, i16, i32, i64);
impl_persisted_by_kind_storage_uint_leaf!(u8, u16, u32, u64);
impl_persisted_by_kind_storage_leaf!(
    Account => { variant: Account, label: "storage account" },
    Timestamp => { variant: Timestamp, label: "storage timestamp" },
    Principal => { variant: Principal, label: "storage principal" },
    Subaccount => { variant: Subaccount, label: "storage subaccount" },
    Ulid => { variant: Ulid, label: "storage ulid" }
);
impl_persisted_by_kind_storage_unit_leaf!(Unit);

// Encode one explicit by-kind owner through the current field-kind structural
// contract.
pub(in crate::db::data::persisted_row::codec) fn encode_explicit_value(
    kind: FieldKind,
    value: &Value,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError> {
    if matches!(value, Value::Null) {
        return Ok(storage_encode::null());
    }

    if storage_key::supports_binary_kind(kind) {
        return storage_key_encode::binary_value(kind, value, field_name)?.ok_or_else(|| {
            InternalError::persisted_row_field_encode_failed(
                field_name,
                "storage-key binary lane rejected a supported field kind",
            )
        });
    }

    by_kind_encode::value(kind, value, field_name)
        .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))
}

// Decode one explicit by-kind owner through the current field-kind structural
// contract.
pub(in crate::db::data::persisted_row::codec) fn decode_explicit_value(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
) -> Result<Option<Value>, InternalError> {
    if storage_decode::is_null(bytes)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?
    {
        return Ok(None);
    }

    let value = if storage_key::supports_binary_kind(kind) {
        storage_key_decode::binary_value(bytes, kind)
            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?
            .ok_or_else(|| {
                InternalError::persisted_row_field_decode_failed(
                    field_name,
                    "storage-key binary lane rejected a supported field kind",
                )
            })?
    } else {
        by_kind_decode::value(bytes, kind)
            .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))?
    };

    if matches!(value, Value::Null) {
        return Ok(None);
    }

    Ok(Some(value))
}

// Detect the explicit structural null sentinel without materializing the
// non-null value. `Option<T>` uses this before delegating non-null payloads to
// the concrete typed owner.
fn decode_explicit_null_by_kind(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<bool, InternalError> {
    storage_decode::is_null(bytes)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
}

impl<T> PersistedByKindCodec for Box<T>
where
    T: PersistedByKindCodec,
{
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        self.as_ref()
            .encode_persisted_slot_payload_by_kind(kind, field_name)
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        T::decode_persisted_option_slot_payload_by_kind(bytes, kind, field_name)
            .map(|value| value.map(Self::new))
    }
}

impl<T> PersistedByKindCodec for Option<T>
where
    T: PersistedByKindCodec,
{
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_with_strategy(
            StorageStrategy::ByKind(kind),
            self.as_ref(),
            field_name,
            |_, value, field_name| value.encode_persisted_slot_payload_by_kind(kind, field_name),
        )
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        if decode_explicit_null_by_kind(bytes, field_name)? {
            return Ok(Some(None));
        }

        T::decode_persisted_option_slot_payload_by_kind(bytes, kind, field_name)
            .map(|value| value.map(Some))
    }
}

// Encode one explicit by-kind wrapper owner through its value-surface
// Decode one nested by-kind payload and require that it materializes a real
// value rather than an incompatible null for this owner type.
fn decode_required_nested_by_kind<T>(
    bytes: &[u8],
    kind: FieldKind,
    field_name: &'static str,
    label: &'static str,
) -> Result<T, InternalError>
where
    T: PersistedByKindCodec,
{
    require_decoded(
        T::decode_persisted_option_slot_payload_by_kind(bytes, kind, field_name)?,
        || {
            InternalError::persisted_row_field_decode_failed(
                field_name,
                format!(
                    "{label} payload did not decode as {}",
                    std::any::type_name::<T>()
                ),
            )
        },
    )
}

// Encode one nested by-kind item selected by a strategy-owned collection or map
// recursion step.
fn encode_nested_by_kind<T>(
    kind: FieldKind,
    value: &T,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedByKindCodec,
{
    value.encode_persisted_slot_payload_by_kind(kind, field_name)
}

// Decode one nested by-kind item selected by a strategy-owned collection or map
// recursion step.
fn decode_nested_by_kind<T>(
    kind: FieldKind,
    bytes: &[u8],
    field_name: &'static str,
    label: &'static str,
) -> Result<T, InternalError>
where
    T: PersistedByKindCodec,
{
    decode_required_nested_by_kind(bytes, kind, field_name, label)
}

// Decode a by-kind set payload while rejecting duplicate logical entries. Valid
// writers iterate a `BTreeSet`, so duplicate decoded items can only come from
// malformed framed payload bytes.
fn decode_by_kind_set<T>(
    values: Vec<T>,
    field_name: &'static str,
) -> Result<BTreeSet<T>, InternalError>
where
    T: Ord,
{
    let mut out = BTreeSet::new();
    for value in values {
        if !out.insert(value) {
            return Err(InternalError::persisted_row_field_decode_failed(
                field_name,
                format!(
                    "by-kind set payload contains duplicate items for BTreeSet<{}>",
                    std::any::type_name::<T>()
                ),
            ));
        }
    }

    Ok(out)
}

impl<T> PersistedByKindCodec for Vec<T>
where
    T: PersistedByKindCodec,
{
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        traversal::encode_collection(kind, self, field_name, encode_nested_by_kind)
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        traversal::decode_collection(kind, bytes, field_name, decode_nested_by_kind).map(Some)
    }
}

impl<T> PersistedByKindCodec for BTreeSet<T>
where
    T: Ord + PersistedByKindCodec,
{
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        traversal::encode_collection(kind, self, field_name, encode_nested_by_kind)
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        traversal::decode_collection(kind, bytes, field_name, decode_nested_by_kind)
            .and_then(|values| decode_by_kind_set(values, field_name))
            .map(Some)
    }
}

impl<K, V> PersistedByKindCodec for BTreeMap<K, V>
where
    K: Ord + PersistedByKindCodec,
    V: PersistedByKindCodec,
{
    fn encode_persisted_slot_payload_by_kind(
        &self,
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        traversal::encode_map(
            kind,
            self,
            field_name,
            encode_nested_by_kind,
            encode_nested_by_kind,
        )
    }

    fn decode_persisted_option_slot_payload_by_kind(
        bytes: &[u8],
        kind: FieldKind,
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        traversal::decode_map(
            kind,
            bytes,
            field_name,
            decode_nested_by_kind,
            decode_nested_by_kind,
        )
        .map(Some)
    }
}
