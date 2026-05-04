use crate::{
    db::data::{
        persisted_row::codec::{encode_with_strategy, strategy::StorageStrategy, traversal},
        storage::{decode as storage_decode, encode as storage_encode},
    },
    error::InternalError,
    traits::PersistedStructuredFieldCodec,
    types::{
        Account, Blob, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128,
        Principal, Subaccount, Timestamp, Ulid, Unit,
    },
};
use std::collections::{BTreeMap, BTreeSet};

// Decode a structured set by reusing strategy-aware collection traversal and
// keeping the duplicate check local to the set contract.
fn decode_structured_set<T>(bytes: &[u8]) -> Result<BTreeSet<T>, InternalError>
where
    T: Ord + PersistedStructuredFieldCodec,
{
    let item_bytes = traversal::decode_structured_collection(bytes, decode_nested_structured)?;
    let mut out = BTreeSet::new();
    for item in item_bytes {
        if !out.insert(item) {
            return Err(traversal::structured_container_decode_failed(&format!(
                "BTreeSet<{}>",
                std::any::type_name::<T>()
            )));
        }
    }

    Ok(out)
}

// Encode an optional structured payload while preserving the structural null
// sentinel without pretending there is an owning field name.
fn encode_structured_option<T>(value: Option<&T>) -> Result<Vec<u8>, InternalError>
where
    T: PersistedStructuredFieldCodec,
{
    value.map_or_else(
        || Ok(storage_encode::null()),
        PersistedStructuredFieldCodec::encode_persisted_structured_payload,
    )
}

// Decode an optional structured payload while preserving explicit null before
// delegating to the concrete structured owner.
fn decode_structured_option<T>(bytes: &[u8]) -> Result<Option<T>, InternalError>
where
    T: PersistedStructuredFieldCodec,
{
    if storage_decode::is_null(bytes).map_err(InternalError::persisted_row_decode_failed)? {
        return Ok(None);
    }

    T::decode_persisted_structured_payload(bytes).map(Some)
}

// Encode one nested structured item selected by structured collection/map
// traversal.
fn encode_nested_structured<T>(value: &T) -> Result<Vec<u8>, InternalError>
where
    T: PersistedStructuredFieldCodec,
{
    value.encode_persisted_structured_payload()
}

// Decode one nested structured item selected by structured collection/map
// traversal.
fn decode_nested_structured<T>(bytes: &[u8]) -> Result<T, InternalError>
where
    T: PersistedStructuredFieldCodec,
{
    T::decode_persisted_structured_payload(bytes)
}
macro_rules! impl_persisted_structured_signed_scalar_codec {
    ($($ty:ty),* $(,)?) => {
        $(
            impl PersistedStructuredFieldCodec for $ty {
                fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
                    Ok(storage_encode::i64(i64::from(*self)))
                }

                fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
                    let value = storage_decode::i64(bytes)
                        .map_err(InternalError::persisted_row_decode_failed)?;

                    <$ty>::try_from(value).map_err(|_| {
                        InternalError::persisted_row_decode_failed(format!(
                            "value payload does not match {}",
                            std::any::type_name::<$ty>()
                        ))
                    })
                }
            }
        )*
    };
}

macro_rules! impl_persisted_structured_unsigned_scalar_codec {
    ($($ty:ty),* $(,)?) => {
        $(
            impl PersistedStructuredFieldCodec for $ty {
                fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
                    Ok(storage_encode::u64(u64::from(*self)))
                }

                fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
                    let value = storage_decode::u64(bytes)
                        .map_err(InternalError::persisted_row_decode_failed)?;

                    <$ty>::try_from(value).map_err(|_| {
                        InternalError::persisted_row_decode_failed(format!(
                            "value payload does not match {}",
                            std::any::type_name::<$ty>()
                        ))
                    })
                }
            }
        )*
    };
}

impl PersistedStructuredFieldCodec for bool {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::bool(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::bool(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for String {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::text(self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::text(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Blob {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::blob(self.as_slice()))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::blob(bytes)
            .map(Self::from)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Account {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        storage_encode::account(*self)
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::account(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Decimal {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::decimal(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::decimal(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Float32 {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::float32(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::float32(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Float64 {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::float64(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::float64(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Principal {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        storage_encode::principal(*self)
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::principal(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Subaccount {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::subaccount(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::subaccount(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Timestamp {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::timestamp(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::timestamp(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Date {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::date(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::date(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Duration {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::duration(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::duration(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Ulid {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::ulid(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::ulid(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Int128 {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::int128(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::int128(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Nat128 {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::nat128(*self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::nat128(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Int {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::int(self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::int(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Nat {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::nat(self))
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::nat(bytes).map_err(InternalError::persisted_row_decode_failed)
    }
}

impl PersistedStructuredFieldCodec for Unit {
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        Ok(storage_encode::unit())
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        storage_decode::unit(bytes)
            .map(|()| Self)
            .map_err(InternalError::persisted_row_decode_failed)
    }
}

impl_persisted_structured_signed_scalar_codec!(i8, i16, i32, i64);
impl_persisted_structured_unsigned_scalar_codec!(u8, u16, u32, u64);

impl<T> PersistedStructuredFieldCodec for Vec<T>
where
    T: PersistedStructuredFieldCodec,
{
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        traversal::encode_structured_collection(self, encode_nested_structured)
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        traversal::decode_structured_collection(bytes, decode_nested_structured)
    }
}

impl<T> PersistedStructuredFieldCodec for Option<T>
where
    T: PersistedStructuredFieldCodec,
{
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        encode_structured_option(self.as_ref())
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structured_option(bytes)
    }
}

impl<T> PersistedStructuredFieldCodec for Box<T>
where
    T: PersistedStructuredFieldCodec,
{
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        self.as_ref().encode_persisted_structured_payload()
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        T::decode_persisted_structured_payload(bytes).map(Self::new)
    }
}

impl<T> PersistedStructuredFieldCodec for BTreeSet<T>
where
    T: Ord + PersistedStructuredFieldCodec,
{
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        traversal::encode_structured_collection(self, encode_nested_structured)
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        decode_structured_set(bytes)
    }
}

impl<K, V> PersistedStructuredFieldCodec for BTreeMap<K, V>
where
    K: Ord + PersistedStructuredFieldCodec,
    V: PersistedStructuredFieldCodec,
{
    fn encode_persisted_structured_payload(&self) -> Result<Vec<u8>, InternalError> {
        traversal::encode_structured_map(self, encode_nested_structured, encode_nested_structured)
    }

    fn decode_persisted_structured_payload(bytes: &[u8]) -> Result<Self, InternalError> {
        traversal::decode_structured_map(bytes, decode_nested_structured, decode_nested_structured)
    }
}

/// Decode one persisted structured payload through the direct structured
/// field codec owner.
pub fn decode_persisted_structured_slot_payload<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<T, InternalError>
where
    T: PersistedStructuredFieldCodec,
{
    T::decode_persisted_structured_payload(bytes)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
}

/// Decode one persisted repeated structured payload through the `Vec<T>`
/// structured codec owner.
pub fn decode_persisted_structured_many_slot_payload<T>(
    bytes: &[u8],
    field_name: &'static str,
) -> Result<Vec<T>, InternalError>
where
    Vec<T>: PersistedStructuredFieldCodec,
{
    <Vec<T>>::decode_persisted_structured_payload(bytes)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(field_name, err))
}

/// Encode one structured field payload through the direct structured field
/// codec owner.
pub fn encode_persisted_structured_slot_payload<T>(
    value: &T,
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedStructuredFieldCodec,
{
    encode_with_strategy(
        StorageStrategy::Structured,
        Some(value),
        field_name,
        |_, value, field_name| {
            value
                .encode_persisted_structured_payload()
                .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))
        },
    )
}

/// Encode one repeated structured payload through the `Vec<T>` structured
/// codec owner.
pub fn encode_persisted_structured_many_slot_payload<T>(
    values: &[T],
    field_name: &'static str,
) -> Result<Vec<u8>, InternalError>
where
    T: PersistedStructuredFieldCodec,
    Vec<T>: PersistedStructuredFieldCodec,
{
    encode_with_strategy(
        StorageStrategy::Structured,
        Some(values),
        field_name,
        |_, values, field_name| {
            traversal::encode_structured_collection(values.iter(), encode_nested_structured)
                .map_err(|err| InternalError::persisted_row_field_encode_failed(field_name, err))
        },
    )
}
