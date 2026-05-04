use crate::{
    db::data::persisted_row::codec::{
        decode_persisted_option_scalar_slot_payload, decode_persisted_option_slot_payload_by_kind,
        decode_persisted_scalar_slot_payload, decode_persisted_slot_payload_by_kind,
        encode_persisted_option_scalar_slot_payload, encode_persisted_scalar_slot_payload,
        encode_persisted_slot_payload_by_kind,
    },
    error::InternalError,
    model::field::FieldKind,
    traits::{FieldTypeMeta, PersistedByKindCodec, PersistedFieldSlotCodec},
    types::{
        Account, Blob, Date, Duration, Float32, Float64, Int, Int128, Nat, Nat128, Principal,
        Subaccount, Timestamp, Ulid, Unit,
    },
};
use std::collections::{BTreeMap, BTreeSet};

///
/// PersistedFieldSlotKind
///
/// PersistedFieldSlotKind gives collection slot-codec impls the item kind they
/// need when they delegate to the existing by-kind collection encoder.
/// This trait is private to the slot-codec adapter layer so derive code cannot
/// depend on or reimplement storage-kind inference.
///

trait PersistedFieldSlotKind {
    const KIND: FieldKind;
}

macro_rules! impl_persisted_field_slot_scalar {
    ($($ty:ty),* $(,)?) => {
        $(
            impl PersistedFieldSlotCodec for $ty {
                fn encode_persisted_slot(
                    &self,
                    field_name: &'static str,
                ) -> Result<Vec<u8>, InternalError> {
                    encode_persisted_scalar_slot_payload(self, field_name)
                }

                fn decode_persisted_slot(
                    bytes: &[u8],
                    field_name: &'static str,
                ) -> Result<Self, InternalError> {
                    decode_persisted_scalar_slot_payload(bytes, field_name)
                }

                fn encode_persisted_option_slot(
                    value: &Option<Self>,
                    field_name: &'static str,
                ) -> Result<Vec<u8>, InternalError> {
                    encode_persisted_option_scalar_slot_payload(value, field_name)
                }

                fn decode_persisted_option_slot(
                    bytes: &[u8],
                    field_name: &'static str,
                ) -> Result<Option<Self>, InternalError> {
                    decode_persisted_option_scalar_slot_payload(bytes, field_name)
                }
            }
        )*
    };
}

macro_rules! impl_persisted_field_slot_kind {
    ($($ty:ty => $kind:expr),* $(,)?) => {
        $(
            impl PersistedFieldSlotKind for $ty {
                const KIND: FieldKind = $kind;
            }
        )*
    };
}

macro_rules! impl_persisted_field_slot_by_kind {
    ($($ty:ty => $kind:expr),* $(,)?) => {
        $(
            impl PersistedFieldSlotCodec for $ty {
                fn encode_persisted_slot(
                    &self,
                    field_name: &'static str,
                ) -> Result<Vec<u8>, InternalError> {
                    encode_persisted_slot_payload_by_kind(self, $kind, field_name)
                }

                fn decode_persisted_slot(
                    bytes: &[u8],
                    field_name: &'static str,
                ) -> Result<Self, InternalError> {
                    decode_persisted_slot_payload_by_kind(bytes, $kind, field_name)
                }

                fn encode_persisted_option_slot(
                    value: &Option<Self>,
                    field_name: &'static str,
                ) -> Result<Vec<u8>, InternalError> {
                    encode_persisted_slot_payload_by_kind(value, $kind, field_name)
                }

                fn decode_persisted_option_slot(
                    bytes: &[u8],
                    field_name: &'static str,
                ) -> Result<Option<Self>, InternalError> {
                    decode_persisted_option_slot_payload_by_kind(bytes, $kind, field_name)
                }
            }
        )*
    };
}

impl<T> PersistedFieldSlotKind for T
where
    T: FieldTypeMeta,
{
    const KIND: FieldKind = T::KIND;
}

impl<T> PersistedFieldSlotCodec for Vec<T>
where
    T: PersistedByKindCodec + PersistedFieldSlotKind,
{
    fn encode_persisted_slot(&self, field_name: &'static str) -> Result<Vec<u8>, InternalError> {
        encode_persisted_slot_payload_by_kind(self, FieldKind::List(&T::KIND), field_name)
    }

    fn decode_persisted_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        decode_persisted_slot_payload_by_kind(bytes, FieldKind::List(&T::KIND), field_name)
    }

    fn encode_persisted_option_slot(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_persisted_slot_payload_by_kind(value, FieldKind::List(&T::KIND), field_name)
    }

    fn decode_persisted_option_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        decode_persisted_option_slot_payload_by_kind(bytes, FieldKind::List(&T::KIND), field_name)
    }
}

impl<T> PersistedFieldSlotCodec for BTreeSet<T>
where
    T: Ord + PersistedByKindCodec + PersistedFieldSlotKind,
{
    fn encode_persisted_slot(&self, field_name: &'static str) -> Result<Vec<u8>, InternalError> {
        encode_persisted_slot_payload_by_kind(self, FieldKind::Set(&T::KIND), field_name)
    }

    fn decode_persisted_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        decode_persisted_slot_payload_by_kind(bytes, FieldKind::Set(&T::KIND), field_name)
    }

    fn encode_persisted_option_slot(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_persisted_slot_payload_by_kind(value, FieldKind::Set(&T::KIND), field_name)
    }

    fn decode_persisted_option_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        decode_persisted_option_slot_payload_by_kind(bytes, FieldKind::Set(&T::KIND), field_name)
    }
}

impl<K, V> PersistedFieldSlotCodec for BTreeMap<K, V>
where
    K: Ord + PersistedByKindCodec + PersistedFieldSlotKind,
    V: PersistedByKindCodec + PersistedFieldSlotKind,
{
    fn encode_persisted_slot(&self, field_name: &'static str) -> Result<Vec<u8>, InternalError> {
        encode_persisted_slot_payload_by_kind(
            self,
            FieldKind::Map {
                key: &K::KIND,
                value: &V::KIND,
            },
            field_name,
        )
    }

    fn decode_persisted_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        decode_persisted_slot_payload_by_kind(
            bytes,
            FieldKind::Map {
                key: &K::KIND,
                value: &V::KIND,
            },
            field_name,
        )
    }

    fn encode_persisted_option_slot(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        encode_persisted_slot_payload_by_kind(
            value,
            FieldKind::Map {
                key: &K::KIND,
                value: &V::KIND,
            },
            field_name,
        )
    }

    fn decode_persisted_option_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        decode_persisted_option_slot_payload_by_kind(
            bytes,
            FieldKind::Map {
                key: &K::KIND,
                value: &V::KIND,
            },
            field_name,
        )
    }
}

impl_persisted_field_slot_scalar!(
    (),
    bool,
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    String,
    Blob,
    Date,
    Duration,
    Float32,
    Float64,
    Principal,
    Subaccount,
    Timestamp,
    Ulid,
    Unit,
);

impl_persisted_field_slot_by_kind!(
    Account => FieldKind::Account,
    Int => FieldKind::IntBig,
    Int128 => FieldKind::Int128,
    Nat => FieldKind::UintBig,
    Nat128 => FieldKind::Uint128,
);

impl_persisted_field_slot_kind!(
    () => FieldKind::Unit,
    bool => FieldKind::Bool,
    i8 => FieldKind::Int,
    i16 => FieldKind::Int,
    i32 => FieldKind::Int,
    i64 => FieldKind::Int,
    u8 => FieldKind::Uint,
    u16 => FieldKind::Uint,
    u32 => FieldKind::Uint,
    u64 => FieldKind::Uint,
    String => FieldKind::Text { max_len: None },
    Account => FieldKind::Account,
    Blob => FieldKind::Blob { max_len: None },
    Date => FieldKind::Date,
    Duration => FieldKind::Duration,
    Float32 => FieldKind::Float32,
    Float64 => FieldKind::Float64,
    Int => FieldKind::IntBig,
    Int128 => FieldKind::Int128,
    Nat => FieldKind::UintBig,
    Nat128 => FieldKind::Uint128,
    Principal => FieldKind::Principal,
    Subaccount => FieldKind::Subaccount,
    Timestamp => FieldKind::Timestamp,
    Ulid => FieldKind::Ulid,
    Unit => FieldKind::Unit,
);
