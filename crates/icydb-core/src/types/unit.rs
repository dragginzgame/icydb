//! Module: types::unit
//! Defines the zero-sized unit key/value wrapper used by schemas that need an
//! explicit empty identity.

use crate::{
    traits::{
        Atomic, EntityKeyBytes, RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind,
        RuntimeValueMeta, SanitizeAuto, SanitizeCustom, StorageKeyCodec, ValidateAuto,
        ValidateCustom, Visitable,
    },
    value::{StorageKey, StorageKeyEncodeError, Value},
};
use candid::CandidType;
use serde::Deserialize;

//
// Unit
//

#[derive(
    CandidType, Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Ord, PartialOrd, Deserialize,
)]
pub struct Unit;

impl EntityKeyBytes for Unit {
    const BYTE_LEN: usize = 0;

    fn write_bytes(&self, out: &mut [u8]) {
        assert_eq!(out.len(), Self::BYTE_LEN);
    }
}

impl RuntimeValueMeta for () {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for () {
    fn to_value(&self) -> Value {
        Value::Unit
    }
}

impl RuntimeValueDecode for () {
    fn from_value(value: &Value) -> Option<Self> {
        matches!(value, Value::Unit).then_some(())
    }
}

impl RuntimeValueMeta for Unit {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Unit {
    fn to_value(&self) -> Value {
        Value::Unit
    }
}

impl RuntimeValueDecode for Unit {
    fn from_value(value: &Value) -> Option<Self> {
        matches!(value, Value::Unit).then_some(Self)
    }
}

impl StorageKeyCodec for Unit {
    fn to_storage_key(&self) -> Result<StorageKey, StorageKeyEncodeError> {
        Ok(StorageKey::Unit)
    }
}

impl SanitizeAuto for Unit {}

impl SanitizeCustom for Unit {}

impl Atomic for Unit {}

impl ValidateAuto for Unit {}

impl ValidateCustom for Unit {}

impl Visitable for Unit {}
