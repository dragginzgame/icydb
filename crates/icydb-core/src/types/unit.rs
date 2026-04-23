//! Module: types::unit
//! Defines the zero-sized unit key/value wrapper used by schemas that need an
//! explicit empty identity.

use crate::{
    traits::{
        Atomic, EntityKeyBytes, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom,
        ValueSurfaceDecode, ValueSurfaceEncode, ValueSurfaceKind, ValueSurfaceMeta, Visitable,
    },
    value::Value,
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

impl ValueSurfaceMeta for () {
    fn kind() -> ValueSurfaceKind {
        ValueSurfaceKind::Atomic
    }
}

impl ValueSurfaceEncode for () {
    fn to_value(&self) -> Value {
        Value::Unit
    }
}

impl ValueSurfaceDecode for () {
    fn from_value(value: &Value) -> Option<Self> {
        matches!(value, Value::Unit).then_some(())
    }
}

impl ValueSurfaceMeta for Unit {
    fn kind() -> ValueSurfaceKind {
        ValueSurfaceKind::Atomic
    }
}

impl ValueSurfaceEncode for Unit {
    fn to_value(&self) -> Value {
        Value::Unit
    }
}

impl ValueSurfaceDecode for Unit {
    fn from_value(value: &Value) -> Option<Self> {
        matches!(value, Value::Unit).then_some(Self)
    }
}

impl SanitizeAuto for Unit {}

impl SanitizeCustom for Unit {}

impl Atomic for Unit {}

impl ValidateAuto for Unit {}

impl ValidateCustom for Unit {}

impl Visitable for Unit {}
