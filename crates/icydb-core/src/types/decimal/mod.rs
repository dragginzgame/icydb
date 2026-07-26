//! Engine operations for the canonical schema-owned `Decimal` atom.

pub use icydb_schema::{Decimal, DecimalParts, ParseDecimalError, ParseDecimalErrorReason};

use crate::{
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{NormalizeAuto, NormalizeCustom, ValidateAuto, ValidateCustom, Visitable},
};

impl RuntimeValueMeta for Decimal {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Decimal {
    fn to_value(&self) -> Value {
        Value::Decimal(*self)
    }
}

impl RuntimeValueDecode for Decimal {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Decimal(value) => Some(*value),
            _ => None,
        }
    }
}

impl NormalizeAuto for Decimal {}

impl NormalizeCustom for Decimal {}

impl ValidateAuto for Decimal {}

impl ValidateCustom for Decimal {}

impl Visitable for Decimal {}
