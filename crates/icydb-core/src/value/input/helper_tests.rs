//! Test-only state and helpers for the parent module.

use super::*;

impl From<Value> for InputValue {
    fn from(value: Value) -> Self {
        Self::try_from_runtime_non_enum(&value)
            .expect("test runtime-to-input conversion must not contain canonical enum IDs")
    }
}

impl From<&Value> for InputValue {
    fn from(value: &Value) -> Self {
        Self::try_from_runtime_non_enum(value)
            .expect("test runtime-to-input conversion must not contain canonical enum IDs")
    }
}
