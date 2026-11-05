//! Test-only state and helpers for the parent module.

use super::*;

impl From<Value> for OutputValue {
    fn from(value: Value) -> Self {
        Self(
            PublicValue::try_from_runtime_non_enum(&value)
                .expect("test output conversion requires accepted enum catalog"),
        )
    }
}
