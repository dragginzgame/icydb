//! Engine operations for the canonical schema-owned `Duration` atom.

pub use icydb_schema::Duration;

use crate::{
    traits::Repr,
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable},
};

impl Repr for Duration {
    type Inner = u64;

    fn repr(&self) -> Self::Inner {
        self.as_millis()
    }

    fn from_repr(inner: Self::Inner) -> Self {
        Self::from_millis(inner)
    }
}

impl RuntimeValueMeta for Duration {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Duration {
    fn to_value(&self) -> Value {
        Value::Duration(*self)
    }
}

impl RuntimeValueDecode for Duration {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Duration(value) => Some(*value),
            _ => None,
        }
    }
}

impl SanitizeAuto for Duration {}

impl SanitizeCustom for Duration {}

impl ValidateAuto for Duration {}

impl ValidateCustom for Duration {}

impl Visitable for Duration {
    fn requires_application_write_callbacks() -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::Duration;

    #[test]
    fn units_and_saturating_arithmetic_remain_millisecond_native() {
        assert_eq!(Duration::from_secs(2).as_millis(), 2_000);
        assert_eq!(
            (Duration::MAX + Duration::from_millis(1)).as_millis(),
            u64::MAX,
        );
        assert_eq!((Duration::ZERO - Duration::from_millis(1)).as_millis(), 0,);
    }
}
