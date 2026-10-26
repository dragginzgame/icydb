use crate::{
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Principal, Subaccount, Timestamp, U256,
        Ulid,
    },
    value::PublicValue,
};

use serde::{Deserialize, Deserializer};

#[cfg(test)]
use crate::value::Value;

/// Public output-side root value boundary.
///
/// Recursive data is owned by `PublicValue`; this sealed wrapper keeps
/// accepted output distinct from caller-authored `InputValue`.
#[repr(transparent)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputValue(PublicValue);

impl OutputValue {
    impl_public_value_scalar_constructors!("output");

    /// Borrow the canonical recursive public value.
    #[must_use]
    pub const fn as_public(&self) -> &PublicValue {
        &self.0
    }

    /// Consume this boundary wrapper without conversion.
    #[must_use]
    pub fn into_public(self) -> PublicValue {
        self.0
    }

    pub(crate) const fn from_public(value: PublicValue) -> Self {
        Self(value)
    }

    /// Build a recursive list output value.
    #[must_use]
    pub const fn list(values: Vec<PublicValue>) -> Self {
        Self(PublicValue::List(values))
    }

    /// Build a recursive map output value.
    #[must_use]
    pub const fn map(entries: Vec<(PublicValue, PublicValue)>) -> Self {
        Self(PublicValue::Map(entries))
    }
}

impl<'de> Deserialize<'de> for OutputValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        PublicValue::deserialize(deserializer).map(Self)
    }
}

#[cfg(test)]
impl From<Value> for OutputValue {
    fn from(value: Value) -> Self {
        Self(
            PublicValue::try_from_runtime_non_enum(&value)
                .expect("test output conversion requires accepted enum catalog"),
        )
    }
}

/// Render one accepted output value into the stable row-projection text form.
#[must_use]
pub fn render_output_value_text(value: &OutputValue) -> String {
    value.as_public().render_text()
}

impl serde::Serialize for OutputValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.0, serializer)
    }
}

// OutputValue delegates both Serde directions to the canonical recursive value.
#[cfg(not(target_family = "wasm"))]
impl ic_wire::typescript::Wire for OutputValue {
    fn native_schema(
        context: &mut ic_wire::typescript::SchemaContext,
    ) -> Result<serde_json::Value, ic_wire::typescript::GenerationError> {
        context.describe::<PublicValue>()
    }
}

#[cfg(test)]
mod tests {
    use crate::value::{OutputValue, PublicValue, Value};

    #[test]
    fn output_value_from_runtime_value_keeps_recursive_collection_shape() {
        let runtime = Value::List(vec![
            Value::Nat64(7),
            Value::Map(vec![(Value::Text("x".to_string()), Value::Bool(true))]),
        ]);

        assert_eq!(
            OutputValue::from(runtime),
            OutputValue::list(vec![
                PublicValue::Nat64(7),
                PublicValue::Map(vec![(
                    PublicValue::Text("x".to_string()),
                    PublicValue::Bool(true),
                )]),
            ]),
        );
    }
}
