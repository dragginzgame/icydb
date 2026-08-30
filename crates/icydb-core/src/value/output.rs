use crate::{
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig, Principal, Subaccount,
        Timestamp, U256, Ulid,
    },
    value::PublicValue,
};
use candid::{CandidType, types::Serializer};
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

    /// Build an account output value.
    #[must_use]
    pub const fn account(value: Account) -> Self {
        Self(PublicValue::Account(value))
    }

    /// Build a blob output value.
    #[must_use]
    pub const fn blob(value: Vec<u8>) -> Self {
        Self(PublicValue::Blob(value))
    }

    /// Build a boolean output value.
    #[must_use]
    pub const fn boolean(value: bool) -> Self {
        Self(PublicValue::Bool(value))
    }

    /// Build a date output value.
    #[must_use]
    pub const fn date(value: Date) -> Self {
        Self(PublicValue::Date(value))
    }

    /// Build a decimal output value.
    #[must_use]
    pub const fn decimal(value: Decimal) -> Self {
        Self(PublicValue::Decimal(value))
    }

    /// Build a duration output value.
    #[must_use]
    pub const fn duration(value: Duration) -> Self {
        Self(PublicValue::Duration(value))
    }

    /// Build a finite 32-bit floating-point output value.
    #[must_use]
    pub const fn float32(value: Float32) -> Self {
        Self(PublicValue::Float32(value))
    }

    /// Build a finite 64-bit floating-point output value.
    #[must_use]
    pub const fn float64(value: Float64) -> Self {
        Self(PublicValue::Float64(value))
    }

    /// Build a 64-bit signed integer output value.
    #[must_use]
    pub const fn int64(value: i64) -> Self {
        Self(PublicValue::Int64(value))
    }

    /// Build a 128-bit signed integer output value.
    #[must_use]
    pub const fn int128(value: i128) -> Self {
        Self(PublicValue::Int128(value))
    }

    /// Build an unbounded signed integer output value.
    #[must_use]
    pub const fn int_big(value: IntBig) -> Self {
        Self(PublicValue::IntBig(value))
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

    /// Build a null output value.
    #[must_use]
    pub const fn null() -> Self {
        Self(PublicValue::Null)
    }

    /// Build a principal output value.
    #[must_use]
    pub const fn principal(value: Principal) -> Self {
        Self(PublicValue::Principal(value))
    }

    /// Build a fixed subaccount output value.
    #[must_use]
    pub const fn subaccount(value: Subaccount) -> Self {
        Self(PublicValue::Subaccount(value))
    }

    /// Build a text output value.
    #[must_use]
    pub const fn text(value: String) -> Self {
        Self(PublicValue::Text(value))
    }

    /// Build a timestamp output value.
    #[must_use]
    pub const fn timestamp(value: Timestamp) -> Self {
        Self(PublicValue::Timestamp(value))
    }

    /// Build a 64-bit natural output value.
    #[must_use]
    pub const fn nat64(value: u64) -> Self {
        Self(PublicValue::Nat64(value))
    }

    /// Build a 128-bit natural output value.
    #[must_use]
    pub const fn nat128(value: u128) -> Self {
        Self(PublicValue::Nat128(value))
    }

    /// Build an unbounded natural output value.
    #[must_use]
    pub const fn nat_big(value: NatBig) -> Self {
        Self(PublicValue::NatBig(value))
    }

    /// Build a ULID output value.
    #[must_use]
    pub const fn ulid(value: Ulid) -> Self {
        Self(PublicValue::Ulid(value))
    }

    /// Build a unit output value.
    #[must_use]
    pub const fn unit() -> Self {
        Self(PublicValue::Unit)
    }

    /// Build a fixed-width unsigned integer output value.
    #[must_use]
    pub const fn u256(value: U256) -> Self {
        Self(PublicValue::U256(value))
    }
}

impl CandidType for OutputValue {
    fn ty() -> candid::types::Type {
        PublicValue::ty()
    }

    fn _ty() -> candid::types::Type {
        PublicValue::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: Serializer,
    {
        self.0.idl_serialize(serializer)
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
