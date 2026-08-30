use crate::{
    db::EntityKey,
    types::{
        Account, Blob, Date, Decimal, Duration, Float32, Float64, Id, IntBig, NatBig, Principal,
        Subaccount, Timestamp, U256, Ulid, Unit,
    },
    value::{PublicEnumValue, PublicValue, Value},
};
use candid::{CandidType, types::Serializer};
use serde::{Deserialize, Deserializer};

//
// InputValue
//
// Public input-side value boundary used by literal-taking API surfaces.
// This stays separate from runtime `Value` so public write/query inputs can
// move off the internal execution representation incrementally.
//

#[repr(transparent)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InputValue(PublicValue);

impl InputValue {
    /// Wrap one caller-authored public value without conversion.
    #[must_use]
    pub const fn from_public(value: PublicValue) -> Self {
        Self(value)
    }

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

    /// Build an account input value.
    #[must_use]
    pub const fn account(value: Account) -> Self {
        Self(PublicValue::Account(value))
    }

    /// Build a blob input value.
    #[must_use]
    pub const fn blob(value: Vec<u8>) -> Self {
        Self(PublicValue::Blob(value))
    }

    /// Build a boolean input value.
    #[must_use]
    pub const fn boolean(value: bool) -> Self {
        Self(PublicValue::Bool(value))
    }

    /// Build a date input value.
    #[must_use]
    pub const fn date(value: Date) -> Self {
        Self(PublicValue::Date(value))
    }

    /// Build a decimal input value.
    #[must_use]
    pub const fn decimal(value: Decimal) -> Self {
        Self(PublicValue::Decimal(value))
    }

    /// Build a duration input value.
    #[must_use]
    pub const fn duration(value: Duration) -> Self {
        Self(PublicValue::Duration(value))
    }

    /// Build a finite 32-bit floating-point input value.
    #[must_use]
    pub const fn float32(value: Float32) -> Self {
        Self(PublicValue::Float32(value))
    }

    /// Build a finite 64-bit floating-point input value.
    #[must_use]
    pub const fn float64(value: Float64) -> Self {
        Self(PublicValue::Float64(value))
    }

    /// Build a 64-bit signed integer input value.
    #[must_use]
    pub const fn int64(value: i64) -> Self {
        Self(PublicValue::Int64(value))
    }

    /// Build a 128-bit signed integer input value.
    #[must_use]
    pub const fn int128(value: i128) -> Self {
        Self(PublicValue::Int128(value))
    }

    /// Build an unbounded signed integer input value.
    #[must_use]
    pub const fn int_big(value: IntBig) -> Self {
        Self(PublicValue::IntBig(value))
    }

    /// Build a recursive list input value.
    #[must_use]
    pub const fn list(values: Vec<PublicValue>) -> Self {
        Self(PublicValue::List(values))
    }

    /// Build a recursive map input value.
    #[must_use]
    pub const fn map(entries: Vec<(PublicValue, PublicValue)>) -> Self {
        Self(PublicValue::Map(entries))
    }

    /// Build a null input value.
    #[must_use]
    pub const fn null() -> Self {
        Self(PublicValue::Null)
    }

    /// Report whether this input is null.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self.0, PublicValue::Null)
    }

    /// Build a principal input value.
    #[must_use]
    pub const fn principal(value: Principal) -> Self {
        Self(PublicValue::Principal(value))
    }

    /// Build a fixed subaccount input value.
    #[must_use]
    pub const fn subaccount(value: Subaccount) -> Self {
        Self(PublicValue::Subaccount(value))
    }

    /// Build a text input value.
    #[must_use]
    pub const fn text(value: String) -> Self {
        Self(PublicValue::Text(value))
    }

    /// Build a timestamp input value.
    #[must_use]
    pub const fn timestamp(value: Timestamp) -> Self {
        Self(PublicValue::Timestamp(value))
    }

    /// Build a 64-bit natural input value.
    #[must_use]
    pub const fn nat64(value: u64) -> Self {
        Self(PublicValue::Nat64(value))
    }

    /// Build a 128-bit natural input value.
    #[must_use]
    pub const fn nat128(value: u128) -> Self {
        Self(PublicValue::Nat128(value))
    }

    /// Build an unbounded natural input value.
    #[must_use]
    pub const fn nat_big(value: NatBig) -> Self {
        Self(PublicValue::NatBig(value))
    }

    /// Build a ULID input value.
    #[must_use]
    pub const fn ulid(value: Ulid) -> Self {
        Self(PublicValue::Ulid(value))
    }

    /// Build a unit input value.
    #[must_use]
    pub const fn unit() -> Self {
        Self(PublicValue::Unit)
    }

    /// Build a fixed-width unsigned integer input value.
    #[must_use]
    pub const fn u256(value: U256) -> Self {
        Self(PublicValue::U256(value))
    }

    /// Build an enum input with an optional schema-visible type path.
    #[must_use]
    pub fn enum_value(variant: &str, path: Option<&str>) -> Self {
        Self(PublicValue::Enum(PublicEnumValue::new(variant, path)))
    }

    /// Build an enum input whose type is resolved from its expected contract.
    #[must_use]
    pub fn loose_enum(variant: impl Into<String>) -> Self {
        Self(PublicValue::Enum(PublicEnumValue::loose(variant)))
    }

    /// Attach one recursive payload to an enum input.
    #[must_use]
    pub fn with_enum_payload(self, payload: Self) -> Option<Self> {
        let PublicValue::Enum(value) = self.0 else {
            return None;
        };
        Some(Self(PublicValue::Enum(
            value.with_payload(payload.into_public()),
        )))
    }

    /// Lower an input that cannot require accepted enum admission.
    ///
    /// Enum input, including nested enum input, stays unresolved and must use
    /// the accepted catalog admission boundary instead.
    pub(crate) fn try_into_runtime_non_enum(self) -> Option<Value> {
        self.0.try_into_runtime_non_enum()
    }

    /// Lift a runtime value without canonical enum IDs into authored input.
    pub(crate) fn try_from_runtime_non_enum(value: &Value) -> Option<Self> {
        PublicValue::try_from_runtime_non_enum(value).map(Self)
    }
}

impl CandidType for InputValue {
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

impl<'de> Deserialize<'de> for InputValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        PublicValue::deserialize(deserializer).map(Self)
    }
}

impl From<PublicValue> for InputValue {
    fn from(value: PublicValue) -> Self {
        Self(value)
    }
}

#[cfg(test)]
impl From<Value> for InputValue {
    fn from(value: Value) -> Self {
        Self::try_from_runtime_non_enum(&value)
            .expect("test runtime-to-input conversion must not contain canonical enum IDs")
    }
}

#[cfg(test)]
impl From<&Value> for InputValue {
    fn from(value: &Value) -> Self {
        Self::try_from_runtime_non_enum(value)
            .expect("test runtime-to-input conversion must not contain canonical enum IDs")
    }
}

impl From<&str> for InputValue {
    fn from(value: &str) -> Self {
        Self(PublicValue::Text(value.to_string()))
    }
}

impl From<String> for InputValue {
    fn from(value: String) -> Self {
        Self(PublicValue::Text(value))
    }
}

impl From<Vec<u8>> for InputValue {
    fn from(value: Vec<u8>) -> Self {
        Self(PublicValue::Blob(value))
    }
}

impl From<Blob> for InputValue {
    fn from(value: Blob) -> Self {
        Self(PublicValue::Blob(value.to_vec()))
    }
}

impl From<bool> for InputValue {
    fn from(value: bool) -> Self {
        Self(PublicValue::Bool(value))
    }
}

impl From<Account> for InputValue {
    fn from(value: Account) -> Self {
        Self(PublicValue::Account(value))
    }
}

impl From<Date> for InputValue {
    fn from(value: Date) -> Self {
        Self(PublicValue::Date(value))
    }
}

impl From<Decimal> for InputValue {
    fn from(value: Decimal) -> Self {
        Self(PublicValue::Decimal(value))
    }
}

impl From<Duration> for InputValue {
    fn from(value: Duration) -> Self {
        Self(PublicValue::Duration(value))
    }
}

impl From<Float32> for InputValue {
    fn from(value: Float32) -> Self {
        Self(PublicValue::Float32(value))
    }
}

impl From<Float64> for InputValue {
    fn from(value: Float64) -> Self {
        Self(PublicValue::Float64(value))
    }
}

impl From<IntBig> for InputValue {
    fn from(value: IntBig) -> Self {
        Self(PublicValue::IntBig(value))
    }
}

impl From<i128> for InputValue {
    fn from(value: i128) -> Self {
        Self(PublicValue::Int128(value))
    }
}

impl From<NatBig> for InputValue {
    fn from(value: NatBig) -> Self {
        Self(PublicValue::NatBig(value))
    }
}

impl From<u128> for InputValue {
    fn from(value: u128) -> Self {
        Self(PublicValue::Nat128(value))
    }
}

impl From<Principal> for InputValue {
    fn from(value: Principal) -> Self {
        Self(PublicValue::Principal(value))
    }
}

impl From<Subaccount> for InputValue {
    fn from(value: Subaccount) -> Self {
        Self(PublicValue::Subaccount(value))
    }
}

impl From<Timestamp> for InputValue {
    fn from(value: Timestamp) -> Self {
        Self(PublicValue::Timestamp(value))
    }
}

impl From<Ulid> for InputValue {
    fn from(value: Ulid) -> Self {
        Self(PublicValue::Ulid(value))
    }
}

impl From<U256> for InputValue {
    fn from(value: U256) -> Self {
        Self(PublicValue::U256(value))
    }
}

impl From<()> for InputValue {
    fn from((): ()) -> Self {
        Self(PublicValue::Unit)
    }
}

impl From<Unit> for InputValue {
    fn from(_value: Unit) -> Self {
        Self(PublicValue::Unit)
    }
}

impl<T> From<Option<T>> for InputValue
where
    T: Into<Self>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => Self(PublicValue::Null),
        }
    }
}

impl<T> From<Box<T>> for InputValue
where
    T: Into<Self>,
{
    fn from(value: Box<T>) -> Self {
        (*value).into()
    }
}

impl<E> From<Id<E>> for InputValue
where
    E: EntityKey,
    E::Key: Into<Self>,
{
    fn from(value: Id<E>) -> Self {
        value.into_key().into()
    }
}

impl<E> From<&Id<E>> for InputValue
where
    E: EntityKey,
    E::Key: Into<Self>,
{
    fn from(value: &Id<E>) -> Self {
        value.key().into()
    }
}

macro_rules! impl_input_value_int {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for InputValue {
                fn from(value: $ty) -> Self {
                    Self(PublicValue::Int64(i64::from(value)))
                }
            }
        )*
    };
}

macro_rules! impl_input_value_nat {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for InputValue {
                fn from(value: $ty) -> Self {
                    Self(PublicValue::Nat64(u64::from(value)))
                }
            }
        )*
    };
}

impl_input_value_int!(i8, i16, i32, i64);
impl_input_value_nat!(u8, u16, u32, u64);

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::value::{InputValue, PublicValue, Value};

    #[test]
    fn runtime_to_input_value_keeps_recursive_collection_shape() {
        let runtime = Value::List(vec![
            Value::Nat64(7),
            Value::Map(vec![(Value::Text("x".to_string()), Value::Bool(true))]),
        ]);

        assert_eq!(
            InputValue::from(runtime),
            InputValue::list(vec![
                PublicValue::Nat64(7),
                PublicValue::Map(vec![(
                    PublicValue::Text("x".to_string()),
                    PublicValue::Bool(true),
                )]),
            ]),
        );
    }

    #[test]
    fn unresolved_enum_input_cannot_lower_without_admission() {
        let direct = InputValue::loose_enum("Active");
        let nested = InputValue::list(vec![direct.clone().into_public()]);

        assert_eq!(direct.try_into_runtime_non_enum(), None);
        assert_eq!(nested.try_into_runtime_non_enum(), None);
    }
}
