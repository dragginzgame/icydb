use crate::{
    traits::EntityKey,
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Id, Int, Int128, Nat, Nat128,
        Principal, Subaccount, Timestamp, Ulid,
    },
    value::{Value, ValueEnum},
};
use candid::CandidType;
use serde::Deserialize;

//
// InputValue
//
// Public input-side value boundary used by literal-taking API surfaces.
// This stays separate from runtime `Value` so public write/query inputs can
// move off the internal execution representation incrementally.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum InputValue {
    Account(Account),
    Blob(Vec<u8>),
    Bool(bool),
    Date(Date),
    Decimal(Decimal),
    Duration(Duration),
    Enum(InputValueEnum),
    Float32(Float32),
    Float64(Float64),
    Int(i64),
    Int128(Int128),
    IntBig(Int),
    List(Vec<Self>),
    Map(Vec<(Self, Self)>),
    Null,
    Principal(Principal),
    Subaccount(Subaccount),
    Text(String),
    Timestamp(Timestamp),
    Uint(u64),
    Uint128(Nat128),
    UintBig(Nat),
    Ulid(Ulid),
    Unit,
}

//
// InputValueEnum
//
// Input-side enum payload contract paired with `InputValue`.
// Payload stays recursive through `InputValue` so explicit enum-valued public
// inputs can cross the boundary without using runtime `Value` directly.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct InputValueEnum {
    variant: String,
    path: Option<String>,
    payload: Option<Box<InputValue>>,
}

impl InputValueEnum {
    #[must_use]
    pub const fn variant(&self) -> &str {
        self.variant.as_str()
    }

    #[must_use]
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    #[must_use]
    pub fn payload(&self) -> Option<&InputValue> {
        self.payload.as_deref()
    }
}

impl From<Value> for InputValue {
    fn from(value: Value) -> Self {
        Self::from(&value)
    }
}

impl From<&Value> for InputValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Account(value) => Self::Account(*value),
            Value::Blob(value) => Self::Blob(value.clone()),
            Value::Bool(value) => Self::Bool(*value),
            Value::Date(value) => Self::Date(*value),
            Value::Decimal(value) => Self::Decimal(*value),
            Value::Duration(value) => Self::Duration(*value),
            Value::Enum(value) => Self::Enum(InputValueEnum::from(value)),
            Value::Float32(value) => Self::Float32(*value),
            Value::Float64(value) => Self::Float64(*value),
            Value::Int(value) => Self::Int(*value),
            Value::Int128(value) => Self::Int128(*value),
            Value::IntBig(value) => Self::IntBig(value.clone()),
            Value::List(values) => Self::List(values.iter().map(Self::from).collect()),
            Value::Map(entries) => Self::Map(
                entries
                    .iter()
                    .map(|(key, value)| (Self::from(key), Self::from(value)))
                    .collect(),
            ),
            Value::Null => Self::Null,
            Value::Principal(value) => Self::Principal(*value),
            Value::Subaccount(value) => Self::Subaccount(*value),
            Value::Text(value) => Self::Text(value.clone()),
            Value::Timestamp(value) => Self::Timestamp(*value),
            Value::Uint(value) => Self::Uint(*value),
            Value::Uint128(value) => Self::Uint128(*value),
            Value::UintBig(value) => Self::UintBig(value.clone()),
            Value::Ulid(value) => Self::Ulid(*value),
            Value::Unit => Self::Unit,
        }
    }
}

impl From<InputValue> for Value {
    fn from(value: InputValue) -> Self {
        Self::from(&value)
    }
}

impl From<&InputValue> for Value {
    fn from(value: &InputValue) -> Self {
        match value {
            InputValue::Account(value) => Self::Account(*value),
            InputValue::Blob(value) => Self::Blob(value.clone()),
            InputValue::Bool(value) => Self::Bool(*value),
            InputValue::Date(value) => Self::Date(*value),
            InputValue::Decimal(value) => Self::Decimal(*value),
            InputValue::Duration(value) => Self::Duration(*value),
            InputValue::Enum(value) => Self::Enum(ValueEnum::from(value)),
            InputValue::Float32(value) => Self::Float32(*value),
            InputValue::Float64(value) => Self::Float64(*value),
            InputValue::Int(value) => Self::Int(*value),
            InputValue::Int128(value) => Self::Int128(*value),
            InputValue::IntBig(value) => Self::IntBig(value.clone()),
            InputValue::List(values) => Self::List(values.iter().map(Self::from).collect()),
            InputValue::Map(entries) => Self::Map(
                entries
                    .iter()
                    .map(|(key, value)| (Self::from(key), Self::from(value)))
                    .collect(),
            ),
            InputValue::Null => Self::Null,
            InputValue::Principal(value) => Self::Principal(*value),
            InputValue::Subaccount(value) => Self::Subaccount(*value),
            InputValue::Text(value) => Self::Text(value.clone()),
            InputValue::Timestamp(value) => Self::Timestamp(*value),
            InputValue::Uint(value) => Self::Uint(*value),
            InputValue::Uint128(value) => Self::Uint128(*value),
            InputValue::UintBig(value) => Self::UintBig(value.clone()),
            InputValue::Ulid(value) => Self::Ulid(*value),
            InputValue::Unit => Self::Unit,
        }
    }
}

impl From<ValueEnum> for InputValueEnum {
    fn from(value: ValueEnum) -> Self {
        Self::from(&value)
    }
}

impl From<&ValueEnum> for InputValueEnum {
    fn from(value: &ValueEnum) -> Self {
        Self {
            variant: value.variant().to_string(),
            path: value.path().map(ToString::to_string),
            payload: value
                .payload()
                .map(|payload| Box::new(InputValue::from(payload))),
        }
    }
}

impl From<InputValueEnum> for ValueEnum {
    fn from(value: InputValueEnum) -> Self {
        Self::from(&value)
    }
}

impl From<&InputValueEnum> for ValueEnum {
    fn from(value: &InputValueEnum) -> Self {
        let mut runtime = Self::new(value.variant(), value.path());
        if let Some(payload) = value.payload() {
            runtime = runtime.with_payload(Value::from(payload));
        }

        runtime
    }
}

impl From<&str> for InputValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_string())
    }
}

impl From<String> for InputValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<Vec<u8>> for InputValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Blob(value)
    }
}

impl From<bool> for InputValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<Account> for InputValue {
    fn from(value: Account) -> Self {
        Self::Account(value)
    }
}

impl From<Date> for InputValue {
    fn from(value: Date) -> Self {
        Self::Date(value)
    }
}

impl From<Decimal> for InputValue {
    fn from(value: Decimal) -> Self {
        Self::Decimal(value)
    }
}

impl From<Duration> for InputValue {
    fn from(value: Duration) -> Self {
        Self::Duration(value)
    }
}

impl From<Float32> for InputValue {
    fn from(value: Float32) -> Self {
        Self::Float32(value)
    }
}

impl From<Float64> for InputValue {
    fn from(value: Float64) -> Self {
        Self::Float64(value)
    }
}

impl From<Int> for InputValue {
    fn from(value: Int) -> Self {
        Self::IntBig(value)
    }
}

impl From<Int128> for InputValue {
    fn from(value: Int128) -> Self {
        Self::Int128(value)
    }
}

impl From<Nat> for InputValue {
    fn from(value: Nat) -> Self {
        Self::UintBig(value)
    }
}

impl From<Nat128> for InputValue {
    fn from(value: Nat128) -> Self {
        Self::Uint128(value)
    }
}

impl From<Principal> for InputValue {
    fn from(value: Principal) -> Self {
        Self::Principal(value)
    }
}

impl From<Subaccount> for InputValue {
    fn from(value: Subaccount) -> Self {
        Self::Subaccount(value)
    }
}

impl From<Timestamp> for InputValue {
    fn from(value: Timestamp) -> Self {
        Self::Timestamp(value)
    }
}

impl From<Ulid> for InputValue {
    fn from(value: Ulid) -> Self {
        Self::Ulid(value)
    }
}

impl From<()> for InputValue {
    fn from((): ()) -> Self {
        Self::Unit
    }
}

impl<T> From<Option<T>> for InputValue
where
    T: Into<Self>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => Self::Null,
        }
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
                    Self::Int(i64::from(value))
                }
            }
        )*
    };
}

macro_rules! impl_input_value_uint {
    ($($ty:ty),* $(,)?) => {
        $(
            impl From<$ty> for InputValue {
                fn from(value: $ty) -> Self {
                    Self::Uint(u64::from(value))
                }
            }
        )*
    };
}

impl_input_value_int!(i8, i16, i32, i64);
impl_input_value_uint!(u8, u16, u32, u64);

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::value::{InputValue, InputValueEnum, Value, ValueEnum};

    #[test]
    fn input_value_round_trip_keeps_recursive_collection_shape() {
        let runtime = Value::List(vec![
            Value::Uint(7),
            Value::Map(vec![(Value::Text("x".to_string()), Value::Bool(true))]),
        ]);

        assert_eq!(Value::from(InputValue::from(runtime.clone())), runtime);
    }

    #[test]
    fn input_value_enum_round_trip_keeps_payload() {
        let runtime =
            ValueEnum::new("Example", Some("test::InputEnum")).with_payload(Value::Uint(9));

        assert_eq!(
            InputValueEnum::from(runtime.clone()),
            InputValueEnum {
                variant: "Example".to_string(),
                path: Some("test::InputEnum".to_string()),
                payload: Some(Box::new(InputValue::Uint(9))),
            },
        );
        assert_eq!(
            ValueEnum::from(InputValueEnum::from(runtime.clone())),
            runtime
        );
    }
}
