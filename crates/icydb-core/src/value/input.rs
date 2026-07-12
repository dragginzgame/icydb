use crate::{
    traits::EntityKey,
    types::{
        Account, Blob, Date, Decimal, Duration, Float32, Float64, Id, IntBig, NatBig, Principal,
        Subaccount, Timestamp, Ulid, Unit,
    },
    value::Value,
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
    #[serde(rename = "Int")]
    Int64(i64),
    Int128(i128),
    IntBig(IntBig),
    List(Vec<Self>),
    Map(Vec<(Self, Self)>),
    Null,
    Principal(Principal),
    Subaccount(Subaccount),
    Text(String),
    Timestamp(Timestamp),
    #[serde(rename = "Nat")]
    Nat64(u64),
    Nat128(u128),
    NatBig(NatBig),
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
    /// Build an enum input with an optional schema-visible type path.
    #[must_use]
    pub fn new(variant: &str, path: Option<&str>) -> Self {
        Self {
            variant: variant.to_string(),
            path: path.map(ToString::to_string),
            payload: None,
        }
    }

    /// Build an enum input whose type is resolved from its expected contract.
    #[must_use]
    pub fn loose(variant: impl Into<String>) -> Self {
        Self {
            variant: variant.into(),
            path: None,
            payload: None,
        }
    }

    /// Attach one unresolved payload value to this enum input.
    #[must_use]
    pub fn with_payload(mut self, payload: InputValue) -> Self {
        self.payload = Some(Box::new(payload));
        self
    }

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

    pub(crate) fn into_parts(self) -> (String, Option<String>, Option<InputValue>) {
        (
            self.variant,
            self.path,
            self.payload.map(|payload| *payload),
        )
    }
}

impl InputValue {
    /// Lower an input that cannot require accepted enum admission.
    ///
    /// Enum input, including nested enum input, stays unresolved and must use
    /// the accepted catalog admission boundary instead.
    pub(crate) fn try_into_runtime_non_enum(self) -> Option<Value> {
        Some(match self {
            Self::Account(value) => Value::Account(value),
            Self::Blob(value) => Value::Blob(value),
            Self::Bool(value) => Value::Bool(value),
            Self::Date(value) => Value::Date(value),
            Self::Decimal(value) => Value::Decimal(value),
            Self::Duration(value) => Value::Duration(value),
            Self::Enum(_) => return None,
            Self::Float32(value) => Value::Float32(value),
            Self::Float64(value) => Value::Float64(value),
            Self::Int64(value) => Value::Int64(value),
            Self::Int128(value) => Value::Int128(value),
            Self::IntBig(value) => Value::IntBig(value),
            Self::List(values) => Value::List(
                values
                    .into_iter()
                    .map(Self::try_into_runtime_non_enum)
                    .collect::<Option<Vec<_>>>()?,
            ),
            Self::Map(entries) => Value::Map(
                entries
                    .into_iter()
                    .map(|(key, value)| {
                        Some((
                            key.try_into_runtime_non_enum()?,
                            value.try_into_runtime_non_enum()?,
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?,
            ),
            Self::Null => Value::Null,
            Self::Principal(value) => Value::Principal(value),
            Self::Subaccount(value) => Value::Subaccount(value),
            Self::Text(value) => Value::Text(value),
            Self::Timestamp(value) => Value::Timestamp(value),
            Self::Nat64(value) => Value::Nat64(value),
            Self::Nat128(value) => Value::Nat128(value),
            Self::NatBig(value) => Value::NatBig(value),
            Self::Ulid(value) => Value::Ulid(value),
            Self::Unit => Value::Unit,
        })
    }

    /// Lift a runtime value without canonical enum IDs into authored input.
    pub(crate) fn try_from_runtime_non_enum(value: &Value) -> Option<Self> {
        Some(match value {
            Value::Account(value) => Self::Account(*value),
            Value::Blob(value) => Self::Blob(value.clone()),
            Value::Bool(value) => Self::Bool(*value),
            Value::Date(value) => Self::Date(*value),
            Value::Decimal(value) => Self::Decimal(*value),
            Value::Duration(value) => Self::Duration(*value),
            Value::Enum(_) => return None,
            Value::Float32(value) => Self::Float32(*value),
            Value::Float64(value) => Self::Float64(*value),
            Value::Int64(value) => Self::Int64(*value),
            Value::Int128(value) => Self::Int128(*value),
            Value::IntBig(value) => Self::IntBig(value.clone()),
            Value::List(values) => Self::List(
                values
                    .iter()
                    .map(Self::try_from_runtime_non_enum)
                    .collect::<Option<Vec<_>>>()?,
            ),
            Value::Map(entries) => Self::Map(
                entries
                    .iter()
                    .map(|(key, value)| {
                        Some((
                            Self::try_from_runtime_non_enum(key)?,
                            Self::try_from_runtime_non_enum(value)?,
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?,
            ),
            Value::Null => Self::Null,
            Value::Principal(value) => Self::Principal(*value),
            Value::Subaccount(value) => Self::Subaccount(*value),
            Value::Text(value) => Self::Text(value.clone()),
            Value::Timestamp(value) => Self::Timestamp(*value),
            Value::Nat64(value) => Self::Nat64(*value),
            Value::Nat128(value) => Self::Nat128(*value),
            Value::NatBig(value) => Self::NatBig(value.clone()),
            Value::Ulid(value) => Self::Ulid(*value),
            Value::Unit => Self::Unit,
        })
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

impl From<Blob> for InputValue {
    fn from(value: Blob) -> Self {
        Self::Blob(value.to_vec())
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

impl From<IntBig> for InputValue {
    fn from(value: IntBig) -> Self {
        Self::IntBig(value)
    }
}

impl From<i128> for InputValue {
    fn from(value: i128) -> Self {
        Self::Int128(value)
    }
}

impl From<NatBig> for InputValue {
    fn from(value: NatBig) -> Self {
        Self::NatBig(value)
    }
}

impl From<u128> for InputValue {
    fn from(value: u128) -> Self {
        Self::Nat128(value)
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

impl From<Unit> for InputValue {
    fn from(_value: Unit) -> Self {
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
                    Self::Int64(i64::from(value))
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
                    Self::Nat64(u64::from(value))
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
    use crate::value::{InputValue, InputValueEnum, Value};

    #[test]
    fn runtime_to_input_value_keeps_recursive_collection_shape() {
        let runtime = Value::List(vec![
            Value::Nat64(7),
            Value::Map(vec![(Value::Text("x".to_string()), Value::Bool(true))]),
        ]);

        assert_eq!(
            InputValue::from(runtime),
            InputValue::List(vec![
                InputValue::Nat64(7),
                InputValue::Map(vec![(
                    InputValue::Text("x".to_string()),
                    InputValue::Bool(true),
                )]),
            ]),
        );
    }

    #[test]
    fn unresolved_enum_input_cannot_lower_without_admission() {
        let direct = InputValue::Enum(InputValueEnum::loose("Active"));
        let nested = InputValue::List(vec![direct.clone()]);

        assert_eq!(direct.try_into_runtime_non_enum(), None);
        assert_eq!(nested.try_into_runtime_non_enum(), None);
    }
}
