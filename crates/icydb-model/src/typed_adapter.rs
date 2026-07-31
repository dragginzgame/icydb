//! Model-owned typed-value adaptation contracts.
//!
//! These contracts describe how authored Rust values cross an abstract value
//! boundary. They deliberately do not depend on IcyDB runtime values, accepted
//! catalogs, row layouts, or persistence. The runtime facade supplies the
//! accepted-schema-bound context when an adapter is used.

use std::collections::{BTreeMap, BTreeSet};

use icydb_schema::{
    Account, Blob, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig, Principal,
    Subaccount, Timestamp, Ulid, Unit,
};

/// A scalar value crossing the model-owned typed-adapter boundary.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypedScalarValue {
    Account(Account),
    Blob(Blob),
    Bool(bool),
    Date(Date),
    Decimal(Decimal),
    Duration(Duration),
    Float32(Float32),
    Float64(Float64),
    Int64(i64),
    Int128(i128),
    IntBig(IntBig),
    Nat64(u64),
    Nat128(u128),
    NatBig(NatBig),
    Principal(Principal),
    Subaccount(Subaccount),
    Text(String),
    Timestamp(Timestamp),
    Ulid(Ulid),
    Unit,
}

/// A source-bound enum output selected through current accepted authority.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedEnumOutput<'a, T> {
    Unit,
    Payload(&'a T),
}

/// Model-value adaptation failure before row or mutation execution.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedValueError {
    /// One immutable authored source identity is absent from the binding.
    SourceUnavailable,
    /// The public value does not match the authored Rust value shape.
    ShapeMismatch,
}

/// Runtime-supplied public-value context for model-owned typed adapters.
///
/// Implementations must resolve names through current accepted source
/// bindings. Authored names are never runtime authority.
#[doc(hidden)]
pub trait TypedAdapterContext {
    type InputValue;
    type OutputValue;

    fn input_scalar(&self, value: TypedScalarValue) -> Self::InputValue;
    fn input_list(&self, values: Vec<Self::InputValue>) -> Self::InputValue;
    fn input_map(&self, entries: Vec<(Self::InputValue, Self::InputValue)>) -> Self::InputValue;
    fn input_null(&self) -> Self::InputValue;
    fn input_enum(
        &self,
        type_source_key: &'static str,
        variant_source_key: &'static str,
        payload: Option<Self::InputValue>,
    ) -> Result<Self::InputValue, TypedValueError>;
    fn input_record(
        &self,
        type_source_key: &'static str,
        fields: Vec<(&'static str, Self::InputValue)>,
    ) -> Result<Self::InputValue, TypedValueError>;

    fn output_scalar(&self, value: &Self::OutputValue) -> Option<TypedScalarValue>;
    fn output_list<'a>(&self, value: &'a Self::OutputValue) -> Option<&'a [Self::OutputValue]>;
    fn output_map<'a>(
        &self,
        value: &'a Self::OutputValue,
    ) -> Option<&'a [(Self::OutputValue, Self::OutputValue)]>;
    fn output_is_null(&self, value: &Self::OutputValue) -> bool;
    fn output_enum_variant<'a>(
        &self,
        type_source_key: &'static str,
        variant_source_key: &'static str,
        value: &'a Self::OutputValue,
    ) -> Result<Option<TypedEnumOutput<'a, Self::OutputValue>>, TypedValueError>;
    fn output_record<'a>(
        &self,
        type_source_key: &'static str,
        member_source_keys: &[&'static str],
        value: &'a Self::OutputValue,
    ) -> Result<Vec<&'a Self::OutputValue>, TypedValueError>;
}

/// Immutable source identity emitted for every authored named type.
#[doc(hidden)]
pub trait TypedNamedType {
    const SOURCE_KEY: &'static str;
}

/// Model-owned conversion from one authored Rust value into a runtime-owned
/// public input value.
#[doc(hidden)]
pub trait TypedInputValue: Sized {
    fn encode_typed_input<C>(self, context: &C) -> Result<C::InputValue, TypedValueError>
    where
        C: TypedAdapterContext;
}

/// Model-owned conversion from a runtime-owned public output value into one
/// authored Rust value.
#[doc(hidden)]
pub trait TypedOutputValue: Sized {
    fn decode_typed_output<C>(context: &C, value: &C::OutputValue) -> Result<Self, TypedValueError>
    where
        C: TypedAdapterContext;
}

macro_rules! impl_typed_scalar_value {
    ($($ty:ty => $variant:ident),* $(,)?) => {
        $(
            impl TypedInputValue for $ty {
                fn encode_typed_input<C>(
                    self,
                    context: &C,
                ) -> Result<C::InputValue, TypedValueError>
                where
                    C: TypedAdapterContext,
                {
                    Ok(context.input_scalar(TypedScalarValue::$variant(self.into())))
                }
            }

            impl TypedOutputValue for $ty {
                fn decode_typed_output<C>(
                    context: &C,
                    value: &C::OutputValue,
                ) -> Result<Self, TypedValueError>
                where
                    C: TypedAdapterContext,
                {
                    match context.output_scalar(value) {
                        Some(TypedScalarValue::$variant(value)) => {
                            Self::try_from(value).map_err(|_| TypedValueError::ShapeMismatch)
                        }
                        _ => Err(TypedValueError::ShapeMismatch),
                    }
                }
            }
        )*
    };
}

impl_typed_scalar_value!(
    Account => Account,
    Blob => Blob,
    bool => Bool,
    Date => Date,
    Decimal => Decimal,
    Duration => Duration,
    Float32 => Float32,
    Float64 => Float64,
    i8 => Int64,
    i16 => Int64,
    i32 => Int64,
    i64 => Int64,
    i128 => Int128,
    IntBig => IntBig,
    NatBig => NatBig,
    Principal => Principal,
    String => Text,
    Subaccount => Subaccount,
    Timestamp => Timestamp,
    u8 => Nat64,
    u16 => Nat64,
    u32 => Nat64,
    u64 => Nat64,
    u128 => Nat128,
    Ulid => Ulid,
);

impl TypedInputValue for Unit {
    fn encode_typed_input<C>(self, context: &C) -> Result<C::InputValue, TypedValueError>
    where
        C: TypedAdapterContext,
    {
        Ok(context.input_scalar(TypedScalarValue::Unit))
    }
}

impl TypedOutputValue for Unit {
    fn decode_typed_output<C>(context: &C, value: &C::OutputValue) -> Result<Self, TypedValueError>
    where
        C: TypedAdapterContext,
    {
        match context.output_scalar(value) {
            Some(TypedScalarValue::Unit) => Ok(Self),
            _ => Err(TypedValueError::ShapeMismatch),
        }
    }
}

impl<T> TypedInputValue for Box<T>
where
    T: TypedInputValue,
{
    fn encode_typed_input<C>(self, context: &C) -> Result<C::InputValue, TypedValueError>
    where
        C: TypedAdapterContext,
    {
        (*self).encode_typed_input(context)
    }
}

impl<T> TypedOutputValue for Box<T>
where
    T: TypedOutputValue,
{
    fn decode_typed_output<C>(context: &C, value: &C::OutputValue) -> Result<Self, TypedValueError>
    where
        C: TypedAdapterContext,
    {
        T::decode_typed_output(context, value).map(Self::new)
    }
}

impl<T> TypedInputValue for Option<T>
where
    T: TypedInputValue,
{
    fn encode_typed_input<C>(self, context: &C) -> Result<C::InputValue, TypedValueError>
    where
        C: TypedAdapterContext,
    {
        self.map_or_else(
            || Ok(context.input_null()),
            |value| value.encode_typed_input(context),
        )
    }
}

impl<T> TypedOutputValue for Option<T>
where
    T: TypedOutputValue,
{
    fn decode_typed_output<C>(context: &C, value: &C::OutputValue) -> Result<Self, TypedValueError>
    where
        C: TypedAdapterContext,
    {
        if context.output_is_null(value) {
            Ok(None)
        } else {
            T::decode_typed_output(context, value).map(Some)
        }
    }
}

impl<T> TypedInputValue for Vec<T>
where
    T: TypedInputValue,
{
    fn encode_typed_input<C>(self, context: &C) -> Result<C::InputValue, TypedValueError>
    where
        C: TypedAdapterContext,
    {
        self.into_iter()
            .map(|value| value.encode_typed_input(context))
            .collect::<Result<Vec<_>, _>>()
            .map(|values| context.input_list(values))
    }
}

impl<T> TypedOutputValue for Vec<T>
where
    T: TypedOutputValue,
{
    fn decode_typed_output<C>(context: &C, value: &C::OutputValue) -> Result<Self, TypedValueError>
    where
        C: TypedAdapterContext,
    {
        context
            .output_list(value)
            .ok_or(TypedValueError::ShapeMismatch)?
            .iter()
            .map(|value| T::decode_typed_output(context, value))
            .collect()
    }
}

impl<K, V> TypedInputValue for BTreeMap<K, V>
where
    K: TypedInputValue,
    V: TypedInputValue,
{
    fn encode_typed_input<C>(self, context: &C) -> Result<C::InputValue, TypedValueError>
    where
        C: TypedAdapterContext,
    {
        self.into_iter()
            .map(|(key, value)| {
                Ok((
                    key.encode_typed_input(context)?,
                    value.encode_typed_input(context)?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|entries| context.input_map(entries))
    }
}

impl<K, V> TypedOutputValue for BTreeMap<K, V>
where
    K: Ord + TypedOutputValue,
    V: TypedOutputValue,
{
    fn decode_typed_output<C>(context: &C, value: &C::OutputValue) -> Result<Self, TypedValueError>
    where
        C: TypedAdapterContext,
    {
        let entries = context
            .output_map(value)
            .ok_or(TypedValueError::ShapeMismatch)?;
        let mut decoded = Self::new();
        for (key, value) in entries {
            let key = K::decode_typed_output(context, key)?;
            let value = V::decode_typed_output(context, value)?;
            if decoded.insert(key, value).is_some() {
                return Err(TypedValueError::ShapeMismatch);
            }
        }
        Ok(decoded)
    }
}

impl<T> TypedInputValue for BTreeSet<T>
where
    T: TypedInputValue,
{
    fn encode_typed_input<C>(self, context: &C) -> Result<C::InputValue, TypedValueError>
    where
        C: TypedAdapterContext,
    {
        self.into_iter()
            .map(|value| value.encode_typed_input(context))
            .collect::<Result<Vec<_>, _>>()
            .map(|values| context.input_list(values))
    }
}

impl<T> TypedOutputValue for BTreeSet<T>
where
    T: Ord + TypedOutputValue,
{
    fn decode_typed_output<C>(context: &C, value: &C::OutputValue) -> Result<Self, TypedValueError>
    where
        C: TypedAdapterContext,
    {
        let values = context
            .output_list(value)
            .ok_or(TypedValueError::ShapeMismatch)?;
        let mut decoded = Self::new();
        for value in values {
            if !decoded.insert(T::decode_typed_output(context, value)?) {
                return Err(TypedValueError::ShapeMismatch);
            }
        }
        Ok(decoded)
    }
}
