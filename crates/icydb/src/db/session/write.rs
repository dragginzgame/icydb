//! Module: db::session::write
//!
//! Responsibility: public `DbSession` write helpers, write-returning projection
//! conversion, and structural mutation facade types.
//! Does not own: core mutation execution, commit staging, or persisted encoding.
//! Boundary: keeps public write semantics and row-returning projection payloads
//! above the core save pipeline.

#[cfg(feature = "query")]
use crate::db::DynamicQueryResult;
use crate::{
    db::{DynamicMutationResult, session::DbSession},
    error::Error,
    traits::CanisterKind,
    types::{
        Account, Blob, Date, Decimal, Duration, Float32, Float64, IntBig, NatBig, Principal,
        Subaccount, Timestamp, Ulid, Unit,
    },
    value::{InputValue, OutputValue},
};
use candid::CandidType;
use icydb_core as core;
use icydb_schema::ScalarType;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    fmt,
};

///
/// WriteCell
///
/// Explicit authored intent for one structural or generated typed write field.
/// The database retains the distinction through accepted-policy resolution.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum WriteCell<T> {
    /// Supply no authored value for this operation.
    Omitted,

    /// Explicitly request the accepted database default.
    Default,

    /// Explicitly author `NULL`.
    Null,

    /// Explicitly author one concrete value.
    Value(T),
}

impl<T> WriteCell<T> {
    /// Map only the authored value while retaining its exact write intent.
    #[must_use]
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> WriteCell<U> {
        match self {
            Self::Omitted => WriteCell::Omitted,
            Self::Default => WriteCell::Default,
            Self::Null => WriteCell::Null,
            Self::Value(value) => WriteCell::Value(map(value)),
        }
    }
}

/// One complete accepted row supplied to an opted-in generated adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputRow {
    binding: core::db::DynamicTypedEntityBinding,
    entity: String,
    accepted_slots: Vec<(u16, usize)>,
    values: Vec<OutputValue>,
}

impl OutputRow {
    fn new(
        binding: &TypedEntityBinding,
        entity: impl Into<String>,
        columns: Vec<String>,
        values: Vec<OutputValue>,
    ) -> Result<Self, TypedAdapterError> {
        if columns.len() != values.len() {
            return Err(TypedAdapterError::RowShapeMismatch);
        }
        let mut seen_slots = BTreeSet::new();
        let mut accepted_slots = Vec::with_capacity(columns.len());
        for (value_index, column) in columns.iter().enumerate() {
            let Some(slot) = binding.inner.output_field_slot(column) else {
                continue;
            };
            if !seen_slots.insert(slot) {
                return Err(TypedAdapterError::RowShapeMismatch);
            }
            accepted_slots.push((slot, value_index));
        }
        Ok(Self {
            binding: binding.inner.clone(),
            entity: entity.into(),
            accepted_slots,
            values,
        })
    }

    /// Borrow the accepted entity display name.
    #[must_use]
    pub const fn entity(&self) -> &str {
        self.entity.as_str()
    }
}

/// Stable typed-adapter boundary failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedAdapterError {
    /// The row belongs to another accepted entity.
    EntityMismatch,

    /// An immutable source key is absent from the binding projection.
    FieldUnavailable,

    /// The generated field contract disagrees with accepted authority.
    IncompatibleField,

    /// The row does not contain the bound accepted field.
    RowFieldUnavailable,

    /// Column and value cardinalities disagree.
    RowShapeMismatch,

    /// The binding no longer matches current accepted authority.
    StaleBinding,

    /// A public output value does not match the generated Rust field shape.
    ValueShapeMismatch,
}

impl fmt::Display for TypedAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EntityMismatch => "typed binding entity mismatch",
            Self::FieldUnavailable => "typed binding field unavailable",
            Self::IncompatibleField => "typed binding field contract is incompatible",
            Self::RowFieldUnavailable => "typed row field unavailable",
            Self::RowShapeMismatch => "typed row shape mismatch",
            Self::StaleBinding => "typed binding is stale",
            Self::ValueShapeMismatch => "typed row value shape mismatch",
        })
    }
}

impl StdError for TypedAdapterError {}

/// Failure while issuing one opaque typed binding.
#[derive(Debug)]
pub enum TypedBindingError {
    /// Generated identity or shape disagrees with accepted authority.
    Adapter(TypedAdapterError),
    /// Accepted database inspection failed.
    Database(Error),
}

impl fmt::Display for TypedBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(error) => error.fmt(formatter),
            Self::Database(error) => error.fmt(formatter),
        }
    }
}

impl StdError for TypedBindingError {}

/// Failure while projecting one accepted dynamic row through an opaque binding.
#[derive(Debug)]
pub enum TypedRowError {
    /// The binding or returned row is stale or mismatched.
    Adapter(TypedAdapterError),
    /// Accepted database inspection failed.
    Database(Error),
}

impl fmt::Display for TypedRowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(error) => error.fmt(formatter),
            Self::Database(error) => error.fmt(formatter),
        }
    }
}

impl StdError for TypedRowError {}

/// Failure while validating or executing one typed write.
#[derive(Debug)]
pub enum TypedWriteError {
    /// The opaque adapter binding is stale or mismatched.
    Adapter(TypedAdapterError),
    /// The accepted database write rejected or failed.
    Database(Error),
}

impl fmt::Display for TypedWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(error) => error.fmt(formatter),
            Self::Database(error) => error.fmt(formatter),
        }
    }
}

impl StdError for TypedWriteError {}

impl From<Error> for TypedWriteError {
    fn from(error: Error) -> Self {
        Self::Database(error)
    }
}

/// Opaque accepted-schema binding for one opted-in generated adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedEntityBinding {
    inner: core::db::DynamicTypedEntityBinding,
}

impl TypedEntityBinding {
    const fn new(inner: core::db::DynamicTypedEntityBinding) -> Self {
        Self { inner }
    }

    #[cfg(feature = "query")]
    pub(crate) const fn inner(&self) -> &core::db::DynamicTypedEntityBinding {
        &self.inner
    }

    #[cfg(feature = "query")]
    pub(crate) const fn entity(&self) -> &str {
        self.inner.entity()
    }

    /// Borrow one bound row value by immutable field source key.
    pub fn row_value<'a>(
        &self,
        field_source_key: &str,
        row: &'a OutputRow,
    ) -> Result<&'a OutputValue, TypedAdapterError> {
        if row.binding != self.inner {
            return Err(TypedAdapterError::StaleBinding);
        }
        if row.entity != self.inner.entity() {
            return Err(TypedAdapterError::EntityMismatch);
        }
        let slot = self
            .inner
            .field_slot(field_source_key)
            .ok_or(TypedAdapterError::FieldUnavailable)?;
        let index = row
            .accepted_slots
            .iter()
            .find_map(|(bound_slot, index)| (*bound_slot == slot).then_some(*index))
            .ok_or(TypedAdapterError::RowFieldUnavailable)?;
        row.values
            .get(index)
            .ok_or(TypedAdapterError::RowShapeMismatch)
    }

    /// Resolve one generated named-type source key through accepted authority.
    #[doc(hidden)]
    #[must_use]
    pub fn named_type_name(&self, source_key: &str) -> Option<&str> {
        self.inner.named_type_name(source_key)
    }

    /// Resolve one generated enum-variant source key through accepted authority.
    #[doc(hidden)]
    #[must_use]
    pub fn enum_variant_name(&self, type_source_key: &str, source_key: &str) -> Option<&str> {
        self.inner.enum_variant_name(type_source_key, source_key)
    }

    /// Resolve one generated record-member source key through accepted authority.
    #[doc(hidden)]
    #[must_use]
    pub fn composite_field_name(&self, type_source_key: &str, source_key: &str) -> Option<&str> {
        self.inner.composite_field_name(type_source_key, source_key)
    }

    /// Resolve and validate one exact source-bound record output projection.
    #[doc(hidden)]
    pub fn record_output_values<'value>(
        &self,
        type_source_key: &str,
        member_source_keys: &[&str],
        value: &'value OutputValue,
    ) -> Result<Vec<&'value OutputValue>, TypedAdapterError> {
        let mut accepted_names = Vec::with_capacity(member_source_keys.len());
        for source_key in member_source_keys {
            let name = self
                .composite_field_name(type_source_key, source_key)
                .ok_or(TypedAdapterError::FieldUnavailable)?;
            accepted_names.push(name);
        }

        exact_record_output_values(accepted_names.as_slice(), value)
    }
}

fn exact_record_output_values<'value>(
    accepted_names: &[&str],
    value: &'value OutputValue,
) -> Result<Vec<&'value OutputValue>, TypedAdapterError> {
    let OutputValue::Map(entries) = value else {
        return Err(TypedAdapterError::ValueShapeMismatch);
    };
    if entries.len() != accepted_names.len()
        || accepted_names
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            != accepted_names.len()
    {
        return Err(TypedAdapterError::ValueShapeMismatch);
    }

    let mut values = vec![None; accepted_names.len()];
    for (key, value) in entries {
        let OutputValue::Text(name) = key else {
            return Err(TypedAdapterError::ValueShapeMismatch);
        };
        let Some(index) = accepted_names.iter().position(|accepted| *accepted == name) else {
            return Err(TypedAdapterError::ValueShapeMismatch);
        };
        if values[index].replace(value).is_some() {
            return Err(TypedAdapterError::ValueShapeMismatch);
        }
    }

    values
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or(TypedAdapterError::ValueShapeMismatch)
}

#[cfg(test)]
mod typed_record_output_tests {
    use super::{TypedAdapterError, exact_record_output_values};
    use crate::value::OutputValue;

    fn text(value: &str) -> OutputValue {
        OutputValue::Text(value.to_string())
    }

    #[test]
    fn exact_record_output_reorders_accepted_members() {
        let value = OutputValue::Map(vec![
            (text("count"), OutputValue::Nat64(7)),
            (text("label"), text("Ada")),
        ]);

        let values = exact_record_output_values(&["label", "count"], &value)
            .expect("exact accepted record output should decode");

        assert!(matches!(values[0], OutputValue::Text(value) if value == "Ada"));
        assert_eq!(values[1], &OutputValue::Nat64(7));
    }

    #[test]
    fn exact_record_output_rejects_duplicate_missing_and_unknown_members() {
        let malformed = [
            OutputValue::Map(vec![
                (text("label"), text("Ada")),
                (text("label"), text("Grace")),
            ]),
            OutputValue::Map(vec![(text("label"), text("Ada"))]),
            OutputValue::Map(vec![
                (text("label"), text("Ada")),
                (text("other"), OutputValue::Nat64(7)),
            ]),
            OutputValue::Map(vec![
                (OutputValue::Nat64(1), text("Ada")),
                (text("count"), OutputValue::Nat64(7)),
            ]),
            OutputValue::List(Vec::new()),
        ];

        for value in &malformed {
            assert_eq!(
                exact_record_output_values(&["label", "count"], value),
                Err(TypedAdapterError::ValueShapeMismatch),
            );
        }
        assert_eq!(
            exact_record_output_values(&["label", "label"], &malformed[0]),
            Err(TypedAdapterError::ValueShapeMismatch),
        );
    }
}

/// Generated logical field shape supplied while issuing an opaque binding.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypedFieldType {
    /// Exact schema-owned scalar contract.
    Scalar(ScalarType),
    /// Ordered repeated values with one exact item contract.
    List(Box<Self>),
    /// Named contract selected by immutable source key.
    Named(&'static str),
}

impl TypedFieldType {
    fn into_core(self) -> core::db::DynamicTypedFieldType {
        match self {
            Self::Scalar(scalar) => core::db::DynamicTypedFieldType::Scalar(scalar),
            Self::List(item) => core::db::DynamicTypedFieldType::List(Box::new(item.into_core())),
            Self::Named(source_key) => {
                core::db::DynamicTypedFieldType::Named(source_key.to_string())
            }
        }
    }
}

/// One generated field contract supplied while issuing an opaque binding.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedFieldBindingRequest {
    field_type: TypedFieldType,
    nullable: bool,
    source_key: &'static str,
}

impl TypedFieldBindingRequest {
    /// Construct one generated field binding request.
    #[must_use]
    pub const fn new(source_key: &'static str, field_type: TypedFieldType, nullable: bool) -> Self {
        Self {
            field_type,
            nullable,
            source_key,
        }
    }

    fn into_core(self) -> core::db::DynamicTypedFieldBindingRequest {
        core::db::DynamicTypedFieldBindingRequest::new(
            self.source_key.to_string(),
            self.field_type.into_core(),
            self.nullable,
        )
    }
}

/// IcyDB-owned decode adapter implemented only by opted-in generated code.
pub trait TypedRowAdapter {
    /// Complete application row produced by decoding.
    type Row;

    /// Decode one accepted output row through an opaque current binding.
    fn decode_row(
        binding: &TypedEntityBinding,
        row: OutputRow,
    ) -> Result<Self::Row, TypedAdapterError>;
}

/// IcyDB-owned binding adapter implemented only by opted-in generated entities.
pub trait TypedEntityAdapter: TypedRowAdapter {
    /// Bind generated source identities to current accepted schema authority.
    fn typed_binding<C>(session: &DbSession<C>) -> Result<TypedEntityBinding, TypedBindingError>
    where
        C: CanisterKind;
}

/// IcyDB-owned write adapter implemented only by opted-in generated inputs.
pub trait TypedWriteAdapter {
    /// Lower explicit application write intent without resolving database policy.
    fn encode_write(self, binding: &TypedEntityBinding) -> Result<TypedWrite, TypedAdapterError>;
}

///
/// TypedInputValue
///
/// Generated-code conversion seam from one application field type into a
/// public IcyDB input value through current accepted editable names.
///

#[doc(hidden)]
pub trait TypedInputValue: Sized {
    /// Encode one application value through an opaque current binding.
    fn encode_typed_input(
        self,
        binding: &TypedEntityBinding,
    ) -> Result<InputValue, TypedAdapterError>;
}

/// Immutable source identity emitted for one generated named type.
#[doc(hidden)]
pub trait TypedNamedType {
    /// Authored source key used to resolve the accepted named type.
    const SOURCE_KEY: &'static str;
}

macro_rules! impl_typed_input_value {
    ($($ty:ty),* $(,)?) => {
        $(
            impl TypedInputValue for $ty {
                fn encode_typed_input(
                    self,
                    _binding: &TypedEntityBinding,
                ) -> Result<InputValue, TypedAdapterError> {
                    Ok(InputValue::from(self))
                }
            }
        )*
    };
}

impl_typed_input_value!(
    Account, Blob, bool, Date, Decimal, Duration, Float32, Float64, i8, i16, i32, i64, i128,
    IntBig, NatBig, Principal, String, Subaccount, Timestamp, u8, u16, u32, u64, u128, Ulid, Unit,
);

impl<T> TypedInputValue for Box<T>
where
    T: TypedInputValue,
{
    fn encode_typed_input(
        self,
        binding: &TypedEntityBinding,
    ) -> Result<InputValue, TypedAdapterError> {
        (*self).encode_typed_input(binding)
    }
}

impl<T> TypedInputValue for Option<T>
where
    T: TypedInputValue,
{
    fn encode_typed_input(
        self,
        binding: &TypedEntityBinding,
    ) -> Result<InputValue, TypedAdapterError> {
        self.map_or(Ok(InputValue::Null), |value| {
            value.encode_typed_input(binding)
        })
    }
}

impl<T> TypedInputValue for Vec<T>
where
    T: TypedInputValue,
{
    fn encode_typed_input(
        self,
        binding: &TypedEntityBinding,
    ) -> Result<InputValue, TypedAdapterError> {
        self.into_iter()
            .map(|value| value.encode_typed_input(binding))
            .collect::<Result<Vec<_>, _>>()
            .map(InputValue::List)
    }
}

impl<K, V> TypedInputValue for BTreeMap<K, V>
where
    K: TypedInputValue,
    V: TypedInputValue,
{
    fn encode_typed_input(
        self,
        binding: &TypedEntityBinding,
    ) -> Result<InputValue, TypedAdapterError> {
        self.into_iter()
            .map(|(key, value)| {
                Ok((
                    key.encode_typed_input(binding)?,
                    value.encode_typed_input(binding)?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(InputValue::Map)
    }
}

impl<T> TypedInputValue for BTreeSet<T>
where
    T: TypedInputValue,
{
    fn encode_typed_input(
        self,
        binding: &TypedEntityBinding,
    ) -> Result<InputValue, TypedAdapterError> {
        self.into_iter()
            .map(|value| value.encode_typed_input(binding))
            .collect::<Result<Vec<_>, _>>()
            .map(InputValue::List)
    }
}

///
/// TypedOutputValue
///
/// Generated-code conversion seam from public IcyDB output values into one
/// application field type. This is macro wiring rather than accepted authority.
///

#[doc(hidden)]
pub trait TypedOutputValue: Sized {
    /// Decode one public output value without consulting generated schema state.
    fn decode_typed_output(
        binding: &TypedEntityBinding,
        value: &OutputValue,
    ) -> Result<Self, TypedAdapterError>;
}

macro_rules! impl_typed_output_value_clone {
    ($($ty:ty => $variant:ident),* $(,)?) => {
        $(
            impl TypedOutputValue for $ty {
                fn decode_typed_output(
                    _binding: &TypedEntityBinding,
                    value: &OutputValue,
                ) -> Result<Self, TypedAdapterError> {
                    match value {
                        OutputValue::$variant(value) => Ok(value.clone()),
                        _ => Err(TypedAdapterError::ValueShapeMismatch),
                    }
                }
            }
        )*
    };
}

macro_rules! impl_typed_output_value_narrow {
    ($($ty:ty => $variant:ident),* $(,)?) => {
        $(
            impl TypedOutputValue for $ty {
                fn decode_typed_output(
                    _binding: &TypedEntityBinding,
                    value: &OutputValue,
                ) -> Result<Self, TypedAdapterError> {
                    let OutputValue::$variant(value) = value else {
                        return Err(TypedAdapterError::ValueShapeMismatch);
                    };

                    Self::try_from(*value).map_err(|_| TypedAdapterError::ValueShapeMismatch)
                }
            }
        )*
    };
}

impl_typed_output_value_clone!(
    Account => Account,
    bool => Bool,
    Date => Date,
    Decimal => Decimal,
    Duration => Duration,
    Float32 => Float32,
    Float64 => Float64,
    i64 => Int64,
    i128 => Int128,
    IntBig => IntBig,
    NatBig => NatBig,
    Principal => Principal,
    String => Text,
    Subaccount => Subaccount,
    Timestamp => Timestamp,
    u64 => Nat64,
    u128 => Nat128,
    Ulid => Ulid,
);
impl_typed_output_value_narrow!(
    i8 => Int64,
    i16 => Int64,
    i32 => Int64,
    u8 => Nat64,
    u16 => Nat64,
    u32 => Nat64,
);

impl TypedOutputValue for Blob {
    fn decode_typed_output(
        _binding: &TypedEntityBinding,
        value: &OutputValue,
    ) -> Result<Self, TypedAdapterError> {
        match value {
            OutputValue::Blob(value) => Ok(Self::from(value.as_slice())),
            _ => Err(TypedAdapterError::ValueShapeMismatch),
        }
    }
}

impl TypedOutputValue for Unit {
    fn decode_typed_output(
        _binding: &TypedEntityBinding,
        value: &OutputValue,
    ) -> Result<Self, TypedAdapterError> {
        match value {
            OutputValue::Unit => Ok(Self),
            _ => Err(TypedAdapterError::ValueShapeMismatch),
        }
    }
}

impl<T> TypedOutputValue for Box<T>
where
    T: TypedOutputValue,
{
    fn decode_typed_output(
        binding: &TypedEntityBinding,
        value: &OutputValue,
    ) -> Result<Self, TypedAdapterError> {
        T::decode_typed_output(binding, value).map(Self::new)
    }
}

impl<T> TypedOutputValue for Option<T>
where
    T: TypedOutputValue,
{
    fn decode_typed_output(
        binding: &TypedEntityBinding,
        value: &OutputValue,
    ) -> Result<Self, TypedAdapterError> {
        match value {
            OutputValue::Null => Ok(None),
            _ => T::decode_typed_output(binding, value).map(Some),
        }
    }
}

impl<T> TypedOutputValue for Vec<T>
where
    T: TypedOutputValue,
{
    fn decode_typed_output(
        binding: &TypedEntityBinding,
        value: &OutputValue,
    ) -> Result<Self, TypedAdapterError> {
        let OutputValue::List(values) = value else {
            return Err(TypedAdapterError::ValueShapeMismatch);
        };

        values
            .iter()
            .map(|value| T::decode_typed_output(binding, value))
            .collect()
    }
}

impl<K, V> TypedOutputValue for BTreeMap<K, V>
where
    K: Ord + TypedOutputValue,
    V: TypedOutputValue,
{
    fn decode_typed_output(
        binding: &TypedEntityBinding,
        value: &OutputValue,
    ) -> Result<Self, TypedAdapterError> {
        let OutputValue::Map(entries) = value else {
            return Err(TypedAdapterError::ValueShapeMismatch);
        };

        let mut decoded = Self::new();
        for (key, value) in entries {
            let key = K::decode_typed_output(binding, key)?;
            let value = V::decode_typed_output(binding, value)?;
            if decoded.insert(key, value).is_some() {
                return Err(TypedAdapterError::ValueShapeMismatch);
            }
        }
        Ok(decoded)
    }
}

impl<T> TypedOutputValue for BTreeSet<T>
where
    T: Ord + TypedOutputValue,
{
    fn decode_typed_output(
        binding: &TypedEntityBinding,
        value: &OutputValue,
    ) -> Result<Self, TypedAdapterError> {
        let OutputValue::List(values) = value else {
            return Err(TypedAdapterError::ValueShapeMismatch);
        };

        let mut decoded = Self::new();
        for value in values {
            if !decoded.insert(T::decode_typed_output(binding, value)?) {
                return Err(TypedAdapterError::ValueShapeMismatch);
            }
        }
        Ok(decoded)
    }
}

/// One generated write lowered through immutable source keys.
#[derive(Clone, Debug)]
pub struct TypedWrite {
    binding: TypedEntityBinding,
    mutation: core::db::DynamicTypedMutation,
}

impl TypedWrite {
    /// Build one insert intent from immutable field source keys.
    pub fn insert<I, S>(binding: &TypedEntityBinding, fields: I) -> Result<Self, TypedAdapterError>
    where
        I: IntoIterator<Item = (S, WriteCell<InputValue>)>,
        S: AsRef<str>,
    {
        let patch = typed_patch_from_binding(binding, fields)?;
        Ok(Self {
            binding: binding.clone(),
            mutation: core::db::DynamicTypedMutation::Insert { patch },
        })
    }

    /// Build one patch/update intent from immutable field source keys.
    pub fn update<I, S>(
        binding: &TypedEntityBinding,
        key: InputValue,
        fields: I,
    ) -> Result<Self, TypedAdapterError>
    where
        I: IntoIterator<Item = (S, WriteCell<InputValue>)>,
        S: AsRef<str>,
    {
        let patch = typed_patch_from_binding(binding, fields)?;
        Ok(Self {
            binding: binding.clone(),
            mutation: core::db::DynamicTypedMutation::Update { key, patch },
        })
    }

    /// Build one replacement intent from immutable field source keys.
    pub fn replace<I, S>(
        binding: &TypedEntityBinding,
        key: InputValue,
        fields: I,
    ) -> Result<Self, TypedAdapterError>
    where
        I: IntoIterator<Item = (S, WriteCell<InputValue>)>,
        S: AsRef<str>,
    {
        let patch = typed_patch_from_binding(binding, fields)?;
        Ok(Self {
            binding: binding.clone(),
            mutation: core::db::DynamicTypedMutation::Replace { key, patch },
        })
    }
}

fn typed_patch_from_binding<I, S>(
    binding: &TypedEntityBinding,
    fields: I,
) -> Result<core::db::DynamicTypedStructuralPatch, TypedAdapterError>
where
    I: IntoIterator<Item = (S, WriteCell<InputValue>)>,
    S: AsRef<str>,
{
    let fields = fields
        .into_iter()
        .map(|(source, cell)| (source.as_ref().to_string(), cell.into_core()))
        .collect();
    binding
        .inner
        .bind_write_fields(fields)
        .ok_or(TypedAdapterError::FieldUnavailable)
}

impl WriteCell<InputValue> {
    fn into_core(self) -> core::db::DynamicWriteCell {
        match self {
            Self::Omitted => core::db::DynamicWriteCell::Omitted,
            Self::Default => core::db::DynamicWriteCell::Default,
            Self::Null => core::db::DynamicWriteCell::Null,
            Self::Value(value) => core::db::DynamicWriteCell::Value(value),
        }
    }
}

///
/// StructuralPatch
///
/// Public field-name-driven structural mutation patch.
/// Names are resolved only when the request is admitted against accepted
/// schema; the patch cannot carry physical row slots or generated field order.
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StructuralPatch {
    fields: Vec<(String, WriteCell<InputValue>)>,
}

impl StructuralPatch {
    /// Build one empty structural patch.
    #[must_use]
    pub const fn new() -> Self {
        Self { fields: Vec::new() }
    }

    /// Append one named field intent.
    #[must_use]
    pub fn field(mut self, name: impl Into<String>, value: WriteCell<InputValue>) -> Self {
        self.fields.push((name.into(), value));
        self
    }

    fn into_core(self) -> core::db::DynamicStructuralPatch {
        core::db::DynamicStructuralPatch::new(
            self.fields
                .into_iter()
                .map(|(name, value)| (name, value.into_core()))
                .collect(),
        )
    }
}

///
/// StructuralMutation
///
/// Entity-name-driven dynamic mutation request.
/// Each variant owns its key requirement and row-existence semantics.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StructuralMutation {
    /// Insert one row and derive its identity from the accepted after-image.
    Insert {
        /// Accepted entity display name.
        entity: String,
        /// Authored insert fields.
        patch: StructuralPatch,
    },
    /// Patch one existing row.
    Update {
        /// Accepted entity display name.
        entity: String,
        /// Public scalar or composite primary key.
        key: InputValue,
        /// Authored update fields.
        patch: StructuralPatch,
    },
    /// Replace one row, inserting it when absent.
    Replace {
        /// Accepted entity display name.
        entity: String,
        /// Public scalar or composite primary key.
        key: InputValue,
        /// Authored replacement fields.
        patch: StructuralPatch,
    },
    /// Delete one existing row.
    Delete {
        /// Accepted entity display name.
        entity: String,
        /// Public scalar or composite primary key.
        key: InputValue,
    },
}

impl StructuralMutation {
    fn into_core(self) -> core::db::DynamicMutation {
        match self {
            Self::Insert { entity, patch } => core::db::DynamicMutation::Insert {
                entity,
                patch: patch.into_core(),
            },
            Self::Update { entity, key, patch } => core::db::DynamicMutation::Update {
                entity,
                key,
                patch: patch.into_core(),
            },
            Self::Replace { entity, key, patch } => core::db::DynamicMutation::Replace {
                entity,
                key,
                patch: patch.into_core(),
            },
            Self::Delete { entity, key } => core::db::DynamicMutation::Delete { entity, key },
        }
    }
}

impl<C: CanisterKind> DbSession<C> {
    /// Execute one trusted structural mutation through accepted schema only.
    ///
    /// This dynamic lane never materializes a generated entity and never runs
    /// application validators or normalizers. The caller must enforce any
    /// required authorization before dispatch.
    pub fn execute_trusted_structural_mutation(
        &self,
        request: StructuralMutation,
    ) -> Result<DynamicMutationResult, Error> {
        Ok(self
            .inner
            .execute_trusted_dynamic_mutation(&request.into_core())?)
    }

    /// Execute one bounded same-entity structural mutation batch atomically.
    ///
    /// Inserts, updates, replacements, and deletes share one accepted snapshot,
    /// operation timestamp, final-row overlay, commit marker, and recovery
    /// outcome. Results retain input order and expose each save after-image or
    /// delete before-image. The batch admits at most 4,096 operations, 16 MiB
    /// of staged canonical keys/rows, and a 1 MiB encoded result. Every
    /// operation must resolve to the same accepted entity and target a distinct
    /// primary key.
    pub fn execute_trusted_structural_mutation_batch(
        &self,
        mutations: Vec<StructuralMutation>,
    ) -> Result<DynamicMutationResult, Error> {
        let mutations = mutations
            .into_iter()
            .map(StructuralMutation::into_core)
            .collect();
        Ok(self
            .inner
            .execute_trusted_dynamic_mutation_batch(mutations)?)
    }

    /// Execute one same-entity structural insert batch atomically.
    ///
    /// All patches bind to one accepted snapshot and share one database-owned
    /// operation timestamp. The batch either commits completely or returns a
    /// typed error without publishing a partial prefix.
    pub fn execute_trusted_structural_insert_batch(
        &self,
        entity: &str,
        patches: Vec<StructuralPatch>,
    ) -> Result<DynamicMutationResult, Error> {
        let patches = patches
            .into_iter()
            .map(StructuralPatch::into_core)
            .collect();
        Ok(self
            .inner
            .execute_trusted_dynamic_insert_batch(entity, patches)?)
    }

    fn typed_output_row(
        &self,
        binding: &TypedEntityBinding,
        entity: &str,
        columns: &[String],
        rows: &[Vec<OutputValue>],
        row_index: usize,
    ) -> Result<OutputRow, TypedRowError> {
        let current = self
            .inner
            .typed_entity_binding_is_current(&binding.inner)
            .map_err(|error| TypedRowError::Database(Error::from(error)))?;
        if !current {
            return Err(TypedRowError::Adapter(TypedAdapterError::StaleBinding));
        }
        if entity != binding.inner.entity() {
            return Err(TypedRowError::Adapter(TypedAdapterError::EntityMismatch));
        }
        let values = rows
            .get(row_index)
            .cloned()
            .ok_or(TypedRowError::Adapter(TypedAdapterError::RowShapeMismatch))?;
        OutputRow::new(binding, entity, columns.to_vec(), values).map_err(TypedRowError::Adapter)
    }

    /// Project one accepted dynamic-query row through a current opaque binding.
    #[cfg(feature = "query")]
    pub fn typed_query_row(
        &self,
        binding: &TypedEntityBinding,
        result: &DynamicQueryResult,
        row_index: usize,
    ) -> Result<OutputRow, TypedRowError> {
        self.typed_output_row(
            binding,
            result.entity.as_str(),
            result.columns.as_slice(),
            result.rows.as_slice(),
            row_index,
        )
    }

    /// Project one accepted structural-mutation row through a current opaque binding.
    pub fn typed_mutation_row(
        &self,
        binding: &TypedEntityBinding,
        result: &DynamicMutationResult,
        row_index: usize,
    ) -> Result<OutputRow, TypedRowError> {
        self.typed_output_row(
            binding,
            result.entity.as_str(),
            result.columns.as_slice(),
            result.rows.as_slice(),
            row_index,
        )
    }

    /// Issue one opaque current accepted binding for generated field contracts.
    #[doc(hidden)]
    pub fn bind_typed_entity<I>(
        &self,
        entity_source_key: &str,
        field_requests: I,
    ) -> Result<TypedEntityBinding, TypedBindingError>
    where
        I: IntoIterator<Item = TypedFieldBindingRequest>,
    {
        let fields = field_requests
            .into_iter()
            .map(TypedFieldBindingRequest::into_core)
            .collect::<Vec<_>>();
        self.inner
            .issue_typed_entity_binding(entity_source_key, fields.as_slice())
            .map(TypedEntityBinding::new)
            .map_err(|error| match error {
                core::db::DynamicTypedBindingError::FieldUnavailable => {
                    TypedBindingError::Adapter(TypedAdapterError::FieldUnavailable)
                }
                core::db::DynamicTypedBindingError::IncompatibleField => {
                    TypedBindingError::Adapter(TypedAdapterError::IncompatibleField)
                }
                core::db::DynamicTypedBindingError::Internal(error) => {
                    TypedBindingError::Database(Error::from(error))
                }
            })
    }

    /// Execute one generated write only while its opaque accepted binding is current.
    pub fn execute_trusted_typed_write(
        &self,
        write: TypedWrite,
    ) -> Result<DynamicMutationResult, TypedWriteError> {
        self.inner
            .execute_trusted_typed_mutation(&write.binding.inner, &write.mutation)
            .map_err(|error| TypedWriteError::Database(Error::from(error)))?
            .ok_or(TypedWriteError::Adapter(TypedAdapterError::StaleBinding))
    }

    /// Build one field-name-driven structural patch.
    #[must_use]
    pub fn structural_patch<I, S>(&self, fields: I) -> StructuralPatch
    where
        I: IntoIterator<Item = (S, WriteCell<InputValue>)>,
        S: Into<String>,
    {
        StructuralPatch {
            fields: fields
                .into_iter()
                .map(|(name, value)| (name.into(), value))
                .collect(),
        }
    }
}
