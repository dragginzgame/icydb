//! Module: db::session::write
//!
//! Responsibility: public `DbSession` write helpers, write-returning projection
//! conversion, and structural mutation facade types.
//! Does not own: core mutation execution, commit staging, or persisted encoding.
//! Boundary: keeps public write semantics and row-returning projection payloads
//! above the core save pipeline.

use crate::{
    db::{DynamicMutationResult, session::DbSession},
    error::Error,
    traits::CanisterKind,
    value::{InputValue, OutputValue, PublicEnumValue, PublicValue},
};
use candid::CandidType;
use icydb_core as core;
use serde::Deserialize;
use std::{
    collections::BTreeSet, error::Error as StdError, fmt, marker::PhantomData, sync::Arc,
    vec::IntoIter,
};

#[doc(hidden)]
pub use icydb_core::db::{TypedEntityDescriptor, TypedFieldDescriptor, TypedFieldType};

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

/// One complete accepted row supplied to an automatic generated adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputRow {
    projection: OutputRowProjection,
    values: Vec<OutputValue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OutputRowProjection(Arc<OutputRowProjectionInner>);

#[derive(Debug, Eq, PartialEq)]
struct OutputRowProjectionInner {
    binding: TypedEntityBinding,
    entity: String,
    accepted_slots: Vec<(u16, usize)>,
    value_count: usize,
}

impl OutputRowProjection {
    pub(crate) fn new(
        binding: &TypedEntityBinding,
        entity: impl Into<String>,
        columns: &[String],
    ) -> Result<Self, TypedAdapterError> {
        let entity = entity.into();
        if entity != binding.entity() {
            return Err(TypedAdapterError::EntityMismatch);
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
        Ok(Self(Arc::new(OutputRowProjectionInner {
            binding: binding.clone(),
            entity,
            accepted_slots,
            value_count: columns.len(),
        })))
    }

    pub(crate) fn project(&self, values: Vec<OutputValue>) -> Result<OutputRow, TypedAdapterError> {
        if values.len() != self.0.value_count {
            return Err(TypedAdapterError::RowShapeMismatch);
        }
        Ok(self.project_validated(values))
    }

    fn project_validated(&self, values: Vec<OutputValue>) -> OutputRow {
        OutputRow {
            projection: self.clone(),
            values,
        }
    }

    fn entity(&self) -> &str {
        self.0.entity.as_str()
    }

    fn binding(&self) -> &TypedEntityBinding {
        &self.0.binding
    }
}

/// Owned output rows sharing one accepted typed projection.
#[doc(hidden)]
pub struct PreparedOutputRows {
    projection: OutputRowProjection,
    rows: IntoIter<Vec<OutputValue>>,
}

impl PreparedOutputRows {
    fn new(
        binding: &TypedEntityBinding,
        entity: String,
        columns: Vec<String>,
        rows: Vec<Vec<OutputValue>>,
    ) -> Result<Self, TypedAdapterError> {
        let projection = OutputRowProjection::new(binding, entity, columns.as_slice())?;
        for values in &rows {
            if values.len() != projection.0.value_count {
                return Err(TypedAdapterError::RowShapeMismatch);
            }
        }
        Ok(Self {
            projection,
            rows: rows.into_iter(),
        })
    }
}

impl Iterator for PreparedOutputRows {
    type Item = OutputRow;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows
            .next()
            .map(|values| self.projection.project_validated(values))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl ExactSizeIterator for PreparedOutputRows {}

impl OutputRow {
    fn new(
        binding: &TypedEntityBinding,
        entity: impl Into<String>,
        columns: &[String],
        values: Vec<OutputValue>,
    ) -> Result<Self, TypedAdapterError> {
        OutputRowProjection::new(binding, entity, columns)?.project(values)
    }

    /// Borrow the accepted entity display name.
    #[must_use]
    pub fn entity(&self) -> &str {
        self.projection.entity()
    }
}

struct MutationResultRowParts {
    affected_rows: u32,
    columns: Vec<String>,
    entity: String,
    values: Vec<OutputValue>,
}

fn mutation_result_row_parts(
    result: DynamicMutationResult,
) -> Result<MutationResultRowParts, TypedAdapterError> {
    let DynamicMutationResult {
        entity,
        columns,
        mut rows,
        affected_rows,
    } = result;
    if rows.len() != 1 {
        return Err(TypedAdapterError::RowShapeMismatch);
    }
    let values = rows.pop().ok_or(TypedAdapterError::RowShapeMismatch)?;
    Ok(MutationResultRowParts {
        affected_rows,
        columns,
        entity,
        values,
    })
}

#[inline(never)]
fn project_single_mutation_result(
    binding: &TypedEntityBinding,
    result: DynamicMutationResult,
) -> Result<OutputRow, TypedAdapterError> {
    let parts = mutation_result_row_parts(result)?;
    OutputRow::new(
        binding,
        parts.entity,
        parts.columns.as_slice(),
        parts.values,
    )
}

#[inline(never)]
fn project_mutation_result_batch(
    binding: &TypedEntityBinding,
    results: Vec<DynamicMutationResult>,
    expected_results: usize,
) -> Result<Vec<OutputRow>, TypedAdapterError> {
    if results.len() != expected_results {
        return Err(TypedAdapterError::RowShapeMismatch);
    }
    let mut results = results.into_iter();
    let Some(first) = results.next() else {
        return Ok(Vec::new());
    };
    let first = mutation_result_row_parts(first)?;
    let projection = OutputRowProjection::new(binding, first.entity, first.columns.as_slice())?;
    let mut projected = Vec::with_capacity(expected_results);
    projected.push(projection.project(first.values)?);

    for result in results {
        let result = mutation_result_row_parts(result)?;
        if result.entity != projection.entity() {
            return Err(TypedAdapterError::EntityMismatch);
        }
        if result.columns != first.columns {
            return Err(TypedAdapterError::RowShapeMismatch);
        }
        projected.push(projection.project(result.values)?);
    }

    Ok(projected)
}

/// Stable typed-adapter boundary failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypedAdapterError {
    /// A typed item handle does not belong to this exact batch result owner.
    BatchHandleMismatch,

    /// The typed batch row selected by this handle has already been decoded.
    BatchRowConsumed,

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
            Self::BatchHandleMismatch => "typed batch handle mismatch",
            Self::BatchRowConsumed => "typed batch row already consumed",
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

impl From<icydb_model::TypedValueError> for TypedAdapterError {
    fn from(error: icydb_model::TypedValueError) -> Self {
        match error {
            icydb_model::TypedValueError::SourceUnavailable => Self::FieldUnavailable,
            icydb_model::TypedValueError::ShapeMismatch => Self::ValueShapeMismatch,
        }
    }
}

/// Failure while binding, decoding, or executing one typed operation.
#[derive(Debug)]
pub enum TypedOperationError {
    /// Generated shape, binding, or returned-row validation failed.
    Adapter(TypedAdapterError),
    /// The accepted database operation rejected or failed.
    Database(Error),
}

impl fmt::Display for TypedOperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(error) => error.fmt(formatter),
            Self::Database(error) => error.fmt(formatter),
        }
    }
}

impl StdError for TypedOperationError {}

impl From<Error> for TypedOperationError {
    fn from(error: Error) -> Self {
        Self::Database(error)
    }
}

impl From<TypedAdapterError> for TypedOperationError {
    fn from(error: TypedAdapterError) -> Self {
        Self::Adapter(error)
    }
}

/// Opaque accepted-schema binding for one automatic generated adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedEntityBinding {
    inner: core::db::DynamicTypedEntityBinding,
}

impl TypedEntityBinding {
    const fn new(inner: core::db::DynamicTypedEntityBinding) -> Self {
        Self { inner }
    }

    pub(crate) const fn inner(&self) -> &core::db::DynamicTypedEntityBinding {
        &self.inner
    }

    pub(crate) const fn entity(&self) -> &str {
        self.inner.entity()
    }

    /// Borrow one bound row value by immutable field source key.
    pub fn row_value<'a>(
        &self,
        field_source_key: &str,
        row: &'a OutputRow,
    ) -> Result<&'a OutputValue, TypedAdapterError> {
        if row.projection.0.binding != *self {
            return Err(TypedAdapterError::StaleBinding);
        }
        if row.projection.0.entity != self.inner.entity() {
            return Err(TypedAdapterError::EntityMismatch);
        }
        let slot = self
            .inner
            .field_slot(field_source_key)
            .ok_or(TypedAdapterError::FieldUnavailable)?;
        let index = row
            .projection
            .0
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
        value: &'value PublicValue,
    ) -> Result<Vec<&'value PublicValue>, TypedAdapterError> {
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

impl icydb_model::TypedAdapterContext for TypedEntityBinding {
    type PublicValue = PublicValue;

    fn input_scalar(&self, value: icydb_model::TypedScalarValue) -> Self::PublicValue {
        match value {
            icydb_model::TypedScalarValue::Account(value) => PublicValue::Account(value),
            icydb_model::TypedScalarValue::Blob(value) => PublicValue::Blob(value.to_vec()),
            icydb_model::TypedScalarValue::Bool(value) => PublicValue::Bool(value),
            icydb_model::TypedScalarValue::Date(value) => PublicValue::Date(value),
            icydb_model::TypedScalarValue::Decimal(value) => PublicValue::Decimal(value),
            icydb_model::TypedScalarValue::Duration(value) => PublicValue::Duration(value),
            icydb_model::TypedScalarValue::Float32(value) => PublicValue::Float32(value),
            icydb_model::TypedScalarValue::Float64(value) => PublicValue::Float64(value),
            icydb_model::TypedScalarValue::Int64(value) => PublicValue::Int64(value),
            icydb_model::TypedScalarValue::Int128(value) => PublicValue::Int128(value),
            icydb_model::TypedScalarValue::IntBig(value) => PublicValue::IntBig(value),
            icydb_model::TypedScalarValue::Nat64(value) => PublicValue::Nat64(value),
            icydb_model::TypedScalarValue::Nat128(value) => PublicValue::Nat128(value),
            icydb_model::TypedScalarValue::NatBig(value) => PublicValue::NatBig(value),
            icydb_model::TypedScalarValue::Principal(value) => PublicValue::Principal(value),
            icydb_model::TypedScalarValue::Subaccount(value) => PublicValue::Subaccount(value),
            icydb_model::TypedScalarValue::Text(value) => PublicValue::Text(value),
            icydb_model::TypedScalarValue::Timestamp(value) => PublicValue::Timestamp(value),
            icydb_model::TypedScalarValue::Ulid(value) => PublicValue::Ulid(value),
            icydb_model::TypedScalarValue::Unit => PublicValue::Unit,
            icydb_model::TypedScalarValue::U256(value) => PublicValue::U256(value),
        }
    }

    fn input_list(&self, values: Vec<Self::PublicValue>) -> Self::PublicValue {
        PublicValue::List(values)
    }

    fn input_map(&self, entries: Vec<(Self::PublicValue, Self::PublicValue)>) -> Self::PublicValue {
        PublicValue::Map(entries)
    }

    fn input_null(&self) -> Self::PublicValue {
        PublicValue::Null
    }

    fn input_enum(
        &self,
        type_source_key: &'static str,
        variant_source_key: &'static str,
        payload: Option<Self::PublicValue>,
    ) -> Result<Self::PublicValue, icydb_model::TypedValueError> {
        let type_name = self
            .named_type_name(type_source_key)
            .ok_or(icydb_model::TypedValueError::SourceUnavailable)?;
        let variant_name = self
            .enum_variant_name(type_source_key, variant_source_key)
            .ok_or(icydb_model::TypedValueError::SourceUnavailable)?;
        let value = PublicEnumValue::new(variant_name, Some(type_name));
        Ok(PublicValue::Enum(match payload {
            Some(payload) => value.with_payload(payload),
            None => value,
        }))
    }

    fn input_record(
        &self,
        type_source_key: &'static str,
        fields: Vec<(&'static str, Self::PublicValue)>,
    ) -> Result<Self::PublicValue, icydb_model::TypedValueError> {
        fields
            .into_iter()
            .map(|(source_key, value)| {
                let name = self
                    .composite_field_name(type_source_key, source_key)
                    .ok_or(icydb_model::TypedValueError::SourceUnavailable)?;
                Ok((PublicValue::Text(name.to_string()), value))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(PublicValue::Map)
    }

    fn output_scalar(&self, value: &Self::PublicValue) -> Option<icydb_model::TypedScalarValue> {
        Some(match value {
            PublicValue::Account(value) => icydb_model::TypedScalarValue::Account(*value),
            PublicValue::Blob(value) => {
                icydb_model::TypedScalarValue::Blob(icydb_schema::Blob::from(value.as_slice()))
            }
            PublicValue::Bool(value) => icydb_model::TypedScalarValue::Bool(*value),
            PublicValue::Date(value) => icydb_model::TypedScalarValue::Date(*value),
            PublicValue::Decimal(value) => icydb_model::TypedScalarValue::Decimal(*value),
            PublicValue::Duration(value) => icydb_model::TypedScalarValue::Duration(*value),
            PublicValue::Float32(value) => icydb_model::TypedScalarValue::Float32(*value),
            PublicValue::Float64(value) => icydb_model::TypedScalarValue::Float64(*value),
            PublicValue::Int64(value) => icydb_model::TypedScalarValue::Int64(*value),
            PublicValue::Int128(value) => icydb_model::TypedScalarValue::Int128(*value),
            PublicValue::IntBig(value) => icydb_model::TypedScalarValue::IntBig(value.clone()),
            PublicValue::Nat64(value) => icydb_model::TypedScalarValue::Nat64(*value),
            PublicValue::Nat128(value) => icydb_model::TypedScalarValue::Nat128(*value),
            PublicValue::NatBig(value) => icydb_model::TypedScalarValue::NatBig(value.clone()),
            PublicValue::Principal(value) => icydb_model::TypedScalarValue::Principal(*value),
            PublicValue::Subaccount(value) => icydb_model::TypedScalarValue::Subaccount(*value),
            PublicValue::Text(value) => icydb_model::TypedScalarValue::Text(value.clone()),
            PublicValue::Timestamp(value) => icydb_model::TypedScalarValue::Timestamp(*value),
            PublicValue::Ulid(value) => icydb_model::TypedScalarValue::Ulid(*value),
            PublicValue::Unit => icydb_model::TypedScalarValue::Unit,
            PublicValue::U256(value) => icydb_model::TypedScalarValue::U256(*value),
            PublicValue::Enum(_)
            | PublicValue::List(_)
            | PublicValue::Map(_)
            | PublicValue::Null => {
                return None;
            }
        })
    }

    fn output_list<'a>(&self, value: &'a Self::PublicValue) -> Option<&'a [Self::PublicValue]> {
        match value {
            PublicValue::List(values) => Some(values.as_slice()),
            _ => None,
        }
    }

    fn output_map<'a>(
        &self,
        value: &'a Self::PublicValue,
    ) -> Option<&'a [(Self::PublicValue, Self::PublicValue)]> {
        match value {
            PublicValue::Map(entries) => Some(entries.as_slice()),
            _ => None,
        }
    }

    fn output_is_null(&self, value: &Self::PublicValue) -> bool {
        matches!(value, PublicValue::Null)
    }

    fn output_enum<'a>(
        &self,
        descriptor: &'static icydb_model::TypedEnumDescriptor,
        value: &'a Self::PublicValue,
    ) -> Result<icydb_model::TypedEnumSelection<'a, Self::PublicValue>, icydb_model::TypedValueError>
    {
        let PublicValue::Enum(value) = value else {
            return Err(icydb_model::TypedValueError::ShapeMismatch);
        };
        let type_name = self
            .named_type_name(descriptor.type_source_key)
            .ok_or(icydb_model::TypedValueError::SourceUnavailable)?;
        if value.path() != Some(type_name) {
            return Err(icydb_model::TypedValueError::ShapeMismatch);
        }
        let variant_source_key = self
            .inner
            .enum_variant_source_key(descriptor.type_source_key, value.variant())
            .ok_or(icydb_model::TypedValueError::ShapeMismatch)?;
        let ordinal = descriptor
            .variants
            .iter()
            .position(|source_key| *source_key == variant_source_key)
            .ok_or(icydb_model::TypedValueError::ShapeMismatch)?;
        Ok(icydb_model::TypedEnumSelection {
            ordinal,
            payload: value.payload(),
        })
    }

    fn output_record<'a>(
        &self,
        type_source_key: &'static str,
        member_source_keys: &[&'static str],
        value: &'a Self::PublicValue,
    ) -> Result<Vec<&'a Self::PublicValue>, icydb_model::TypedValueError> {
        self.record_output_values(type_source_key, member_source_keys, value)
            .map_err(|error| match error {
                TypedAdapterError::FieldUnavailable => {
                    icydb_model::TypedValueError::SourceUnavailable
                }
                _ => icydb_model::TypedValueError::ShapeMismatch,
            })
    }
}

fn exact_record_output_values<'value>(
    accepted_names: &[&str],
    value: &'value PublicValue,
) -> Result<Vec<&'value PublicValue>, TypedAdapterError> {
    let PublicValue::Map(entries) = value else {
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
        let PublicValue::Text(name) = key else {
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
    use crate::value::PublicValue;

    fn text(value: &str) -> PublicValue {
        PublicValue::Text(value.to_string())
    }

    #[test]
    fn exact_record_output_reorders_accepted_members() {
        let value = PublicValue::Map(vec![
            (text("count"), PublicValue::Nat64(7)),
            (text("label"), text("Ada")),
        ]);

        let values = exact_record_output_values(&["label", "count"], &value)
            .expect("exact accepted record output should decode");

        assert!(matches!(values[0], PublicValue::Text(value) if value == "Ada"));
        assert_eq!(values[1], &PublicValue::Nat64(7));
    }

    #[test]
    fn exact_record_output_rejects_duplicate_missing_and_unknown_members() {
        let malformed = [
            PublicValue::Map(vec![
                (text("label"), text("Ada")),
                (text("label"), text("Grace")),
            ]),
            PublicValue::Map(vec![(text("label"), text("Ada"))]),
            PublicValue::Map(vec![
                (text("label"), text("Ada")),
                (text("other"), PublicValue::Nat64(7)),
            ]),
            PublicValue::Map(vec![
                (PublicValue::Nat64(1), text("Ada")),
                (text("count"), PublicValue::Nat64(7)),
            ]),
            PublicValue::List(Vec::new()),
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

    #[test]
    fn typed_input_failures_retain_source_and_shape_classes() {
        assert_eq!(
            TypedAdapterError::from(icydb_model::TypedValueError::SourceUnavailable),
            TypedAdapterError::FieldUnavailable,
        );
        assert_eq!(
            TypedAdapterError::from(icydb_model::TypedValueError::ShapeMismatch),
            TypedAdapterError::ValueShapeMismatch,
        );
    }
}

/// IcyDB-owned decode adapter implemented by runtime-enabled generated code.
pub trait TypedRowAdapter {
    /// Complete application row produced by decoding.
    type Row;

    /// Decode one accepted output row through an opaque current binding.
    fn decode_row(
        binding: &TypedEntityBinding,
        row: OutputRow,
    ) -> Result<Self::Row, TypedAdapterError>;
}

/// IcyDB-owned binding adapter implemented by runtime-enabled generated entities.
pub trait TypedEntityAdapter: TypedRowAdapter {
    /// Static generated source contract validated before an opaque binding is issued.
    #[doc(hidden)]
    const DESCRIPTOR: &'static TypedEntityDescriptor;

    /// Bind generated source identities to current accepted schema authority.
    fn typed_binding<C>(session: &DbSession<C>) -> Result<TypedEntityBinding, TypedOperationError>
    where
        C: CanisterKind,
    {
        session.bind_typed_entity(Self::DESCRIPTOR)
    }
}

/// IcyDB-owned write adapter implemented by runtime-enabled generated inputs.
pub trait TypedWriteAdapter {
    /// Generated entity that owns this operation-specific write input.
    type Entity: TypedEntityAdapter;

    /// Lower explicit application write intent without resolving database policy.
    fn encode_write(self, binding: &TypedEntityBinding) -> Result<TypedWrite, TypedAdapterError>;
}

/// Concrete generated-field encoder over one opaque accepted binding.
#[doc(hidden)]
pub struct BoundWriteEncoder {
    binding: TypedEntityBinding,
    fields: Vec<(usize, core::db::DynamicWriteCell)>,
}

impl BoundWriteEncoder {
    /// Prepare one generated write with its exact authored-field capacity.
    #[must_use]
    pub fn new(binding: &TypedEntityBinding, field_count: usize) -> Self {
        Self {
            binding: binding.clone(),
            fields: Vec::with_capacity(field_count),
        }
    }

    /// Append one generated write cell by binding-local descriptor ordinal.
    pub fn push(&mut self, descriptor_ordinal: usize, cell: WriteCell<InputValue>) {
        self.fields.push((descriptor_ordinal, cell.into_core()));
    }

    /// Finish one insert intent through the opaque accepted binding.
    pub fn insert(self) -> Result<TypedWrite, TypedAdapterError> {
        let (binding, patch) = self.into_bound_patch()?;
        Ok(TypedWrite {
            binding,
            mutation: core::db::DynamicTypedMutation::Insert { patch },
        })
    }

    /// Finish one patch/update intent through the opaque accepted binding.
    pub fn update(self, key: InputValue) -> Result<TypedWrite, TypedAdapterError> {
        let (binding, patch) = self.into_bound_patch()?;
        Ok(TypedWrite {
            binding,
            mutation: core::db::DynamicTypedMutation::Update { key, patch },
        })
    }

    /// Finish one replacement intent through the opaque accepted binding.
    pub fn replace(self, key: InputValue) -> Result<TypedWrite, TypedAdapterError> {
        let (binding, patch) = self.into_bound_patch()?;
        Ok(TypedWrite {
            binding,
            mutation: core::db::DynamicTypedMutation::Replace { key, patch },
        })
    }

    fn into_bound_patch(
        self,
    ) -> Result<(TypedEntityBinding, core::db::DynamicTypedStructuralPatch), TypedAdapterError>
    {
        let Self { binding, fields } = self;
        let patch = binding
            .inner
            .bind_write_ordinals(fields)
            .ok_or(TypedAdapterError::FieldUnavailable)?;
        Ok((binding, patch))
    }
}

/// One generated write lowered through binding-owned accepted field identities.
#[derive(Clone, Debug)]
pub struct TypedWrite {
    binding: TypedEntityBinding,
    mutation: core::db::DynamicTypedMutation,
}

impl TypedWrite {
    /// Build one delete intent from an accepted primary-key value.
    #[must_use]
    pub fn delete(binding: &TypedEntityBinding, key: InputValue) -> Self {
        Self {
            binding: binding.clone(),
            mutation: core::db::DynamicTypedMutation::Delete { key },
        }
    }
}

/// Sealed reference to one item accepted by a typed write-batch builder.
///
/// Handles are process-local facade values. They are not serializable,
/// durable, authoritative, or reusable with another builder result.
pub struct TypedWriteHandle<E> {
    owner: Arc<()>,
    position: usize,
    entity: PhantomData<fn() -> E>,
}

impl<E> Clone for TypedWriteHandle<E> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            position: self.position,
            entity: PhantomData,
        }
    }
}

/// One ephemeral mixed-entity typed batch over the canonical write executor.
pub struct TrustedTypedWriteBatch<'session, C: CanisterKind> {
    session: &'session DbSession<C>,
    owner: Arc<()>,
    writes: Vec<TypedWrite>,
    bindings: Vec<TypedEntityBinding>,
}

impl<'session, C: CanisterKind> TrustedTypedWriteBatch<'session, C> {
    fn new(session: &'session DbSession<C>) -> Self {
        Self {
            session,
            owner: Arc::new(()),
            writes: Vec::new(),
            bindings: Vec::new(),
        }
    }

    /// Resolve and encode one generated write under its generated entity.
    pub fn push<W>(&mut self, input: W) -> Result<TypedWriteHandle<W::Entity>, TypedOperationError>
    where
        W: TypedWriteAdapter,
    {
        let binding = W::Entity::typed_binding(self.session)?;
        let write = input.encode_write(&binding)?;
        if write.binding != binding {
            return Err(TypedAdapterError::EntityMismatch.into());
        }
        let position = self.writes.len();
        self.writes.push(write);
        self.bindings.push(binding);
        Ok(TypedWriteHandle {
            owner: self.owner.clone(),
            position,
            entity: PhantomData,
        })
    }

    /// Consume this builder and execute its writes in one canonical batch.
    pub fn execute(self) -> Result<TypedWriteBatchResults, TypedOperationError> {
        let results = self
            .session
            .execute_trusted_typed_write_batch(self.writes)?;
        let results = project_typed_write_batch_results(self.bindings, results)?;
        Ok(TypedWriteBatchResults {
            owner: self.owner,
            results,
        })
    }
}

/// One projected result retained by a mixed-entity typed batch.
pub struct TypedWriteBatchResult {
    affected_rows: u32,
    projection: OutputRowProjection,
    row: Option<OutputRow>,
}

impl TypedWriteBatchResult {
    /// Return the accepted entity display name for this result.
    #[must_use]
    pub fn entity(&self) -> &str {
        self.projection.entity()
    }

    /// Return the number of rows affected by this write.
    #[must_use]
    pub const fn affected_rows(&self) -> u32 {
        self.affected_rows
    }

    fn take_row(&mut self) -> Result<OutputRow, TypedAdapterError> {
        self.row.take().ok_or(TypedAdapterError::BatchRowConsumed)
    }
}

/// Ordered projected results from one mixed-entity typed batch.
pub struct TypedWriteBatchResults {
    owner: Arc<()>,
    results: Vec<TypedWriteBatchResult>,
}

impl TypedWriteBatchResults {
    fn handle_position<E>(&self, handle: &TypedWriteHandle<E>) -> Result<usize, TypedAdapterError> {
        if !Arc::ptr_eq(&self.owner, &handle.owner) || handle.position >= self.results.len() {
            return Err(TypedAdapterError::BatchHandleMismatch);
        }
        Ok(handle.position)
    }

    /// Borrow the projected result selected by a builder-issued handle.
    pub fn result<E>(
        &self,
        handle: &TypedWriteHandle<E>,
    ) -> Result<&TypedWriteBatchResult, TypedAdapterError> {
        let position = self.handle_position(handle)?;
        self.results
            .get(position)
            .ok_or(TypedAdapterError::BatchHandleMismatch)
    }

    /// Consume and decode the selected result's row through its retained binding.
    ///
    /// Each builder handle owns exactly one row decode. A second decode attempt
    /// returns [`TypedAdapterError::BatchRowConsumed`].
    pub fn row<E>(&mut self, handle: &TypedWriteHandle<E>) -> Result<E::Row, TypedOperationError>
    where
        E: TypedEntityAdapter,
    {
        let position = self.handle_position(handle)?;
        let result = self
            .results
            .get_mut(position)
            .ok_or(TypedAdapterError::BatchHandleMismatch)?;
        let row = result.take_row()?;
        Ok(E::decode_row(result.projection.binding(), row)?)
    }
}

fn project_typed_write_batch_results(
    bindings: Vec<TypedEntityBinding>,
    results: Vec<DynamicMutationResult>,
) -> Result<Vec<TypedWriteBatchResult>, TypedAdapterError> {
    if bindings.len() != results.len() {
        return Err(TypedAdapterError::RowShapeMismatch);
    }
    let mut projected = Vec::with_capacity(results.len());
    for (binding, result) in bindings.into_iter().zip(results) {
        let result = mutation_result_row_parts(result)?;
        let projection =
            OutputRowProjection::new(&binding, result.entity, result.columns.as_slice())?;
        let row = projection.project(result.values)?;
        projected.push(TypedWriteBatchResult {
            affected_rows: result.affected_rows,
            projection,
            row: Some(row),
        });
    }
    Ok(projected)
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
    fn entity(&self) -> &str {
        match self {
            Self::Insert { entity, .. }
            | Self::Update { entity, .. }
            | Self::Replace { entity, .. }
            | Self::Delete { entity, .. } => entity,
        }
    }

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
    #[inline(never)]
    fn ensure_typed_write_binding_is_current(
        &self,
        binding: &TypedEntityBinding,
    ) -> Result<(), TypedOperationError> {
        let current = self
            .inner
            .typed_entity_binding_is_current(&binding.inner)
            .map_err(|error| TypedOperationError::Database(Error::from(error)))?;
        if !current {
            return Err(TypedAdapterError::StaleBinding.into());
        }
        Ok(())
    }

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

    /// Execute one bounded same-store structural mutation batch atomically.
    ///
    /// Inserts, updates, replacements, and deletes share one accepted snapshot,
    /// operation timestamp, final-row overlay, commit marker, and recovery
    /// outcome. Results retain input order, with one single-row result per
    /// request. The batch admits at most 4,096 operations across 64 accepted
    /// entities, 16 MiB of staged canonical keys/rows, and a 1 MiB encoded
    /// result. Every entity must belong to the same store and each operation
    /// must target a distinct entity-qualified primary key.
    pub fn execute_trusted_structural_mutation_batch(
        &self,
        mutations: Vec<StructuralMutation>,
    ) -> Result<Vec<DynamicMutationResult>, Error> {
        let mutations = mutations
            .into_iter()
            .map(StructuralMutation::into_core)
            .collect();
        Ok(self
            .inner
            .execute_trusted_dynamic_mutation_batch(mutations)?)
    }

    /// Execute and project one same-entity structural mutation batch.
    ///
    /// The binding is checked before execution, every mutation must target its
    /// accepted entity, and every result must contain exactly one row. This
    /// concrete terminal keeps mutation execution, cardinality validation and
    /// accepted-field projection outside generated entity monomorphs; callers
    /// retain only their final [`TypedRowAdapter::decode_row`] step.
    #[inline(never)]
    pub fn execute_trusted_structural_mutation_batch_rows(
        &self,
        binding: &TypedEntityBinding,
        mutations: Vec<StructuralMutation>,
    ) -> Result<Vec<OutputRow>, TypedOperationError> {
        self.ensure_typed_write_binding_is_current(binding)?;
        if mutations
            .iter()
            .any(|mutation| mutation.entity() != binding.entity())
        {
            return Err(TypedAdapterError::EntityMismatch.into());
        }

        let expected_results = mutations.len();
        let mutations = mutations
            .into_iter()
            .map(StructuralMutation::into_core)
            .collect();
        let results = self
            .inner
            .execute_trusted_dynamic_mutation_batch(mutations)
            .map_err(|error| TypedOperationError::Database(Error::from(error)))?;
        Ok(project_mutation_result_batch(
            binding,
            results,
            expected_results,
        )?)
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

    /// Prepare one owned dynamic row batch for non-entity-generic decoding.
    #[doc(hidden)]
    pub fn prepare_typed_output_rows(
        &self,
        binding: &TypedEntityBinding,
        entity: String,
        columns: Vec<String>,
        rows: Vec<Vec<OutputValue>>,
    ) -> Result<PreparedOutputRows, TypedOperationError> {
        let current = self
            .inner
            .typed_entity_binding_is_current(&binding.inner)
            .map_err(|error| TypedOperationError::Database(Error::from(error)))?;
        if !current {
            return Err(TypedOperationError::Adapter(
                TypedAdapterError::StaleBinding,
            ));
        }
        PreparedOutputRows::new(binding, entity, columns, rows)
            .map_err(TypedOperationError::Adapter)
    }

    /// Issue one opaque current accepted binding for generated field contracts.
    #[doc(hidden)]
    pub fn bind_typed_entity(
        &self,
        descriptor: &TypedEntityDescriptor,
    ) -> Result<TypedEntityBinding, TypedOperationError> {
        self.inner
            .issue_typed_entity_binding(descriptor)
            .map(TypedEntityBinding::new)
            .map_err(|error| match error {
                core::db::DynamicTypedBindingError::FieldUnavailable => {
                    TypedOperationError::Adapter(TypedAdapterError::FieldUnavailable)
                }
                core::db::DynamicTypedBindingError::IncompatibleField => {
                    TypedOperationError::Adapter(TypedAdapterError::IncompatibleField)
                }
                core::db::DynamicTypedBindingError::Internal(error) => {
                    TypedOperationError::Database(Error::from(error))
                }
            })
    }

    /// Encode one generated value as a structural input through an exact
    /// current accepted binding.
    ///
    /// Generated records, enums, and collections reuse their existing
    /// [`icydb_model::TypedInputValue`] implementation. Accepted source
    /// bindings resolve every named type, record member, and enum variant;
    /// the returned [`InputValue`] enters the ordinary structural write
    /// admission path.
    pub fn bind_typed_input<T>(
        &self,
        binding: &TypedEntityBinding,
        value: T,
    ) -> Result<InputValue, TypedOperationError>
    where
        T: icydb_model::TypedInputValue,
    {
        self.ensure_typed_write_binding_is_current(binding)?;
        let value = value
            .encode_typed_input(binding)
            .map_err(TypedAdapterError::from)?;
        Ok(InputValue::from_public(value))
    }

    /// Execute one generated write only while its opaque accepted binding is current.
    pub fn execute_trusted_typed_write(
        &self,
        write: TypedWrite,
    ) -> Result<DynamicMutationResult, TypedOperationError> {
        self.inner
            .execute_trusted_typed_mutation(&write.binding.inner, &write.mutation)
            .map_err(|error| TypedOperationError::Database(Error::from(error)))?
            .ok_or(TypedOperationError::Adapter(
                TypedAdapterError::StaleBinding,
            ))
    }

    /// Execute one generated write and project its required single row.
    ///
    /// `TypedWrite` already owns its accepted binding, so execution, exact
    /// single-row validation and accepted-field projection remain in one
    /// concrete non-entity terminal. Generated adapters need only decode the
    /// returned row into their final Rust entity.
    #[inline(never)]
    pub fn execute_trusted_typed_write_row(
        &self,
        write: TypedWrite,
    ) -> Result<OutputRow, TypedOperationError> {
        let TypedWrite { binding, mutation } = write;
        let result = self
            .inner
            .execute_trusted_typed_mutation(&binding.inner, &mutation)
            .map_err(|error| TypedOperationError::Database(Error::from(error)))?
            .ok_or(TypedOperationError::Adapter(
                TypedAdapterError::StaleBinding,
            ))?;
        Ok(project_single_mutation_result(&binding, result)?)
    }

    /// Execute one non-empty same-store generated write batch atomically.
    ///
    /// Each exact current binding is revalidated from one captured accepted
    /// root. The canonical structural batch owner enforces the entity,
    /// operation, staged-byte, and result-byte limits before publishing any
    /// durable effect. Results retain input order with one item per write.
    pub fn execute_trusted_typed_write_batch(
        &self,
        writes: Vec<TypedWrite>,
    ) -> Result<Vec<DynamicMutationResult>, TypedOperationError> {
        let requests = writes
            .into_iter()
            .map(|write| (write.binding.inner, write.mutation))
            .collect();
        self.inner
            .execute_trusted_typed_mutation_batch(requests)
            .map_err(|error| TypedOperationError::Database(Error::from(error)))?
            .ok_or(TypedOperationError::Adapter(
                TypedAdapterError::StaleBinding,
            ))
    }

    /// Execute and prepare one non-empty same-entity generated write batch.
    ///
    /// Every write must carry the supplied exact binding. The existing typed
    /// mutation batch remains the atomic commit and recovery owner; this
    /// terminal adds only exact result cardinality and one shared accepted-row
    /// projection before generated code performs its final typed decode.
    #[inline(never)]
    pub fn execute_trusted_typed_write_batch_rows(
        &self,
        binding: &TypedEntityBinding,
        writes: Vec<TypedWrite>,
    ) -> Result<PreparedOutputRows, TypedOperationError> {
        if writes.iter().any(|write| write.binding != *binding) {
            return Err(TypedAdapterError::EntityMismatch.into());
        }
        let expected_results = writes.len();
        let requests = writes.into_iter().map(|write| write.mutation).collect();
        let result = self
            .inner
            .execute_trusted_same_entity_typed_mutation_batch(binding.inner(), requests)
            .map_err(|error| TypedOperationError::Database(Error::from(error)))?
            .ok_or(TypedOperationError::Adapter(
                TypedAdapterError::StaleBinding,
            ))?;
        let DynamicMutationResult {
            entity,
            columns,
            rows,
            affected_rows: _,
        } = result;
        if rows.len() != expected_results {
            return Err(TypedAdapterError::RowShapeMismatch.into());
        }
        Ok(PreparedOutputRows::new(binding, entity, columns, rows)?)
    }

    /// Start one mixed-entity generated typed-write batch.
    ///
    /// `push` resolves each generated input's entity binding automatically;
    /// `execute` dispatches exactly once through the canonical typed batch.
    #[must_use]
    pub fn trusted_typed_write_batch(&self) -> TrustedTypedWriteBatch<'_, C> {
        TrustedTypedWriteBatch::new(self)
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
