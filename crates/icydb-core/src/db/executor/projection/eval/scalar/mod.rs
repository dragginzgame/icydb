//! Module: db::executor::projection::eval::scalar
//! Responsibility: executor-local readers for the unified compiled expression engine.
//! Does not own: expression semantics, expression tree evaluation, or planner lowering.
//! Boundary: adapts executor row/value sources to `CompiledExprValueReader`.

use super::contracts::{
    CompiledExpr, CompiledExprValueReader, ProjectionEvalError,
    collapse_true_only_boolean_admission,
};
use crate::{
    db::data::{ScalarSlotValueRef, ScalarValueRef},
    db::schema::{LeafCodec, ScalarCodec},
};
use crate::{
    db::{
        data::CanonicalSlotReader,
        executor::projection::path::{PathCache, PathError, PathSource, PathValue},
    },
    error::InternalError,
    value::Value,
};
use std::{borrow::Cow, cell::RefCell};

// Preserve reader-owned error taxonomy when the unified expression evaluator
// reports failures back through projection/query execution boundaries.
fn reader_error(err: InternalError) -> ProjectionEvalError {
    ProjectionEvalError::ReaderFailed {
        class: err.class(),
        origin: err.origin(),
    }
}

// Preserve persisted-row decode classifications for nested path traversal
// while keeping storage decoding outside the compiled expression module.
fn field_path_error(err: InternalError) -> ProjectionEvalError {
    ProjectionEvalError::FieldPathEvaluationFailed {
        class: err.class(),
        origin: err.origin(),
    }
}

// Apply the reader-specific missing-path policy after path traversal. Scalar
// projections materialize NULL for missing nested values; filter readers use a
// hard missing-path signal so `IS NULL` cannot admit absent subfields.
fn materialize_missing_field_path(
    value: Option<Cow<'_, Value>>,
    missing_is_null: bool,
) -> Option<Cow<'_, Value>> {
    match (value, missing_is_null) {
        (Some(value), _) => Some(value),
        (None, true) => Some(Cow::Owned(Value::Null)),
        (None, false) => None,
    }
}

fn materialize_path(value: PathValue<'_>, missing_is_null: bool) -> Option<Cow<'_, Value>> {
    match value {
        PathValue::MissingRoot => None,
        PathValue::Leaf(value) => materialize_missing_field_path(value, missing_is_null),
    }
}

///
/// ValueRefSlotReader
///
/// ValueRefSlotReader adapts borrowed slot-reader callbacks to the compiled
/// expression reader contract.
/// Retained-row, scalar aggregate, and structural ordering paths use it to keep
/// hot row-local reads borrowed while still sharing `CompiledExpr::evaluate`.
///

pub(in crate::db::executor::projection) struct ValueRefSlotReader<'reader, 'value> {
    read_slot: RefCell<&'reader mut dyn FnMut(usize) -> Option<&'value Value>>,
    field_path_missing_is_null: bool,
    paths: RefCell<PathCache<'reader, 'value>>,
}

impl<'reader, 'value> ValueRefSlotReader<'reader, 'value> {
    /// Keep path selection across one row's authored output expressions.
    pub(in crate::db::executor::projection) fn new(
        expressions: &'reader [CompiledExpr],
        read_slot: &'reader mut dyn FnMut(usize) -> Option<&'value Value>,
    ) -> Self {
        Self {
            read_slot: RefCell::new(read_slot),
            field_path_missing_is_null: true,
            paths: RefCell::new(PathCache::new(expressions)),
        }
    }

    /// Evaluate through the existing compiled-expression owner.
    pub(in crate::db::executor::projection) fn evaluate(
        &self,
        expr: &CompiledExpr,
    ) -> Result<Value, ProjectionEvalError> {
        expr.evaluate(self).map(Cow::into_owned)
    }
}

impl CompiledExprValueReader for ValueRefSlotReader<'_, '_> {
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
        (self.read_slot.borrow_mut())(slot).map(Cow::Borrowed)
    }

    fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_field_path(
        &self,
        root_slot: usize,
        _segments: &[String],
        segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        let value = self
            .paths
            .try_borrow_mut()
            .map_err(|_| field_path_error(InternalError::query_executor_invariant()))?
            .resolve(root_slot, segment_bytes, || {
                let root = (self
                    .read_slot
                    .try_borrow_mut()
                    .map_err(|_| InternalError::query_executor_invariant())?)(
                    root_slot
                );
                Ok(root.map_or(PathSource::Missing, |root| {
                    PathSource::Value(Cow::Borrowed(root))
                }))
            })
            .map_err(PathError::into_projection_error)?;
        Ok(materialize_path(value, self.field_path_missing_is_null))
    }
}

///
/// ValueCowSlotReader
///
/// ValueCowSlotReader adapts mixed borrowed/owned slot readers to the compiled
/// expression reader contract for structural filter paths.
/// It keeps the execution context in the reader while `CompiledExpr` remains
/// the only expression engine.
///

struct ValueCowSlotReader<'reader, 'value> {
    read_slot: RefCell<&'reader mut dyn FnMut(usize) -> Option<Cow<'value, Value>>>,
    field_path_missing_is_null: bool,
    paths: RefCell<PathCache<'reader, 'value>>,
}

impl CompiledExprValueReader for ValueCowSlotReader<'_, '_> {
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
        (self.read_slot.borrow_mut())(slot).map(|value| match value {
            Cow::Borrowed(value) => Cow::Borrowed(value),
            Cow::Owned(value) => Cow::Owned(value),
        })
    }

    fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_field_path(
        &self,
        root_slot: usize,
        _segments: &[String],
        segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        let value = self
            .paths
            .try_borrow_mut()
            .map_err(|_| field_path_error(InternalError::query_executor_invariant()))?
            .resolve(root_slot, segment_bytes, || {
                let root = (self
                    .read_slot
                    .try_borrow_mut()
                    .map_err(|_| InternalError::query_executor_invariant())?)(
                    root_slot
                );
                Ok(root.map_or(PathSource::Missing, PathSource::Value))
            })
            .map_err(PathError::into_projection_error)?;
        Ok(materialize_path(value, self.field_path_missing_is_null))
    }
}

///
/// CanonicalSlotExprReader
///
/// CanonicalSlotExprReader adapts raw structural rows to the compiled
/// expression reader contract.
/// It owns field-path decoding because nested persisted bytes are an executor
/// row-access detail, while `CompiledExpr` owns only the expression dispatch.
///

pub(in crate::db::executor::projection) struct CanonicalSlotExprReader<'reader, 'record> {
    slots: &'reader dyn CanonicalSlotReader,
    record_slot: RefCell<&'record mut dyn FnMut(usize)>,
    field_path_missing_is_null: bool,
    paths: RefCell<PathCache<'reader, 'reader>>,
}

impl<'reader, 'record> CanonicalSlotExprReader<'reader, 'record> {
    /// Retain selection across expressions while the same raw row is borrowed.
    pub(in crate::db::executor::projection) fn new(
        expressions: &'reader [CompiledExpr],
        slots: &'reader dyn CanonicalSlotReader,
        record_slot: &'record mut dyn FnMut(usize),
        field_path_missing_is_null: bool,
    ) -> Self {
        Self {
            slots,
            record_slot: RefCell::new(record_slot),
            field_path_missing_is_null,
            paths: RefCell::new(PathCache::new(expressions)),
        }
    }

    /// Preserve scalar byte-length specialization before general evaluation.
    pub(in crate::db::executor::projection) fn evaluate_projection(
        &self,
        expr: &CompiledExpr,
    ) -> Result<Value, InternalError> {
        if let Some(slot) = expr.direct_octet_length_slot()
            && let Some(value) = eval_direct_scalar_octet_length(
                self.slots,
                &mut **self
                    .record_slot
                    .try_borrow_mut()
                    .map_err(|_| InternalError::query_executor_invariant())?,
                slot,
            )?
        {
            return Ok(value);
        }
        expr.evaluate(self)
            .map(Cow::into_owned)
            .map_err(ProjectionEvalError::into_internal_error)
    }
}

impl CompiledExprValueReader for CanonicalSlotExprReader<'_, '_> {
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
        (self.record_slot.borrow_mut())(slot);
        self.slots.required_value_by_contract_cow(slot).ok()
    }

    fn read_slot_checked(
        &self,
        slot: usize,
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        (self.record_slot.borrow_mut())(slot);
        self.slots
            .required_value_by_contract_cow(slot)
            .map(Some)
            .map_err(reader_error)
    }

    fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_field_path(
        &self,
        root_slot: usize,
        _segments: &[String],
        segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        (self.record_slot.borrow_mut())(root_slot);
        let value = self
            .paths
            .try_borrow_mut()
            .map_err(|_| field_path_error(InternalError::query_executor_invariant()))?
            .resolve(root_slot, segment_bytes, || {
                if self.slots.get_bytes(root_slot).is_some() {
                    Ok(PathSource::Raw(self.slots.required_bytes(root_slot)?))
                } else {
                    self.slots
                        .required_value_by_contract_cow(root_slot)
                        .map(PathSource::Value)
                }
            })
            .map_err(PathError::into_projection_error)?;
        Ok(materialize_path(value, self.field_path_missing_is_null))
    }
}

/// Evaluate an expression from borrowed or synthesized slots, owning only its result.
pub(in crate::db::executor) fn eval_compiled_expr_with_value_cow_reader<'a>(
    expr: &CompiledExpr,
    read_slot: &mut dyn FnMut(usize) -> Option<Cow<'a, Value>>,
) -> Result<Value, ProjectionEvalError> {
    let reader = ValueCowSlotReader {
        read_slot: RefCell::new(read_slot),
        field_path_missing_is_null: true,
        paths: RefCell::new(PathCache::new(std::slice::from_ref(expr))),
    };

    expr.evaluate(&reader).map(Cow::into_owned)
}

/// Evaluate one compiled expression through a borrowed value slot reader.
pub(in crate::db::executor) fn eval_compiled_expr_with_value_ref_reader<'a>(
    expr: &CompiledExpr,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<Value, ProjectionEvalError> {
    ValueRefSlotReader::new(std::slice::from_ref(expr), read_slot).evaluate(expr)
}

// Evaluate direct `OCTET_LENGTH(field)` over scalar text/blob slots without
// materializing `Value::Text` or `Value::Blob`. Unsupported slot shapes return
// `None` so the normal compiled-expression evaluator preserves existing
// diagnostics for non-scalar and expression-derived inputs.
fn eval_direct_scalar_octet_length(
    slots: &dyn CanonicalSlotReader,
    record_slot: &mut dyn FnMut(usize),
    slot: usize,
) -> Result<Option<Value>, InternalError> {
    let leaf_codec = slots
        .field_leaf_codec(slot)
        .map_err(|_| ProjectionEvalError::missing_slot_value(slot).into_internal_error())?;
    if !matches!(
        leaf_codec,
        LeafCodec::Scalar(ScalarCodec::Blob | ScalarCodec::Text)
    ) {
        return Ok(None);
    }

    record_slot(slot);
    let value = match slots.required_scalar(slot)? {
        ScalarSlotValueRef::Null => Value::Null,
        ScalarSlotValueRef::Value(ScalarValueRef::Blob(bytes)) => {
            Value::Nat64(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Text(text)) => {
            Value::Nat64(u64::try_from(text.len()).unwrap_or(u64::MAX))
        }
        ScalarSlotValueRef::Value(_) => return Ok(None),
    };

    Ok(Some(value))
}

/// Evaluate one compiled boolean filter over a canonical structural row.
pub(in crate::db) fn eval_compiled_filter_expr_with_required_slot_reader(
    expr: &CompiledExpr,
    slots: &dyn CanonicalSlotReader,
) -> Result<bool, InternalError> {
    let mut noop = |_| {};
    let reader = CanonicalSlotExprReader::new(std::slice::from_ref(expr), slots, &mut noop, false);
    let value = match expr.evaluate(&reader) {
        Ok(value) => value.into_owned(),
        Err(ProjectionEvalError::MissingFieldPathValue { .. }) => return Ok(false),
        Err(err) => return Err(err.into_internal_error()),
    };

    collapse_true_only_boolean_admission(value, |_found| {
        InternalError::query_invalid_logical_plan()
    })
}

/// Evaluate one compiled boolean filter over a borrowed-or-owned value row.
pub(in crate::db) fn eval_compiled_filter_expr_with_value_cow_reader<'a>(
    expr: &CompiledExpr,
    read_slot: &mut dyn FnMut(usize) -> Option<Cow<'a, Value>>,
) -> Result<bool, InternalError> {
    let reader = ValueCowSlotReader {
        read_slot: RefCell::new(read_slot),
        field_path_missing_is_null: false,
        paths: RefCell::new(PathCache::new(std::slice::from_ref(expr))),
    };
    let value = match expr.evaluate(&reader) {
        Ok(value) => value.into_owned(),
        Err(ProjectionEvalError::MissingFieldPathValue { .. }) => return Ok(false),
        Err(err) => {
            return Err(match err {
                ProjectionEvalError::MissingFieldValue { .. } => {
                    InternalError::query_invalid_logical_plan()
                }
                err => err.into_internal_error(),
            });
        }
    };

    collapse_true_only_boolean_admission(value, |_found| {
        InternalError::query_invalid_logical_plan()
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(in crate::db::executor) use helper_tests::eval_compiled_expr_with_required_slot_reader_cow;
