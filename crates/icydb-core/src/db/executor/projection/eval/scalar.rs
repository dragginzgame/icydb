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
        executor::projection::path::{resolve_slot_field_path, resolve_value_field_path},
    },
    error::InternalError,
    value::Value,
};
use std::{borrow::Cow, cell::RefCell};

///
/// ValueSlotReader
///
/// ValueSlotReader adapts owned slot-reader callbacks to the compiled
/// expression reader contract.
/// It exists for test and ordering paths that synthesize slot values on demand
/// instead of exposing a borrowed row slice.
///

struct ValueSlotReader<'reader> {
    read_slot: RefCell<&'reader mut dyn FnMut(usize) -> Option<Value>>,
    field_path_missing_is_null: bool,
}

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

impl CompiledExprValueReader for ValueSlotReader<'_> {
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
        (self.read_slot.borrow_mut())(slot).map(Cow::Owned)
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
        segments: &[String],
        _segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        let Some(root) = (self.read_slot.borrow_mut())(root_slot) else {
            return Ok(None);
        };
        let value = resolve_value_field_path(&root, segments)?.cloned();

        Ok(materialize_missing_field_path(
            value.map(Cow::Owned),
            self.field_path_missing_is_null,
        ))
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

struct ValueRefSlotReader<'reader, 'value> {
    read_slot: RefCell<&'reader mut dyn FnMut(usize) -> Option<&'value Value>>,
    field_path_missing_is_null: bool,
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
        segments: &[String],
        _segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        let Some(root) = (self.read_slot.borrow_mut())(root_slot) else {
            return Ok(None);
        };
        let value = resolve_value_field_path(root, segments)?;

        Ok(materialize_missing_field_path(
            value.map(Cow::Borrowed),
            self.field_path_missing_is_null,
        ))
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
        segments: &[String],
        _segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        let Some(root) = (self.read_slot.borrow_mut())(root_slot) else {
            return Ok(None);
        };
        match root {
            Cow::Borrowed(root) => {
                let value = resolve_value_field_path(root, segments)?;

                Ok(materialize_missing_field_path(
                    value.map(Cow::Borrowed),
                    self.field_path_missing_is_null,
                ))
            }
            Cow::Owned(root) => {
                let value = resolve_value_field_path(&root, segments)?.cloned();

                Ok(materialize_missing_field_path(
                    value.map(Cow::Owned),
                    self.field_path_missing_is_null,
                ))
            }
        }
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

struct CanonicalSlotExprReader<'reader, 'record> {
    slots: &'reader dyn CanonicalSlotReader,
    record_slot: RefCell<&'record mut dyn FnMut(usize)>,
    field_path_missing_is_null: bool,
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
        let value = resolve_slot_field_path(self.slots, root_slot, segment_bytes)
            .map_err(field_path_error)?;
        Ok(materialize_missing_field_path(
            value.map(Cow::Owned),
            self.field_path_missing_is_null,
        ))
    }
}

/// Evaluate one compiled expression through an owned value slot reader.
pub(in crate::db::executor) fn eval_compiled_expr_with_value_reader(
    expr: &CompiledExpr,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Value, ProjectionEvalError> {
    let reader = ValueSlotReader {
        read_slot: RefCell::new(read_slot),
        field_path_missing_is_null: true,
    };

    expr.evaluate(&reader).map(Cow::into_owned)
}

/// Evaluate one compiled expression through a borrowed value slot reader.
pub(in crate::db::executor) fn eval_compiled_expr_with_value_ref_reader<'a>(
    expr: &CompiledExpr,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<Value, ProjectionEvalError> {
    let reader = ValueRefSlotReader {
        read_slot: RefCell::new(read_slot),
        field_path_missing_is_null: true,
    };

    expr.evaluate(&reader).map(Cow::into_owned)
}

/// Evaluate one compiled expression through a canonical raw-row slot reader.
pub(in crate::db::executor) fn eval_compiled_expr_with_required_slot_reader_cow<'a>(
    expr: &'a CompiledExpr,
    slots: &'a dyn CanonicalSlotReader,
    record_slot: &'a mut dyn FnMut(usize),
) -> Result<Cow<'a, Value>, InternalError> {
    if let Some(slot) = expr.direct_octet_length_slot()
        && let Some(value) = eval_direct_scalar_octet_length(slots, record_slot, slot)?
    {
        return Ok(Cow::Owned(value));
    }

    let reader = CanonicalSlotExprReader {
        slots,
        record_slot: RefCell::new(record_slot),
        field_path_missing_is_null: true,
    };

    expr.evaluate(&reader)
        .map(Cow::into_owned)
        .map(Cow::Owned)
        .map_err(ProjectionEvalError::into_internal_error)
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
    let reader = CanonicalSlotExprReader {
        slots,
        record_slot: RefCell::new(&mut noop),
        field_path_missing_is_null: false,
    };
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
mod tests {
    use super::{
        CompiledExpr, CompiledExprValueReader, ValueCowSlotReader, ValueRefSlotReader,
        ValueSlotReader,
    };
    use crate::{db::query::plan::expr::BinaryOp, value::Value};
    use std::{borrow::Cow, cell::RefCell};

    struct SliceReader<'row> {
        row: &'row [Value],
    }

    impl CompiledExprValueReader for SliceReader<'_> {
        fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
            self.row.get(slot).map(Cow::Borrowed)
        }

        fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
            None
        }

        fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
            None
        }
    }

    #[test]
    fn compiled_expression_readers_agree_on_slot_arithmetic() {
        let expr = reader_dispatch_expr();
        for base in 0..4 {
            let row = [
                Value::Nat64(base + 1),
                Value::Nat64(2),
                Value::Nat64(base + 3),
                Value::Nat64(5),
            ];
            let expected = expr
                .evaluate(&SliceReader { row: &row })
                .expect("slice expression")
                .into_owned();
            let mut borrowed = |slot| row.get(slot);
            let mut cow = |slot| row.get(slot).map(Cow::Borrowed);
            let mut owned = |slot| row.get(slot).cloned();
            let readers: [&dyn CompiledExprValueReader; 3] = [
                &ValueRefSlotReader {
                    read_slot: RefCell::new(&mut borrowed),
                    field_path_missing_is_null: true,
                },
                &ValueCowSlotReader {
                    read_slot: RefCell::new(&mut cow),
                    field_path_missing_is_null: true,
                },
                &ValueSlotReader {
                    read_slot: RefCell::new(&mut owned),
                    field_path_missing_is_null: true,
                },
            ];
            for reader in readers {
                assert_eq!(
                    expr.evaluate(reader).expect("callback expression").as_ref(),
                    &expected
                );
            }
        }
    }

    fn reader_dispatch_expr() -> CompiledExpr {
        CompiledExpr::Binary {
            op: BinaryOp::Add,
            left: Box::new(CompiledExpr::Add {
                left_slot: 0,
                right_slot: 1,
            }),
            right: Box::new(CompiledExpr::Mul {
                left_slot: 2,
                right_slot: 3,
            }),
        }
    }
}
