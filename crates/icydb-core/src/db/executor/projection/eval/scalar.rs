//! Module: db::executor::projection::eval::scalar
//! Responsibility: executor-local readers for the unified compiled expression engine.
//! Does not own: expression semantics, expression tree evaluation, or planner lowering.
//! Boundary: adapts executor row/value sources to `CompiledExprValueReader`.

use super::contracts::{
    CompiledExpr, CompiledExprValueReader, ProjectionEvalError,
    collapse_true_only_boolean_admission,
};
#[cfg(any(test, feature = "sql"))]
use crate::{
    db::data::{ScalarSlotValueRef, ScalarValueRef},
    model::field::{LeafCodec, ScalarCodec},
};
use crate::{
    db::{
        data::{CanonicalSlotReader, decode_structural_value_storage_bytes},
        executor::projection::path::resolve_path_segments,
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
fn field_path_error(_field: &str, err: InternalError) -> ProjectionEvalError {
    ProjectionEvalError::FieldPathEvaluationFailed {
        class: err.class(),
        origin: err.origin(),
    }
}

// Convert low-level structural field decode failures into the persisted-row
// decode taxonomy expected by projection callers.
fn field_path_decode_error(field: &str, _err: impl Sized) -> ProjectionEvalError {
    field_path_error(
        field,
        InternalError::persisted_row_field_decode_corruption(field),
    )
}

// Walk a materialized `Value::Map` field path for retained-row and aggregate
// readers. Missing nested keys are distinct from malformed path roots so
// filter readers can reject missing paths before NULL-test semantics.
fn resolve_value_field_path<'value>(
    root: &'value Value,
    field: &str,
    segments: &[String],
) -> Result<Option<&'value Value>, ProjectionEvalError> {
    let mut current = root;
    for segment in segments {
        let entries = current.as_map().ok_or_else(|| {
            field_path_error(
                field,
                InternalError::persisted_row_field_decode_failed(
                    field,
                    "field-path traversal requires a map value",
                ),
            )
        })?;
        let Some((_, value)) = entries
            .iter()
            .find(|(key, _)| matches!(key, Value::Text(text) if text == segment))
        else {
            return Ok(None);
        };
        current = value;
    }

    Ok(Some(current))
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
        field: &str,
        segments: &[String],
        _segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        let Some(root) = (self.read_slot.borrow_mut())(root_slot) else {
            return Ok(None);
        };
        let value = resolve_value_field_path(&root, field, segments)?.cloned();

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
        field: &str,
        segments: &[String],
        _segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        let Some(root) = (self.read_slot.borrow_mut())(root_slot) else {
            return Ok(None);
        };
        let value = resolve_value_field_path(root, field, segments)?;

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
        field: &str,
        segments: &[String],
        _segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        let Some(root) = (self.read_slot.borrow_mut())(root_slot) else {
            return Ok(None);
        };
        match root {
            Cow::Borrowed(root) => {
                let value = resolve_value_field_path(root, field, segments)?;

                Ok(materialize_missing_field_path(
                    value.map(Cow::Borrowed),
                    self.field_path_missing_is_null,
                ))
            }
            Cow::Owned(root) => {
                let value = resolve_value_field_path(&root, field, segments)?.cloned();

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
        field: &str,
        _segments: &[String],
        segment_bytes: &[Box<[u8]>],
    ) -> Result<Option<Cow<'_, Value>>, ProjectionEvalError> {
        (self.record_slot.borrow_mut())(root_slot);
        let raw_bytes = self
            .slots
            .required_bytes(root_slot)
            .map_err(|err| field_path_error(field, err))?;
        let value_bytes = resolve_path_segments(raw_bytes, segment_bytes)
            .map_err(|err| field_path_decode_error(field, err))?;
        let Some(value_bytes) = value_bytes else {
            return Ok(materialize_missing_field_path(
                None,
                self.field_path_missing_is_null,
            ));
        };
        let value = decode_structural_value_storage_bytes(value_bytes)
            .map_err(|err| field_path_decode_error(field, err))?;

        Ok(Some(Cow::Owned(value)))
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
#[cfg(any(test, feature = "sql"))]
pub(in crate::db::executor) fn eval_compiled_expr_with_required_slot_reader_cow<'a>(
    expr: &'a CompiledExpr,
    slots: &'a dyn CanonicalSlotReader,
    record_slot: &'a mut dyn FnMut(usize),
) -> Result<Cow<'a, Value>, InternalError> {
    if let Some((slot, field)) = expr.direct_octet_length_slot()
        && let Some(value) = eval_direct_scalar_octet_length(slots, record_slot, slot, field)?
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
        .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)
}

// Evaluate direct `OCTET_LENGTH(field)` over scalar text/blob slots without
// materializing `Value::Text` or `Value::Blob`. Unsupported slot shapes return
// `None` so the normal compiled-expression evaluator preserves existing
// diagnostics for non-scalar and expression-derived inputs.
#[cfg(any(test, feature = "sql"))]
fn eval_direct_scalar_octet_length(
    slots: &dyn CanonicalSlotReader,
    record_slot: &mut dyn FnMut(usize),
    slot: usize,
    field: &str,
) -> Result<Option<Value>, InternalError> {
    let leaf_codec = slots.field_leaf_codec(slot).map_err(|_| {
        let _ = field;

        ProjectionEvalError::missing_slot_value(slot).into_invalid_logical_plan_internal_error()
    })?;
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
        Err(err) => return Err(err.into_invalid_logical_plan_internal_error()),
    };

    collapse_true_only_boolean_admission(value, |_found| {
        InternalError::query_invalid_logical_plan()
    })
}

/// Evaluate one compiled boolean filter over a borrowed-or-owned value row.
pub(in crate::db) fn eval_compiled_filter_expr_with_value_cow_reader<'a>(
    expr: &CompiledExpr,
    read_slot: &mut dyn FnMut(usize) -> Option<Cow<'a, Value>>,
    _missing_slot_context: &str,
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
                err => err.into_invalid_logical_plan_internal_error(),
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
    use std::{
        borrow::Cow,
        cell::RefCell,
        hint::black_box,
        time::{Duration, Instant},
    };

    const READER_DISPATCH_ROWS: usize = 512;
    const READER_DISPATCH_ITERATIONS: usize = 1_024;

    type ReaderDispatchRow = [Value; 4];

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

    // Run explicitly when assessing whether reader dispatch deserves
    // specialization. The report is informational; correctness stays limited to
    // proving every measured path evaluates the same compiled expression.
    #[test]
    #[ignore = "native microbenchmark: run explicitly with --ignored --nocapture"]
    fn compiled_expr_reader_dispatch_microbenchmark_report() {
        let rows = reader_dispatch_rows();
        let expr = reader_dispatch_expr();
        let expected = direct_slice_checksum(&expr, &rows);

        println!();
        println!("Compiled expression reader dispatch microbenchmark");
        println!(
            "rows={READER_DISPATCH_ROWS} iterations={READER_DISPATCH_ITERATIONS} expression=slot arithmetic"
        );
        println!();

        report_reader_dispatch_result(
            "direct slice reader",
            expected,
            measure_reader_dispatch(|| direct_slice_checksum(&expr, &rows)),
        );
        report_reader_dispatch_result(
            "borrowed callback reader",
            expected,
            measure_reader_dispatch(|| borrowed_callback_checksum(&expr, &rows)),
        );
        report_reader_dispatch_result(
            "cow callback reader",
            expected,
            measure_reader_dispatch(|| cow_callback_checksum(&expr, &rows)),
        );
        report_reader_dispatch_result(
            "owned callback reader",
            expected,
            measure_reader_dispatch(|| owned_callback_checksum(&expr, &rows)),
        );
    }

    fn reader_dispatch_rows() -> Vec<ReaderDispatchRow> {
        (0..READER_DISPATCH_ROWS)
            .map(|index| {
                let base = u64::try_from(index).expect("benchmark row index should fit u64");

                [
                    Value::Nat64(base + 1),
                    Value::Nat64(2),
                    Value::Nat64((base % 7) + 3),
                    Value::Nat64(5),
                ]
            })
            .collect()
    }

    fn reader_dispatch_expr() -> CompiledExpr {
        CompiledExpr::Binary {
            op: BinaryOp::Add,
            left: Box::new(CompiledExpr::Add {
                left_slot: 0,
                left_field: "a".to_string(),
                right_slot: 1,
                right_field: "b".to_string(),
            }),
            right: Box::new(CompiledExpr::Mul {
                left_slot: 2,
                left_field: "c".to_string(),
                right_slot: 3,
                right_field: "d".to_string(),
            }),
        }
    }

    fn measure_reader_dispatch(mut checksum: impl FnMut() -> usize) -> (Duration, usize) {
        let warm = black_box(checksum());
        assert!(warm > 0, "reader dispatch benchmark should exercise rows");

        let mut measured = 0usize;
        let started_at = Instant::now();
        for _ in 0..READER_DISPATCH_ITERATIONS {
            measured = measured.saturating_add(black_box(checksum()));
        }

        (started_at.elapsed(), measured)
    }

    fn report_reader_dispatch_result(
        label: &'static str,
        expected: usize,
        result: (Duration, usize),
    ) {
        let (elapsed, checksum) = result;
        let expected_total = expected.saturating_mul(READER_DISPATCH_ITERATIONS);

        assert_eq!(checksum, expected_total, "{label} checksum drifted");
        let iterations =
            u128::try_from(READER_DISPATCH_ITERATIONS).expect("iteration count should fit u128");
        println!(
            "{label:<28} total_ns={:<14} avg_ns_per_iteration={}",
            elapsed.as_nanos(),
            elapsed.as_nanos() / iterations,
        );
    }

    fn direct_slice_checksum(expr: &CompiledExpr, rows: &[ReaderDispatchRow]) -> usize {
        checksum_rows(rows, |row| {
            let reader = SliceReader {
                row: row.as_slice(),
            };

            eval_reader_checksum(
                expr,
                &reader,
                "direct slice reader expression should evaluate",
            )
        })
    }

    fn borrowed_callback_checksum(expr: &CompiledExpr, rows: &[ReaderDispatchRow]) -> usize {
        checksum_rows(rows, |row| {
            let mut read_slot = |slot| row.get(slot);
            let reader = ValueRefSlotReader {
                read_slot: RefCell::new(&mut read_slot),
                field_path_missing_is_null: true,
            };

            eval_reader_checksum(
                expr,
                &reader,
                "borrowed callback reader expression should evaluate",
            )
        })
    }

    fn cow_callback_checksum(expr: &CompiledExpr, rows: &[ReaderDispatchRow]) -> usize {
        checksum_rows(rows, |row| {
            let mut read_slot = |slot| row.get(slot).map(Cow::Borrowed);
            let reader = ValueCowSlotReader {
                read_slot: RefCell::new(&mut read_slot),
                field_path_missing_is_null: true,
            };

            eval_reader_checksum(
                expr,
                &reader,
                "cow callback reader expression should evaluate",
            )
        })
    }

    fn owned_callback_checksum(expr: &CompiledExpr, rows: &[ReaderDispatchRow]) -> usize {
        checksum_rows(rows, |row| {
            let mut read_slot = |slot| row.get(slot).cloned();
            let reader = ValueSlotReader {
                read_slot: RefCell::new(&mut read_slot),
                field_path_missing_is_null: true,
            };

            eval_reader_checksum(
                expr,
                &reader,
                "owned callback reader expression should evaluate",
            )
        })
    }

    fn checksum_rows(
        rows: &[ReaderDispatchRow],
        checksum_row: impl FnMut(&ReaderDispatchRow) -> usize,
    ) -> usize {
        rows.iter().map(checksum_row).sum()
    }

    fn eval_reader_checksum(
        expr: &CompiledExpr,
        reader: &dyn CompiledExprValueReader,
        context: &'static str,
    ) -> usize {
        let value = expr.evaluate(reader).expect(context);

        integer_checksum(value)
    }

    fn integer_checksum(value: Cow<'_, Value>) -> usize {
        match value.as_ref() {
            Value::Nat64(value) => {
                usize::try_from(*value).expect("benchmark value should fit usize")
            }
            Value::Decimal(value) => {
                assert_eq!(value.scale(), 0, "benchmark decimal should stay integral");
                usize::try_from(value.mantissa()).expect("benchmark value should fit usize")
            }
            found => panic!("reader dispatch expression returned {found:?}"),
        }
    }
}
