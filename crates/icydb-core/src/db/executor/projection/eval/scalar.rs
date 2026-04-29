//! Module: db::executor::projection::eval::scalar
//! Responsibility: compiled scalar-only projection expression evaluation on top of the shared scalar-expression seam.
//! Does not own: grouped projection execution, generic `Expr` evaluation, or planner validation.
//! Boundary: structural projection materialization calls into this file when a projection stays entirely on the scalar seam.

#[cfg(test)]
use crate::db::{
    data::SlotReader,
    scalar_expr::{
        eval_canonical_scalar_value_program, eval_scalar_value_program,
        scalar_expr_value_into_value,
    },
};
use crate::{
    db::{
        data::{
            BoundedValueStorageScalar, CanonicalSlotReader,
            decode_bounded_structural_value_storage_scalar_bytes,
            decode_structural_value_storage_bytes,
        },
        executor::projection::{
            eval::ProjectionEvalError,
            eval::operators,
            path::{resolve_and_decode, resolve_path_compiled},
        },
        query::plan::expr::{
            BinaryOp, Function, ProjectionFunctionEvalError, ScalarProjectionExpr,
            ScalarProjectionField, ScalarProjectionFieldPath, collapse_true_only_boolean_admission,
            eval_projection_function_call_checked,
        },
    },
    error::InternalError,
    value::Value,
};
#[cfg(any(test, feature = "sql"))]
use std::array;
#[cfg(any(test, feature = "sql"))]
use std::borrow::Cow;
#[cfg(any(test, feature = "sql"))]
use std::cell::{Cell, RefCell};

/// Evaluate one compiled scalar projection expression against one slot reader.
#[cfg(test)]
pub(in crate::db::executor) fn eval_scalar_projection_expr(
    expr: &ScalarProjectionExpr,
    slots: &mut dyn SlotReader,
) -> Result<Value, InternalError> {
    eval_scalar_projection_expr_core(
        expr,
        &mut |field| {
            // Scalar fields keep the fast scalar-expression seam in tests.
            // Non-scalar projected fields still need to compile for
            // planner/executor contract tests, so fall back to slot-contract
            // decoding there.
            let value = if let Some(program) = field.program() {
                let Some(value) = eval_scalar_value_program(program, slots)? else {
                    return Err(
                        missing_field_value(field).into_invalid_logical_plan_internal_error()
                    );
                };

                scalar_expr_value_into_value(value)
            } else {
                let Some(value) = slots.get_value(field.slot())? else {
                    return Err(
                        missing_field_value(field).into_invalid_logical_plan_internal_error()
                    );
                };

                value
            };

            Ok(Cow::Owned(value))
        },
        &mut |path| {
            Err(field_path_projection_error(path).into_invalid_logical_plan_internal_error())
        },
        &mut ProjectionEvalError::into_invalid_logical_plan_internal_error,
    )
    .map(Cow::into_owned)
}

/// Evaluate one compiled scalar projection expression against one canonical
/// slot reader where declared slots must already exist.
#[cfg(test)]
pub(in crate::db::executor) fn eval_canonical_scalar_projection_expr(
    expr: &ScalarProjectionExpr,
    slots: &dyn CanonicalSlotReader,
) -> Result<Value, InternalError> {
    eval_scalar_projection_expr_core(
        expr,
        &mut |field| {
            let value = if let Some(program) = field.program() {
                scalar_expr_value_into_value(eval_canonical_scalar_value_program(program, slots)?)
            } else {
                slots.required_value_by_contract(field.slot())?
            };

            Ok(Cow::Owned(value))
        },
        &mut |path| {
            Err(field_path_projection_error(path).into_invalid_logical_plan_internal_error())
        },
        &mut ProjectionEvalError::into_invalid_logical_plan_internal_error,
    )
    .map(Cow::into_owned)
}

/// Evaluate one compiled scalar projection expression through a canonical slot reader.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_canonical_scalar_projection_expr_with_required_slot_reader_cow<'a>(
    expr: &'a ScalarProjectionExpr,
    slots: &'a dyn CanonicalSlotReader,
    record_slot: &mut dyn FnMut(usize),
) -> Result<Cow<'a, Value>, InternalError> {
    let record_slot = RefCell::new(record_slot);

    eval_scalar_projection_expr_core(
        expr,
        &mut |field| {
            (record_slot.borrow_mut())(field.slot());

            slots.required_value_by_contract_cow(field.slot())
        },
        &mut |path| {
            (record_slot.borrow_mut())(path.root_slot());
            eval_field_path_projection(path, slots).map(Cow::Owned)
        },
        &mut ProjectionEvalError::into_invalid_logical_plan_internal_error,
    )
}

/// Evaluate one compiled scalar filter expression through a canonical slot
/// reader while treating missing nested FieldPath leaves as row rejection.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_canonical_scalar_filter_expr_with_required_slot_reader_cow<'a>(
    expr: &'a ScalarProjectionExpr,
    slots: &'a dyn CanonicalSlotReader,
) -> Result<Option<Cow<'a, Value>>, InternalError> {
    let missing_field_path = Cell::new(false);
    let value = eval_scalar_projection_expr_core(
        expr,
        &mut |field| slots.required_value_by_contract_cow(field.slot()),
        &mut |path| {
            if let Some(value) = eval_field_path_filter_fallback(path, slots)? {
                return Ok(Cow::Owned(value));
            }

            missing_field_path.set(true);

            Ok(Cow::Owned(Value::Null))
        },
        &mut ProjectionEvalError::into_invalid_logical_plan_internal_error,
    )?;

    if missing_field_path.get() {
        return Ok(None);
    }

    Ok(Some(value))
}

// Attempt the narrow FieldPath/literal predicate fast path before scalar
// filtering materializes the resolved bytes into a full runtime `Value`.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn try_eval_field_path_literal_filter_expr(
    expr: &ScalarProjectionExpr,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<bool>, InternalError> {
    let ScalarProjectionExpr::Binary { op, left, right } = expr else {
        return Ok(None);
    };

    match (left.as_ref(), right.as_ref()) {
        (ScalarProjectionExpr::FieldPath(path), ScalarProjectionExpr::Literal(literal))
        | (ScalarProjectionExpr::Literal(literal), ScalarProjectionExpr::FieldPath(path)) => {
            eval_field_path_literal_compare(*op, path, literal, slots)
        }
        _ => Ok(None),
    }
}

/// Evaluate one compiled scalar projection expression through one canonical
/// reader that can borrow decoded slot values from the structural row cache.
/// This keeps repeated field references on the retained-slot structural path from
/// cloning cached `Value`s before an operator actually needs ownership.
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn eval_canonical_scalar_projection_expr_with_required_value_reader_cow<'a>(
    expr: &'a ScalarProjectionExpr,
    read_slot: &mut dyn FnMut(usize) -> Result<Cow<'a, Value>, InternalError>,
) -> Result<Cow<'a, Value>, InternalError> {
    let mut cached_read_slot = CowSlotEvaluationCache::new(read_slot);

    #[cfg(not(test))]
    {
        eval_scalar_projection_expr_core(
            expr,
            &mut |field| cached_read_slot.read(field.slot()),
            &mut |path| {
                Err(field_path_projection_error(path).into_invalid_logical_plan_internal_error())
            },
            &mut ProjectionEvalError::into_invalid_logical_plan_internal_error,
        )
    }

    #[cfg(test)]
    eval_scalar_projection_expr_core(
        expr,
        &mut |field| cached_read_slot.read(field.slot()),
        &mut |path| {
            Err(field_path_projection_error(path).into_invalid_logical_plan_internal_error())
        },
        &mut ProjectionEvalError::into_invalid_logical_plan_internal_error,
    )
}

/// Evaluate one compiled scalar projection expression through one pure value
/// reader that resolves slots directly into runtime `Value`s.
pub(in crate::db::executor) fn eval_scalar_projection_expr_with_value_reader(
    expr: &ScalarProjectionExpr,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> Result<Value, ProjectionEvalError> {
    eval_scalar_projection_expr_core(
        expr,
        &mut |field| {
            let Some(value) = read_slot(field.slot()) else {
                return Err(missing_field_value(field));
            };

            Ok(Cow::Owned(value))
        },
        &mut |path| Err(field_path_projection_error(path)),
        &mut |err| err,
    )
    .map(Cow::into_owned)
}

/// Evaluate one compiled scalar projection expression through one borrowed
/// value reader and materialize the final runtime `Value`.
pub(in crate::db::executor) fn eval_scalar_projection_expr_with_value_ref_reader<'a>(
    expr: &ScalarProjectionExpr,
    read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
) -> Result<Value, ProjectionEvalError> {
    let mut entries: [Option<(usize, Option<&'a Value>)>; 8] = [None; 8];
    let mut entry_count = 0usize;
    let mut cached_read_slot = |slot| {
        for entry in entries.iter().take(entry_count).flatten() {
            if entry.0 == slot {
                return entry.1;
            }
        }

        let value = read_slot(slot);
        if entry_count < entries.len() {
            entries[entry_count] = Some((slot, value));
            entry_count += 1;
        }

        value
    };
    let value = eval_scalar_projection_expr_core(
        expr,
        &mut |field| {
            let Some(value) = cached_read_slot(field.slot()) else {
                return Err(missing_field_value(field));
            };

            Ok(Cow::Borrowed(value))
        },
        &mut |path| Err(field_path_projection_error(path)),
        &mut |err| err,
    );

    value.map(Cow::into_owned)
}

fn eval_scalar_projection_expr_core<'a, E>(
    expr: &'a ScalarProjectionExpr,
    eval_field: &mut dyn FnMut(&ScalarProjectionField) -> Result<Cow<'a, Value>, E>,
    eval_field_path: &mut dyn FnMut(&ScalarProjectionFieldPath) -> Result<Cow<'a, Value>, E>,
    map_projection_error: &mut dyn FnMut(ProjectionEvalError) -> E,
) -> Result<Cow<'a, Value>, E> {
    match expr {
        ScalarProjectionExpr::Field(field) => eval_field(field),
        ScalarProjectionExpr::FieldPath(path) => eval_field_path(path),
        ScalarProjectionExpr::Literal(value) => Ok(Cow::Borrowed(value)),
        ScalarProjectionExpr::FunctionCall { function, args } => {
            let value = eval_function_call_expr(
                *function,
                args.as_slice(),
                eval_field,
                eval_field_path,
                map_projection_error,
            )?;

            Ok(Cow::Owned(value))
        }
        ScalarProjectionExpr::Unary { op, expr } => {
            let operand = eval_scalar_projection_expr_core(
                expr,
                eval_field,
                eval_field_path,
                map_projection_error,
            )?;
            operators::eval_unary_expr(*op, operand.as_ref())
                .map(Cow::Owned)
                .map_err(map_projection_error)
        }
        ScalarProjectionExpr::Case {
            when_then_arms,
            else_expr,
        } => {
            for arm in when_then_arms {
                let condition = eval_scalar_projection_expr_core(
                    arm.condition(),
                    eval_field,
                    eval_field_path,
                    map_projection_error,
                )?;
                if collapse_true_only_boolean_admission(condition.into_owned(), |found| {
                    map_projection_error(ProjectionEvalError::InvalidCaseCondition { found })
                })? {
                    return eval_scalar_projection_expr_core(
                        arm.result(),
                        eval_field,
                        eval_field_path,
                        map_projection_error,
                    );
                }
            }

            eval_scalar_projection_expr_core(
                else_expr.as_ref(),
                eval_field,
                eval_field_path,
                map_projection_error,
            )
        }
        ScalarProjectionExpr::Binary { op, left, right } => {
            let left = eval_scalar_projection_expr_core(
                left,
                eval_field,
                eval_field_path,
                map_projection_error,
            )?;
            let right = eval_scalar_projection_expr_core(
                right,
                eval_field,
                eval_field_path,
                map_projection_error,
            )?;

            operators::eval_binary_expr(*op, left.as_ref(), right.as_ref())
                .map(Cow::Owned)
                .map_err(map_projection_error)
        }
    }
}

///
/// CowSlotEvaluationCache
///
/// CowSlotEvaluationCache memoizes successful row-local slot reads for the
/// canonical scalar projection evaluator.
/// It keeps common repeated field references on stack storage and only clones
/// cached owned values when the caller's slot reader cannot provide a borrowed
/// value.
///

#[cfg(any(test, feature = "sql"))]
struct CowSlotEvaluationCache<'reader, 'value> {
    read_slot: &'reader mut dyn FnMut(usize) -> Result<Cow<'value, Value>, InternalError>,
    entries: [Option<(usize, Cow<'value, Value>)>; 8],
    len: usize,
}

#[cfg(any(test, feature = "sql"))]
impl<'reader, 'value> CowSlotEvaluationCache<'reader, 'value> {
    // Build one empty stack cache around the caller-owned COW slot reader.
    fn new(
        read_slot: &'reader mut dyn FnMut(usize) -> Result<Cow<'value, Value>, InternalError>,
    ) -> Self {
        Self {
            read_slot,
            entries: array::from_fn(|_| None),
            len: 0,
        }
    }

    // Read one slot from cache when possible. Successful reads are cached,
    // while error reads are returned immediately so error construction and
    // ordering remain exactly caller-owned.
    fn read(&mut self, slot: usize) -> Result<Cow<'value, Value>, InternalError> {
        for entry in self.entries.iter().take(self.len).flatten() {
            if entry.0 == slot {
                return Ok(match &entry.1 {
                    Cow::Borrowed(value) => Cow::Borrowed(value),
                    Cow::Owned(value) => Cow::Owned(value.clone()),
                });
            }
        }

        let value = (self.read_slot)(slot)?;
        if self.len < self.entries.len() {
            self.entries[self.len] = Some((slot, clone_cow_value(&value)));
            self.len += 1;
        }

        Ok(value)
    }
}

#[cfg(any(test, feature = "sql"))]
fn clone_cow_value<'value>(value: &Cow<'value, Value>) -> Cow<'value, Value> {
    match value {
        Cow::Borrowed(value) => Cow::Borrowed(value),
        Cow::Owned(value) => Cow::Owned(value.clone()),
    }
}

// Evaluate one scalar function call without first staging borrowed argument
// COWs into a temporary vector. Most scalar functions have arity 0, 1, or 2,
// so those paths stay on stack arrays and only larger calls allocate.
fn eval_function_call_expr<'a, E>(
    function: Function,
    args: &'a [ScalarProjectionExpr],
    eval_field: &mut dyn FnMut(&ScalarProjectionField) -> Result<Cow<'a, Value>, E>,
    eval_field_path: &mut dyn FnMut(&ScalarProjectionFieldPath) -> Result<Cow<'a, Value>, E>,
    map_projection_error: &mut dyn FnMut(ProjectionEvalError) -> E,
) -> Result<Value, E> {
    match args {
        [] => eval_function_call_checked(function, &[], map_projection_error),
        [arg] => {
            let arg = eval_scalar_projection_expr_core(
                arg,
                eval_field,
                eval_field_path,
                map_projection_error,
            )?
            .into_owned();
            let args = [arg];

            eval_function_call_checked(function, &args, map_projection_error)
        }
        [left, right] => {
            let left = eval_scalar_projection_expr_core(
                left,
                eval_field,
                eval_field_path,
                map_projection_error,
            )?
            .into_owned();
            let right = eval_scalar_projection_expr_core(
                right,
                eval_field,
                eval_field_path,
                map_projection_error,
            )?
            .into_owned();
            let args = [left, right];

            eval_function_call_checked(function, &args, map_projection_error)
        }
        args => {
            let mut evaluated_args = Vec::with_capacity(args.len());
            for arg in args {
                evaluated_args.push(
                    eval_scalar_projection_expr_core(
                        arg,
                        eval_field,
                        eval_field_path,
                        map_projection_error,
                    )?
                    .into_owned(),
                );
            }

            eval_function_call_checked(function, evaluated_args.as_slice(), map_projection_error)
        }
    }
}

// Normalize scalar function-call errors through the existing projection error
// taxonomy while allowing callers to choose stack or heap argument storage.
fn eval_function_call_checked<E>(
    function: Function,
    args: &[Value],
    map_projection_error: &mut dyn FnMut(ProjectionEvalError) -> E,
) -> Result<Value, E> {
    eval_projection_function_call_checked(function, args).map_err(|err| match err {
        ProjectionFunctionEvalError::Numeric(err) => {
            map_projection_error(ProjectionEvalError::Numeric(err))
        }
        ProjectionFunctionEvalError::Query(err) => {
            map_projection_error(ProjectionEvalError::InvalidFunctionCall {
                function: function.projection_eval_name().to_string(),
                message: err.to_string(),
            })
        }
    })
}

// Build one stable missing-field diagnostic from one compiled scalar field.
fn missing_field_value(field: &ScalarProjectionField) -> ProjectionEvalError {
    ProjectionEvalError::MissingFieldValue {
        field: field.field().to_string(),
        index: field.slot(),
    }
}

fn field_path_projection_error(path: &ScalarProjectionFieldPath) -> ProjectionEvalError {
    ProjectionEvalError::MissingFieldValue {
        field: render_field_path_label(path),
        index: path.root_slot(),
    }
}

#[cfg(any(test, feature = "sql"))]
fn eval_field_path_projection(
    path: &ScalarProjectionFieldPath,
    slots: &dyn CanonicalSlotReader,
) -> Result<Value, InternalError> {
    Ok(eval_field_path_projection_optional(path, slots)?.unwrap_or(Value::Null))
}

#[cfg(any(test, feature = "sql"))]
fn eval_field_path_projection_optional(
    path: &ScalarProjectionFieldPath,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<Value>, InternalError> {
    let raw_bytes = slots.required_bytes(path.root_slot())?;

    resolve_and_decode(raw_bytes, path.compiled_path())
        .map_err(|err| InternalError::persisted_row_field_decode_failed(path.root(), err))
}

#[cfg(any(test, feature = "sql"))]
fn eval_field_path_filter_fallback(
    path: &ScalarProjectionFieldPath,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<Value>, InternalError> {
    let raw_bytes = slots.required_bytes(path.root_slot())?;
    let Some(value_bytes) = resolve_path_compiled(raw_bytes, path.compiled_path())
        .map_err(|err| InternalError::persisted_row_field_decode_failed(path.root(), err))?
    else {
        return Ok(None);
    };

    decode_structural_value_storage_bytes(value_bytes)
        .map(Some)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(path.root(), err))
}

// Compare one resolved FieldPath value against one scalar literal by decoding
// the bounded value-storage slice directly. Returning `None` deliberately
// falls back to full `Value` materialization for unsupported literals,
// operators, or cross-type numeric coercions.
#[cfg(any(test, feature = "sql"))]
fn eval_field_path_literal_compare(
    op: BinaryOp,
    path: &ScalarProjectionFieldPath,
    literal: &Value,
    slots: &dyn CanonicalSlotReader,
) -> Result<Option<bool>, InternalError> {
    if !matches!(op, BinaryOp::Eq | BinaryOp::Ne) {
        return Ok(None);
    }

    let raw_bytes = slots.required_bytes(path.root_slot())?;
    let Some(value_bytes) = resolve_path_compiled(raw_bytes, path.compiled_path())
        .map_err(|err| InternalError::persisted_row_field_decode_failed(path.root(), err))?
    else {
        return Ok(Some(false));
    };

    let scalar = decode_bounded_structural_value_storage_scalar_bytes(value_bytes)
        .map_err(|err| InternalError::persisted_row_field_decode_failed(path.root(), err))?;
    if matches!(scalar, BoundedValueStorageScalar::Null) {
        return Ok(Some(false));
    }

    let Some(equal) = eval_bounded_scalar_literal_eq(scalar, literal) else {
        return Ok(None);
    };

    Ok(Some(match op {
        BinaryOp::Eq => equal,
        BinaryOp::Ne => !equal,
        BinaryOp::Or
        | BinaryOp::And
        | BinaryOp::Lt
        | BinaryOp::Lte
        | BinaryOp::Gt
        | BinaryOp::Gte
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div => unreachable!("field path literal fast path admits equality only"),
    }))
}

#[cfg(any(test, feature = "sql"))]
fn eval_bounded_scalar_literal_eq(
    scalar: BoundedValueStorageScalar<'_>,
    literal: &Value,
) -> Option<bool> {
    match (scalar, literal) {
        (BoundedValueStorageScalar::Bool(found), Value::Bool(value)) => Some(found == *value),
        (BoundedValueStorageScalar::I64(found), Value::Int(value)) => Some(found == *value),
        (BoundedValueStorageScalar::U64(found), Value::Uint(value)) => Some(found == *value),
        (BoundedValueStorageScalar::Text(found), Value::Text(value)) => {
            Some(found == value.as_str())
        }
        _ => None,
    }
}

fn render_field_path_label(path: &ScalarProjectionFieldPath) -> String {
    let mut label = path.root().to_string();
    for segment in path.segments() {
        label.push('.');
        label.push_str(segment);
    }

    label
}
