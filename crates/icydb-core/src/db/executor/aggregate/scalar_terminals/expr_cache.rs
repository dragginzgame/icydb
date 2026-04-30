//! Module: executor::aggregate::scalar_terminals::expr_cache
//! Responsibility: interned scalar aggregate expression caching and evaluation.
//! Boundary: owns per-row cache buffers used by reducer execution.

use crate::{
    db::executor::{projection::ProjectionEvalError, terminal::KernelRow},
    db::query::plan::expr::{CompiledExpr, admit_true_only_boolean_value},
    error::InternalError,
    value::Value,
};

///
/// ScalarTerminalExprCache
///
/// ScalarTerminalExprCache owns the interned input/filter expression tables and
/// one row-local value cache per table.
/// Reducer execution resets these caches once per row, then asks this owner to
/// evaluate each shared expression at most once for that row.
///

pub(super) struct ScalarTerminalExprCache {
    input_exprs: Vec<CompiledExpr>,
    filter_exprs: Vec<CompiledExpr>,
    input_values: Vec<Option<Value>>,
    filter_values: Vec<Option<Value>>,
}

impl ScalarTerminalExprCache {
    pub(super) fn new(input_exprs: Vec<CompiledExpr>, filter_exprs: Vec<CompiledExpr>) -> Self {
        let input_values = Vec::with_capacity(input_exprs.len());
        let filter_values = Vec::with_capacity(filter_exprs.len());

        Self {
            input_exprs,
            filter_exprs,
            input_values,
            filter_values,
        }
    }

    pub(super) fn reset_for_row(&mut self) {
        reset_scalar_terminal_expr_values(&mut self.input_values, self.input_exprs.len());
        reset_scalar_terminal_expr_values(&mut self.filter_values, self.filter_exprs.len());
    }

    pub(super) fn input_value(
        &mut self,
        row: &KernelRow,
        index: usize,
        #[cfg(feature = "diagnostics")] evaluation_count: &mut u64,
    ) -> Result<&Value, InternalError> {
        cached_scalar_terminal_expr_value(
            self.input_exprs.as_slice(),
            row,
            &mut self.input_values,
            index,
            "input",
            #[cfg(feature = "diagnostics")]
            evaluation_count,
        )
    }

    pub(super) fn filter_matches(
        &mut self,
        filter_index: Option<usize>,
        row: &KernelRow,
        #[cfg(feature = "diagnostics")] filter_evaluation_count: &mut u64,
    ) -> Result<bool, InternalError> {
        let Some(filter_index) = filter_index else {
            return Ok(true);
        };
        let value = cached_scalar_terminal_expr_value(
            self.filter_exprs.as_slice(),
            row,
            &mut self.filter_values,
            filter_index,
            "filter",
            #[cfg(feature = "diagnostics")]
            filter_evaluation_count,
        )?;

        admit_true_only_boolean_value(value, |found| {
            InternalError::query_executor_invariant(format!(
                "scalar aggregate terminal filter expression produced non-boolean value: {found:?}",
            ))
        })
    }
}

pub(super) fn intern_scalar_terminal_expr(
    exprs: &mut Vec<CompiledExpr>,
    expr: CompiledExpr,
) -> usize {
    if let Some(index) = exprs.iter().position(|candidate| candidate == &expr) {
        return index;
    }

    let index = exprs.len();
    exprs.push(expr);

    index
}

fn reset_scalar_terminal_expr_values(values: &mut Vec<Option<Value>>, len: usize) {
    values.clear();
    values.resize_with(len, || None);
}

fn cached_scalar_terminal_expr_value<'a>(
    exprs: &[CompiledExpr],
    row: &KernelRow,
    values: &'a mut [Option<Value>],
    index: usize,
    label: &str,
    #[cfg(feature = "diagnostics")] evaluation_count: &mut u64,
) -> Result<&'a Value, InternalError> {
    let expr = exprs.get(index).ok_or_else(|| {
        InternalError::query_executor_invariant(format!(
            "scalar aggregate terminal {label} expression index missing from expression table",
        ))
    })?;
    let value = values.get_mut(index).ok_or_else(|| {
        InternalError::query_executor_invariant(format!(
            "scalar aggregate terminal {label} expression index missing from row buffer",
        ))
    })?;
    if value.is_none() {
        #[cfg(feature = "diagnostics")]
        {
            *evaluation_count = evaluation_count.saturating_add(1);
        }
        *value = Some(evaluate_scalar_terminal_expr(expr, row)?);
    }

    value.as_ref().ok_or_else(|| {
        InternalError::query_executor_invariant(format!(
            "scalar aggregate terminal {label} expression evaluation produced no row value",
        ))
    })
}

fn evaluate_scalar_terminal_expr(
    expr: &CompiledExpr,
    row: &KernelRow,
) -> Result<Value, InternalError> {
    let mut read_slot = |slot: usize| row.slot_ref(slot);

    crate::db::executor::projection::eval_compiled_expr_with_value_ref_reader(expr, &mut read_slot)
        .map_err(ProjectionEvalError::into_invalid_logical_plan_internal_error)
}
