//! Module: db::executor::pipeline::operators::post_access::coordinator::runtime::guard_filter
//! Defines guard-filter helpers used by post-access coordinator runtime.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        cursor::CursorBoundary,
        executor::pipeline::operators::post_access::coordinator::PostAccessPlan,
        executor::{
            OrderReadableRow, projection::eval_scalar_filter_expr_with_required_value_reader_cow,
        },
        query::plan::EffectiveRuntimeFilterProgram,
    },
    error::InternalError,
};

impl<K> PostAccessPlan<'_, K> {
    // Enforce load/delete cursor compatibility before execution phases.
    pub(in crate::db::executor::pipeline::operators::post_access::coordinator::runtime) fn validate_cursor_mode(
        &self,
        cursor: Option<&CursorBoundary>,
    ) -> Result<(), InternalError> {
        if cursor.is_some() && !self.contract.mode().is_load() {
            return Err(InternalError::query_invalid_logical_plan(
                "delete plans must not carry cursor boundaries",
            ));
        }

        Ok(())
    }

    // Filter phase (already normalized and validated during planning).
    pub(in crate::db::executor::pipeline::operators::post_access::coordinator::runtime) fn apply_filter_phase<
        R,
    >(
        &self,
        rows: &mut Vec<R>,
        filter_program: Option<&EffectiveRuntimeFilterProgram>,
        filter_preapplied: bool,
    ) -> Result<(bool, usize), InternalError>
    where
        R: OrderReadableRow,
    {
        let filtered = if self.contract.has_filter() {
            if filter_preapplied {
                return Ok((true, rows.len()));
            }

            let Some(filter_program) = filter_program else {
                return Err(InternalError::query_executor_invariant(
                    "post-access filtering requires one compiled residual filter program",
                ));
            };

            compact_rows_in_place_result(rows, |row| {
                row_matches_filter_program(row, filter_program)
            })?;
            true
        } else {
            false
        };

        Ok((filtered, rows.len()))
    }
}

// Evaluate one planner-frozen residual filter program against one generic
// post-access row without collapsing expression errors into row rejection.
fn row_matches_filter_program<R: OrderReadableRow>(
    row: &R,
    filter_program: &EffectiveRuntimeFilterProgram,
) -> Result<bool, InternalError> {
    match filter_program {
        EffectiveRuntimeFilterProgram::Predicate(predicate_program) => Ok(predicate_program
            .eval_with_slot_value_cow_reader(&mut |slot| row.read_order_slot_cow(slot))),
        EffectiveRuntimeFilterProgram::Expr(filter_expr) => {
            eval_scalar_filter_expr_with_required_value_reader_cow(filter_expr, &mut |slot| {
                let Some(value) = row.read_order_slot_cow(slot) else {
                    return Err(InternalError::query_invalid_logical_plan(format!(
                        "post-access scalar filter expression could not read slot {slot}",
                    )));
                };

                Ok(value)
            })
        }
    }
}

// Compact one row vector in place under one keep predicate so the generic
// post-access coordinator stays on the same straight-line filter loop as the
// shared scalar page kernel.
fn compact_rows_in_place_result<R>(
    rows: &mut Vec<R>,
    mut keep_row: impl FnMut(&R) -> Result<bool, InternalError>,
) -> Result<usize, InternalError> {
    let mut kept = 0usize;

    for read_index in 0..rows.len() {
        if !keep_row(&rows[read_index])? {
            continue;
        }

        if kept != read_index {
            rows.swap(kept, read_index);
        }
        kept = kept.saturating_add(1);
    }

    rows.truncate(kept);

    Ok(kept)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::compact_rows_in_place_result;
    use crate::{db::executor::OrderReadableRow, value::Value};
    use std::borrow::Cow;

    struct TestRow(Option<Value>);

    impl OrderReadableRow for TestRow {
        fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
            if slot != 0 {
                return None;
            }

            self.0.as_ref().map(Cow::Borrowed)
        }
    }

    #[test]
    fn compact_rows_in_place_preserves_kept_order() {
        let mut rows = vec![
            TestRow(Some(Value::Uint(1))),
            TestRow(Some(Value::Uint(2))),
            TestRow(Some(Value::Uint(3))),
            TestRow(Some(Value::Uint(4))),
        ];

        let kept = compact_rows_in_place_result(&mut rows, |row| {
            Ok(matches!(
                row.read_order_slot_cow(0).as_deref(),
                Some(Value::Uint(value)) if value % 2 == 0
            ))
        })
        .expect("infallible test compaction should succeed");

        assert_eq!(kept, 2);
        assert_eq!(
            rows.into_iter().map(|row| row.0).collect::<Vec<_>>(),
            vec![Some(Value::Uint(2)), Some(Value::Uint(4))]
        );
    }
}
