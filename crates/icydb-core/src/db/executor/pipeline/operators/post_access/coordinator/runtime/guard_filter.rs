//! Module: db::executor::pipeline::operators::post_access::coordinator::runtime::guard_filter
//! Defines guard-filter helpers used by post-access coordinator runtime.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        cursor::CursorBoundary, executor::OrderReadableRow,
        executor::pipeline::operators::post_access::coordinator::PostAccessPlan,
        predicate::PredicateProgram,
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

    // Predicate phase (already normalized and validated during planning).
    pub(in crate::db::executor::pipeline::operators::post_access::coordinator::runtime) fn apply_filter_phase<
        R,
    >(
        &self,
        rows: &mut Vec<R>,
        compiled_predicate: Option<&PredicateProgram>,
        predicate_preapplied: bool,
    ) -> Result<(bool, usize), InternalError>
    where
        R: OrderReadableRow,
    {
        let filtered = if self.contract.has_predicate() {
            if predicate_preapplied {
                return Ok((true, rows.len()));
            }

            let Some(compiled_predicate) = compiled_predicate else {
                return Err(InternalError::scalar_page_predicate_slots_required());
            };

            compact_rows_in_place(rows, |row| {
                compiled_predicate
                    .eval_with_slot_value_cow_reader(&mut |slot| row.read_order_slot_cow(slot))
            });
            true
        } else {
            false
        };

        Ok((filtered, rows.len()))
    }
}

// Compact one row vector in place under one keep predicate so the generic
// post-access coordinator stays on the same straight-line filter loop as the
// shared scalar page kernel.
fn compact_rows_in_place<R>(rows: &mut Vec<R>, mut keep_row: impl FnMut(&R) -> bool) -> usize {
    let mut kept = 0usize;

    for read_index in 0..rows.len() {
        if !keep_row(&rows[read_index]) {
            continue;
        }

        if kept != read_index {
            rows.swap(kept, read_index);
        }
        kept = kept.saturating_add(1);
    }

    rows.truncate(kept);

    kept
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::compact_rows_in_place;
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

        let kept = compact_rows_in_place(
            &mut rows,
            |row| matches!(row.read_order_slot_cow(0).as_deref(), Some(Value::Uint(value)) if value % 2 == 0),
        );

        assert_eq!(kept, 2);
        assert_eq!(
            rows.into_iter().map(|row| row.0).collect::<Vec<_>>(),
            vec![Some(Value::Uint(2)), Some(Value::Uint(4))]
        );
    }
}
