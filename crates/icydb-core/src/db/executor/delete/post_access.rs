//! Module: executor::delete::post_access
//! Responsibility: delete-only residual filtering, ordering, and affected-row windows.
//! Does not own: candidate row access, output projection, or commit application.
//! Boundary: applies planner-frozen delete semantics to already-materialized candidates.

use crate::{
    db::{
        executor::{
            OrderReadableRow, apply_offset_limit_window, apply_structural_order_window,
            delete::types::{DeleteRow, PreparedDeleteExecutionState},
            projection::eval_effective_runtime_filter_program_with_value_cow_reader,
            route::access_order_satisfied_by_route_mode,
        },
        query::plan::{AccessPlannedQuery, EffectiveRuntimeFilterProgram},
    },
    entity::{EntityKind, EntityValue},
    error::InternalError,
    value::Value,
};
use std::borrow::Cow;

impl<E> OrderReadableRow for DeleteRow<E>
where
    E: EntityKind + EntityValue,
{
    fn read_order_slot_ref(&self, _slot: usize) -> Option<&Value> {
        None
    }

    fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.entity.get_value_by_index(slot).map(Cow::Owned)
    }
}

/// Apply the canonical delete-only post-access sequence after candidate rows
/// have been materialized in the caller's row representation.
pub(in crate::db::executor::delete) fn apply_delete_post_access_rows<R>(
    prepared: &PreparedDeleteExecutionState,
    rows: &mut Vec<R>,
) -> Result<(), InternalError>
where
    R: OrderReadableRow,
{
    let plan = prepared.logical_plan.as_ref();

    apply_delete_residual_filter(plan, rows)?;
    apply_delete_order(plan, rows)?;

    if let Some(window) = plan.scalar_plan().delete_limit.as_ref() {
        apply_offset_limit_window(rows, window.offset, window.limit);
    }

    Ok(())
}

// Apply the planner-selected residual program exactly once against the
// materialized delete candidates.
fn apply_delete_residual_filter<R>(
    plan: &AccessPlannedQuery,
    rows: &mut Vec<R>,
) -> Result<(), InternalError>
where
    R: OrderReadableRow,
{
    let filter_program = plan.effective_runtime_filter_program();
    if plan.has_any_residual_filter() && filter_program.is_none() {
        return Err(InternalError::query_executor_invariant());
    }
    let Some(filter_program) = filter_program else {
        return Ok(());
    };

    compact_rows_in_place_result(rows, |row| row_matches_filter_program(row, filter_program))?;

    Ok(())
}

// Apply planner-resolved delete ordering after filtering. A logical order
// without its resolved executor contract is an invariant failure rather than
// permission to continue in access order.
fn apply_delete_order<R>(plan: &AccessPlannedQuery, rows: &mut Vec<R>) -> Result<(), InternalError>
where
    R: OrderReadableRow,
{
    let Some(order) = plan.scalar_plan().order.as_ref() else {
        return Ok(());
    };
    if order.fields.is_empty() {
        return Ok(());
    }

    let resolved_order = plan.require_resolved_order()?;
    if rows.len() > 1 && !access_order_satisfied_by_route_mode(plan) {
        apply_structural_order_window(rows, resolved_order, None);
    }

    Ok(())
}

// Evaluate one planner-frozen residual filter program against one delete row
// without collapsing expression errors into row rejection.
fn row_matches_filter_program<R: OrderReadableRow>(
    row: &R,
    filter_program: &EffectiveRuntimeFilterProgram,
) -> Result<bool, InternalError> {
    eval_effective_runtime_filter_program_with_value_cow_reader(
        filter_program,
        &mut |slot| row.read_order_slot_cow(slot),
        "delete residual filter expression could not read slot",
    )
}

// Compact one row vector in place under a fallible keep predicate while
// preserving the relative order of surviving candidates.
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

#[cfg(test)]
mod tests {
    use crate::{db::executor::OrderReadableRow, value::Value};
    use std::borrow::Cow;

    use super::compact_rows_in_place_result;

    struct TestRow(Option<Value>);

    impl OrderReadableRow for TestRow {
        fn read_order_slot_ref(&self, slot: usize) -> Option<&Value> {
            if slot != 0 {
                return None;
            }

            self.0.as_ref()
        }

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
            TestRow(Some(Value::Nat64(1))),
            TestRow(Some(Value::Nat64(2))),
            TestRow(Some(Value::Nat64(3))),
            TestRow(Some(Value::Nat64(4))),
        ];

        let kept = compact_rows_in_place_result(&mut rows, |row| {
            Ok(matches!(
                row.read_order_slot_cow(0).as_deref(),
                Some(Value::Nat64(value)) if value % 2 == 0
            ))
        })
        .expect("infallible test compaction should succeed");

        assert_eq!(kept, 2);
        assert_eq!(
            rows.into_iter().map(|row| row.0).collect::<Vec<_>>(),
            vec![Some(Value::Nat64(2)), Some(Value::Nat64(4))]
        );
    }
}
