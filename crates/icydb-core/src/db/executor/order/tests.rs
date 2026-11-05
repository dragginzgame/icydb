use super::{BoundedOrderRows, reorder_rows_by_original_indices};

#[cfg(feature = "sql")]
#[test]
fn expression_order_cache_and_cursor_agree_for_borrowed_and_synthesized_slots() {
    use super::*;
    use crate::db::query::plan::{
        ResolvedOrderField,
        expr::{BinaryOp, CompiledExpr},
    };

    struct Row(Value, bool);
    impl OrderReadableRow for Row {
        fn read_order_slot_ref(&self, slot: usize) -> Option<&Value> {
            (slot == 0).then_some(&self.0)
        }

        fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
            self.read_order_slot_ref(slot).map(|value| {
                if self.1 {
                    Cow::Owned(value.clone())
                } else {
                    Cow::Borrowed(value)
                }
            })
        }
    }

    let order = ResolvedOrder::new(vec![ResolvedOrderField::new(
        ResolvedOrderValueSource::expression(CompiledExpr::Binary {
            op: BinaryOp::Eq,
            left: Box::new(CompiledExpr::Slot { slot: 0 }),
            right: Box::new(CompiledExpr::Literal(Value::Text("match".into()))),
        }),
        OrderDirection::Asc,
    )]);
    for (value, expected) in [
        (Value::Text("match".into()), Some(Value::Bool(true))),
        (Value::Text("other".into()), Some(Value::Bool(false))),
        (Value::Null, None),
    ] {
        let borrowed = Row(value.clone(), false);
        let synthesized = Row(value, true);
        for row in [&borrowed, &synthesized] {
            let cached = cache_order_values_from_row(row, &order)
                .into_values()
                .collect::<Vec<_>>();
            assert_eq!(cached, vec![expected.clone()]);
            assert_eq!(
                cursor_boundary_from_orderable_row(row, &order).slots,
                vec![
                    expected
                        .clone()
                        .map_or(CursorBoundarySlot::Missing, CursorBoundarySlot::Present)
                ]
            );
        }
    }
}

#[test]
fn bounded_selection_matches_sorted_prefixes_in_both_directions() {
    let input = [4, 1, 7, 7, 2, 9, 0, 5];
    for descending in [false, true] {
        let compare = |left: &i32, right: &i32| {
            if descending {
                right.cmp(left)
            } else {
                left.cmp(right)
            }
        };
        for keep_count in 0..=input.len() + 1 {
            let mut window = BoundedOrderRows::new(keep_count);
            for (index, value) in input.into_iter().enumerate() {
                window.push(value, compare);
                let mut expected = input[..=index].to_vec();
                expected.sort_by(compare);
                expected.truncate(keep_count);
                let mut actual = window.rows.clone();
                actual.sort_by(compare);
                assert_eq!(actual, expected);
            }
        }
    }
}

#[test]
fn equal_candidates_preserve_retained_row_identity() {
    let mut window = BoundedOrderRows::new(2);
    for row in [(1, 'a'), (1, 'b'), (1, 'c')] {
        window.push(row, |left, right| left.0.cmp(&right.0));
    }
    assert_eq!(window.into_rows(), vec![(1, 'a'), (1, 'b')]);
}

#[test]
fn bounded_heap_preserves_linear_selection_and_comparison_bounds() {
    use std::{cell::Cell, cmp::Ordering};

    for descending in [false, true] {
        let compare = |left: &(usize, usize), right: &(usize, usize)| {
            let order = left.0.cmp(&right.0);
            if descending { order.reverse() } else { order }
        };
        for keep_count in 0..=33 {
            let mut window = BoundedOrderRows::new(keep_count);
            let mut reference = Vec::new();
            for index in 0..257 {
                let row = ((index * 37) % 19, index);
                if reference.len() < keep_count {
                    reference.push(row);
                } else if !reference.is_empty() {
                    let mut worst = 0;
                    for slot in 1..reference.len() {
                        if compare(&reference[slot], &reference[worst]) == Ordering::Greater {
                            worst = slot;
                        }
                    }
                    if compare(&row, &reference[worst]) == Ordering::Less {
                        reference[worst] = row;
                    }
                }
                let bound = window.comparison_bound();
                let comparisons = Cell::new(0usize);
                window.push(row, |left, right| {
                    comparisons.set(comparisons.get() + 1);
                    compare(left, right)
                });
                assert!(comparisons.get() <= bound);
                assert_eq!(window.rows, reference);
            }
        }
    }
}

#[test]
fn bounded_large_window_stays_within_sort_comparison_budget() {
    use crate::db::resource::budget::{
        HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
        current_execution_budget_usage, with_execution_budget_for_tests,
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
    };

    for descending_input in [false, true] {
        let budget = HardExecutionBudget::uniform_for_tests(
            32_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        );
        let context = HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::TrustedRead,
            0,
        );
        with_execution_budget_for_tests(
            budget,
            context,
            || {
                let mut window = BoundedOrderRows::new(1_000);
                for index in 0..40_000 {
                    let value = if descending_input {
                        39_999 - index
                    } else {
                        index
                    };
                    window.charge_candidate_work(size_of::<i32>() as u64)?;
                    window.push(value, i32::cmp);
                }
                let usage = current_execution_budget_usage()?;
                assert!(
                    usage.observed(DiagnosticExecutionBudgetResource::SortComparisons) < 1_000_000
                );
                assert_eq!(
                    usage.observed(DiagnosticExecutionBudgetResource::SortTemporaryBytes),
                    40_000 * size_of::<i32>() as u64 + 1_000 * size_of::<usize>() as u64
                );
                let mut rows = window.into_rows();
                rows.sort_unstable();
                assert_eq!(rows, (0..1_000).collect::<Vec<_>>());
                Ok::<_, crate::error::InternalError>(())
            },
            std::convert::identity,
        )
        .unwrap();
    }
}

#[cfg(feature = "sql")]
#[test]
fn bounded_strategies_preserve_budget_charges_and_cache_each_candidate_once() {
    use super::*;
    use crate::db::{
        query::plan::{ResolvedOrderField, expr::CompiledExpr},
        resource::budget::{
            HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
            current_execution_budget_usage, with_execution_budget_for_tests,
        },
    };
    use icydb_diagnostic_code::{DiagnosticExecutionBudgetScope, DiagnosticExecutionLane};
    use std::{cell::Cell, rc::Rc};

    struct Row(Value, Rc<Cell<usize>>);
    impl OrderReadableRow for Row {
        fn read_order_slot_ref(&self, slot: usize) -> Option<&Value> {
            (slot == 0).then_some(&self.0)
        }
        fn read_order_slot_cow(&self, slot: usize) -> Option<Cow<'_, Value>> {
            self.1.set(self.1.get() + 1);
            self.read_order_slot_ref(slot).map(Cow::Borrowed)
        }
        fn order_slots_are_borrowed(&self) -> bool {
            true
        }
    }

    for cached in [false, true] {
        let source = if cached {
            ResolvedOrderValueSource::expression(CompiledExpr::Slot { slot: 0 })
        } else {
            ResolvedOrderValueSource::direct_field(0)
        };
        let order = ResolvedOrder::new(vec![ResolvedOrderField::new(source, OrderDirection::Asc)]);
        let reads = Rc::new(Cell::new(0));
        let budget = HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        );
        let context = HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            DiagnosticExecutionLane::TrustedRead,
            0,
        );
        with_execution_budget_for_tests(
            budget,
            context,
            || {
                let mut window = BoundedOrderWindow::new(2, &order);
                let mut expected_bytes = 2 * size_of::<usize>() as u64;
                for value in [4, 1, 5, 0] {
                    let row = Row(Value::Int64(value), Rc::clone(&reads));
                    expected_bytes += row.retained_order_backing_bytes();
                    if cached {
                        expected_bytes += runtime_value_work(&row.0).0;
                    }
                    window.push(row)?;
                }
                let usage = current_execution_budget_usage()?;
                assert_eq!(
                    usage.observed(DiagnosticExecutionBudgetResource::SortEntries),
                    4
                );
                assert_eq!(
                    usage.observed(DiagnosticExecutionBudgetResource::SortComparisons),
                    7
                );
                assert_eq!(
                    usage.observed(DiagnosticExecutionBudgetResource::SortTemporaryBytes),
                    expected_bytes
                );
                let rows = window.into_pending_rows().apply_order(&order, Some(2))?;
                assert_eq!(
                    rows.into_iter().map(|row| row.0).collect::<Vec<_>>(),
                    vec![Value::Int64(0), Value::Int64(1)]
                );
                assert_eq!(reads.get(), if cached { 4 } else { 0 });
                Ok::<_, InternalError>(())
            },
            std::convert::identity,
        )
        .unwrap();
    }
}

#[test]
fn compact_order_indices_reorder_complete_and_bounded_rows() {
    assert_eq!(
        reorder_rows_by_original_indices(vec!['a', 'b', 'c', 'd'], &[2, 0, 3, 1])
            .expect("complete permutation should reorder"),
        vec!['c', 'a', 'd', 'b'],
    );
    assert_eq!(
        reorder_rows_by_original_indices(vec!['a', 'b', 'c', 'd'], &[3, 1])
            .expect("bounded permutation should retain selected rows"),
        vec!['d', 'b'],
    );
    assert!(reorder_rows_by_original_indices(vec!['a'], &[1]).is_err());
}
