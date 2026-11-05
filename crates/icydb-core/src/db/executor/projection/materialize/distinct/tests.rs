mod canonical_keys;

use super::*;
use crate::db::{
    QueryError,
    cursor::CursorBoundarySlot,
    resource::budget::{
        HardExecutionBudget, HardExecutionContext, HardExecutionFailureHeadroom,
        with_query_execution_budget_for_tests,
    },
};
use icydb_diagnostic_code::{
    DiagnosticDetail, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane, RuntimeBoundaryCode,
};

const TEST_HEADROOM: HardExecutionFailureHeadroom = HardExecutionFailureHeadroom::new(500, 256);
const TEST_CONTEXT: HardExecutionContext = HardExecutionContext::new(
    DiagnosticExecutionBudgetScope::Execution,
    DiagnosticExecutionLane::TrustedRead,
    0x2220_0007_0000_0001,
);

fn boundary(value: u64) -> CursorBoundary {
    CursorBoundary {
        slots: vec![CursorBoundarySlot::Present(Value::Nat64(value))],
    }
}

fn text_candidate(value: &str, source_order: u64) -> DistinctProjectedRow {
    DistinctProjectedRow::new(
        RowView::owned(vec![Value::Text(value.to_string())]),
        Some(boundary(source_order)),
    )
}

fn collect(
    strategy: ProjectionDistinctStrategy,
    window: ProjectionDistinctWindow,
    rows: Vec<DistinctProjectedRow>,
) -> Result<DistinctProjectionPage, QueryError> {
    let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
    with_query_execution_budget_for_tests(budget, TEST_CONTEXT, || {
        collect_distinct_projected_rows(strategy, window, rows, |_| Ok(true), Ok)
            .map_err(QueryError::execute)
    })
}

fn text_rows(rows: Vec<RowView>) -> Vec<Vec<Value>> {
    rows.into_iter().map(RowView::into_owned).collect()
}

#[test]
fn borrowed_row_hash_preserves_typed_failure() {
    use crate::value::{test_hash_budget_error, with_test_hash_override};
    with_test_hash_override(Err(test_hash_budget_error), || {
        let row = RowView::owned(vec![Value::Nat64(7)]);
        let error = stable_hash_projected_row(&row).unwrap_err();
        assert_eq!(error.diagnostic(), test_hash_budget_error().diagnostic());
        assert_eq!(
            error.diagnostic_facts(),
            test_hash_budget_error().diagnostic_facts()
        );
    });
}

fn collect_with_output_limit(
    strategy: ProjectionDistinctStrategy,
    rows: Vec<DistinctProjectedRow>,
    output_limit: usize,
) -> Result<DistinctProjectionPage, QueryError> {
    let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM);
    with_query_execution_budget_for_tests(budget, TEST_CONTEXT, || {
        let mut admitted = 0_usize;
        collect_distinct_projected_rows(
            strategy,
            ProjectionDistinctWindow::new(0, None),
            rows,
            |_| {
                if admitted >= output_limit {
                    return Ok(false);
                }
                admitted = admitted.saturating_add(1);
                Ok(true)
            },
            Ok,
        )
        .map_err(QueryError::execute)
    })
}

#[test]
fn direct_projection_requires_a_complete_leading_order_equivalence() {
    assert!(direct_projection_is_leading_order_equivalence(
        &[1],
        &[1, 0]
    ));
    assert!(direct_projection_is_leading_order_equivalence(
        &[1, 2],
        &[2, 1, 0]
    ));
    assert!(!direct_projection_is_leading_order_equivalence(
        &[1],
        &[0, 1]
    ));
    assert!(!direct_projection_is_leading_order_equivalence(
        &[1, 2],
        &[1, 0, 2]
    ));
}

#[test]
fn adjacent_distinct_closes_the_emitted_group_before_its_cursor_boundary() {
    let first = collect(
        ProjectionDistinctStrategy::OrderedAdjacent,
        ProjectionDistinctWindow::new(0, Some(2)),
        vec![
            text_candidate("a", 1),
            text_candidate("a", 2),
            text_candidate("b", 3),
            text_candidate("b", 4),
            text_candidate("c", 5),
            text_candidate("c", 6),
        ],
    )
    .expect("adjacent DISTINCT page should execute");
    let (rows, last_emitted, has_more) = first.into_parts();

    assert_eq!(
        text_rows(rows),
        vec![
            vec![Value::Text("a".to_string())],
            vec![Value::Text("b".to_string())],
        ]
    );
    assert_eq!(last_emitted, Some(boundary(4)));
    assert!(has_more);
    let resumed = collect(
        ProjectionDistinctStrategy::OrderedAdjacent,
        ProjectionDistinctWindow::new(0, Some(2)),
        vec![
            text_candidate("c", 5),
            text_candidate("c", 6),
            text_candidate("d", 7),
        ],
    )
    .expect("adjacent DISTINCT resume should execute");
    let (rows, last_emitted, has_more) = resumed.into_parts();

    assert_eq!(
        text_rows(rows),
        vec![
            vec![Value::Text("c".to_string())],
            vec![Value::Text("d".to_string())],
        ]
    );
    assert_eq!(last_emitted, Some(boundary(7)));
    assert!(!has_more);
}

#[test]
fn global_distinct_replay_handles_nonadjacent_duplicates_across_pages() {
    let candidates = || {
        vec![
            text_candidate("a", 1),
            text_candidate("b", 2),
            text_candidate("a", 3),
            text_candidate("c", 4),
            text_candidate("b", 5),
        ]
    };
    let first = collect(
        ProjectionDistinctStrategy::GlobalReplay,
        ProjectionDistinctWindow::new(0, Some(2)),
        candidates(),
    )
    .expect("first global DISTINCT page should execute");
    let (rows, last_emitted, has_more) = first.into_parts();

    assert_eq!(
        text_rows(rows),
        vec![
            vec![Value::Text("a".to_string())],
            vec![Value::Text("b".to_string())],
        ]
    );
    assert_eq!(last_emitted, Some(boundary(2)));
    assert!(has_more);
    let second = collect(
        ProjectionDistinctStrategy::GlobalReplay,
        ProjectionDistinctWindow::new(2, Some(2)),
        candidates(),
    )
    .expect("global DISTINCT replay should execute");
    let (rows, last_emitted, has_more) = second.into_parts();

    assert_eq!(text_rows(rows), vec![vec![Value::Text("c".to_string())]]);
    assert_eq!(last_emitted, Some(boundary(4)));
    assert!(!has_more);
}

#[test]
fn distinct_output_admission_preserves_the_last_returned_group_boundary() {
    for strategy in [
        ProjectionDistinctStrategy::OrderedAdjacent,
        ProjectionDistinctStrategy::GlobalReplay,
    ] {
        let page = collect_with_output_limit(
            strategy,
            vec![
                text_candidate("a", 1),
                text_candidate("a", 2),
                text_candidate("b", 3),
                text_candidate("c", 4),
            ],
            1,
        )
        .expect("output admission should return resumable DISTINCT progress");
        let (rows, last_emitted, has_more) = page.into_parts();

        assert_eq!(text_rows(rows), vec![vec![Value::Text("a".to_string())]]);
        let expected_boundary = match strategy {
            ProjectionDistinctStrategy::OrderedAdjacent => boundary(2),
            ProjectionDistinctStrategy::GlobalReplay => boundary(1),
        };
        assert_eq!(last_emitted, Some(expected_boundary));
        assert!(has_more);
    }
}

#[test]
fn global_distinct_owns_canonical_null_and_nested_keys() {
    let nested = Value::List(vec![Value::Map(vec![(
        Value::Text("key".to_string()),
        Value::Nat64(7),
    )])]);
    let candidates = vec![
        DistinctProjectedRow::new(
            RowView::owned(vec![Value::Null, nested.clone()]),
            Some(boundary(1)),
        ),
        DistinctProjectedRow::new(
            RowView::owned(vec![Value::Null, nested.clone()]),
            Some(boundary(2)),
        ),
        DistinctProjectedRow::new(
            RowView::owned(vec![Value::Text("x".to_string()), nested]),
            Some(boundary(3)),
        ),
    ];
    let page = collect(
        ProjectionDistinctStrategy::GlobalReplay,
        ProjectionDistinctWindow::new(0, None),
        candidates,
    )
    .expect("nested DISTINCT keys should execute");
    let (rows, last_emitted, has_more) = page.into_parts();

    assert_eq!(rows.len(), 2);
    assert_eq!(last_emitted, Some(boundary(3)));
    assert!(!has_more);
}

#[test]
fn distinct_state_exhaustion_returns_a_typed_hard_budget_error() {
    let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
        .with_limit_for_tests(
            DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
            0,
        );
    let result = with_query_execution_budget_for_tests(budget, TEST_CONTEXT, || {
        collect_distinct_projected_rows(
            ProjectionDistinctStrategy::GlobalReplay,
            ProjectionDistinctWindow::new(0, None),
            vec![text_candidate("too-large", 1)],
            |_| Ok(true),
            Ok,
        )
        .map_err(QueryError::execute)
    });
    let Err(error) = result else {
        panic!("DISTINCT state must obey the hard budget")
    };

    assert!(matches!(
        error.diagnostic().detail(),
        Some(DiagnosticDetail::RuntimeBoundary {
            boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
        })
    ));
}

#[test]
fn global_distinct_hash_capacity_is_part_of_the_typed_state_budget() {
    let candidate = text_candidate("one", 1);
    let canonical = canonical_projected_row(&candidate.row).expect("canonical key");
    let canonical_bytes = runtime_value_work(&canonical).0;
    let budget = HardExecutionBudget::uniform_for_tests(u64::MAX, TEST_HEADROOM)
        .with_limit_for_tests(
            DiagnosticExecutionBudgetResource::GroupDistinctStateBytes,
            canonical_bytes,
        );
    let result = with_query_execution_budget_for_tests(budget, TEST_CONTEXT, || {
        collect_distinct_projected_rows(
            ProjectionDistinctStrategy::GlobalReplay,
            ProjectionDistinctWindow::new(0, None),
            vec![candidate],
            |_| Ok(true),
            Ok,
        )
        .map_err(QueryError::execute)
    });
    let Err(error) = result else {
        panic!("canonical bytes alone must not admit hidden hash capacity")
    };

    assert!(matches!(
        error.diagnostic().detail(),
        Some(DiagnosticDetail::RuntimeBoundary {
            boundary: RuntimeBoundaryCode::ExecutionBudgetExceeded,
        })
    ));
}
