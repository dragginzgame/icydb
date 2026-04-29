use crate::db::query::{
    builder::{
        ExistingRowsRequest, ExistingRowsTerminalStrategy, NumericFieldRequest,
        NumericFieldStrategy, OrderRequest, OrderSensitiveTerminalStrategy, ProjectionRequest,
        ProjectionStrategy,
    },
    plan::{AggregateKind, FieldSlot},
};

#[test]
fn numeric_field_strategy_sum_distinct_preserves_request_shape() {
    let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
    let strategy = NumericFieldStrategy::sum_distinct_by_slot(rank_slot.clone());

    assert_eq!(
        strategy.aggregate_kind(),
        AggregateKind::Sum,
        "sum(distinct field) should preserve SUM aggregate kind",
    );
    assert_eq!(
        strategy.projected_field(),
        "rank",
        "sum(distinct field) should preserve projected field labels",
    );
    assert!(
        strategy.aggregate().is_distinct(),
        "sum(distinct field) should preserve DISTINCT aggregate shape",
    );
    assert_eq!(
        strategy.target_field(),
        &rank_slot,
        "sum(distinct field) should preserve the resolved planner field slot",
    );
    assert_eq!(
        strategy.request(),
        NumericFieldRequest::SumDistinct,
        "sum(distinct field) should project the numeric DISTINCT request",
    );
}

#[test]
fn existing_rows_terminal_strategy_count_preserves_request_shape() {
    let strategy = ExistingRowsTerminalStrategy::count_rows();

    assert_eq!(
        strategy.aggregate().kind(),
        AggregateKind::Count,
        "count() should preserve the explain-visible aggregate kind",
    );
    assert_eq!(
        strategy.request(),
        &ExistingRowsRequest::CountRows,
        "count() should project the existing-rows count request",
    );
}

#[test]
fn existing_rows_terminal_strategy_exists_preserves_request_shape() {
    let strategy = ExistingRowsTerminalStrategy::exists_rows();

    assert_eq!(
        strategy.aggregate().kind(),
        AggregateKind::Exists,
        "exists() should preserve the explain-visible aggregate kind",
    );
    assert_eq!(
        strategy.request(),
        &ExistingRowsRequest::ExistsRows,
        "exists() should project the existing-rows exists request",
    );
}

#[test]
fn numeric_field_strategy_avg_preserves_request_shape() {
    let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
    let strategy = NumericFieldStrategy::avg_by_slot(rank_slot.clone());

    assert_eq!(
        strategy.aggregate_kind(),
        AggregateKind::Avg,
        "avg(field) should preserve AVG aggregate kind",
    );
    assert_eq!(
        strategy.projected_field(),
        "rank",
        "avg(field) should preserve projected field labels",
    );
    assert!(
        !strategy.aggregate().is_distinct(),
        "avg(field) should stay non-distinct unless requested explicitly",
    );
    assert_eq!(
        strategy.target_field(),
        &rank_slot,
        "avg(field) should preserve the resolved planner field slot",
    );
    assert_eq!(
        strategy.request(),
        NumericFieldRequest::Avg,
        "avg(field) should project the numeric AVG request",
    );
}

#[test]
fn order_sensitive_terminal_strategy_first_preserves_explain_and_request_shape() {
    let strategy = OrderSensitiveTerminalStrategy::first();

    assert_eq!(
        strategy
            .explain_aggregate()
            .map(|aggregate| aggregate.kind()),
        Some(AggregateKind::First),
        "first() should preserve the explain-visible aggregate kind",
    );
    assert_eq!(
        strategy.request(),
        &OrderRequest::ResponseOrder {
            kind: AggregateKind::First,
        },
        "first() should project the response-order request",
    );
}

#[test]
fn order_sensitive_terminal_strategy_nth_preserves_field_order_request_shape() {
    let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
    let strategy = OrderSensitiveTerminalStrategy::nth_by_slot(rank_slot.clone(), 2);

    assert_eq!(
        strategy.explain_aggregate(),
        None,
        "nth_by(field, nth) should stay off the current explain aggregate surface",
    );
    assert_eq!(
        strategy.request(),
        &OrderRequest::NthBySlot {
            target_field: rank_slot,
            nth: 2,
        },
        "nth_by(field, nth) should preserve the resolved field-order request",
    );
}

#[test]
fn projection_strategy_count_distinct_preserves_request_shape() {
    let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
    let strategy = ProjectionStrategy::count_distinct_by_slot(rank_slot.clone());
    let explain = strategy.explain_descriptor();

    assert_eq!(
        strategy.target_field(),
        &rank_slot,
        "count_distinct_by(field) should preserve the resolved planner field slot",
    );
    assert_eq!(
        strategy.request(),
        ProjectionRequest::CountDistinct,
        "count_distinct_by(field) should project the distinct-count request",
    );
    assert_eq!(
        explain.terminal_label(),
        "count_distinct_by",
        "count_distinct_by(field) should project the stable explain terminal label",
    );
    assert_eq!(
        explain.field_label(),
        "rank",
        "count_distinct_by(field) should project the stable explain field label",
    );
    assert_eq!(
        explain.output_label(),
        "count",
        "count_distinct_by(field) should project the stable explain output label",
    );
}

#[test]
fn projection_strategy_terminal_value_preserves_request_shape() {
    let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
    let strategy = ProjectionStrategy::last_value_by_slot(rank_slot.clone());
    let explain = strategy.explain_descriptor();

    assert_eq!(
        strategy.target_field(),
        &rank_slot,
        "last_value_by(field) should preserve the resolved planner field slot",
    );
    assert_eq!(
        strategy.request(),
        ProjectionRequest::TerminalValue {
            terminal_kind: AggregateKind::Last,
        },
        "last_value_by(field) should project the terminal-value request",
    );
    assert_eq!(
        explain.terminal_label(),
        "last_value_by",
        "last_value_by(field) should project the stable explain terminal label",
    );
    assert_eq!(
        explain.field_label(),
        "rank",
        "last_value_by(field) should project the stable explain field label",
    );
    assert_eq!(
        explain.output_label(),
        "terminal_value",
        "last_value_by(field) should project the stable explain output label",
    );
}
