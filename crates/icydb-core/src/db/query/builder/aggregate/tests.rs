use crate::db::query::{
    builder::aggregate::{
        AggregateExplain, AvgBySlotTerminal, CountDistinctBySlotTerminal, CountRowsTerminal,
        ExistsRowsTerminal, FirstIdTerminal, LastIdTerminal, LastValueBySlotTerminal,
        NthIdBySlotTerminal, ScalarNumericFieldBoundaryRequest, ScalarProjectionBoundaryRequest,
        ScalarTerminalBoundaryRequest, SumDistinctBySlotTerminal,
    },
    plan::{AggregateKind, FieldSlot},
};

#[test]
fn numeric_field_strategy_sum_distinct_preserves_request_shape() {
    let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
    let strategy = SumDistinctBySlotTerminal::new(rank_slot.clone());

    assert_eq!(
        strategy.explain_aggregate_kind(),
        Some(AggregateKind::Sum),
        "sum(distinct field) should preserve SUM aggregate kind",
    );
    assert_eq!(
        strategy.explain_projected_field(),
        Some("rank"),
        "sum(distinct field) should preserve projected field labels",
    );
    let (target_field, request) = strategy.into_executor_request();
    assert_eq!(
        target_field, rank_slot,
        "sum(distinct field) should preserve the resolved planner field slot",
    );
    let ScalarNumericFieldBoundaryRequest::SumDistinct = request else {
        panic!("sum(distinct field) should project the numeric DISTINCT request");
    };
}

#[test]
fn existing_rows_terminal_strategy_count_preserves_request_shape() {
    assert_eq!(
        CountRowsTerminal::aggregate().kind(),
        AggregateKind::Count,
        "count() should preserve the explain-visible aggregate kind",
    );
    let ScalarTerminalBoundaryRequest::Count = CountRowsTerminal::new().into_executor_request()
    else {
        panic!("count() should project the existing-rows count request");
    };
}

#[test]
fn existing_rows_terminal_strategy_exists_preserves_request_shape() {
    assert_eq!(
        ExistsRowsTerminal::aggregate().kind(),
        AggregateKind::Exists,
        "exists() should preserve the explain-visible aggregate kind",
    );
    let ScalarTerminalBoundaryRequest::Exists = ExistsRowsTerminal::new().into_executor_request()
    else {
        panic!("exists() should project the existing-rows exists request");
    };
}

#[test]
fn numeric_field_strategy_avg_preserves_request_shape() {
    let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
    let strategy = AvgBySlotTerminal::new(rank_slot.clone());

    assert_eq!(
        strategy.explain_aggregate_kind(),
        Some(AggregateKind::Avg),
        "avg(field) should preserve AVG aggregate kind",
    );
    assert_eq!(
        strategy.explain_projected_field(),
        Some("rank"),
        "avg(field) should preserve projected field labels",
    );
    let (target_field, request) = strategy.into_executor_request();
    assert_eq!(
        target_field, rank_slot,
        "avg(field) should preserve the resolved planner field slot",
    );
    let ScalarNumericFieldBoundaryRequest::Avg = request else {
        panic!("avg(field) should project the numeric AVG request");
    };
}

#[test]
fn order_sensitive_terminal_strategy_first_preserves_explain_and_request_shape() {
    assert_eq!(
        FirstIdTerminal::explain_aggregate().kind(),
        AggregateKind::First,
        "first() should preserve the explain-visible aggregate kind",
    );
    let ScalarTerminalBoundaryRequest::IdTerminal { kind } =
        FirstIdTerminal::new().into_executor_request()
    else {
        panic!("first() should project the response-order request");
    };
    assert_eq!(kind, AggregateKind::First);
}

#[test]
fn order_sensitive_terminal_strategy_last_preserves_explain_and_request_shape() {
    assert_eq!(
        LastIdTerminal::explain_aggregate().kind(),
        AggregateKind::Last,
        "last() should preserve the explain-visible aggregate kind",
    );
    let ScalarTerminalBoundaryRequest::IdTerminal { kind } =
        LastIdTerminal::new().into_executor_request()
    else {
        panic!("last() should project the response-order request");
    };
    assert_eq!(kind, AggregateKind::Last);
}

#[test]
fn order_sensitive_terminal_strategy_nth_preserves_field_order_request_shape() {
    let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
    let ScalarTerminalBoundaryRequest::NthBySlot { target_field, nth } =
        NthIdBySlotTerminal::new(rank_slot.clone(), 2).into_executor_request()
    else {
        panic!("nth_by(field, nth) should preserve the resolved field-order request");
    };
    assert_eq!(
        target_field, rank_slot,
        "nth_by(field, nth) should preserve the resolved field-order request",
    );
    assert_eq!(nth, 2);
}

#[test]
fn projection_strategy_count_distinct_preserves_request_shape() {
    let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
    let strategy = CountDistinctBySlotTerminal::new(rank_slot.clone());
    let explain = strategy.explain_descriptor();

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
    let (target_field, request) = strategy.into_executor_request();
    assert_eq!(target_field, rank_slot);
    let ScalarProjectionBoundaryRequest::CountDistinct = request else {
        panic!("count_distinct_by(field) should project the distinct-count request");
    };
}

#[test]
fn projection_strategy_terminal_value_preserves_request_shape() {
    let rank_slot = FieldSlot::from_parts_for_test(7, "rank");
    let strategy = LastValueBySlotTerminal::new(rank_slot.clone());
    let explain = strategy.explain_descriptor();

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
    let (target_field, request) = strategy.into_executor_request();
    assert_eq!(target_field, rank_slot);
    let ScalarProjectionBoundaryRequest::TerminalValue { terminal_kind } = request else {
        panic!("last_value_by(field) should project the terminal-value request");
    };
    assert_eq!(terminal_kind, AggregateKind::Last);
}
