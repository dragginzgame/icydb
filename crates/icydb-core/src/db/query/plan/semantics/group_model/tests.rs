use crate::{
    db::{
        query::{
            builder::{AggregateExpr, count, min_by, sum},
            plan::{AggregateKind, GroupAggregateSpec, expr::Expr},
        },
        schema::AcceptedFieldKind,
    },
    types::EntityTag,
    value::Value,
};

use super::{
    canonicalize_grouped_having_numeric_literal_for_accepted_kind, group_aggregate_spec_expr,
};

#[test]
fn owned_group_aggregate_preserves_operands_and_raw_shape() {
    let aggregate = min_by("rank")
        .with_filter_expr(Expr::Literal(Value::Text("x".repeat(4096))))
        .distinct();
    let expected = aggregate.clone().into_shape();
    let input_address = std::ptr::from_ref(aggregate.input_expr().expect("input"));
    let filter_address = std::ptr::from_ref(aggregate.filter_expr().expect("filter"));

    let grouped = GroupAggregateSpec::from_aggregate_expr(aggregate);
    assert_eq!(grouped.shape(), &expected);
    assert!(grouped.raw_distinct());
    assert!(!grouped.semantic_distinct());
    assert_eq!(
        std::ptr::from_ref(grouped.input_expr().expect("input")),
        input_address
    );
    assert_eq!(
        std::ptr::from_ref(grouped.filter_expr().expect("filter")),
        filter_address
    );
}

#[test]
fn aggregate_wrappers_preserve_raw_and_semantic_equality_domains() {
    let raw_min = min_by("rank");
    let raw_distinct_min = min_by("rank").distinct();
    assert_ne!(raw_min, raw_distinct_min);

    let grouped_min = GroupAggregateSpec::from_aggregate_expr(raw_min);
    let grouped_distinct_min = GroupAggregateSpec::from_aggregate_expr(raw_distinct_min);
    assert_eq!(grouped_min, grouped_distinct_min);
    assert!(grouped_distinct_min.raw_distinct());
    assert!(!grouped_distinct_min.semantic_distinct());

    let raw_count_rows = count();
    let raw_count_literal =
        AggregateExpr::from_expression_input(AggregateKind::Count, Expr::Literal(Value::Nat64(1)));
    assert_ne!(raw_count_rows, raw_count_literal);
    assert_eq!(
        GroupAggregateSpec::from_aggregate_expr(raw_count_rows),
        GroupAggregateSpec::from_aggregate_expr(raw_count_literal),
    );

    assert_ne!(
        GroupAggregateSpec::from_aggregate_expr(sum("rank")),
        GroupAggregateSpec::from_aggregate_expr(sum("rank").distinct()),
    );
    assert_ne!(
        GroupAggregateSpec::from_aggregate_expr(
            sum("rank").with_filter_expr(Expr::Literal(Value::Bool(true))),
        ),
        GroupAggregateSpec::from_aggregate_expr(
            sum("rank").with_filter_expr(Expr::Literal(Value::Bool(false))),
        ),
    );
}

#[test]
fn grouped_projection_round_trip_normalizes_only_semantic_distinct() {
    let grouped = GroupAggregateSpec::from_aggregate_expr(min_by("rank").distinct());
    let projected = group_aggregate_spec_expr(&grouped);

    assert_eq!(projected, min_by("rank"));
    assert!(!projected.is_distinct());
    assert!(grouped.raw_distinct());
}

#[test]
fn accepted_grouped_having_literal_canonicalization_recurses_through_relations() {
    let relation = AcceptedFieldKind::Relation {
        target_path: "demo::Target".to_string(),
        target_entity_name: "Target".to_string(),
        target_entity_tag: EntityTag::new(1),
        target_store_path: "demo::store::TargetStore".to_string(),
        key_kind: Box::new(AcceptedFieldKind::Nat64),
    };

    let mut value = Value::Int64(7);
    crate::db::query::preparation::with_preparation_work(|work| {
        canonicalize_grouped_having_numeric_literal_for_accepted_kind(&relation, &mut value, work)
    })
    .expect("literal preparation");
    assert_eq!(value, Value::Nat64(7));
}

#[test]
fn accepted_grouped_having_literal_canonicalization_recurses_through_lists() {
    let list = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Int64));

    let mut value = Value::List(vec![Value::Nat64(3), Value::Int64(5)]);
    crate::db::query::preparation::with_preparation_work(|work| {
        canonicalize_grouped_having_numeric_literal_for_accepted_kind(&list, &mut value, work)
    })
    .expect("literal preparation");
    assert_eq!(value, Value::List(vec![Value::Int64(3), Value::Int64(5)]));
}

#[test]
fn accepted_grouped_having_literal_canonicalization_does_not_widen_ulid_text() {
    let mut value = Value::Text("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string());
    crate::db::query::preparation::with_preparation_work(|work| {
        canonicalize_grouped_having_numeric_literal_for_accepted_kind(
            &AcceptedFieldKind::Ulid,
            &mut value,
            work,
        )
    })
    .expect("literal preparation");
    assert_eq!(value, Value::Text("01ARZ3NDEKTSV4RRFFQ69G5FAV".to_string()));
}
