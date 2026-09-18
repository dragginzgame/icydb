//! Grouped layout classification preserves output positions and identity eligibility.

use super::{
    extend_unique_grouped_aggregate_specs_from_expr,
    planned_projection_layout_and_aggregate_specs_from_spec,
};
use crate::{
    db::{
        query::{
            builder::{AggregateExpr, count, min_by, sum},
            plan::{
                AggregateKind, AggregateSemanticKeyRef, FieldSlot, GroupAggregateSpec, GroupField,
                GroupFieldSet,
                expr::{BinaryOp, Expr, FieldPath, ProjectionField, ProjectionSpec},
            },
        },
        schema::AcceptedFieldKind,
    },
    value::Value,
};

fn plus_zero(expr: Expr) -> Expr {
    Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(expr),
        right: Box::new(Expr::Literal(Value::Int64(0))),
    }
}

#[test]
fn aggregate_collection_preserves_first_seen_slots_and_semantic_distinctions() {
    let filtered = |keep| sum("amount").with_filter_expr(Expr::Literal(Value::Bool(keep)));
    let mut specs = Vec::new();
    for (aggregate, expected_slot, introduced) in [
        (sum("amount"), 0, 1),
        (count(), 1, 1),
        (sum("amount"), 0, 0),
        (sum("amount").distinct(), 2, 1),
        (filtered(false), 3, 1),
        (filtered(true), 4, 1),
        (min_by("amount"), 5, 1),
        (min_by("amount").distinct(), 5, 0),
        (
            AggregateExpr::from_expression_input(
                AggregateKind::Count,
                Expr::Literal(Value::Nat64(1)),
            ),
            1,
            0,
        ),
        (
            AggregateExpr::from_expression_input(AggregateKind::Count, Expr::Literal(Value::Null)),
            6,
            1,
        ),
        (filtered(false), 3, 0),
    ] {
        let expression = Expr::Aggregate(aggregate.clone());
        let scan =
            extend_unique_grouped_aggregate_specs_from_expr(&mut specs, &expression).unwrap();
        assert!(scan.contains_aggregate());
        assert_eq!(scan.introduced_aggregate_count(), introduced);
        assert_eq!(
            specs[expected_slot].semantic_key(),
            AggregateSemanticKeyRef::from_aggregate_expr(&aggregate)
        );
    }
    assert_eq!(specs.len(), 7);

    // A subsequent HAVING-like expression reuses both previously assigned slots.
    let expression = Expr::Binary {
        op: BinaryOp::Add,
        left: Box::new(Expr::Aggregate(sum("amount"))),
        right: Box::new(Expr::Aggregate(filtered(false))),
    };
    let scan = extend_unique_grouped_aggregate_specs_from_expr(&mut specs, &expression).unwrap();
    assert!(scan.contains_aggregate());
    assert_eq!(scan.introduced_aggregate_count(), 0);
    assert_eq!(specs.len(), 7);
}

fn assert_layout(
    keys: &GroupFieldSet,
    expressions: Vec<Expr>,
    group_positions: &[usize],
    aggregate_positions: &[usize],
    identity: bool,
) {
    let projection = ProjectionSpec::from_fields_for_test(
        expressions
            .into_iter()
            .map(|expr| ProjectionField::Scalar {
                expr,
                alias: Some("output".into()),
            })
            .collect(),
    );
    let (layout, aggregates, actual_identity) =
        planned_projection_layout_and_aggregate_specs_from_spec(
            &projection,
            keys,
            &[GroupAggregateSpec::from_aggregate_expr(count())],
        )
        .unwrap();
    assert_eq!(layout.group_field_positions, group_positions);
    assert_eq!(layout.aggregate_positions, aggregate_positions);
    assert_eq!(actual_identity, identity);
    assert_eq!(aggregates.len(), 1);
    assert_eq!(aggregates[0].kind(), count().kind());
}

#[test]
fn grouped_layout_only_direct_outputs_preserve_identity() {
    for (keys, key) in [
        (
            GroupFieldSet::Direct(vec![FieldSlot::from_test_slot(0, "key")]),
            Expr::Field("key".into()),
        ),
        (
            GroupFieldSet::PathAware(vec![GroupField::scalar_path_for_test(
                "profile.key",
                "profile",
                vec!["key".into()],
                0,
                AcceptedFieldKind::Int64,
            )]),
            Expr::FieldPath(FieldPath::new("profile", vec!["key".into()])),
        ),
    ] {
        for (output, identity) in [
            (key.clone(), true),
            (plus_zero(key), false),
            (Expr::Literal(Value::Int64(7)), false),
        ] {
            assert_layout(
                &keys,
                vec![output, Expr::Aggregate(count())],
                &[0],
                &[1],
                identity,
            );
        }
    }
}

#[test]
fn grouped_layout_preserves_reordered_and_computed_aggregate_positions() {
    let keys = GroupFieldSet::Direct(vec![FieldSlot::from_test_slot(0, "key")]);
    let key = Expr::Field("key".into());
    assert_layout(
        &keys,
        vec![Expr::Aggregate(count()), key.clone()],
        &[1],
        &[0],
        false,
    );
    assert_layout(
        &keys,
        vec![
            key,
            plus_zero(Expr::Aggregate(count())),
            Expr::Aggregate(count()),
        ],
        &[0],
        &[1, 2],
        false,
    );
}
