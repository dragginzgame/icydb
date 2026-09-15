//! Grouped layout classification preserves output positions and identity eligibility.

use super::planned_projection_layout_and_aggregate_specs_from_spec;
use crate::{
    db::{
        query::{
            builder::count,
            plan::{
                FieldSlot, GroupAggregateSpec, GroupField, GroupFieldSet,
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
