use super::{residual_burden_for_candidate, residual_burden_for_plan};
use crate::db::{
    access::AccessPlan,
    predicate::{MissingRowPolicy, Predicate},
    query::plan::{
        AccessPlannedQuery, LogicalPlan,
        expr::{Expr, FieldId, FieldPath, ProjectionSelection},
    },
};
use std::{hint::black_box, time::Instant};

fn candidate_fixture(width: usize) -> AccessPlannedQuery {
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Error);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!("scalar fixture");
    };
    scalar.predicate = Some(Predicate::And(
        (0..width)
            .map(|index| Predicate::IsNull {
                field: format!("field_{index}"),
            })
            .collect(),
    ));
    scalar.filter_expr = Some(Expr::Field(FieldId::new("enabled")));
    plan.projection_selection = ProjectionSelection::Fields(
        (0..width)
            .map(|index| FieldId::new(format!("field_{index}")))
            .collect(),
    );
    plan
}

#[test]
fn candidate_residual_matches_plan_semantics_without_mutating_inputs() {
    let access = AccessPlan::full_scan();
    for width in [0, 1, 8] {
        for filter_expr in [
            None,
            Some(Expr::Field(FieldId::new("enabled"))),
            Some(Expr::FieldPath(FieldPath::new(
                FieldId::new("account"),
                vec!["enabled".into()],
            ))),
        ] {
            for covered in [false, true] {
                let mut plan = candidate_fixture(width);
                let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
                    unreachable!("scalar fixture");
                };
                scalar.filter_expr = filter_expr.clone();
                scalar.predicate_covers_filter_expr = covered;
                if width == 0 {
                    scalar.predicate = None;
                }
                let before = plan.clone();
                let expected = residual_burden_for_plan(&plan);
                assert_eq!(residual_burden_for_candidate(&plan, &access), expected);
                assert_eq!(plan, before);
                assert_eq!(residual_burden_for_candidate(&plan, &access), expected);
            }
        }
    }
}

#[test]
fn candidate_residual_preserves_predicate_and_expression_categories() {
    let mut plan = candidate_fixture(8);
    let access = AccessPlan::full_scan();
    let mixed = residual_burden_for_candidate(&plan, &access);
    assert_eq!((mixed.kind_rank, mixed.predicate_term_count), (2, 8));

    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!("scalar fixture");
    };
    scalar.predicate_covers_filter_expr = true;
    let predicate = residual_burden_for_candidate(&plan, &access);
    assert_eq!(
        (predicate.kind_rank, predicate.predicate_term_count),
        (1, 8)
    );
    assert!(predicate < mixed);

    let empty = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Error);
    assert!(residual_burden_for_candidate(&empty, &access).is_empty());
}

// Isolate residual comparison from schema admission, route construction and
// row execution. Fixture creation is deliberately outside the timed loop.
#[test]
#[ignore = "manual native candidate residual comparison timing"]
fn candidate_residual_native_timing() {
    const ITERATIONS: usize = 20_000;
    for width in [1, 8, 64] {
        let plan = candidate_fixture(width);
        let access = AccessPlan::full_scan();
        for sample in 0..5 {
            let started = Instant::now();
            for _ in 0..ITERATIONS {
                black_box(residual_burden_for_candidate(
                    black_box(&plan),
                    black_box(&access),
                ));
            }
            eprintln!(
                "candidate_residual width={width} sample={sample} ns/op={}",
                started.elapsed().as_nanos() / ITERATIONS as u128,
            );
        }
    }
}
