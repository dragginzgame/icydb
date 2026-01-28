use super::*;
use crate::{
    db::query::{
        FieldRef, ReadConsistency,
        plan::{ExplainAccessPath, OrderDirection, OrderSpec, PlanError, planner::PlannerEntity},
        predicate::{CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate},
    },
    types::Ulid,
    value::Value,
};

#[test]
fn fluent_chain_builds_predicate_tree() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(FieldRef::new("name").eq("ice"))
        .filter(FieldRef::new("age").gt(10))
        .filter(FieldRef::new("deleted_at").is_null());

    let expected = Predicate::And(vec![
        Predicate::And(vec![
            Predicate::Compare(ComparePredicate {
                field: "name".to_string(),
                op: CompareOp::Eq,
                value: Value::Text("ice".to_string()),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            Predicate::Compare(ComparePredicate {
                field: "age".to_string(),
                op: CompareOp::Gt,
                value: Value::Int(10),
                coercion: CoercionSpec::new(CoercionId::NumericWiden),
            }),
        ]),
        Predicate::IsNull {
            field: "deleted_at".to_string(),
        },
    ]);

    assert_eq!(query.predicate, Some(expected));
}

#[test]
fn eq_ci_uses_text_casefold() {
    let predicate = FieldRef::new("name").eq_ci("ICE");
    let Predicate::Compare(cmp) = predicate else {
        panic!("expected compare predicate");
    };

    assert_eq!(cmp.op, CompareOp::Eq);
    assert_eq!(cmp.coercion.id, CoercionId::TextCasefold);
}

#[test]
fn filter_chains_are_nested() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(FieldRef::new("a").eq(1))
        .filter(FieldRef::new("b").eq(2))
        .filter(FieldRef::new("c").eq(3));

    let expected = Predicate::And(vec![
        Predicate::And(vec![
            Predicate::Compare(ComparePredicate {
                field: "a".to_string(),
                op: CompareOp::Eq,
                value: Value::Int(1),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
            Predicate::Compare(ComparePredicate {
                field: "b".to_string(),
                op: CompareOp::Eq,
                value: Value::Int(2),
                coercion: CoercionSpec::new(CoercionId::Strict),
            }),
        ]),
        Predicate::Compare(ComparePredicate {
            field: "c".to_string(),
            op: CompareOp::Eq,
            value: Value::Int(3),
            coercion: CoercionSpec::new(CoercionId::Strict),
        }),
    ]);

    assert_eq!(query.predicate, Some(expected));
}

#[test]
fn order_accumulates() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .order_by(FieldRef::new("a"))
        .order_by_desc(FieldRef::new("b"));

    assert_eq!(
        query.order,
        Some(OrderSpec {
            fields: vec![
                ("a".to_string(), OrderDirection::Asc),
                ("b".to_string(), OrderDirection::Desc),
            ],
        })
    );
}

#[test]
fn limit_and_offset_set_window() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .offset(10)
        .limit(25);

    assert!(matches!(
        query.mode,
        QueryMode::Load(LoadSpec {
            limit: Some(25),
            offset: 10,
        })
    ));
}

#[test]
fn delete_limit_requires_order() {
    let err = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .limit(5)
        .plan();

    assert!(matches!(
        err,
        Err(QueryError::Intent(IntentError::DeleteLimitRequiresOrder))
    ));
}

#[test]
fn delete_clears_load_bounds() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .offset(10)
        .limit(5)
        .delete();

    assert!(matches!(
        query.mode,
        QueryMode::Delete(DeleteSpec { limit: None })
    ));
}

#[test]
fn delete_limit_sets_spec() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .limit(5);

    assert!(matches!(
        query.mode,
        QueryMode::Delete(DeleteSpec { limit: Some(5) })
    ));
}

#[test]
fn intent_has_no_planning_access_types() {
    let type_name = std::any::type_name::<Query<PlannerEntity>>();
    assert!(!type_name.contains("AccessPlan"));
    assert!(!type_name.contains("AccessPath"));
    assert!(!type_name.contains("LogicalPlan"));
}

#[test]
fn planning_allows_composite_access_plans() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(FieldRef::new("id").eq(Ulid::default()))
        .filter(FieldRef::new("idx_a").eq("alpha"));

    let plan = query.plan().expect("composite plan");
    let explain = plan.explain();
    assert!(matches!(explain.access, ExplainAccessPath::Intersection(_)));
}

#[test]
fn plan_is_deterministic_for_same_query() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(FieldRef::new("id").eq(Ulid::default()))
        .order_by(FieldRef::new("idx_a"));

    let plan_a = query.plan().expect("first plan");
    let plan_b = query.plan().expect("second plan");

    assert_eq!(plan_a.explain(), plan_b.explain());
}

#[test]
fn plan_is_deterministic_for_equivalent_predicates() {
    let id = Ulid::default();
    let query_a = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(FieldRef::new("id").eq(id))
        .filter(FieldRef::new("other").eq("x"));
    let query_b = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(FieldRef::new("other").eq("x"))
        .filter(FieldRef::new("id").eq(id));

    let plan_a = query_a.plan().expect("plan a");
    let plan_b = query_b.plan().expect("plan b");

    assert_eq!(plan_a.explain(), plan_b.explain());
}

#[test]
fn query_explain_matches_plan() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(FieldRef::new("id").eq(Ulid::default()))
        .order_by(FieldRef::new("idx_a"));

    let plan = query.plan().expect("plan");
    let explain = query.explain().expect("explain");

    assert_eq!(explain, plan.explain());
}

#[test]
fn query_explain_rejects_invalid_order() {
    let query =
        Query::<PlannerEntity>::new(ReadConsistency::MissingOk).order_by(FieldRef::new("missing"));

    let err = query.explain().expect_err("invalid order");

    assert!(matches!(
        err,
        QueryError::Plan(PlanError::UnknownOrderField { .. })
    ));
}
