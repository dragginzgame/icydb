use super::*;
use crate::db::query::v2::plan::planner::PlannerEntity;
use crate::db::query::v2::{
    plan::{OrderDirection, OrderSpec, PageSpec},
    predicate::{CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate},
};
use crate::value::Value;

#[test]
fn fluent_chain_builds_predicate_tree() {
    let spec = QueryBuilder::<PlannerEntity>::new()
        .filter(eq("name", "ice"))
        .and(gt("age", 10))
        .or(is_null("deleted_at"))
        .build();

    let expected = Predicate::Or(vec![
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

    assert_eq!(spec.predicate, Some(expected));
}

#[test]
fn eq_ci_uses_text_casefold() {
    let predicate = eq_ci("name", "ICE");
    let Predicate::Compare(cmp) = predicate else {
        panic!("expected compare predicate");
    };

    assert_eq!(cmp.op, CompareOp::Eq);
    assert_eq!(cmp.coercion.id, CoercionId::TextCasefold);
}

#[test]
fn and_chains_are_nested() {
    let spec = QueryBuilder::<PlannerEntity>::new()
        .filter(eq("a", 1))
        .and(eq("b", 2))
        .and(eq("c", 3))
        .build();

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

    assert_eq!(spec.predicate, Some(expected));
}

#[test]
fn order_and_pagination_accumulate() {
    let spec = QueryBuilder::<PlannerEntity>::new()
        .order_by("a")
        .order_by_desc("b")
        .limit(25)
        .offset(10)
        .build();

    assert_eq!(
        spec.order,
        Some(OrderSpec {
            fields: vec![
                ("a".to_string(), OrderDirection::Asc),
                ("b".to_string(), OrderDirection::Desc),
            ],
        })
    );
    assert_eq!(
        spec.page,
        Some(PageSpec {
            limit: Some(25),
            offset: 10,
        })
    );
}

#[test]
fn builder_has_no_planning_access_types() {
    let type_name = std::any::type_name::<QuerySpec>();
    assert!(!type_name.contains("AccessPlan"));
    assert!(!type_name.contains("AccessPath"));
    assert!(!type_name.contains("LogicalPlan"));
}
