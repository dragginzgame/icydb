/*
use super::*;
use crate::{
    db::query::{
        FieldRef, ReadConsistency,
        expr::{FilterExpr, SortExpr},
        plan::{ExplainAccessPath, OrderDirection, OrderSpec, PlanError},
        predicate::{CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate},
    },
    traits::{
        FieldValues, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, View, Visitable,
    },
    types::{Ulid, Unit},
    value::Value,
};
use serde::{Deserialize, Serialize};

/// UnitEntity
/// Test-only entity with a unit primary key.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct UnitEntity {
    id: Unit,
}

crate::test_entity! {
    entity UnitEntity {
        path: "planner_test::UnitEntity",
        pk: id: Unit,

        fields { id: Unit }
    }
}

impl View for UnitEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for UnitEntity {}
impl SanitizeCustom for UnitEntity {}
impl ValidateAuto for UnitEntity {}
impl ValidateCustom for UnitEntity {}
impl Visitable for UnitEntity {}

impl FieldValues for UnitEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(Value::Unit),
            _ => None,
        }
    }
}

/// UnorderableEntity
/// Test-only entity with a non-orderable list field.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct UnorderableEntity {
    id: Ulid,
    tags: Vec<String>,
}

crate::test_entity! {
    entity UnorderableEntity {
        path: "planner_test::UnorderableEntity",
        pk: id: Ulid,

        fields { id: Ulid, tags: List<Text> }
    }
}

impl View for UnorderableEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl SanitizeAuto for UnorderableEntity {}
impl SanitizeCustom for UnorderableEntity {}
impl ValidateAuto for UnorderableEntity {}
impl ValidateCustom for UnorderableEntity {}
impl Visitable for UnorderableEntity {}

impl FieldValues for UnorderableEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(Value::Ulid(self.id)),
            "tags" => Some(Value::List(
                self.tags
                    .iter()
                    .map(|tag| Value::Text(tag.clone()))
                    .collect(),
            )),
            _ => None,
        }
    }
}

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
fn text_eq_ci_uses_text_casefold() {
    let predicate = FieldRef::new("name").text_eq_ci("ICE");
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
fn delete_limit_requires_non_empty_order() {
    let err = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .sort_expr(SortExpr { fields: vec![] });

    assert!(matches!(
        err,
        Err(QueryError::Intent(IntentError::EmptyOrderSpec))
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
fn many_plans_as_primary_key_access() {
    let keys = vec![Ulid::from_u128(1), Ulid::from_u128(2)];
    let expected = vec![
        Value::Ulid(Ulid::from_u128(1)),
        Value::Ulid(Ulid::from_u128(2)),
    ];
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk).by_keys(keys);

    let plan = query.plan().expect("plan");
    let explain = plan.explain();

    assert!(matches!(
        explain.access,
        ExplainAccessPath::ByKeys { keys: access_keys } if access_keys == expected
    ));
}

#[test]
fn many_empty_plans_as_primary_key_access() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk).by_keys(Vec::new());
    let plan = query.plan().expect("plan");

    assert!(matches!(
        plan.explain().access,
        ExplainAccessPath::ByKeys { keys } if keys.is_empty()
    ));
}

#[test]
fn many_rejects_predicates() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .by_keys(vec![Ulid::from_u128(1)])
        .filter(FieldRef::new("other").eq("x"));

    let err = query.plan().expect_err("many with predicate");
    assert!(matches!(
        err,
        QueryError::Intent(IntentError::ManyWithPredicate)
    ));
}

#[test]
fn only_plans_without_schema_initialization() {
    let plan = Query::<UnitEntity>::new(ReadConsistency::MissingOk)
        .only()
        .plan()
        .expect("plan");

    assert!(matches!(
        plan.explain().access,
        ExplainAccessPath::ByKey { key } if key == Value::Unit
    ));
}

#[test]
fn only_rejects_predicates() {
    let query = Query::<UnitEntity>::new(ReadConsistency::MissingOk)
        .only()
        .filter(FieldRef::new("id").eq(()));

    let err = query.plan().expect_err("only with predicate");
    assert!(matches!(
        err,
        QueryError::Intent(IntentError::OnlyWithPredicate)
    ));
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
fn filter_expr_rejects_unknown_field() {
    let expr = FilterExpr(Predicate::IsNull {
        field: "missing".to_string(),
    });

    let err = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter_expr(expr)
        .expect_err("unknown field");

    assert!(matches!(
        err,
        QueryError::Validate(ValidateError::UnknownField { .. })
    ));
}

#[test]
fn sort_expr_rejects_unknown_field() {
    let expr = SortExpr {
        fields: vec![("missing".to_string(), OrderDirection::Asc)],
    };

    let err = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .sort_expr(expr)
        .expect_err("unknown order field");

    assert!(matches!(
        err,
        QueryError::Plan(PlanError::UnknownOrderField { .. })
    ));
}

#[test]
fn sort_expr_rejects_unorderable_field() {
    let expr = SortExpr {
        fields: vec![("tags".to_string(), OrderDirection::Asc)],
    };

    let err = Query::<UnorderableEntity>::new(ReadConsistency::MissingOk)
        .sort_expr(expr)
        .expect_err("unorderable field");

    assert!(matches!(
        err,
        QueryError::Plan(PlanError::UnorderableField { .. })
    ));
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
*/
