use super::*;
use crate::db::query::{
    ReadConsistency, eq, eq_ci, gt,
    plan::{ExplainAccessPath, OrderDirection, OrderSpec, PlanError, planner::PlannerEntity},
    predicate::{CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate},
};
use crate::{
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
        index::IndexModel,
    },
    traits::{
        CanisterKind, EntityKind, FieldValues, Path, SanitizeAuto, SanitizeCustom, StoreKind,
        ValidateAuto, ValidateCustom, View, ViewError, Visitable,
    },
    types::Ulid,
    value::Value,
};
use serde::{Deserialize, Serialize};

#[test]
fn fluent_chain_builds_predicate_tree() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(eq("name", "ice"))
        .filter(gt("age", 10))
        .filter(Predicate::IsNull {
            field: "deleted_at".to_string(),
        });

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
    let predicate = eq_ci("name", "ICE");
    let Predicate::Compare(cmp) = predicate else {
        panic!("expected compare predicate");
    };

    assert_eq!(cmp.op, CompareOp::Eq);
    assert_eq!(cmp.coercion.id, CoercionId::TextCasefold);
}

#[test]
fn filter_chains_are_nested() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(eq("a", 1))
        .filter(eq("b", 2))
        .filter(eq("c", 3));

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
        .order_by("a")
        .order_by_desc("b");

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
fn page_sets_window() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk).page(25, 10);

    assert_eq!(query.page, Some(Page::new(25, 10)));
}

#[test]
fn delete_limit_requires_order() {
    let err = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .delete_limit(5)
        .delete()
        .plan();

    assert!(matches!(
        err,
        Err(QueryError::Intent(IntentError::DeleteLimitRequiresOrder))
    ));
}

#[test]
fn delete_rejects_pagination() {
    let err = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .page(10, 0)
        .delete()
        .plan();

    assert!(matches!(
        err,
        Err(QueryError::Intent(
            IntentError::DeletePaginationNotSupported
        ))
    ));
}

#[test]
fn delete_rejects_limit_with_pagination() {
    let err = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .page(10, 0)
        .delete_limit(5)
        .delete()
        .plan();

    assert!(matches!(
        err,
        Err(QueryError::Intent(IntentError::DeleteLimitWithPagination))
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
        .filter(eq("id", Value::Ulid(Ulid::default())))
        .filter(eq("idx_a", "alpha"));

    let plan = query.plan().expect("composite plan");
    let explain = plan.explain();
    assert!(matches!(explain.access, ExplainAccessPath::Intersection(_)));
}

#[test]
fn plan_is_deterministic_for_same_query() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(eq("id", Ulid::default()))
        .order_by("idx_a");

    let plan_a = query.plan().expect("first plan");
    let plan_b = query.plan().expect("second plan");

    assert_eq!(plan_a.explain(), plan_b.explain());
}

#[test]
fn plan_is_deterministic_for_equivalent_predicates() {
    let id = Ulid::default();
    let query_a = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(eq("id", id))
        .filter(eq("other", "x"));
    let query_b = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(eq("other", "x"))
        .filter(eq("id", id));

    let plan_a = query_a.plan().expect("plan a");
    let plan_b = query_b.plan().expect("plan b");

    assert_eq!(plan_a.explain(), plan_b.explain());
}

#[test]
fn query_explain_matches_plan() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
        .filter(eq("id", Ulid::default()))
        .order_by("idx_a");

    let plan = query.plan().expect("plan");
    let explain = query.explain().expect("explain");

    assert_eq!(explain, plan.explain());
}

#[test]
fn query_explain_rejects_invalid_order() {
    let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk).order_by("missing");

    let err = query.explain().expect_err("invalid order");

    assert!(matches!(
        err,
        QueryError::Plan(PlanError::UnknownOrderField { .. })
    ));
}

#[test]
fn broken_model_rejected_without_panic() {
    let query =
        Query::<BrokenEntity>::new(ReadConsistency::MissingOk).filter(eq("id", Ulid::default()));

    let Err(err) = query.plan() else {
        panic!("broken model should fail")
    };

    match err {
        QueryError::Validate(_) | QueryError::Plan(_) | QueryError::Intent(_) => {}
        QueryError::Execute(err) => panic!("unexpected execute error: {err}"),
    }
}

const BROKEN_ENTITY_PATH: &str = "planner_test::BrokenEntity";

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct BrokenEntity {
    id: Ulid,
}

impl Path for BrokenEntity {
    const PATH: &'static str = BROKEN_ENTITY_PATH;
}

impl View for BrokenEntity {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        self.clone()
    }

    fn from_view(view: Self::ViewType) -> Result<Self, ViewError> {
        Ok(view)
    }
}

impl SanitizeAuto for BrokenEntity {}
impl SanitizeCustom for BrokenEntity {}
impl ValidateAuto for BrokenEntity {}
impl ValidateCustom for BrokenEntity {}
impl Visitable for BrokenEntity {}

impl FieldValues for BrokenEntity {
    fn get_value(&self, field: &str) -> Option<Value> {
        match field {
            "id" => Some(Value::Ulid(self.id)),
            _ => None,
        }
    }
}

struct BrokenCanister;

impl Path for BrokenCanister {
    const PATH: &'static str = "planner_test::BrokenCanister";
}

impl CanisterKind for BrokenCanister {}

struct BrokenStore;

impl Path for BrokenStore {
    const PATH: &'static str = "planner_test::BrokenStore";
}

impl StoreKind for BrokenStore {
    type Canister = BrokenCanister;
}

const BROKEN_FIELDS: [EntityFieldModel; 1] = [EntityFieldModel {
    name: "id",
    kind: EntityFieldKind::Ulid,
}];
const BROKEN_PK_FIELD: EntityFieldModel = EntityFieldModel {
    name: "missing",
    kind: EntityFieldKind::Ulid,
};
const BROKEN_MODEL: EntityModel = EntityModel {
    path: BROKEN_ENTITY_PATH,
    entity_name: "BrokenEntity",
    primary_key: &BROKEN_PK_FIELD,
    fields: &BROKEN_FIELDS,
    indexes: &[],
};

impl EntityKind for BrokenEntity {
    type PrimaryKey = Ulid;
    type Store = BrokenStore;
    type Canister = BrokenCanister;

    const ENTITY_NAME: &'static str = "BrokenEntity";
    const PRIMARY_KEY: &'static str = "id";
    const FIELDS: &'static [&'static str] = &["id"];
    const INDEXES: &'static [&'static IndexModel] = &[];
    const MODEL: &'static EntityModel = &BROKEN_MODEL;

    fn key(&self) -> crate::key::Key {
        self.id.into()
    }

    fn primary_key(&self) -> Self::PrimaryKey {
        self.id
    }

    fn set_primary_key(&mut self, key: Self::PrimaryKey) {
        self.id = key;
    }
}
