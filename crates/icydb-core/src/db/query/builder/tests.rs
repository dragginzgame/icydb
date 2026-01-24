use super::*;
use crate::db::query::{
    plan::{OrderDirection, OrderSpec, PageSpec, PlanError, cache, planner::PlannerEntity},
    predicate::{CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate},
};
use crate::{
    error::ErrorClass,
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
        index::IndexModel,
    },
    traits::{
        CanisterKind, EntityKind, FieldValues, Path, SanitizeAuto, SanitizeCustom, StoreKind,
        ValidateAuto, ValidateCustom, View, Visitable,
    },
    types::Ulid,
    value::Value,
};
use icydb_schema::{
    build::{get_schema, schema_write},
    node::{
        Canister, Def, Entity, Field, FieldList, Index, Item, ItemTarget, Schema, SchemaNode,
        Store, Type, Value as SchemaValue,
    },
    types::{Cardinality, Primitive, StoreType},
};
use serde::{Deserialize, Serialize};
use std::sync::Once;

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

#[test]
fn builder_rejects_composite_access_plans() {
    init_schema();

    let spec = QueryBuilder::<PlannerEntity>::new()
        .filter(eq("id", Value::Ulid(Ulid::default())))
        .and(eq("idx_a", "alpha"))
        .build();

    let schema = get_schema().expect("schema ready");
    let err = spec
        .plan::<PlannerEntity>(&schema)
        .expect_err("composite plan");

    assert!(err.message.contains("composite access plan"));
}

#[test]
fn logical_plan_is_deterministic_for_same_query() {
    let spec = QueryBuilder::<PlannerEntity>::new()
        .filter(eq("id", Ulid::default()))
        .order_by("idx_a")
        .build();

    let schema = Schema::new();
    let plan_a = spec.plan::<PlannerEntity>(&schema).expect("first plan");
    let plan_b = spec.plan::<PlannerEntity>(&schema).expect("second plan");

    assert_eq!(plan_a, plan_b);
}

#[test]
fn logical_plan_is_deterministic_for_equivalent_predicates() {
    let id = Ulid::default();
    let spec_a = QueryBuilder::<PlannerEntity>::new()
        .filter(eq("id", id))
        .and(eq("other", "x"))
        .build();
    let spec_b = QueryBuilder::<PlannerEntity>::new()
        .filter(eq("other", "x"))
        .and(eq("id", id))
        .build();

    let schema = Schema::new();
    let plan_a = spec_a.plan::<PlannerEntity>(&schema).expect("plan a");
    let plan_b = spec_b.plan::<PlannerEntity>(&schema).expect("plan b");

    assert_eq!(plan_a, plan_b);
}

#[test]
fn query_explain_matches_plan() {
    let spec = QueryBuilder::<PlannerEntity>::new()
        .filter(eq("id", Ulid::default()))
        .order_by("idx_a")
        .build();

    let schema = Schema::new();
    let plan = spec.plan::<PlannerEntity>(&schema).expect("plan");
    let explain = spec.explain::<PlannerEntity>(&schema).expect("explain");

    assert_eq!(explain.explain, plan.explain());
    assert_eq!(explain.fingerprint, plan.fingerprint());
}

#[test]
fn query_explain_rejects_invalid_order() {
    let spec = QueryBuilder::<PlannerEntity>::new()
        .order_by("missing")
        .build();

    let schema = Schema::new();
    let err = spec
        .explain::<PlannerEntity>(&schema)
        .expect_err("invalid order");

    assert!(matches!(
        err,
        QueryError::Plan(PlanError::UnknownOrderField { .. })
    ));
}

#[test]
fn query_explain_is_deterministic_for_equivalent_queries() {
    let id = Ulid::default();
    let spec_a = QueryBuilder::<PlannerEntity>::new()
        .filter(eq("id", id))
        .and(eq("other", "x"))
        .build();
    let spec_b = QueryBuilder::<PlannerEntity>::new()
        .filter(eq("other", "x"))
        .and(eq("id", id))
        .build();

    let schema = Schema::new();
    let explain_a = spec_a.explain::<PlannerEntity>(&schema).expect("explain a");
    let explain_b = spec_b.explain::<PlannerEntity>(&schema).expect("explain b");

    assert_eq!(explain_a, explain_b);
}

#[test]
fn plan_cache_hits_for_same_query() {
    cache::with_cache_enabled(|| {
        cache::reset();

        let spec = QueryBuilder::<PlannerEntity>::new()
            .filter(eq("id", Ulid::default()))
            .build();

        let schema = Schema::new();
        let plan_a = spec.plan::<PlannerEntity>(&schema).expect("plan a");
        let plan_b = spec.plan::<PlannerEntity>(&schema).expect("plan b");

        let stats = cache::stats();
        assert_eq!(stats.size, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    });
}

#[test]
fn plan_cache_hits_for_equivalent_queries() {
    cache::with_cache_enabled(|| {
        cache::reset();

        let id = Ulid::default();
        let spec_a = QueryBuilder::<PlannerEntity>::new()
            .filter(eq("id", id))
            .and(eq("other", "x"))
            .build();
        let spec_b = QueryBuilder::<PlannerEntity>::new()
            .filter(eq("other", "x"))
            .and(eq("id", id))
            .build();

        let schema = Schema::new();
        let plan_a = spec_a.plan::<PlannerEntity>(&schema).expect("plan a");
        let plan_b = spec_b.plan::<PlannerEntity>(&schema).expect("plan b");

        let stats = cache::stats();
        assert_eq!(stats.size, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    });
}

#[test]
fn plan_cache_misses_for_different_queries() {
    cache::with_cache_enabled(|| {
        cache::reset();

        let spec_a = QueryBuilder::<PlannerEntity>::new()
            .filter(eq("id", Ulid::default()))
            .build();
        let spec_b = QueryBuilder::<PlannerEntity>::new()
            .filter(eq("other", "x"))
            .build();

        let schema = Schema::new();
        let _ = spec_a.plan::<PlannerEntity>(&schema).expect("plan a");
        let _ = spec_b.plan::<PlannerEntity>(&schema).expect("plan b");

        let stats = cache::stats();
        assert_eq!(stats.size, 2);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 2);
    });
}

#[test]
fn invalid_queries_do_not_populate_cache() {
    cache::with_cache_enabled(|| {
        cache::reset();

        let spec = QueryBuilder::<PlannerEntity>::new()
            .order_by("missing")
            .build();

        let schema = Schema::new();
        let _ = spec.explain::<PlannerEntity>(&schema).expect_err("invalid");

        let stats = cache::stats();
        assert_eq!(stats.size, 0);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
    });
}

#[test]
fn broken_model_rejected_without_panic() {
    let spec = QueryBuilder::<BrokenEntity>::new()
        .filter(eq("id", Ulid::default()))
        .build();

    let schema = Schema::new();
    let err = spec
        .plan::<BrokenEntity>(&schema)
        .expect_err("broken model should fail");

    assert_eq!(err.class, ErrorClass::Unsupported);
}

const TEST_MODULE: &str = "planner_test";
const CANISTER_PATH: &str = "planner_test::PlannerCanister";
const DATA_STORE_PATH: &str = "planner_test::PlannerData";
const INDEX_STORE_PATH: &str = "planner_test::PlannerIndex";

static INIT_SCHEMA: Once = Once::new();

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

    fn from_view(view: Self::ViewType) -> Self {
        view
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

#[allow(clippy::too_many_lines)]
fn init_schema() {
    INIT_SCHEMA.call_once(|| {
        static INDEX_FIELDS: [&str; 2] = ["idx_a", "idx_b"];
        static INDEXES: [Index; 1] = [Index {
            store: INDEX_STORE_PATH,
            fields: &INDEX_FIELDS,
            unique: false,
        }];

        static FIELDS: [Field; 4] = [
            Field {
                ident: "id",
                value: SchemaValue {
                    cardinality: Cardinality::One,
                    item: Item {
                        target: ItemTarget::Primitive(Primitive::Ulid),
                        relation: None,
                        validators: &[],
                        sanitizers: &[],
                        indirect: false,
                    },
                },
                default: None,
            },
            Field {
                ident: "idx_a",
                value: SchemaValue {
                    cardinality: Cardinality::One,
                    item: Item {
                        target: ItemTarget::Primitive(Primitive::Text),
                        relation: None,
                        validators: &[],
                        sanitizers: &[],
                        indirect: false,
                    },
                },
                default: None,
            },
            Field {
                ident: "idx_b",
                value: SchemaValue {
                    cardinality: Cardinality::One,
                    item: Item {
                        target: ItemTarget::Primitive(Primitive::Text),
                        relation: None,
                        validators: &[],
                        sanitizers: &[],
                        indirect: false,
                    },
                },
                default: None,
            },
            Field {
                ident: "other",
                value: SchemaValue {
                    cardinality: Cardinality::One,
                    item: Item {
                        target: ItemTarget::Primitive(Primitive::Text),
                        relation: None,
                        validators: &[],
                        sanitizers: &[],
                        indirect: false,
                    },
                },
                default: None,
            },
        ];

        let mut schema = schema_write();
        if schema.get_node(PlannerEntity::PATH).is_some() {
            return;
        }

        let canister = Canister {
            def: Def {
                module_path: TEST_MODULE,
                ident: "PlannerCanister",
                comments: None,
            },
            memory_min: 0,
            memory_max: 1,
        };

        let data_store = Store {
            def: Def {
                module_path: TEST_MODULE,
                ident: "PlannerData",
                comments: None,
            },
            ident: "PLANNER_DATA",
            ty: StoreType::Data,
            canister: CANISTER_PATH,
            memory_id: 0,
        };

        let index_store = Store {
            def: Def {
                module_path: TEST_MODULE,
                ident: "PlannerIndex",
                comments: None,
            },
            ident: "PLANNER_INDEX",
            ty: StoreType::Index,
            canister: CANISTER_PATH,
            memory_id: 1,
        };

        let entity = Entity {
            def: Def {
                module_path: TEST_MODULE,
                ident: "PlannerEntity",
                comments: None,
            },
            store: DATA_STORE_PATH,
            primary_key: "id",
            name: None,
            indexes: &INDEXES,
            fields: FieldList { fields: &FIELDS },
            ty: Type {
                sanitizers: &[],
                validators: &[],
            },
        };

        schema.insert_node(SchemaNode::Canister(canister));
        schema.insert_node(SchemaNode::Store(data_store));
        schema.insert_node(SchemaNode::Store(index_store));
        schema.insert_node(SchemaNode::Entity(entity));
    });
}
