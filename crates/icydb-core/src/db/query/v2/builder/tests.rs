use super::*;
use crate::db::query::v2::plan::planner::PlannerEntity;
use crate::db::query::v2::{
    plan::{OrderDirection, OrderSpec, PageSpec},
    predicate::{CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate},
};
use crate::{traits::Path, types::Ulid, value::Value};
use icydb_schema::{
    build::{get_schema, schema_write},
    node::{
        Canister, Def, Entity, Field, FieldList, Index, Item, ItemTarget, SchemaNode, Store, Type,
        Value as SchemaValue,
    },
    types::{Cardinality, Primitive, StoreType},
};
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

const TEST_MODULE: &str = "planner_test";
const CANISTER_PATH: &str = "planner_test::PlannerCanister";
const DATA_STORE_PATH: &str = "planner_test::PlannerData";
const INDEX_STORE_PATH: &str = "planner_test::PlannerIndex";

static INIT_SCHEMA: Once = Once::new();

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
