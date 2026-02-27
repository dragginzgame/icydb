use crate::{
    db::{
        access::{AccessPath, AccessPlan, AccessPlanError},
        contracts::{ReadConsistency, SchemaInfo},
        query::{
            intent::{DeleteSpec, LoadSpec, QueryMode},
            plan::validate::{
                OrderPlanError, PlanError, PolicyPlanError, validate_query_semantics,
            },
            plan::{
                AccessPlannedQuery, DeleteLimitSpec, LogicalPlan, OrderDirection, OrderSpec,
                PageSpec,
            },
        },
    },
    model::{entity::EntityModel, field::FieldKind, index::IndexModel},
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel =
    IndexModel::new("test::idx_tag", "test::IndexStore", &INDEX_FIELDS, false);

crate::test_entity! {
    ident = PlanValidateIndexedEntity,
    id = Ulid,
    entity_name = "IndexedEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tag", FieldKind::Text),
        ("rank", FieldKind::Int),
    ],
    indexes = [&INDEX_MODEL],
}

crate::test_entity! {
    ident = PlanValidateListEntity,
    id = Ulid,
    entity_name = "ListEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tags", FieldKind::List(&FieldKind::Text)),
    ],
    indexes = [],
}

fn model_with_index() -> &'static EntityModel {
    <PlanValidateIndexedEntity as EntitySchema>::MODEL
}

#[test]
fn plan_rejects_unorderable_field() {
    let model = <PlanValidateListEntity as EntitySchema>::MODEL;

    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("tags".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        },
        access: AccessPlan::path(AccessPath::FullScan),
    };

    let err = validate_query_semantics(&schema, model, &plan).expect_err("unorderable field");
    assert!(matches!(err, PlanError::Order(inner) if matches!(
        inner.as_ref(),
        OrderPlanError::UnorderableField { .. }
    )));
}

#[test]
fn plan_rejects_duplicate_non_primary_order_field() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![
                    ("rank".to_string(), OrderDirection::Asc),
                    ("rank".to_string(), OrderDirection::Desc),
                    ("id".to_string(), OrderDirection::Asc),
                ],
            }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        },
        access: AccessPlan::path(AccessPath::FullScan),
    };

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("duplicate non-primary order field must fail");
    assert!(matches!(err, PlanError::Order(inner) if matches!(
        inner.as_ref(),
        OrderPlanError::DuplicateOrderField { field } if field == "rank"
    )));
}

#[test]
fn plan_rejects_index_prefix_too_long() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        },
        access: AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
        }),
    };

    let err = validate_query_semantics(&schema, model, &plan).expect_err("index prefix too long");
    assert!(matches!(err, PlanError::Access(inner) if matches!(
        inner.as_ref(),
        AccessPlanError::IndexPrefixTooLong { .. }
    )));
}

#[test]
fn plan_rejects_empty_index_prefix() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        },
        access: AccessPlan::path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![],
        }),
    };

    let err = validate_query_semantics(&schema, model, &plan).expect_err("index prefix empty");
    assert!(matches!(err, PlanError::Access(inner) if matches!(
        inner.as_ref(),
        AccessPlanError::IndexPrefixEmpty
    )));
}

#[test]
fn plan_accepts_model_based_validation() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        },
        access: AccessPlan::path(AccessPath::ByKey(Value::Ulid(Ulid::nil()))),
    };

    validate_query_semantics(&schema, model, &plan).expect("valid plan");
}

#[test]
fn plan_rejects_empty_order_spec() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec { fields: vec![] }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        },
        access: AccessPlan::path(AccessPath::FullScan),
    };

    let err = validate_query_semantics(&schema, model, &plan).expect_err("empty order must fail");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PolicyPlanError::EmptyOrderSpec
    )));
}

#[test]
fn delete_limit_requires_order() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Delete(DeleteSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: Some(DeleteLimitSpec { max_rows: 10 }),
            page: None,
            consistency: ReadConsistency::MissingOk,
        },
        access: AccessPlan::path(AccessPath::FullScan),
    };

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("delete limit without order must fail");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PolicyPlanError::DeleteLimitRequiresOrder
    )));
}

#[test]
fn delete_plan_rejects_pagination() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Delete(DeleteSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(1),
                offset: 0,
            }),
            consistency: ReadConsistency::MissingOk,
        },
        access: AccessPlan::path(AccessPath::FullScan),
    };

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("delete plans must not carry pagination");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PolicyPlanError::DeletePlanWithPagination
    )));
}

#[test]
fn load_plan_rejects_delete_limit() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: Some(DeleteLimitSpec { max_rows: 1 }),
            page: None,
            consistency: ReadConsistency::MissingOk,
        },
        access: AccessPlan::path(AccessPath::FullScan),
    };

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("load plans must not carry delete limits");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PolicyPlanError::LoadPlanWithDeleteLimit
    )));
}

#[test]
fn plan_rejects_unordered_pagination() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(10),
                offset: 2,
            }),
            consistency: ReadConsistency::MissingOk,
        },
        access: AccessPlan::path(AccessPath::FullScan),
    };

    let err = validate_query_semantics(&schema, model, &plan)
        .expect_err("pagination without ordering must be rejected");
    assert!(matches!(err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PolicyPlanError::UnorderedPagination
    )));
}

#[test]
fn plan_accepts_ordered_pagination() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: Some(PageSpec {
                limit: Some(10),
                offset: 2,
            }),
            consistency: ReadConsistency::MissingOk,
        },
        access: AccessPlan::path(AccessPath::FullScan),
    };

    validate_query_semantics(&schema, model, &plan).expect("ordered pagination is valid");
}

#[test]
fn plan_rejects_order_without_terminal_primary_key_tie_break() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("tag".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        },
        access: AccessPlan::path(AccessPath::FullScan),
    };

    let err = validate_query_semantics(&schema, model, &plan).expect_err("missing PK tie-break");
    assert!(matches!(err, PlanError::Order(inner) if matches!(
        inner.as_ref(),
        OrderPlanError::MissingPrimaryKeyTieBreak { .. }
    )));
}
