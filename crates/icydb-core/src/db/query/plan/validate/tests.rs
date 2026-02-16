// NOTE: Invalid helpers remain only for intentionally invalid schemas.
use super::{
    PlanError, SecondaryOrderPushdownEligibility, SecondaryOrderPushdownRejection,
    assess_secondary_order_pushdown, validate_logical_plan_model,
};
use crate::{
    db::query::{
        plan::{AccessPath, AccessPlan, LogicalPlan, OrderDirection, OrderSpec, PageSpec},
        predicate::{SchemaInfo, ValidateError},
    },
    model::{
        entity::EntityModel,
        field::{EntityFieldKind, EntityFieldModel},
        index::IndexModel,
    },
    test_fixtures::InvalidEntityModelBuilder,
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};

fn field(name: &'static str, kind: EntityFieldKind) -> EntityFieldModel {
    EntityFieldModel { name, kind }
}

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel =
    IndexModel::new("test::idx_tag", "test::IndexStore", &INDEX_FIELDS, false);

crate::test_entity_schema! {
    PlanValidateIndexedEntity,
    id = Ulid,
    path = "plan_validate::IndexedEntity",
    entity_name = "IndexedEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", EntityFieldKind::Ulid),
        ("tag", EntityFieldKind::Text),
        ("rank", EntityFieldKind::Int),
    ],
    indexes = [&INDEX_MODEL],
}

crate::test_entity_schema! {
    PlanValidateListEntity,
    id = Ulid,
    path = "plan_validate::ListEntity",
    entity_name = "ListEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", EntityFieldKind::Ulid),
        ("tags", EntityFieldKind::List(&EntityFieldKind::Text)),
    ],
    indexes = [],
}

// Helper for tests that need the indexed model derived from a typed schema.
fn model_with_index() -> &'static EntityModel {
    <PlanValidateIndexedEntity as EntitySchema>::MODEL
}

#[test]
fn model_rejects_missing_primary_key() {
    // Invalid test scaffolding: models are hand-built to exercise
    // validation failures that helpers intentionally prevent.
    let fields: &'static [EntityFieldModel] =
        Box::leak(vec![field("id", EntityFieldKind::Ulid)].into_boxed_slice());
    let missing_pk = Box::leak(Box::new(field("missing", EntityFieldKind::Ulid)));

    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        missing_pk,
        fields,
        &[],
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::InvalidPrimaryKey { .. })
    ));
}

#[test]
fn model_rejects_duplicate_fields() {
    let model = InvalidEntityModelBuilder::from_fields(
        vec![
            field("dup", EntityFieldKind::Text),
            field("dup", EntityFieldKind::Text),
        ],
        0,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::DuplicateField { .. })
    ));
}

#[test]
fn model_rejects_invalid_primary_key_type() {
    let model = InvalidEntityModelBuilder::from_fields(
        vec![field("pk", EntityFieldKind::List(&EntityFieldKind::Text))],
        0,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::InvalidPrimaryKeyType { .. })
    ));
}

#[test]
fn model_rejects_index_unknown_field() {
    const INDEX_FIELDS: [&str; 1] = ["missing"];
    const INDEX_MODEL: IndexModel = IndexModel::new(
        "test::idx_missing",
        "test::IndexStore",
        &INDEX_FIELDS,
        false,
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [EntityFieldModel] =
        Box::leak(vec![field("id", EntityFieldKind::Ulid)].into_boxed_slice());
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::IndexFieldUnknown { .. })
    ));
}

#[test]
fn model_rejects_index_non_queryable_field() {
    const INDEX_FIELDS: [&str; 1] = ["broken"];
    const INDEX_MODEL: IndexModel =
        IndexModel::new("test::idx_broken", "test::IndexStore", &INDEX_FIELDS, false);
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [EntityFieldModel] = Box::leak(
        vec![
            field("id", EntityFieldKind::Ulid),
            field("broken", EntityFieldKind::Structured { queryable: false }),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::IndexFieldNotQueryable { .. })
    ));
}

#[test]
fn model_rejects_index_map_field_in_0_7_x() {
    const INDEX_FIELDS: [&str; 1] = ["attributes"];
    const INDEX_MODEL: IndexModel = IndexModel::new(
        "test::idx_attributes",
        "test::IndexStore",
        &INDEX_FIELDS,
        false,
    );
    const INDEXES: [&IndexModel; 1] = [&INDEX_MODEL];

    let fields: &'static [EntityFieldModel] = Box::leak(
        vec![
            field("id", EntityFieldKind::Ulid),
            field(
                "attributes",
                EntityFieldKind::Map {
                    key: &EntityFieldKind::Text,
                    value: &EntityFieldKind::Uint,
                },
            ),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::IndexFieldMapNotQueryable { .. })
    ));
}

#[test]
fn model_rejects_duplicate_index_names() {
    const INDEX_FIELDS_A: [&str; 1] = ["id"];
    const INDEX_FIELDS_B: [&str; 1] = ["other"];
    const INDEX_A: IndexModel = IndexModel::new(
        "test::dup_index",
        "test::IndexStore",
        &INDEX_FIELDS_A,
        false,
    );
    const INDEX_B: IndexModel = IndexModel::new(
        "test::dup_index",
        "test::IndexStore",
        &INDEX_FIELDS_B,
        false,
    );
    const INDEXES: [&IndexModel; 2] = [&INDEX_A, &INDEX_B];

    let fields: &'static [EntityFieldModel] = Box::leak(
        vec![
            field("id", EntityFieldKind::Ulid),
            field("other", EntityFieldKind::Text),
        ]
        .into_boxed_slice(),
    );
    let model = InvalidEntityModelBuilder::from_static(
        "test::Entity",
        "TestEntity",
        &fields[0],
        fields,
        &INDEXES,
    );

    assert!(matches!(
        SchemaInfo::from_entity_model(&model),
        Err(ValidateError::DuplicateIndexName { .. })
    ));
}

#[test]
fn plan_rejects_unorderable_field() {
    let model = <PlanValidateListEntity as EntitySchema>::MODEL;

    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
        access: AccessPlan::Path(AccessPath::FullScan),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![("tags".to_string(), OrderDirection::Asc)],
        }),
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    let err = validate_logical_plan_model(&schema, model, &plan).expect_err("unorderable field");
    assert!(matches!(err, PlanError::UnorderableField { .. }));
}

#[test]
fn plan_rejects_index_prefix_too_long() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
        access: AccessPlan::Path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
        }),
        predicate: None,
        order: None,
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    let err =
        validate_logical_plan_model(&schema, model, &plan).expect_err("index prefix too long");
    assert!(matches!(err, PlanError::IndexPrefixTooLong { .. }));
}

#[test]
fn plan_rejects_empty_index_prefix() {
    let model = model_with_index();
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
        access: AccessPlan::Path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![],
        }),
        predicate: None,
        order: None,
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    let err = validate_logical_plan_model(&schema, model, &plan).expect_err("index prefix empty");
    assert!(matches!(err, PlanError::IndexPrefixEmpty));
}

#[test]
fn plan_accepts_model_based_validation() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
        access: AccessPlan::Path(AccessPath::ByKey(Value::Ulid(Ulid::nil()))),
        predicate: None,
        order: None,
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    validate_logical_plan_model(&schema, model, &plan).expect("valid plan");
}

#[test]
fn plan_rejects_unordered_pagination() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
        access: AccessPlan::Path(AccessPath::FullScan),
        predicate: None,
        order: None,
        delete_limit: None,
        page: Some(PageSpec {
            limit: Some(10),
            offset: 2,
        }),
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    let err = validate_logical_plan_model(&schema, model, &plan)
        .expect_err("pagination without ordering must be rejected");
    assert!(matches!(err, PlanError::UnorderedPagination));
}

#[test]
fn plan_accepts_ordered_pagination() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
        access: AccessPlan::Path(AccessPath::FullScan),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        }),
        delete_limit: None,
        page: Some(PageSpec {
            limit: Some(10),
            offset: 2,
        }),
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    validate_logical_plan_model(&schema, model, &plan).expect("ordered pagination is valid");
}

#[test]
fn plan_rejects_order_without_terminal_primary_key_tie_break() {
    let model = <PlanValidateIndexedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
        access: AccessPlan::Path(AccessPath::FullScan),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![("tag".to_string(), OrderDirection::Asc)],
        }),
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    let err = validate_logical_plan_model(&schema, model, &plan).expect_err("missing PK tie-break");
    assert!(matches!(err, PlanError::MissingPrimaryKeyTieBreak { .. }));
}

#[test]
fn secondary_order_pushdown_accepts_index_prefix_with_pk_only_order() {
    let model = model_with_index();
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
        access: AccessPlan::Path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![Value::Text("a".to_string())],
        }),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        }),
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    assert_eq!(
        assess_secondary_order_pushdown(model, &plan),
        SecondaryOrderPushdownEligibility::Eligible {
            index: INDEX_MODEL.name,
            prefix_len: 1,
        }
    );
}

#[test]
fn secondary_order_pushdown_rejects_non_index_order_field() {
    let model = model_with_index();
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
        access: AccessPlan::Path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![Value::Text("a".to_string())],
        }),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        }),
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    let eligibility = assess_secondary_order_pushdown(model, &plan);
    assert!(matches!(
        eligibility,
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex { .. }
        )
    ));
}

#[test]
fn secondary_order_pushdown_rejects_full_scan_access_path() {
    let model = model_with_index();
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
        access: AccessPlan::Path(AccessPath::FullScan),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        }),
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    assert_eq!(
        assess_secondary_order_pushdown(model, &plan),
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix
        )
    );
}

#[test]
fn secondary_order_pushdown_rejects_descending_primary_key() {
    let model = model_with_index();
    let plan: LogicalPlan<Value> = LogicalPlan {
        mode: crate::db::query::QueryMode::Load(crate::db::query::LoadSpec::new()),
        access: AccessPlan::Path(AccessPath::IndexPrefix {
            index: INDEX_MODEL,
            values: vec![Value::Text("a".to_string())],
        }),
        predicate: None,
        order: Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        }),
        delete_limit: None,
        page: None,
        consistency: crate::db::query::ReadConsistency::MissingOk,
    };

    assert_eq!(
        assess_secondary_order_pushdown(model, &plan),
        SecondaryOrderPushdownEligibility::Rejected(
            SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending {
                field: "id".to_string(),
            }
        )
    );
}
