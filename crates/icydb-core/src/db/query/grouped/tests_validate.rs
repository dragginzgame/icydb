use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        contracts::{ReadConsistency, SchemaInfo},
        query::{
            grouped::{
                GroupAggregateKind, GroupAggregateSpec, GroupPlanError, GroupSpec,
                GroupedExecutionConfig, GroupedPlan, validate_group_query_semantics,
            },
            intent::{LoadSpec, QueryMode},
            plan::validate::PlanError,
            plan::{AccessPlannedQuery, LogicalPlan},
        },
    },
    model::{field::FieldKind, index::IndexModel},
    traits::EntitySchema,
    types::Ulid,
    value::Value,
};

const INDEX_FIELDS: [&str; 1] = ["tag"];
const INDEX_MODEL: IndexModel =
    IndexModel::new("test::idx_tag", "test::IndexStore", &INDEX_FIELDS, false);

crate::test_entity! {
    ident = PlanValidateGroupedEntity,
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

fn load_plan(access: AccessPlan<Value>) -> AccessPlannedQuery<Value> {
    AccessPlannedQuery {
        logical: LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        },
        access,
    }
}

fn grouped_plan(
    base: AccessPlannedQuery<Value>,
    group_fields: Vec<&str>,
    aggregates: Vec<GroupAggregateSpec>,
) -> GroupedPlan<Value> {
    GroupedPlan::from_parts(
        base,
        GroupSpec {
            group_fields: group_fields.into_iter().map(str::to_string).collect(),
            aggregates,
            execution: GroupedExecutionConfig::unbounded(),
        },
    )
}

#[test]
fn grouped_plan_rejects_empty_group_fields() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        Vec::new(),
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("empty group-fields spec must fail");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::EmptyGroupFields
    )));
}

#[test]
fn grouped_plan_rejects_unknown_group_field() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["missing_group_field"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("unknown group field must fail");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::UnknownGroupField { field } if field == "missing_group_field"
    )));
}

#[test]
fn grouped_plan_rejects_empty_aggregate_spec_list() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        Vec::new(),
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("empty grouped aggregate list must fail");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::EmptyAggregates
    )));
}

#[test]
fn grouped_plan_rejects_unknown_aggregate_target_field() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Min,
            target_field: Some("missing_target".to_string()),
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("unknown grouped aggregate target field must fail");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::UnknownAggregateTargetField { index, field }
            if *index == 0 && field == "missing_target"
    )));
}

#[test]
fn grouped_plan_rejects_field_target_non_extrema_kind() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let grouped = grouped_plan(
        load_plan(AccessPlan::path(AccessPath::FullScan)),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: Some("rank".to_string()),
        }],
    );

    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("field-target grouped non-extrema terminal must fail");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::FieldTargetRequiresExtrema { index, kind, field }
            if *index == 0 && kind == "Count" && field == "rank"
    )));
}
