use crate::{
    db::{
        access::{AccessPath, AccessPlan},
        contracts::{ReadConsistency, SchemaInfo},
        query::{
            grouped::{
                GroupAggregateKind, GroupAggregateSpec, GroupPlanError, GroupSpec,
                GroupedExecutionConfig, GroupedPlan, grouped_executor_handoff,
                validate_group_query_semantics,
            },
            intent::{LoadSpec, QueryMode},
            plan::validate::{PlanError, PolicyPlanError, validate_query_semantics},
            plan::{AccessPlannedQuery, DeleteLimitSpec, LogicalPlan, OrderDirection, OrderSpec},
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

#[test]
fn grouped_executor_handoff_preserves_group_fields_aggregates_and_execution_config() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = GroupedPlan::from_parts(
        base,
        GroupSpec {
            group_fields: vec!["rank".to_string(), "tag".to_string()],
            aggregates: vec![
                GroupAggregateSpec {
                    kind: GroupAggregateKind::Count,
                    target_field: None,
                },
                GroupAggregateSpec {
                    kind: GroupAggregateKind::Max,
                    target_field: Some("rank".to_string()),
                },
            ],
            execution: GroupedExecutionConfig::with_hard_limits(11, 2048),
        },
    );

    let handoff = grouped_executor_handoff(&grouped);

    assert_eq!(
        handoff.group_fields(),
        &["rank".to_string(), "tag".to_string()]
    );
    assert_eq!(handoff.aggregates().len(), 2);
    assert_eq!(handoff.aggregates()[0].kind, GroupAggregateKind::Count);
    assert_eq!(handoff.aggregates()[0].target_field, None);
    assert_eq!(handoff.aggregates()[1].kind, GroupAggregateKind::Max);
    assert_eq!(
        handoff.aggregates()[1].target_field.as_deref(),
        Some("rank")
    );
    assert_eq!(handoff.execution().max_groups(), 11);
    assert_eq!(handoff.execution().max_group_bytes(), 2048);
    assert_eq!(
        handoff.base().logical.consistency,
        grouped.base.logical.consistency
    );
}

#[test]
fn grouped_executor_handoff_contract_matrix_vectors_are_frozen() {
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped_cases = [
        GroupSpec {
            group_fields: vec!["rank".to_string()],
            aggregates: vec![GroupAggregateSpec {
                kind: GroupAggregateKind::Count,
                target_field: None,
            }],
            execution: GroupedExecutionConfig::unbounded(),
        },
        GroupSpec {
            group_fields: vec!["tag".to_string(), "rank".to_string()],
            aggregates: vec![
                GroupAggregateSpec {
                    kind: GroupAggregateKind::Max,
                    target_field: Some("rank".to_string()),
                },
                GroupAggregateSpec {
                    kind: GroupAggregateKind::Min,
                    target_field: None,
                },
            ],
            execution: GroupedExecutionConfig::with_hard_limits(11, 2048),
        },
    ];

    #[allow(clippy::type_complexity)]
    let actual_vectors: Vec<(
        Vec<String>,
        Vec<(GroupAggregateKind, Option<String>)>,
        u64,
        u64,
    )> = grouped_cases
        .iter()
        .map(|group| {
            let grouped = GroupedPlan::from_parts(base.clone(), group.clone());
            let handoff = grouped_executor_handoff(&grouped);
            let aggregate_vector = handoff
                .aggregates()
                .iter()
                .map(|aggregate| (aggregate.kind, aggregate.target_field.clone()))
                .collect::<Vec<_>>();

            (
                handoff.group_fields().to_vec(),
                aggregate_vector,
                handoff.execution().max_groups(),
                handoff.execution().max_group_bytes(),
            )
        })
        .collect();
    let expected_vectors = vec![
        (
            vec!["rank".to_string()],
            vec![(GroupAggregateKind::Count, None::<String>)],
            u64::MAX,
            u64::MAX,
        ),
        (
            vec!["tag".to_string(), "rank".to_string()],
            vec![
                (GroupAggregateKind::Max, Some("rank".to_string())),
                (GroupAggregateKind::Min, None::<String>),
            ],
            11,
            2048,
        ),
    ];

    assert_eq!(actual_vectors, expected_vectors);
}

#[test]
fn grouped_invalid_spec_does_not_change_scalar_plan_validation_outcome() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let base = load_plan(AccessPlan::path(AccessPath::FullScan));
    let grouped = grouped_plan(
        base.clone(),
        vec!["missing_group_field"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
        }],
    );

    validate_query_semantics(&schema, model, &base)
        .expect("scalar plan validation must not require grouped spec");
    let err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("grouped validation must enforce grouped spec");
    assert!(matches!(err, PlanError::Group(inner) if matches!(
        inner.as_ref(),
        GroupPlanError::UnknownGroupField { field } if field == "missing_group_field"
    )));
}

#[test]
fn grouped_validation_preserves_scalar_policy_errors_on_base_plan() {
    let model = <PlanValidateGroupedEntity as EntitySchema>::MODEL;
    let schema = SchemaInfo::from_entity_model(model).expect("valid model");
    let mut base = load_plan(AccessPlan::path(AccessPath::FullScan));
    base.logical.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });
    base.logical.delete_limit = Some(DeleteLimitSpec { max_rows: 1 });
    let grouped = grouped_plan(
        base.clone(),
        vec!["rank"],
        vec![GroupAggregateSpec {
            kind: GroupAggregateKind::Count,
            target_field: None,
        }],
    );

    let scalar_err = validate_query_semantics(&schema, model, &base)
        .expect_err("invalid scalar base plan must fail scalar policy validation");
    assert!(matches!(scalar_err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PolicyPlanError::LoadPlanWithDeleteLimit
    )));
    let grouped_err = validate_group_query_semantics(&schema, model, &grouped)
        .expect_err("grouped validation must preserve scalar base-plan policy failures");
    assert!(matches!(grouped_err, PlanError::Policy(inner) if matches!(
        inner.as_ref(),
        PolicyPlanError::LoadPlanWithDeleteLimit
    )));
}
