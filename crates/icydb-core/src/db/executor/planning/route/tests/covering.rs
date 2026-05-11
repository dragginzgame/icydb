use super::*;

#[test]
fn route_plan_load_terminal_covering_read_contract_requires_coverable_projection() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("rank")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let contract = derive_load_terminal_fast_path_contract_for_test(&projected, true)
        .expect("direct projected indexed field should derive one covering-read route contract");

    let LoadTerminalFastPathContract::CoveringRead(covering) = contract;
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "rank");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::Constant(Value::Uint(7)),
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner,
    );

    let materialized = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    assert!(
        derive_load_terminal_fast_path_contract_for_test(&materialized, true).is_none(),
        "all-field entity projection should stay on the materialized load route",
    );
}

#[test]
fn route_plan_execution_route_plan_retains_covering_read_contract() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: crate::db::access::SemanticIndexAccessContract::model_only_from_generated_index(
                ROUTE_CAPABILITY_INDEX_MODELS[0],
            ),
            values: vec![Value::Uint(7)],
        },
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("rank")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let route_plan = build_load_route_plan(&projected)
        .expect("execution route plan should build for coverable projected load");
    let covering = route_plan
        .load_terminal_fast_path()
        .expect("execution route plan should retain the route-owned covering-read contract");
    let LoadTerminalFastPathContract::CoveringRead(covering) = covering;

    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "rank");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::Constant(Value::Uint(7)),
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner,
    );
}

#[test]
fn route_plan_initial_secondary_covering_is_planner_proven() {
    let plan = secondary_order_covering_plan();
    let route_plan = build_initial_load_route_plan(&plan)
        .expect("initial secondary covering route plan should build");
    let covering = route_plan
        .load_terminal_fast_path()
        .expect("initial secondary covering route should retain a covering-read contract");
    let LoadTerminalFastPathContract::CoveringRead(covering) = covering;

    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner,
        "secondary covering routes should now carry planner-owned visibility semantics directly",
    );
}

#[test]
fn route_plan_initial_composite_secondary_covering_is_planner_proven() {
    let plan = composite_secondary_order_covering_plan(OrderDirection::Asc);
    let route_plan = build_initial_load_route_plan(&plan)
        .expect("initial composite secondary covering route plan should build");
    let covering = route_plan.load_terminal_fast_path().expect(
        "initial composite secondary covering route should retain a covering-read contract",
    );
    let LoadTerminalFastPathContract::CoveringRead(covering) = covering;

    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner,
        "admitted composite secondary covering should share the same planner-proven contract",
    );
}

#[test]
fn route_plan_load_terminal_covering_read_contract_marks_pk_only_full_scan_as_planner_proven() {
    let mut projected =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let contract = derive_load_terminal_fast_path_contract_for_test(&projected, true)
        .expect("PK-only full scan should derive one planner-proven covering-read route contract");

    let LoadTerminalFastPathContract::CoveringRead(covering) = contract;
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner,
    );
}

#[test]
fn route_plan_load_terminal_covering_read_contract_marks_pk_only_key_range_as_planner_proven() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::<Value>::KeyRange {
            start: Value::Ulid(Ulid::from_u128(9_511)),
            end: Value::Ulid(Ulid::from_u128(9_512)),
        },
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let contract = derive_load_terminal_fast_path_contract_for_test(&projected, true)
        .expect("PK-only key range should derive one planner-proven covering-read route contract");

    let LoadTerminalFastPathContract::CoveringRead(covering) = contract;
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner,
    );
}

#[test]
fn route_plan_execution_route_plan_retains_pk_only_planner_proven_covering_contract() {
    let mut projected =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let route_plan = build_load_route_plan(&projected)
        .expect("execution route plan should build for PK-only planner-proven covering load");
    let covering = route_plan
        .load_terminal_fast_path()
        .expect("execution route plan should retain the planner-proven covering-read contract");
    let LoadTerminalFastPathContract::CoveringRead(covering) = covering;

    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner,
    );
}

#[test]
fn route_plan_execution_route_plan_retains_pk_only_key_range_covering_contract() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::<Value>::KeyRange {
            start: Value::Ulid(Ulid::from_u128(9_511)),
            end: Value::Ulid(Ulid::from_u128(9_512)),
        },
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let route_plan = build_load_route_plan(&projected)
        .expect("execution route plan should build for PK-only planner-proven covering key range");
    let covering = route_plan
        .load_terminal_fast_path()
        .expect("execution route plan should retain the planner-proven covering-read contract");
    let LoadTerminalFastPathContract::CoveringRead(covering) = covering;

    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::ProvenByPlanner,
    );
}

#[test]
fn route_plan_load_terminal_covering_read_contract_marks_pk_only_by_key_as_row_check_required() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::<Value>::ByKey(Value::Ulid(Ulid::from_u128(9_511))),
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let contract = derive_load_terminal_fast_path_contract_for_test(&projected, true)
        .expect("PK-only by-key lookup should derive one row-check covering-read route contract");

    let LoadTerminalFastPathContract::CoveringRead(covering) = contract;
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::RequiresRowPresenceCheck,
    );
}

#[test]
fn route_plan_load_terminal_covering_read_contract_marks_pk_only_by_keys_as_row_check_required() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::<Value>::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(9_511)),
            Value::Ulid(Ulid::from_u128(9_513)),
        ]),
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Asc,
        )],
    });

    let contract = derive_load_terminal_fast_path_contract_for_test(&projected, true)
        .expect("PK-only by-keys lookup should derive one row-check covering-read route contract");

    let LoadTerminalFastPathContract::CoveringRead(covering) = contract;
    assert_eq!(covering.fields.len(), 1);
    assert_eq!(covering.fields[0].field_slot.field(), "id");
    assert_eq!(
        covering.fields[0].source,
        CoveringReadFieldSource::PrimaryKey
    );
    assert_eq!(
        covering.existing_row_mode,
        CoveringExistingRowMode::RequiresRowPresenceCheck,
    );
}

#[test]
fn route_plan_load_terminal_covering_read_contract_rejects_pk_only_by_keys_desc_for_now() {
    let mut projected = AccessPlannedQuery::new(
        AccessPath::<Value>::ByKeys(vec![
            Value::Ulid(Ulid::from_u128(9_511)),
            Value::Ulid(Ulid::from_u128(9_513)),
        ]),
        MissingRowPolicy::Ignore,
    );
    projected.projection_selection = ProjectionSelection::Fields(vec![FieldId::new("id")]);
    projected.scalar_plan_mut().order = Some(OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "id",
            OrderDirection::Desc,
        )],
    });

    assert!(
        derive_load_terminal_fast_path_contract_for_test(&projected, true).is_none(),
        "phase-1 multi-key PK covering should stay fail-closed on descending order until exact-key reorder is explicit",
    );
}
