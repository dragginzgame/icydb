//! Module: db::executor::tests::semantics
//! Responsibility: module-local ownership and contracts for db::executor::tests::semantics.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

#![expect(clippy::similar_names)]
use super::*;
use crate::db::{
    IndexStore,
    data::DataKey,
    query::{
        explain::{ExplainAccessPath, ExplainExecutionNodeType},
        plan::expr::{ProjectionField, ProjectionSpec},
    },
};
use std::collections::{BTreeMap, BTreeSet};

fn id_in_predicate(ids: &[u128]) -> Predicate {
    Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::In,
        Value::List(
            ids.iter()
                .copied()
                .map(|id| Value::Ulid(Ulid::from_u128(id)))
                .collect(),
        ),
        CoercionId::Strict,
    ))
}

// Seed one deterministic expression-index fixture used by parity tests.
fn seed_expression_casefold_parity_rows(base_id: u128) {
    let save = SaveExecutor::<ExpressionCasefoldParityEntity>::new(DB, false);
    for (offset, email, label) in [
        (1_u128, "Alice@Example.Com", "alice"),
        (2_u128, "BOB@example.com", "bob"),
        (3_u128, "carol@example.com", "carol"),
    ] {
        save.insert(ExpressionCasefoldParityEntity {
            id: Ulid::from_u128(base_id.saturating_add(offset)),
            email: email.to_string(),
            label: label.to_string(),
        })
        .expect("seed row save should succeed");
    }
}

// Seed one deterministic unsupported-expression index fixture for parity tests.
fn seed_expression_upper_parity_rows(base_id: u128) {
    let save = SaveExecutor::<ExpressionUpperParityEntity>::new(DB, false);
    for (offset, email, label) in [
        (1_u128, "Alice@Example.Com", "alice"),
        (2_u128, "BOB@example.com", "bob"),
        (3_u128, "carol@example.com", "carol"),
    ] {
        save.insert(ExpressionUpperParityEntity {
            id: Ulid::from_u128(base_id.saturating_add(offset)),
            email: email.to_string(),
            label: label.to_string(),
        })
        .expect("seed row save should succeed");
    }
}

// Assert one verbose diagnostics map selects the expected expression index.
fn assert_expression_index_access_choice_selected(diagnostics: &BTreeMap<String, String>) {
    assert_eq!(
        diagnostics.get(DIAG_ROUTE_ACCESS_CHOICE_CHOSEN),
        Some(&format!(
            "index:{}",
            EXPRESSION_CASEFOLD_PARITY_INDEX_MODELS[0].name()
        )),
        "access-choice must select the same expression index chosen by planner access lowering",
    );
    assert_eq!(
        diagnostics.get(DIAG_ROUTE_ACCESS_CHOICE_CHOSEN_REASON),
        Some(&"single_candidate".to_string()),
        "expression lookup parity matrix expects deterministic single-candidate selection",
    );
}

// Assert one verbose diagnostics map remains on non-index full-scan access choice.
fn assert_full_scan_access_choice_selected(diagnostics: &BTreeMap<String, String>) {
    assert_eq!(
        diagnostics.get(DIAG_ROUTE_ACCESS_CHOICE_CHOSEN),
        Some(&"full_scan".to_string()),
        "unsupported expression lookup must keep full-scan access-choice authority",
    );
    assert_eq!(
        diagnostics.get(DIAG_ROUTE_ACCESS_CHOICE_CHOSEN_REASON),
        Some(&"non_index_access".to_string()),
        "unsupported expression lookup must classify as non-index access",
    );
}

// Remove one pushdown row from the primary store while keeping index entries.
fn remove_pushdown_row_data(id: u128) {
    let raw_key = DataKey::try_new::<PushdownParityEntity>(Ulid::from_u128(id))
        .expect("pushdown data key should build")
        .to_raw()
        .expect("pushdown data key should encode");

    DATA_STORE.with(|store| {
        let removed = store.borrow_mut().remove(&raw_key);
        assert!(
            removed.is_some(),
            "expected pushdown row to exist before data-only removal"
        );
    });
}

fn verbose_diagnostics_lines(verbose: &str) -> Vec<String> {
    verbose
        .lines()
        .filter(|line| line.starts_with("diagnostic."))
        .map(ToOwned::to_owned)
        .collect()
}

fn verbose_diagnostics_map(verbose: &str) -> BTreeMap<String, String> {
    let mut diagnostics = BTreeMap::new();
    for line in verbose_diagnostics_lines(verbose) {
        let Some((key, value)) = line.split_once('=') else {
            panic!("diagnostic line must contain '=': {line}");
        };
        diagnostics.insert(key.to_string(), value.to_string());
    }

    diagnostics
}

fn collect_execution_node_types(
    descriptor: &crate::db::ExplainExecutionNodeDescriptor,
    out: &mut BTreeSet<&'static str>,
) {
    out.insert(descriptor.node_type().as_str());
    for child in descriptor.children() {
        collect_execution_node_types(child, out);
    }
}

fn query_execution_pipeline_snapshot<E>(query: &Query<E>) -> String
where
    E: EntityKind + EntityValue,
{
    // Phase 1: compile query intent into one executor-owned executable plan contract.
    let compiled = query
        .plan()
        .expect("execution pipeline snapshot should build compiled query");
    let executable = crate::db::executor::ExecutablePlan::from(compiled);

    // Phase 2: derive canonical execution descriptor JSON from executable-plan contracts.
    let descriptor_json = executable
        .explain_load_execution_node_descriptor()
        .expect("execution pipeline snapshot should build execution descriptor")
        .render_json_canonical();

    // Phase 3: join executable-plan and explain-descriptor snapshots into one payload.
    [
        executable.render_snapshot_canonical(),
        format!("execution_descriptor_json={descriptor_json}"),
    ]
    .join("\n")
}

fn projection_columns_snapshot(projection: &ProjectionSpec) -> Vec<String> {
    projection
        .fields()
        .enumerate()
        .map(|(index, field)| match field {
            ProjectionField::Scalar { expr, alias } => {
                let alias_label = alias.as_ref().map_or("none", |value| value.as_str());
                format!("column[{index}]::expr={expr:?}::alias={alias_label}")
            }
        })
        .collect()
}

fn query_execution_pipeline_projection_snapshot<E>(query: &Query<E>) -> String
where
    E: EntityKind + EntityValue,
{
    // Phase 1: compile query intent into one executable plan + canonical projection columns.
    let compiled = query
        .plan()
        .expect("execution pipeline projection snapshot should build compiled query");
    let projection_columns = projection_columns_snapshot(&compiled.projection_spec());
    let executable = crate::db::executor::ExecutablePlan::from(compiled);

    // Phase 2: derive canonical execution descriptor JSON from executable-plan contracts.
    let descriptor_json = executable
        .explain_load_execution_node_descriptor()
        .expect("execution pipeline projection snapshot should build execution descriptor")
        .render_json_canonical();

    // Phase 3: join executable-plan, explain-descriptor, and projection-column snapshots.
    [
        executable.render_snapshot_canonical(),
        format!("projection_columns={projection_columns:?}"),
        format!("execution_descriptor_json={descriptor_json}"),
    ]
    .join("\n")
}

fn query_grouped_execution_pipeline_snapshot<E>(query: &Query<E>) -> String
where
    E: EntityKind + EntityValue,
{
    // Phase 1: compile grouped query intent into one executor-owned executable plan contract.
    let compiled = query
        .plan()
        .expect("grouped execution pipeline snapshot should build compiled query");
    let executable = crate::db::executor::ExecutablePlan::from(compiled);
    let grouped_handoff = executable
        .grouped_handoff()
        .expect("grouped execution pipeline snapshot should project grouped handoff");

    // Phase 2: derive grouped route observability from grouped handoff contracts.
    let route_plan =
        LoadExecutor::<E>::build_execution_route_plan_for_grouped_handoff(grouped_handoff);
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped execution pipeline snapshot should project grouped observability");

    // Phase 3: join executable snapshot and grouped route observability into one payload.
    [
        executable.render_snapshot_canonical(),
        format!(
            "route_execution_mode_case={:?}",
            route_plan.shape().execution_mode_case()
        ),
        format!(
            "route_execution_mode={:?}",
            route_plan.shape().execution_mode(),
        ),
        format!(
            "route_continuation_mode={:?}",
            route_plan.continuation().mode()
        ),
        format!("grouped_outcome={:?}", grouped_observability.outcome()),
        format!(
            "grouped_rejection={:?}",
            grouped_observability.rejection_reason()
        ),
        format!("grouped_eligible={}", grouped_observability.eligible()),
        format!(
            "grouped_execution_mode={:?}",
            grouped_observability.execution_mode()
        ),
        format!(
            "grouped_execution_strategy={:?}",
            grouped_observability.grouped_execution_strategy()
        ),
    ]
    .join("\n")
}

const DIAG_ROUTE_SECONDARY_ORDER_PUSHDOWN: &str = "diagnostic.route.secondary_order_pushdown";
const DIAG_ROUTE_TOP_N_SEEK: &str = "diagnostic.route.top_n_seek";
const DIAG_ROUTE_INDEX_RANGE_LIMIT_PUSHDOWN: &str = "diagnostic.route.index_range_limit_pushdown";
const DIAG_ROUTE_PREDICATE_STAGE: &str = "diagnostic.route.predicate_stage";
const DIAG_ROUTE_PROJECTED_FIELDS: &str = "diagnostic.route.projected_fields";
const DIAG_ROUTE_PROJECTION_PUSHDOWN: &str = "diagnostic.route.projection_pushdown";
const DIAG_ROUTE_ACCESS_CHOICE_CHOSEN: &str = "diagnostic.route.access_choice_chosen";
const DIAG_ROUTE_ACCESS_CHOICE_CHOSEN_REASON: &str = "diagnostic.route.access_choice_chosen_reason";
const DIAG_ROUTE_ACCESS_CHOICE_ALTERNATIVES: &str = "diagnostic.route.access_choice_alternatives";
const DIAG_ROUTE_ACCESS_CHOICE_REJECTIONS: &str = "diagnostic.route.access_choice_rejections";
const DIAG_DESCRIPTOR_HAS_TOP_N_SEEK: &str = "diagnostic.descriptor.has_top_n_seek";
const DIAG_DESCRIPTOR_HAS_INDEX_RANGE_LIMIT_PUSHDOWN: &str =
    "diagnostic.descriptor.has_index_range_limit_pushdown";
const DIAG_PLAN_MODE: &str = "diagnostic.plan.mode";
const DIAG_PLAN_PREDICATE_PUSHDOWN: &str = "diagnostic.plan.predicate_pushdown";

#[test]
fn singleton_unit_key_insert_and_only_load_round_trip() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SingletonUnitEntity>::new(DB, false);
    let load = LoadExecutor::<SingletonUnitEntity>::new(DB, false);
    let expected = SingletonUnitEntity {
        id: (),
        label: "project".to_string(),
    };

    save.insert(expected.clone())
        .expect("singleton save should succeed");

    let plan = Query::<SingletonUnitEntity>::new(MissingRowPolicy::Ignore)
        .only()
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("singleton load plan should build");
    let response = load.execute(plan).expect("singleton load should succeed");

    assert_eq!(
        response.len(),
        1,
        "singleton only() should match exactly one row"
    );
    assert_eq!(
        response[0].entity_ref(),
        &expected,
        "loaded singleton should match inserted row"
    );
}

#[test]
fn load_by_ids_dedups_duplicate_input_ids() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let id_a = Ulid::from_u128(1001);
    let id_b = Ulid::from_u128(1002);
    for id in [id_a, id_b] {
        save.insert(SimpleEntity { id })
            .expect("seed row save should succeed");
    }

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .by_ids([id_a, id_a, id_b, id_a])
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("by_ids plan should build");
    let response = load.execute(plan).expect("by_ids load should succeed");

    let mut ids: Vec<Ulid> = response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec![id_a, id_b],
        "duplicate by_ids entries should not emit duplicate rows"
    );
}

#[test]
fn load_union_or_predicate_dedups_overlapping_pk_paths() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let id_a = Ulid::from_u128(1201);
    let id_b = Ulid::from_u128(1202);
    for id in [id_a, id_b] {
        save.insert(SimpleEntity { id })
            .expect("seed row save should succeed");
    }

    let predicate = Predicate::Or(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Eq,
            Value::Ulid(id_a),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![Value::Ulid(id_a), Value::Ulid(id_b)]),
            CoercionId::Strict,
        )),
    ]);
    let explain = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("id")
        .explain()
        .expect("union explain should build");
    assert!(
        matches!(
            explain.access(),
            crate::db::query::explain::ExplainAccessPath::Union(_)
        ),
        "OR predicate over PK paths should plan as union access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("union load plan should build");
    let response = load.execute(plan).expect("union load should succeed");
    let ids: Vec<Ulid> = response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();

    assert_eq!(
        ids,
        vec![id_a, id_b],
        "union execution must keep canonical order and suppress overlapping keys"
    );
}

#[test]
fn load_union_or_predicate_explain_execution_projects_recursive_access_children() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let id_a = Ulid::from_u128(2201);
    let id_b = Ulid::from_u128(2202);
    for id in [id_a, id_b] {
        save.insert(SimpleEntity { id })
            .expect("seed row save should succeed");
    }

    let predicate = Predicate::Or(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Eq,
            Value::Ulid(id_a),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![Value::Ulid(id_a), Value::Ulid(id_b)]),
            CoercionId::Strict,
        )),
    ]);
    let descriptor = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("id")
        .explain_execution()
        .expect("union explain execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::Union,
        "OR predicate over PK paths should project union root access node",
    );
    assert!(
        explain_execution_contains_node_type(&descriptor, ExplainExecutionNodeType::ByKeyLookup)
            || explain_execution_contains_node_type(
                &descriptor,
                ExplainExecutionNodeType::ByKeysLookup,
            ),
        "union access descriptor should retain recursive access children",
    );
    let descriptor_json = descriptor.render_json_canonical();
    assert!(
        descriptor_json.contains("\"type\":\"Union\"")
            && descriptor_json.contains("\"children\":["),
        "union execution descriptor json should preserve recursive access shape",
    );
}

#[test]
fn load_intersection_asc_keeps_overlap_in_canonical_order() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1211_u128, 1212, 1213, 1214, 1215, 1216] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("seed row save should succeed");
    }

    let predicate = Predicate::And(vec![
        id_in_predicate(&[1211, 1212, 1213, 1214]),
        id_in_predicate(&[1213, 1214, 1215, 1216]),
    ]);
    let explain = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("id")
        .explain()
        .expect("intersection explain should build");
    assert!(
        matches!(explain.access(), ExplainAccessPath::Intersection(_)),
        "AND predicate over key sets should plan as intersection access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("intersection load plan should build");
    let response = load
        .execute(plan)
        .expect("intersection load should succeed");
    let ids: Vec<Ulid> = response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();

    assert_eq!(
        ids,
        vec![Ulid::from_u128(1213), Ulid::from_u128(1214)],
        "intersection execution should emit the overlap in ascending canonical order"
    );
}

#[test]
fn load_intersection_explain_execution_projects_recursive_access_children() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [2211_u128, 2212, 2213, 2214, 2215, 2216] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("seed row save should succeed");
    }

    let predicate = Predicate::And(vec![
        id_in_predicate(&[2211, 2212, 2213, 2214]),
        id_in_predicate(&[2213, 2214, 2215, 2216]),
    ]);
    let descriptor = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("id")
        .explain_execution()
        .expect("intersection explain execution should build");

    assert_eq!(
        descriptor.node_type(),
        ExplainExecutionNodeType::Intersection,
        "AND predicate over PK sets should project intersection root access node",
    );
    assert!(
        matches!(
            descriptor.access_strategy(),
            Some(ExplainAccessPath::Intersection(_))
        ),
        "intersection descriptor root should retain intersection access projection",
    );
    assert!(
        explain_execution_contains_node_type(&descriptor, ExplainExecutionNodeType::ByKeysLookup),
        "intersection descriptor should include recursive key-set access children",
    );
    let descriptor_json = descriptor.render_json_canonical();
    assert!(
        descriptor_json.contains("\"type\":\"Intersection\"")
            && descriptor_json.contains("\"children\":["),
        "intersection execution descriptor json should preserve recursive access shape",
    );
}

#[test]
fn load_intersection_desc_keeps_overlap_in_desc_order() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1221_u128, 1222, 1223, 1224, 1225, 1226] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("seed row save should succeed");
    }

    let predicate = Predicate::And(vec![
        id_in_predicate(&[1221, 1222, 1223, 1224]),
        id_in_predicate(&[1223, 1224, 1225, 1226]),
    ]);
    let explain = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by_desc("id")
        .explain()
        .expect("intersection DESC explain should build");
    assert!(
        matches!(explain.access(), ExplainAccessPath::Intersection(_)),
        "AND predicate over key sets should plan as intersection access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("intersection DESC load plan should build");
    let response = load
        .execute(plan)
        .expect("intersection DESC load should succeed");
    let ids: Vec<Ulid> = response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();

    assert_eq!(
        ids,
        vec![Ulid::from_u128(1224), Ulid::from_u128(1223)],
        "intersection execution should emit the overlap in descending canonical order"
    );
}

#[test]
fn load_intersection_no_overlap_returns_empty() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1231_u128, 1232, 1233, 1234] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("seed row save should succeed");
    }

    let predicate = Predicate::And(vec![
        id_in_predicate(&[1231, 1232]),
        id_in_predicate(&[1233, 1234]),
    ]);
    let explain = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("id")
        .explain()
        .expect("intersection no-overlap explain should build");
    assert!(
        matches!(explain.access(), ExplainAccessPath::Intersection(_)),
        "disjoint AND key predicates should still plan as intersection access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("intersection no-overlap plan should build");
    let response = load
        .execute(plan)
        .expect("intersection no-overlap load should succeed");
    assert!(
        response.is_empty(),
        "intersection with no shared keys should return no rows"
    );
}

#[test]
fn load_intersection_suppresses_duplicate_keys() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1241_u128, 1242, 1243] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("seed row save should succeed");
    }

    let predicate = Predicate::And(vec![
        id_in_predicate(&[1241, 1241, 1242, 1243]),
        id_in_predicate(&[1241, 1241, 1243, 1243]),
    ]);
    let explain = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("id")
        .explain()
        .expect("intersection duplicate explain should build");
    assert!(
        matches!(explain.access(), ExplainAccessPath::Intersection(_)),
        "duplicate AND key predicates should still plan as intersection access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("intersection duplicate plan should build");
    let response = load
        .execute(plan)
        .expect("intersection duplicate load should succeed");
    let ids: Vec<Ulid> = response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();
    let unique: BTreeSet<Ulid> = ids.iter().copied().collect();

    assert_eq!(
        ids,
        vec![Ulid::from_u128(1241), Ulid::from_u128(1243)],
        "intersection should return shared ids once in canonical order"
    );
    assert_eq!(
        unique.len(),
        ids.len(),
        "intersection execution must not emit duplicate rows"
    );
}

#[test]
fn load_intersection_nested_union_children_matches_expected_overlap() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1251_u128, 1252, 1253, 1254, 1255, 1256, 1257, 1258] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("seed row save should succeed");
    }

    let predicate = Predicate::And(vec![
        Predicate::Or(vec![
            id_in_predicate(&[1251, 1252, 1253, 1254]),
            id_in_predicate(&[1253, 1254, 1255]),
        ]),
        Predicate::Or(vec![
            id_in_predicate(&[1252, 1253, 1256]),
            id_in_predicate(&[1253, 1257, 1258]),
        ]),
    ]);
    let explain = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("id")
        .explain()
        .expect("nested intersection explain should build");
    let ExplainAccessPath::Intersection(children) = explain.access() else {
        panic!("nested AND predicate should plan as intersection access");
    };
    assert_eq!(
        children.len(),
        2,
        "nested intersection should preserve both composite children"
    );
    assert!(
        children
            .iter()
            .all(|child| matches!(child, ExplainAccessPath::Union(_))),
        "nested intersection children should remain union composites"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("nested intersection plan should build");
    let response = load
        .execute(plan)
        .expect("nested intersection load should succeed");
    let ids: Vec<Ulid> = response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();

    assert_eq!(
        ids,
        vec![Ulid::from_u128(1252), Ulid::from_u128(1253)],
        "nested composite intersection should match overlap of union children"
    );
}

#[test]
fn load_intersection_desc_limit_continuation_has_no_duplicate_or_omission() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [
        1261_u128, 1262, 1263, 1264, 1265, 1266, 1267, 1268, 1269, 1270,
    ] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("seed row save should succeed");
    }

    let predicate = Predicate::And(vec![
        id_in_predicate(&[1261, 1262, 1263, 1264, 1265, 1266, 1267, 1268]),
        id_in_predicate(&[1264, 1265, 1266, 1267, 1268, 1269, 1270]),
    ]);
    let explain = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by_desc("id")
        .limit(2)
        .explain()
        .expect("intersection pagination explain should build");
    assert!(
        matches!(explain.access(), ExplainAccessPath::Intersection(_)),
        "overlapping AND predicate should plan as intersection access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let mut paged_ids = Vec::new();
    let mut cursor: Option<CursorBoundary> = None;
    loop {
        let page_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate.clone())
            .order_by_desc("id")
            .limit(2)
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("intersection desc paged plan should build");
        let page = load
            .execute_paged_with_cursor(page_plan, cursor.clone())
            .expect("intersection desc paged load should succeed");
        let page_ids: Vec<Ulid> = page
            .items
            .into_iter()
            .map(|row| row.entity_ref().id)
            .collect();
        paged_ids.extend(page_ids);

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        let token = next_cursor;
        cursor = Some(token.boundary().clone());
    }

    let full_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("intersection desc full plan should build");
    let full_response = load
        .execute(full_plan)
        .expect("intersection desc full load should succeed");
    let full_ids: Vec<Ulid> = full_response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();
    let unique: BTreeSet<Ulid> = paged_ids.iter().copied().collect();

    assert_eq!(
        paged_ids, full_ids,
        "paged intersection DESC traversal with limit must match full execution"
    );
    assert_eq!(
        unique.len(),
        paged_ids.len(),
        "intersection DESC paged traversal must not duplicate rows"
    );
}

#[test]
fn load_union_desc_limit_continuation_has_no_duplicate_or_omission() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1301_u128, 1302, 1303, 1304, 1305, 1306] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("seed row save should succeed");
    }

    let predicate = Predicate::Or(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![
                Value::Ulid(Ulid::from_u128(1301)),
                Value::Ulid(Ulid::from_u128(1302)),
                Value::Ulid(Ulid::from_u128(1303)),
                Value::Ulid(Ulid::from_u128(1304)),
            ]),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![
                Value::Ulid(Ulid::from_u128(1303)),
                Value::Ulid(Ulid::from_u128(1304)),
                Value::Ulid(Ulid::from_u128(1305)),
                Value::Ulid(Ulid::from_u128(1306)),
            ]),
            CoercionId::Strict,
        )),
    ]);
    let explain = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by_desc("id")
        .limit(2)
        .explain()
        .expect("union pagination explain should build");
    assert!(
        matches!(
            explain.access(),
            crate::db::query::explain::ExplainAccessPath::Union(_)
        ),
        "overlapping OR predicate should plan as union access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let mut paged_ids = Vec::new();
    let mut cursor: Option<CursorBoundary> = None;
    loop {
        let page_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate.clone())
            .order_by_desc("id")
            .limit(2)
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("union desc paged plan should build");
        let page = load
            .execute_paged_with_cursor(page_plan, cursor.clone())
            .expect("union desc paged load should succeed");
        let page_ids: Vec<Ulid> = page
            .items
            .into_iter()
            .map(|row| row.entity_ref().id)
            .collect();
        paged_ids.extend(page_ids);

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        let token = next_cursor;
        cursor = Some(token.boundary().clone());
    }

    let full_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("union desc full plan should build");
    let full_response = load
        .execute(full_plan)
        .expect("union desc full load should succeed");
    let full_ids: Vec<Ulid> = full_response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();

    assert_eq!(
        paged_ids, full_ids,
        "paged union DESC traversal with limit must match full execution"
    );
    let unique: BTreeSet<Ulid> = paged_ids.iter().copied().collect();
    assert_eq!(
        unique.len(),
        paged_ids.len(),
        "union DESC paged traversal must not duplicate rows"
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn load_union_three_children_desc_limit_continuation_has_no_duplicate_or_omission() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [1401_u128, 1402, 1403, 1404, 1405, 1406, 1407, 1408, 1409] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("seed row save should succeed");
    }

    let predicate = Predicate::Or(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![
                Value::Ulid(Ulid::from_u128(1401)),
                Value::Ulid(Ulid::from_u128(1402)),
                Value::Ulid(Ulid::from_u128(1403)),
                Value::Ulid(Ulid::from_u128(1404)),
            ]),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![
                Value::Ulid(Ulid::from_u128(1403)),
                Value::Ulid(Ulid::from_u128(1404)),
                Value::Ulid(Ulid::from_u128(1405)),
                Value::Ulid(Ulid::from_u128(1406)),
            ]),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![
                Value::Ulid(Ulid::from_u128(1406)),
                Value::Ulid(Ulid::from_u128(1407)),
                Value::Ulid(Ulid::from_u128(1408)),
                Value::Ulid(Ulid::from_u128(1409)),
            ]),
            CoercionId::Strict,
        )),
    ]);
    let explain = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by_desc("id")
        .limit(3)
        .explain()
        .expect("three-child union pagination explain should build");
    assert!(
        matches!(
            explain.access(),
            crate::db::query::explain::ExplainAccessPath::Union(_)
        ),
        "three-child overlapping OR predicate should plan as union access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let mut paged_ids = Vec::new();
    let mut cursor: Option<CursorBoundary> = None;
    loop {
        let page_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate.clone())
            .order_by_desc("id")
            .limit(3)
            .plan()
            .map(crate::db::executor::ExecutablePlan::from)
            .expect("three-child union desc paged plan should build");
        let page = load
            .execute_paged_with_cursor(page_plan, cursor.clone())
            .expect("three-child union desc paged load should succeed");
        let page_ids: Vec<Ulid> = page
            .items
            .into_iter()
            .map(|row| row.entity_ref().id)
            .collect();
        paged_ids.extend(page_ids);

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        let token = next_cursor;
        cursor = Some(token.boundary().clone());
    }

    let full_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("three-child union desc full plan should build");
    let full_response = load
        .execute(full_plan)
        .expect("three-child union desc full load should succeed");
    let full_ids: Vec<Ulid> = full_response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();

    assert_eq!(
        paged_ids, full_ids,
        "three-child paged union DESC traversal with limit must match full execution"
    );
    let unique: BTreeSet<Ulid> = paged_ids.iter().copied().collect();
    assert_eq!(
        unique.len(),
        paged_ids.len(),
        "three-child union DESC paged traversal must not duplicate rows"
    );
}

#[test]
fn delete_applies_order_and_delete_limit() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    for id in [30_u128, 10_u128, 20_u128] {
        save.insert(SimpleEntity {
            id: Ulid::from_u128(id),
        })
        .expect("save should succeed");
    }

    let delete = DeleteExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .order_by("id")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("delete plan should build");

    let response = delete.execute(plan).expect("delete should succeed");
    assert_eq!(response.len(), 1, "delete limit should remove one row");
    assert_eq!(
        response[0].entity_ref().id,
        Ulid::from_u128(10),
        "delete limit should run after canonical ordering by id"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let remaining_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .order_by("id")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("remaining load plan should build");
    let remaining = load
        .execute(remaining_plan)
        .expect("remaining load should succeed");
    let remaining_ids: Vec<Ulid> = remaining
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();

    assert_eq!(
        remaining_ids,
        vec![Ulid::from_u128(20), Ulid::from_u128(30)],
        "only the first ordered row should have been deleted"
    );
}

#[test]
fn load_filter_after_access_with_optional_equality() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    let id = Ulid::from_u128(501);
    save.insert(PhaseEntity {
        id,
        opt_rank: Some(7),
        rank: 7,
        tags: vec![1, 2, 3],
        label: "alpha".to_string(),
    })
    .expect("save should succeed");

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let equals_opt_value = Predicate::Compare(ComparePredicate::with_coercion(
        "opt_rank",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::Strict,
    ));
    let match_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .by_id(id)
        .filter(equals_opt_value)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("optional equality plan should build");
    let match_response = load
        .execute(match_plan)
        .expect("optional equality should load");
    assert_eq!(
        match_response.len(),
        1,
        "filter should run after by_id access and keep matching rows"
    );

    let no_match = Predicate::Compare(ComparePredicate::with_coercion(
        "opt_rank",
        CompareOp::Eq,
        Value::Uint(99),
        CoercionId::Strict,
    ));
    let mismatch_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .by_id(id)
        .filter(no_match)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("mismatch plan should build");
    let mismatch_response = load
        .execute(mismatch_plan)
        .expect("mismatch predicate should execute");
    assert_eq!(
        mismatch_response.len(),
        0,
        "filter should be applied after access and drop non-matching rows"
    );
}

#[test]
fn load_in_and_text_ops_respect_ordered_pagination() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(601),
            opt_rank: Some(30),
            rank: 30,
            tags: vec![1, 3],
            label: "needle alpha".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(602),
            opt_rank: Some(10),
            rank: 10,
            tags: vec![2],
            label: "other".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(603),
            opt_rank: Some(20),
            rank: 20,
            tags: vec![9],
            label: "NEEDLE beta".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(604),
            opt_rank: Some(40),
            rank: 40,
            tags: vec![4],
            label: "needle gamma".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "rank",
            CompareOp::In,
            Value::List(vec![Value::Uint(20), Value::Uint(30), Value::Uint(40)]),
            CoercionId::Strict,
        )),
        Predicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("needle".to_string()),
        },
    ]);

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank")
        .limit(1)
        .offset(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("in+text ordered page plan should build");
    let response = load
        .execute(plan)
        .expect("in+text ordered page should load");

    assert_eq!(
        response.len(),
        1,
        "ordered pagination should return one row"
    );
    assert_eq!(
        response[0].entity_ref().rank,
        30,
        "pagination should apply to the filtered+ordered window"
    );
}

#[test]
fn secondary_in_explain_uses_index_multi_lookup_access_shape() {
    let explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::In,
            Value::List(vec![Value::Uint(7), Value::Uint(8), Value::Uint(9)]),
            CoercionId::Strict,
        )))
        .explain()
        .expect("secondary IN explain should build");

    assert!(
        matches!(explain.access(), ExplainAccessPath::IndexMultiLookup { .. }),
        "secondary IN predicates should lower to the dedicated index-multi-lookup access shape",
    );
}

#[test]
fn secondary_or_eq_explain_uses_index_multi_lookup_access_shape() {
    let explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "group",
                CompareOp::Eq,
                Value::Uint(8),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "group",
                CompareOp::Eq,
                Value::Uint(7),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "group",
                CompareOp::Eq,
                Value::Uint(8),
                CoercionId::Strict,
            )),
        ]))
        .explain()
        .expect("secondary OR equality explain should build");

    assert!(
        matches!(explain.access(), ExplainAccessPath::IndexMultiLookup { .. }),
        "same-field strict OR equality should lower to index-multi-lookup access shape",
    );
}

#[test]
fn expression_casefold_eq_planner_access_choice_and_runtime_route_stay_in_parity() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();
    seed_expression_casefold_parity_rows(8_100);

    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::Eq,
        Value::Text("ALICE@EXAMPLE.COM".to_string()),
        CoercionId::TextCasefold,
    ));

    // Phase 1: planner eligibility must lower to one canonical expression-index prefix shape.
    let explain = Query::<ExpressionCasefoldParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain()
        .expect("expression eq explain should build");
    let ExplainAccessPath::IndexPrefix {
        name,
        fields,
        prefix_len,
        values,
    } = explain.access()
    else {
        panic!("expression eq should lower to index-prefix access");
    };
    assert_eq!(name, &EXPRESSION_CASEFOLD_PARITY_INDEX_MODELS[0].name());
    assert_eq!(fields.as_slice(), ["email"]);
    assert_eq!(*prefix_len, 1);
    assert_eq!(
        values.as_slice(),
        [Value::Text("alice@example.com".to_string())]
    );

    // Phase 2: access-choice diagnostics must project the same chosen index authority.
    let verbose = Query::<ExpressionCasefoldParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain_execution_verbose()
        .expect("expression eq verbose explain should build");
    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_expression_index_access_choice_selected(&diagnostics);

    // Phase 3: runtime route selection must execute through the same index-prefix route.
    let execution = Query::<ExpressionCasefoldParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain_execution()
        .expect("expression eq execution explain should build");
    assert!(
        explain_execution_contains_node_type(&execution, ExplainExecutionNodeType::IndexPrefixScan),
        "execution route must preserve expression eq index-prefix route selection",
    );

    let load = LoadExecutor::<ExpressionCasefoldParityEntity>::new(DB, false);
    let plan = Query::<ExpressionCasefoldParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("expression eq load plan should build");
    let response = load
        .execute(plan)
        .expect("expression eq load should execute");
    let ids = response
        .iter()
        .map(|row| row.entity_ref().id)
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![Ulid::from_u128(8_101)],
        "expression eq execution results must match canonical lower(email) semantics",
    );
}

#[test]
fn expression_casefold_in_planner_access_choice_and_runtime_route_stay_in_parity() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();
    seed_expression_casefold_parity_rows(8_200);

    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::In,
        Value::List(vec![
            Value::Text("BOB@EXAMPLE.COM".to_string()),
            Value::Text("alice@example.com".to_string()),
            Value::Text("bob@example.com".to_string()),
        ]),
        CoercionId::TextCasefold,
    ));

    // Phase 1: planner eligibility must lower to one canonical expression-index multi-lookup shape.
    let explain = Query::<ExpressionCasefoldParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain()
        .expect("expression IN explain should build");
    let ExplainAccessPath::IndexMultiLookup {
        name,
        fields,
        values,
    } = explain.access()
    else {
        panic!("expression IN should lower to index-multi-lookup access");
    };
    assert_eq!(name, &EXPRESSION_CASEFOLD_PARITY_INDEX_MODELS[0].name());
    assert_eq!(fields.as_slice(), ["email"]);
    assert_eq!(
        values.as_slice(),
        [
            Value::Text("alice@example.com".to_string()),
            Value::Text("bob@example.com".to_string())
        ],
    );

    // Phase 2: access-choice diagnostics must project the same chosen index authority.
    let verbose = Query::<ExpressionCasefoldParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain_execution_verbose()
        .expect("expression IN verbose explain should build");
    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_expression_index_access_choice_selected(&diagnostics);

    // Phase 3: runtime route selection must execute through the same index-multi-lookup route.
    let execution = Query::<ExpressionCasefoldParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain_execution()
        .expect("expression IN execution explain should build");
    assert!(
        explain_execution_contains_node_type(
            &execution,
            ExplainExecutionNodeType::IndexMultiLookup
        ),
        "execution route must preserve expression IN index-multi-lookup route selection",
    );

    let load = LoadExecutor::<ExpressionCasefoldParityEntity>::new(DB, false);
    let plan = Query::<ExpressionCasefoldParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("expression IN load plan should build");
    let response = load
        .execute(plan)
        .expect("expression IN load should execute");
    let mut ids = response
        .iter()
        .map(|row| row.entity_ref().id)
        .collect::<Vec<_>>();
    ids.sort();
    assert_eq!(
        ids,
        vec![Ulid::from_u128(8_201), Ulid::from_u128(8_202)],
        "expression IN execution results must match canonical lower(email) set semantics",
    );
}

#[test]
fn expression_upper_casefold_eq_fails_closed_across_planner_access_choice_and_runtime() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();
    seed_expression_upper_parity_rows(8_300);

    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::Eq,
        Value::Text("ALICE@EXAMPLE.COM".to_string()),
        CoercionId::TextCasefold,
    ));

    let explain = Query::<ExpressionUpperParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain()
        .expect("unsupported expression eq explain should build");
    assert!(
        matches!(explain.access(), ExplainAccessPath::FullScan),
        "unsupported expression lookup must fail closed to full scan at planner boundary",
    );

    let verbose = Query::<ExpressionUpperParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain_execution_verbose()
        .expect("unsupported expression eq verbose explain should build");
    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_full_scan_access_choice_selected(&diagnostics);
    assert_eq!(
        diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"fallback(non_strict_compare_coercion)".to_string()),
        "unsupported expression lookup should remain on non-strict compare fallback diagnostics",
    );

    let execution = Query::<ExpressionUpperParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain_execution()
        .expect("unsupported expression eq execution explain should build");
    assert!(
        explain_execution_contains_node_type(&execution, ExplainExecutionNodeType::FullScan),
        "runtime route must preserve full-scan fallback for unsupported expression lookup",
    );

    let load = LoadExecutor::<ExpressionUpperParityEntity>::new(DB, false);
    let plan = Query::<ExpressionUpperParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("unsupported expression eq load plan should build");
    let response = load
        .execute(plan)
        .expect("unsupported expression eq load should execute");
    let ids = response
        .iter()
        .map(|row| row.entity_ref().id)
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![Ulid::from_u128(8_301)],
        "fallback execution must still apply text-casefold row filtering semantics",
    );
}

#[test]
fn expression_upper_casefold_in_fails_closed_across_planner_access_choice_and_runtime() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();
    seed_expression_upper_parity_rows(8_400);

    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::In,
        Value::List(vec![
            Value::Text("alice@example.com".to_string()),
            Value::Text("BOB@EXAMPLE.COM".to_string()),
            Value::Text("bob@example.com".to_string()),
        ]),
        CoercionId::TextCasefold,
    ));

    let explain = Query::<ExpressionUpperParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain()
        .expect("unsupported expression IN explain should build");
    assert!(
        matches!(explain.access(), ExplainAccessPath::FullScan),
        "unsupported expression IN lookup must fail closed to full scan at planner boundary",
    );

    let verbose = Query::<ExpressionUpperParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain_execution_verbose()
        .expect("unsupported expression IN verbose explain should build");
    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_full_scan_access_choice_selected(&diagnostics);
    assert_eq!(
        diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"fallback(non_strict_compare_coercion)".to_string()),
        "unsupported expression IN lookup should remain on non-strict compare fallback diagnostics",
    );

    let execution = Query::<ExpressionUpperParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .explain_execution()
        .expect("unsupported expression IN execution explain should build");
    assert!(
        explain_execution_contains_node_type(&execution, ExplainExecutionNodeType::FullScan),
        "runtime route must preserve full-scan fallback for unsupported expression IN lookup",
    );

    let load = LoadExecutor::<ExpressionUpperParityEntity>::new(DB, false);
    let plan = Query::<ExpressionUpperParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("unsupported expression IN load plan should build");
    let response = load
        .execute(plan)
        .expect("unsupported expression IN load should execute");
    let mut ids = response
        .iter()
        .map(|row| row.entity_ref().id)
        .collect::<Vec<_>>();
    ids.sort();
    assert_eq!(
        ids,
        vec![Ulid::from_u128(8_401), Ulid::from_u128(8_402)],
        "fallback execution must preserve canonical casefold IN row semantics",
    );
}

#[test]
fn query_explain_execution_text_and_json_surfaces_are_stable() {
    let id = Ulid::from_u128(9_101);
    let query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore).by_id(id);
    let descriptor = query
        .explain_execution()
        .expect("execution descriptor explain should build");

    let text = query
        .explain_execution_text()
        .expect("execution text explain should build");
    assert!(
        text.contains("ByKeyLookup"),
        "execution text surface should expose access-root node type"
    );
    assert_eq!(
        text,
        descriptor.render_text_tree(),
        "execution text surface should be canonical descriptor text rendering",
    );

    let json = query
        .explain_execution_json()
        .expect("execution json explain should build");
    assert!(
        json.contains("\"node_type\":\"ByKeyLookup\""),
        "execution json surface should expose canonical root node type"
    );
    assert_eq!(
        json,
        descriptor.render_json_canonical(),
        "execution json surface should be canonical descriptor json rendering",
    );
}

#[test]
fn query_execution_pipeline_snapshot_for_by_key_shape_is_stable() {
    let query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore).by_id(Ulid::from_u128(9_101));
    let actual = query_execution_pipeline_snapshot(&query);
    let expected_descriptor_json = r#"{"node_id":0,"node_type":"ByKeyLookup","layer":"scan","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":{"type":"ByKey","key":"Ulid(Ulid(Ulid(9101)))"},"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":false,"fast_path_reason":"materialized_fallback","residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[],"node_properties":{"access_choice_alternatives":"List([])","access_choice_chosen":"Text(\"by_key\")","access_choice_chosen_reason":"Text(\"non_index_access\")","access_choice_rejections":"List([])","continuation_mode":"Text(\"initial\")","covering_scan_reason":"Text(\"access_not_covering_index_shape\")","fast_path_rejections":"List([Text(\"primary_key=pk_order_fast_path_ineligible\"), Text(\"secondary_prefix=secondary_order_not_applicable\"), Text(\"index_range=index_range_limit_pushdown_disabled\")])","fast_path_selected":"Text(\"none\")","fast_path_selected_reason":"Text(\"materialized_fallback\")","projected_fields":"List([Text(\"id\")])","projection_pushdown":"Bool(false)","resume_from":"Text(\"none\")","scan_direction":"Text(\"asc\")"}}"#;
    let expected = vec![
        "snapshot_version=1".to_string(),
        "plan_hash=aadeab9a078a08c89fc76826504ee8c027854392786d07f24b5ad22fb4a729b0"
            .to_string(),
        "mode=Load(LoadSpec { limit: None, offset: 0 })".to_string(),
        "is_grouped=false".to_string(),
        "execution_strategy=PrimaryKey".to_string(),
        "ordering_direction=Asc".to_string(),
        "distinct_execution_strategy=None".to_string(),
        "projection_selection=All".to_string(),
        "projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId(\"id\")), alias: None }] }".to_string(),
        "order_spec=None".to_string(),
        "page_spec=None".to_string(),
        "projection_coverage_flag=false".to_string(),
        "continuation_signature=39b38cf7347a2f0cd6c26008951fed2b29b87d1feb463ce1878238276f1b5919"
            .to_string(),
        "index_prefix_specs=[]".to_string(),
        "index_range_specs=[]".to_string(),
        "explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: None, offset: 0 }), access: ByKey { key: Ulid(Ulid(Ulid(9101))) }, predicate: None, predicate_model: None, order_by: None, distinct: false, grouping: None, order_pushdown: MissingModelContext, page: None, delete_limit: None, consistency: Ignore }".to_string(),
        format!("execution_descriptor_json={expected_descriptor_json}"),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "execution pipeline snapshot drifted; query->executable->explain serialization is a stabilized 0.51 surface",
    );
}

#[test]
fn query_execution_pipeline_snapshot_for_by_key_shape_with_projection_columns_is_stable() {
    let query =
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore).by_id(Ulid::from_u128(9_101));
    let actual = query_execution_pipeline_projection_snapshot(&query);
    let expected_descriptor_json = r#"{"node_id":0,"node_type":"ByKeyLookup","layer":"scan","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":{"type":"ByKey","key":"Ulid(Ulid(Ulid(9101)))"},"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":false,"fast_path_reason":"materialized_fallback","residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[],"node_properties":{"access_choice_alternatives":"List([])","access_choice_chosen":"Text(\"by_key\")","access_choice_chosen_reason":"Text(\"non_index_access\")","access_choice_rejections":"List([])","continuation_mode":"Text(\"initial\")","covering_scan_reason":"Text(\"access_not_covering_index_shape\")","fast_path_rejections":"List([Text(\"primary_key=pk_order_fast_path_ineligible\"), Text(\"secondary_prefix=secondary_order_not_applicable\"), Text(\"index_range=index_range_limit_pushdown_disabled\")])","fast_path_selected":"Text(\"none\")","fast_path_selected_reason":"Text(\"materialized_fallback\")","projected_fields":"List([Text(\"id\"), Text(\"group\"), Text(\"rank\"), Text(\"label\")])","projection_pushdown":"Bool(false)","resume_from":"Text(\"none\")","scan_direction":"Text(\"asc\")"}}"#;
    let expected = vec![
        "snapshot_version=1".to_string(),
        "plan_hash=aadeab9a078a08c89fc76826504ee8c027854392786d07f24b5ad22fb4a729b0"
            .to_string(),
        "mode=Load(LoadSpec { limit: None, offset: 0 })".to_string(),
        "is_grouped=false".to_string(),
        "execution_strategy=PrimaryKey".to_string(),
        "ordering_direction=Asc".to_string(),
        "distinct_execution_strategy=None".to_string(),
        "projection_selection=All".to_string(),
        "projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId(\"id\")), alias: None }, Scalar { expr: Field(FieldId(\"group\")), alias: None }, Scalar { expr: Field(FieldId(\"rank\")), alias: None }, Scalar { expr: Field(FieldId(\"label\")), alias: None }] }".to_string(),
        "order_spec=None".to_string(),
        "page_spec=None".to_string(),
        "projection_coverage_flag=false".to_string(),
        "continuation_signature=355c1739abb9dd4cd89e22d9ac3902c76e6054c27f51684814f299061274e637"
            .to_string(),
        "index_prefix_specs=[]".to_string(),
        "index_range_specs=[]".to_string(),
        "explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: None, offset: 0 }), access: ByKey { key: Ulid(Ulid(Ulid(9101))) }, predicate: None, predicate_model: None, order_by: None, distinct: false, grouping: None, order_pushdown: MissingModelContext, page: None, delete_limit: None, consistency: Ignore }".to_string(),
        "projection_columns=[\"column[0]::expr=Field(FieldId(\\\"id\\\"))::alias=none\", \"column[1]::expr=Field(FieldId(\\\"group\\\"))::alias=none\", \"column[2]::expr=Field(FieldId(\\\"rank\\\"))::alias=none\", \"column[3]::expr=Field(FieldId(\\\"label\\\"))::alias=none\"]".to_string(),
        format!("execution_descriptor_json={expected_descriptor_json}"),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "execution pipeline + projection-column snapshot drifted; query->executable->explain->projection-columns is a stabilized 0.51 surface",
    );
}

#[test]
fn query_execution_pipeline_snapshot_for_secondary_index_ordered_shape_is_stable() {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_by("rank")
        .limit(5);
    let actual = query_execution_pipeline_snapshot(&query);
    let expected_descriptor_json = r#"{"node_id":0,"node_type":"IndexPrefixScan","layer":"scan","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":{"type":"IndexPrefix","name":"group_rank","fields":["group","rank"],"prefix_len":1,"values":["Uint(7)"]},"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":true,"fast_path_reason":"secondary_order_pushdown_eligible","residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[{"node_id":1,"node_type":"IndexPredicatePrefilter","layer":"pipeline","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"full","predicate_pushdown":"strict_all_or_none","fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"pushdown":"Text(\"group=Uint(7)\")"}},{"node_id":2,"node_type":"SecondaryOrderPushdown","layer":"pipeline","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"index":"Text(\"group_rank\")","prefix_len":"Uint(1)"}},{"node_id":3,"node_type":"OrderByMaterializedSort","layer":"pipeline","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"order_satisfied_by_index":"Bool(false)"}},{"node_id":4,"node_type":"LimitOffset","layer":"terminal","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":5,"cursor":false,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"offset":"Uint(0)"}}],"node_properties":{"access_choice_alternatives":"List([])","access_choice_chosen":"Text(\"index:group_rank\")","access_choice_chosen_reason":"Text(\"single_candidate\")","access_choice_rejections":"List([])","continuation_mode":"Text(\"initial\")","covering_scan_reason":"Text(\"order_requires_materialization\")","fast_path_rejections":"List([Text(\"primary_key=pk_order_fast_path_ineligible\"), Text(\"index_range=index_range_limit_pushdown_disabled\")])","fast_path_selected":"Text(\"secondary_prefix\")","fast_path_selected_reason":"Text(\"secondary_order_pushdown_eligible\")","prefix_len":"Uint(1)","projected_fields":"List([Text(\"id\"), Text(\"group\"), Text(\"rank\"), Text(\"label\")])","projection_pushdown":"Bool(false)","resume_from":"Text(\"none\")","scan_direction":"Text(\"asc\")"}}"#;
    let expected = vec![
        "snapshot_version=1".to_string(),
        "plan_hash=108ea6b7dbe368e6e1ebad89ff465f497937a483ca6fd56e8b4f3c3ee151a0e7"
            .to_string(),
        "mode=Load(LoadSpec { limit: Some(5), offset: 0 })".to_string(),
        "is_grouped=false".to_string(),
        "execution_strategy=Ordered".to_string(),
        "ordering_direction=Asc".to_string(),
        "distinct_execution_strategy=None".to_string(),
        "projection_selection=All".to_string(),
        "projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId(\"id\")), alias: None }, Scalar { expr: Field(FieldId(\"group\")), alias: None }, Scalar { expr: Field(FieldId(\"rank\")), alias: None }, Scalar { expr: Field(FieldId(\"label\")), alias: None }] }".to_string(),
        "order_spec=Some(OrderSpec { fields: [(\"rank\", Asc), (\"id\", Asc)] })".to_string(),
        "page_spec=Some(PageSpec { limit: Some(5), offset: 0 })".to_string(),
        "projection_coverage_flag=false".to_string(),
        "continuation_signature=6f28b609075e3776a6bf77842d95991827048fb0d2f1fa33da262a38bce3d340"
            .to_string(),
        "index_prefix_specs=[{index:group_rank,bound_type:equality,lower:included(len:345:head:00001f5075736864:tail:0007000100000100),upper:included(len:4503:head:00001f5075736864:tail:ffffffffffffffff)}]".to_string(),
        "index_range_specs=[]".to_string(),
        "explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: Some(5), offset: 0 }), access: IndexPrefix { name: \"group_rank\", fields: [\"group\", \"rank\"], prefix_len: 1, values: [Uint(7)] }, predicate: Compare { field: \"group\", op: Eq, value: Uint(7), coercion: CoercionSpec { id: Strict, params: {} } }, predicate_model: Some(Compare(ComparePredicate { field: \"group\", op: Eq, value: Uint(7), coercion: CoercionSpec { id: Strict, params: {} } })), order_by: Fields([ExplainOrder { field: \"rank\", direction: Asc }, ExplainOrder { field: \"id\", direction: Asc }]), distinct: false, grouping: None, order_pushdown: MissingModelContext, page: Page { limit: Some(5), offset: 0 }, delete_limit: None, consistency: Ignore }".to_string(),
        format!("execution_descriptor_json={expected_descriptor_json}"),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "secondary-index ordered execution pipeline snapshot drifted; planner/executor boundary must remain stable",
    );
}

#[test]
fn query_execution_pipeline_snapshot_for_index_range_shape_is_stable() {
    let query = Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Gte,
                Value::Uint(100),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Lte,
                Value::Uint(500),
                CoercionId::Strict,
            )),
        ]))
        .order_by("code")
        .limit(3);
    let actual = query_execution_pipeline_snapshot(&query);
    let expected_descriptor_json = r#"{"node_id":0,"node_type":"IndexRangeScan","layer":"scan","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":{"type":"IndexRange","name":"code_unique","fields":["code"],"prefix_len":0,"prefix":[],"lower":"Included(Uint(100))","upper":"Included(Uint(500))"},"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":true,"fast_path_reason":"secondary_order_pushdown_eligible","residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[{"node_id":1,"node_type":"IndexPredicatePrefilter","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"full","predicate_pushdown":"strict_all_or_none","fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"pushdown":"Text(\"code>=Uint(100) AND code<=Uint(500)\")"}},{"node_id":2,"node_type":"SecondaryOrderPushdown","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"index":"Text(\"code_unique\")","prefix_len":"Uint(0)"}},{"node_id":3,"node_type":"IndexRangeLimitPushdown","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"fetch":"Uint(4)"}},{"node_id":4,"node_type":"OrderByAccessSatisfied","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"order_satisfied_by_index":"Bool(true)"}},{"node_id":5,"node_type":"LimitOffset","layer":"terminal","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":3,"cursor":false,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"offset":"Uint(0)"}}],"node_properties":{"access_choice_alternatives":"List([])","access_choice_chosen":"Text(\"index:code_unique\")","access_choice_chosen_reason":"Text(\"single_candidate\")","access_choice_rejections":"List([])","continuation_mode":"Text(\"initial\")","covering_scan_reason":"Text(\"order_requires_materialization\")","fast_path_rejections":"List([Text(\"primary_key=pk_order_fast_path_ineligible\")])","fast_path_selected":"Text(\"secondary_prefix\")","fast_path_selected_reason":"Text(\"secondary_order_pushdown_eligible\")","fetch":"Uint(4)","prefix_len":"Uint(0)","projected_fields":"List([Text(\"id\"), Text(\"code\"), Text(\"label\")])","projection_pushdown":"Bool(false)","resume_from":"Text(\"none\")","scan_direction":"Text(\"asc\")"}}"#;
    let expected = vec![
        "snapshot_version=1".to_string(),
        "plan_hash=1584d2ec357518f61a1a0e0783233027a0979e891c4ed1775f3216a1608abf40"
            .to_string(),
        "mode=Load(LoadSpec { limit: Some(3), offset: 0 })".to_string(),
        "is_grouped=false".to_string(),
        "execution_strategy=Ordered".to_string(),
        "ordering_direction=Asc".to_string(),
        "distinct_execution_strategy=None".to_string(),
        "projection_selection=All".to_string(),
        "projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId(\"id\")), alias: None }, Scalar { expr: Field(FieldId(\"code\")), alias: None }, Scalar { expr: Field(FieldId(\"label\")), alias: None }] }".to_string(),
        "order_spec=Some(OrderSpec { fields: [(\"code\", Asc), (\"id\", Asc)] })".to_string(),
        "page_spec=Some(PageSpec { limit: Some(3), offset: 0 })".to_string(),
        "projection_coverage_flag=false".to_string(),
        "continuation_signature=8e12a2f46097653cad9d9ca37ee324e5a633c288e2665e82a335b12c1661c26f"
            .to_string(),
        "index_prefix_specs=[]".to_string(),
        "index_range_specs=[{index:code_unique,bound_type:range,lower:included(len:342:head:00001b556e697175:tail:0000000064000100),upper:included(len:405:head:00001b556e697175:tail:ffffffffffffffff)}]".to_string(),
        "explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: Some(3), offset: 0 }), access: IndexRange { name: \"code_unique\", fields: [\"code\"], prefix_len: 0, prefix: [], lower: Included(Uint(100)), upper: Included(Uint(500)) }, predicate: And([Compare { field: \"code\", op: Lte, value: Uint(500), coercion: CoercionSpec { id: Strict, params: {} } }, Compare { field: \"code\", op: Gte, value: Uint(100), coercion: CoercionSpec { id: Strict, params: {} } }]), predicate_model: Some(And([Compare(ComparePredicate { field: \"code\", op: Lte, value: Uint(500), coercion: CoercionSpec { id: Strict, params: {} } }), Compare(ComparePredicate { field: \"code\", op: Gte, value: Uint(100), coercion: CoercionSpec { id: Strict, params: {} } })])), order_by: Fields([ExplainOrder { field: \"code\", direction: Asc }, ExplainOrder { field: \"id\", direction: Asc }]), distinct: false, grouping: None, order_pushdown: MissingModelContext, page: Page { limit: Some(3), offset: 0 }, delete_limit: None, consistency: Ignore }".to_string(),
        format!("execution_descriptor_json={expected_descriptor_json}"),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "index-range execution pipeline snapshot drifted; planner/executor boundary must remain stable",
    );
}

#[test]
fn query_execution_pipeline_snapshot_for_grouped_aggregate_shape_is_stable() {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .group_by("group")
        .expect("group_by(group) should build")
        .aggregate(crate::db::count())
        .limit(2);
    let actual = query_grouped_execution_pipeline_snapshot(&query);
    let expected = vec![
        "snapshot_version=1".to_string(),
        "plan_hash=65a14b322e6c2839b6146b919ed2edc4321c260f8c20b3116faf9dc56838169f"
            .to_string(),
        "mode=Load(LoadSpec { limit: Some(2), offset: 0 })".to_string(),
        "is_grouped=true".to_string(),
        "execution_strategy=Grouped".to_string(),
        "ordering_direction=Asc".to_string(),
        "distinct_execution_strategy=None".to_string(),
        "projection_selection=All".to_string(),
        "projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId(\"group\")), alias: None }, Scalar { expr: Aggregate(AggregateExpr { kind: Count, target_field: None, distinct: false }), alias: None }] }".to_string(),
        "order_spec=None".to_string(),
        "page_spec=Some(PageSpec { limit: Some(2), offset: 0 })".to_string(),
        "projection_coverage_flag=true".to_string(),
        "continuation_signature=1135fea1b0913c016c24038bd41769f2bc1eaa27ae9cba5511c638429caea2a1"
            .to_string(),
        "index_prefix_specs=[{index:group_rank,bound_type:equality,lower:included(len:345:head:00001f5075736864:tail:0007000100000100),upper:included(len:4503:head:00001f5075736864:tail:ffffffffffffffff)}]".to_string(),
        "index_range_specs=[]".to_string(),
        "explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: Some(2), offset: 0 }), access: IndexPrefix { name: \"group_rank\", fields: [\"group\", \"rank\"], prefix_len: 1, values: [Uint(7)] }, predicate: Compare { field: \"group\", op: Eq, value: Uint(7), coercion: CoercionSpec { id: Strict, params: {} } }, predicate_model: Some(Compare(ComparePredicate { field: \"group\", op: Eq, value: Uint(7), coercion: CoercionSpec { id: Strict, params: {} } })), order_by: None, distinct: false, grouping: Grouped { strategy: HashGroup, group_fields: [ExplainGroupField { slot_index: 1, field: \"group\" }], aggregates: [ExplainGroupAggregate { kind: Count, target_field: None, distinct: false }], having: None, max_groups: 18446744073709551615, max_group_bytes: 18446744073709551615 }, order_pushdown: MissingModelContext, page: Page { limit: Some(2), offset: 0 }, delete_limit: None, consistency: Ignore }".to_string(),
        "route_execution_mode_case=AggregateGrouped".to_string(),
        "route_execution_mode=Materialized".to_string(),
        "route_continuation_mode=Initial".to_string(),
        "grouped_outcome=MaterializedFallback".to_string(),
        "grouped_rejection=None".to_string(),
        "grouped_eligible=true".to_string(),
        "grouped_execution_mode=Materialized".to_string(),
        "grouped_execution_strategy=HashMaterialized".to_string(),
    ]
    .join("\n");

    assert_eq!(
        actual, expected,
        "grouped aggregate execution pipeline snapshot drifted; grouped planner/executor boundary must remain stable",
    );
}

#[test]
fn query_explain_execution_verbose_includes_route_diagnostics() {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_by("label");

    let verbose = query
        .explain_execution_verbose()
        .expect("execution verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_eq!(
        diagnostics.get(DIAG_ROUTE_SECONDARY_ORDER_PUSHDOWN),
        Some(&"rejected(OrderFieldsDoNotMatchIndex(index=group_rank,prefix_len=1,expected_suffix=[\"rank\"],expected_full=[\"group\", \"rank\"],actual=[\"label\"]))".to_string()),
        "verbose execution explain should expose explicit route rejection reason",
    );
    assert_eq!(
        diagnostics.get(DIAG_PLAN_MODE),
        Some(&"Load(LoadSpec { limit: None, offset: 0 })".to_string()),
        "verbose execution explain should include logical plan mode diagnostics",
    );
}

#[test]
fn query_explain_execution_verbose_reports_top_n_seek_hints() {
    let verbose = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("id")
        .offset(2)
        .limit(3)
        .explain_execution_verbose()
        .expect("top-n verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_eq!(
        diagnostics.get(DIAG_ROUTE_TOP_N_SEEK),
        Some(&"fetch(6)".to_string()),
        "verbose execution explain should freeze top-n seek fetch diagnostics",
    );
    assert_eq!(
        diagnostics.get(DIAG_DESCRIPTOR_HAS_TOP_N_SEEK),
        Some(&"true".to_string()),
        "descriptor diagnostics should report TopNSeek node presence",
    );
}

#[test]
fn query_explain_execution_verbose_diagnostics_snapshot_for_top_n_seek_shape() {
    let verbose = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("id")
        .offset(2)
        .limit(3)
        .explain_execution_verbose()
        .expect("top-n verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diagnostic.route.execution_mode=Streaming",
        "diagnostic.route.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diagnostic.route.continuation_applied=false",
        "diagnostic.route.limit=Some(3)",
        "diagnostic.route.secondary_order_pushdown=not_applicable",
        "diagnostic.route.top_n_seek=fetch(6)",
        "diagnostic.route.index_range_limit_pushdown=disabled",
        "diagnostic.route.predicate_stage=none",
        "diagnostic.route.projected_fields=[\"id\"]",
        "diagnostic.route.projection_pushdown=false",
        "diagnostic.route.access_choice_chosen=full_scan",
        "diagnostic.route.access_choice_chosen_reason=non_index_access",
        "diagnostic.route.access_choice_alternatives=[]",
        "diagnostic.route.access_choice_rejections=[]",
        "diagnostic.descriptor.has_top_n_seek=true",
        "diagnostic.descriptor.has_index_range_limit_pushdown=false",
        "diagnostic.descriptor.has_index_predicate_prefilter=false",
        "diagnostic.descriptor.has_residual_predicate_filter=false",
        "diagnostic.plan.mode=Load(LoadSpec { limit: Some(3), offset: 2 })",
        "diagnostic.plan.order_pushdown=missing_model_context",
        "diagnostic.plan.predicate_pushdown=none",
        "diagnostic.plan.distinct=false",
        "diagnostic.plan.page=Page { limit: Some(3), offset: 2 }",
        "diagnostic.plan.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "top-n verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn query_explain_execution_verbose_reports_temporal_ranked_order_shape_parity() {
    let top_like_verbose = Query::<TemporalBoundaryEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("occurred_on")
        .order_by("id")
        .limit(2)
        .explain_execution_verbose()
        .expect("temporal top-like verbose explain should build");
    let bottom_like_verbose = Query::<TemporalBoundaryEntity>::new(MissingRowPolicy::Ignore)
        .order_by("occurred_on")
        .order_by("id")
        .limit(2)
        .explain_execution_verbose()
        .expect("temporal bottom-like verbose explain should build");

    let top_like = verbose_diagnostics_map(&top_like_verbose);
    let bottom_like = verbose_diagnostics_map(&bottom_like_verbose);
    let parity_keys = [
        "diagnostic.route.execution_mode",
        "diagnostic.route.fast_path_order",
        "diagnostic.route.continuation_applied",
        "diagnostic.route.limit",
        DIAG_ROUTE_SECONDARY_ORDER_PUSHDOWN,
        DIAG_ROUTE_TOP_N_SEEK,
        DIAG_ROUTE_INDEX_RANGE_LIMIT_PUSHDOWN,
        DIAG_ROUTE_PREDICATE_STAGE,
        DIAG_ROUTE_PROJECTED_FIELDS,
        DIAG_ROUTE_PROJECTION_PUSHDOWN,
        DIAG_ROUTE_ACCESS_CHOICE_CHOSEN,
        DIAG_ROUTE_ACCESS_CHOICE_CHOSEN_REASON,
        DIAG_ROUTE_ACCESS_CHOICE_ALTERNATIVES,
        DIAG_ROUTE_ACCESS_CHOICE_REJECTIONS,
        DIAG_DESCRIPTOR_HAS_TOP_N_SEEK,
        DIAG_DESCRIPTOR_HAS_INDEX_RANGE_LIMIT_PUSHDOWN,
        "diagnostic.descriptor.has_index_predicate_prefilter",
        "diagnostic.descriptor.has_residual_predicate_filter",
        DIAG_PLAN_MODE,
        "diagnostic.plan.order_pushdown",
        DIAG_PLAN_PREDICATE_PUSHDOWN,
        "diagnostic.plan.distinct",
        "diagnostic.plan.page",
        "diagnostic.plan.consistency",
    ];
    for key in parity_keys {
        assert_eq!(
            top_like.get(key),
            bottom_like.get(key),
            "temporal top-like vs bottom-like ranked query shapes should keep verbose diagnostic parity for key {key}",
        );
    }
}

#[test]
fn query_explain_execution_verbose_diagnostics_snapshot_for_temporal_ranked_shape() {
    let verbose = Query::<TemporalBoundaryEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("occurred_on")
        .order_by("id")
        .limit(2)
        .explain_execution_verbose()
        .expect("temporal ranked verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diagnostic.route.execution_mode=Materialized",
        "diagnostic.route.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diagnostic.route.continuation_applied=false",
        "diagnostic.route.limit=Some(2)",
        "diagnostic.route.secondary_order_pushdown=not_applicable",
        "diagnostic.route.top_n_seek=disabled",
        "diagnostic.route.index_range_limit_pushdown=disabled",
        "diagnostic.route.predicate_stage=none",
        "diagnostic.route.projected_fields=[\"id\", \"occurred_on\", \"occurred_at\", \"elapsed\"]",
        "diagnostic.route.projection_pushdown=false",
        "diagnostic.route.access_choice_chosen=full_scan",
        "diagnostic.route.access_choice_chosen_reason=non_index_access",
        "diagnostic.route.access_choice_alternatives=[]",
        "diagnostic.route.access_choice_rejections=[]",
        "diagnostic.descriptor.has_top_n_seek=false",
        "diagnostic.descriptor.has_index_range_limit_pushdown=false",
        "diagnostic.descriptor.has_index_predicate_prefilter=false",
        "diagnostic.descriptor.has_residual_predicate_filter=false",
        "diagnostic.plan.mode=Load(LoadSpec { limit: Some(2), offset: 0 })",
        "diagnostic.plan.order_pushdown=missing_model_context",
        "diagnostic.plan.predicate_pushdown=none",
        "diagnostic.plan.distinct=false",
        "diagnostic.plan.page=Page { limit: Some(2), offset: 0 }",
        "diagnostic.plan.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "temporal ranked verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn query_explain_execution_verbose_reports_index_range_limit_pushdown_hints() {
    let range_predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Gte,
            Value::Uint(100),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Lt,
            Value::Uint(200),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::Eq,
            Value::Text("keep".to_string()),
            CoercionId::Strict,
        )),
    ]);

    let verbose = Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(range_predicate)
        .order_by("code")
        .order_by("id")
        .limit(2)
        .explain_execution_verbose()
        .expect("index-range verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&verbose);
    assert_eq!(
        diagnostics.get(DIAG_ROUTE_INDEX_RANGE_LIMIT_PUSHDOWN),
        Some(&"fetch(3)".to_string()),
        "verbose execution explain should freeze index-range pushdown fetch diagnostics",
    );
    assert_eq!(
        diagnostics.get(DIAG_DESCRIPTOR_HAS_INDEX_RANGE_LIMIT_PUSHDOWN),
        Some(&"true".to_string()),
        "descriptor diagnostics should report index-range pushdown node presence",
    );
    assert_eq!(
        diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        Some(&"residual_post_access".to_string()),
        "verbose execution explain should freeze predicate-stage diagnostics",
    );
}

#[test]
fn query_explain_execution_verbose_diagnostics_snapshot_for_index_range_pushdown_shape() {
    let range_predicate = Predicate::And(vec![
        Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Gte,
            Value::Uint(100),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Lt,
            Value::Uint(200),
            CoercionId::Strict,
        )),
        Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::Eq,
            Value::Text("keep".to_string()),
            CoercionId::Strict,
        )),
    ]);

    let verbose = Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(range_predicate)
        .order_by("code")
        .order_by("id")
        .limit(2)
        .explain_execution_verbose()
        .expect("index-range verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diagnostic.route.execution_mode=Streaming",
        "diagnostic.route.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diagnostic.route.continuation_applied=false",
        "diagnostic.route.limit=Some(2)",
        "diagnostic.route.secondary_order_pushdown=eligible(index=code_unique,prefix_len=0)",
        "diagnostic.route.top_n_seek=disabled",
        "diagnostic.route.index_range_limit_pushdown=fetch(3)",
        "diagnostic.route.predicate_stage=residual_post_access",
        "diagnostic.route.projected_fields=[\"id\", \"code\", \"label\"]",
        "diagnostic.route.projection_pushdown=false",
        "diagnostic.route.access_choice_chosen=index:code_unique",
        "diagnostic.route.access_choice_chosen_reason=single_candidate",
        "diagnostic.route.access_choice_alternatives=[]",
        "diagnostic.route.access_choice_rejections=[]",
        "diagnostic.descriptor.has_top_n_seek=false",
        "diagnostic.descriptor.has_index_range_limit_pushdown=true",
        "diagnostic.descriptor.has_index_predicate_prefilter=false",
        "diagnostic.descriptor.has_residual_predicate_filter=true",
        "diagnostic.plan.mode=Load(LoadSpec { limit: Some(2), offset: 0 })",
        "diagnostic.plan.order_pushdown=missing_model_context",
        "diagnostic.plan.predicate_pushdown=applied(index_range)",
        "diagnostic.plan.distinct=false",
        "diagnostic.plan.page=Page { limit: Some(2), offset: 0 }",
        "diagnostic.plan.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "index-range verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn query_explain_execution_verbose_diagnostics_snapshot_for_rejection_shape() {
    let verbose = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_by("label")
        .explain_execution_verbose()
        .expect("execution verbose explain should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diagnostic.route.execution_mode=Materialized",
        "diagnostic.route.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diagnostic.route.continuation_applied=false",
        "diagnostic.route.limit=None",
        "diagnostic.route.secondary_order_pushdown=rejected(OrderFieldsDoNotMatchIndex(index=group_rank,prefix_len=1,expected_suffix=[\"rank\"],expected_full=[\"group\", \"rank\"],actual=[\"label\"]))",
        "diagnostic.route.top_n_seek=disabled",
        "diagnostic.route.index_range_limit_pushdown=disabled",
        "diagnostic.route.predicate_stage=index_prefilter(strict_all_or_none)",
        "diagnostic.route.projected_fields=[\"id\", \"group\", \"rank\", \"label\"]",
        "diagnostic.route.projection_pushdown=false",
        "diagnostic.route.access_choice_chosen=index:group_rank",
        "diagnostic.route.access_choice_chosen_reason=single_candidate",
        "diagnostic.route.access_choice_alternatives=[]",
        "diagnostic.route.access_choice_rejections=[]",
        "diagnostic.descriptor.has_top_n_seek=false",
        "diagnostic.descriptor.has_index_range_limit_pushdown=false",
        "diagnostic.descriptor.has_index_predicate_prefilter=true",
        "diagnostic.descriptor.has_residual_predicate_filter=false",
        "diagnostic.plan.mode=Load(LoadSpec { limit: None, offset: 0 })",
        "diagnostic.plan.order_pushdown=missing_model_context",
        "diagnostic.plan.predicate_pushdown=applied(index_prefix)",
        "diagnostic.plan.distinct=false",
        "diagnostic.plan.page=None",
        "diagnostic.plan.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "verbose diagnostics snapshot drifted; output ordering and values are part of the explain contract",
    );
}

#[test]
fn query_explain_execution_scalar_surface_defers_projection_and_grouped_node_families() {
    let by_key = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .by_id(Ulid::from_u128(9_301))
        .explain_execution()
        .expect("by-key execution descriptor should build");
    let pushdown_rejected = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_by("label")
        .explain_execution()
        .expect("pushdown-rejected descriptor should build");
    let index_range = Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Gte,
                Value::Uint(100),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Lt,
                Value::Uint(200),
                CoercionId::Strict,
            )),
        ]))
        .order_by("code")
        .order_by("id")
        .limit(2)
        .explain_execution()
        .expect("index-range descriptor should build");

    let mut emitted = BTreeSet::new();
    collect_execution_node_types(&by_key, &mut emitted);
    collect_execution_node_types(&pushdown_rejected, &mut emitted);
    collect_execution_node_types(&index_range, &mut emitted);

    let deferred = [
        ExplainExecutionNodeType::ProjectionMaterialized.as_str(),
        ExplainExecutionNodeType::ProjectionIndexOnly.as_str(),
        ExplainExecutionNodeType::GroupedAggregateHashMaterialized.as_str(),
        ExplainExecutionNodeType::GroupedAggregateOrderedMaterialized.as_str(),
    ];

    for node_type in deferred {
        assert!(
            !emitted.contains(node_type),
            "scalar execution descriptors intentionally defer node family {node_type} in 0.42.x",
        );
    }
}

#[test]
fn query_explain_execution_verbose_reports_is_null_predicate_pushdown_reason_paths() {
    let primary_key_is_null_verbose = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::IsNull {
            field: "id".to_string(),
        })
        .explain_execution_verbose()
        .expect("primary-key is-null verbose explain should build");
    let secondary_is_null_verbose = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::IsNull {
            field: "group".to_string(),
        })
        .explain_execution_verbose()
        .expect("secondary is-null verbose explain should build");

    let primary_key_diagnostics = verbose_diagnostics_map(&primary_key_is_null_verbose);
    let secondary_diagnostics = verbose_diagnostics_map(&secondary_is_null_verbose);

    assert_eq!(
        primary_key_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"applied(empty_access_contract)".to_string()),
        "impossible primary-key IS NULL should surface empty-contract predicate pushdown diagnostics",
    );
    assert_eq!(
        secondary_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"fallback(is_null_full_scan)".to_string()),
        "non-primary IS NULL should surface full-scan fallback predicate diagnostics",
    );
}

#[test]
fn query_explain_execution_verbose_reports_non_strict_predicate_fallback_reason_path() {
    let non_strict_verbose = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::NumericWiden,
        )))
        .explain_execution_verbose()
        .expect("non-strict predicate verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&non_strict_verbose);
    assert_eq!(
        diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"fallback(non_strict_compare_coercion)".to_string()),
        "non-strict indexed compare should surface full-scan fallback predicate diagnostics",
    );
    assert_eq!(
        diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        Some(&"residual_post_access".to_string()),
        "non-strict indexed compare should execute as residual post-access predicate stage",
    );
}

#[test]
fn query_explain_execution_verbose_reports_empty_prefix_starts_with_fallback_reason_path() {
    let empty_prefix_verbose = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text(String::new()),
            CoercionId::Strict,
        )))
        .explain_execution_verbose()
        .expect("empty-prefix starts-with verbose explain should build");
    let non_empty_prefix_verbose = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text("label".to_string()),
            CoercionId::Strict,
        )))
        .explain_execution_verbose()
        .expect("non-empty starts-with verbose explain should build");

    let empty_prefix_diagnostics = verbose_diagnostics_map(&empty_prefix_verbose);
    let non_empty_prefix_diagnostics = verbose_diagnostics_map(&non_empty_prefix_verbose);
    assert_eq!(
        empty_prefix_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"fallback(starts_with_empty_prefix)".to_string()),
        "empty-prefix starts-with should surface the explicit empty-prefix fallback reason",
    );
    assert_eq!(
        non_empty_prefix_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"fallback(full_scan)".to_string()),
        "non-empty starts-with over a non-indexed field should remain generic full-scan fallback",
    );
    assert_eq!(
        empty_prefix_diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        Some(&"residual_post_access".to_string()),
        "empty-prefix starts-with fallback should preserve residual predicate stage diagnostics",
    );
}

#[test]
fn query_explain_execution_verbose_reports_text_operator_fallback_reason_path() {
    let text_contains_ci_verbose = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::TextContainsCi {
            field: "label".to_string(),
            value: Value::Text("label".to_string()),
        })
        .explain_execution_verbose()
        .expect("text-contains-ci verbose explain should build");
    let ends_with_verbose = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::EndsWith,
            Value::Text("fix".to_string()),
            CoercionId::Strict,
        )))
        .explain_execution_verbose()
        .expect("ends-with verbose explain should build");

    let text_contains_ci_diagnostics = verbose_diagnostics_map(&text_contains_ci_verbose);
    let ends_with_diagnostics = verbose_diagnostics_map(&ends_with_verbose);
    assert_eq!(
        text_contains_ci_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"fallback(text_operator_full_scan)".to_string()),
        "text contains-ci should surface dedicated text-operator full-scan fallback reason",
    );
    assert_eq!(
        ends_with_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"fallback(text_operator_full_scan)".to_string()),
        "ends-with compare should surface dedicated text-operator full-scan fallback reason",
    );
    assert_eq!(
        text_contains_ci_diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        Some(&"residual_post_access".to_string()),
        "text-operator fallback should preserve residual predicate-stage diagnostics",
    );
}

#[test]
fn query_explain_execution_verbose_non_strict_ends_with_uses_non_strict_fallback_precedence() {
    let non_strict_ends_with_verbose = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::EndsWith,
            Value::Text("fix".to_string()),
            CoercionId::TextCasefold,
        )))
        .explain_execution_verbose()
        .expect("non-strict ends-with verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&non_strict_ends_with_verbose);
    assert_eq!(
        diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"fallback(non_strict_compare_coercion)".to_string()),
        "non-strict ends-with should report non-strict compare fallback reason",
    );
    assert_ne!(
        diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"fallback(text_operator_full_scan)".to_string()),
        "non-strict ends-with should not be classified as text-operator fallback",
    );
}

#[test]
fn query_explain_execution_verbose_keeps_collection_contains_on_generic_full_scan_fallback() {
    let collection_contains_verbose = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "tags",
            CompareOp::Contains,
            Value::Uint(7),
            CoercionId::CollectionElement,
        )))
        .explain_execution_verbose()
        .expect("collection contains verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&collection_contains_verbose);
    assert_eq!(
        diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"fallback(non_strict_compare_coercion)".to_string()),
        "collection-element contains should continue to report non-strict compare fallback",
    );
    assert_ne!(
        diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"fallback(text_operator_full_scan)".to_string()),
        "collection-element contains should not be classified as text-operator fallback",
    );
}

#[test]
fn query_explain_execution_verbose_diagnostics_snapshot_for_is_null_fallback_shape() {
    let verbose = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::IsNull {
            field: "group".to_string(),
        })
        .explain_execution_verbose()
        .expect("is-null fallback verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diagnostic.route.execution_mode=Materialized",
        "diagnostic.route.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diagnostic.route.continuation_applied=false",
        "diagnostic.route.limit=None",
        "diagnostic.route.secondary_order_pushdown=not_applicable",
        "diagnostic.route.top_n_seek=disabled",
        "diagnostic.route.index_range_limit_pushdown=disabled",
        "diagnostic.route.predicate_stage=residual_post_access",
        "diagnostic.route.projected_fields=[\"id\", \"group\", \"rank\", \"label\"]",
        "diagnostic.route.projection_pushdown=false",
        "diagnostic.route.access_choice_chosen=full_scan",
        "diagnostic.route.access_choice_chosen_reason=non_index_access",
        "diagnostic.route.access_choice_alternatives=[]",
        "diagnostic.route.access_choice_rejections=[]",
        "diagnostic.descriptor.has_top_n_seek=false",
        "diagnostic.descriptor.has_index_range_limit_pushdown=false",
        "diagnostic.descriptor.has_index_predicate_prefilter=false",
        "diagnostic.descriptor.has_residual_predicate_filter=true",
        "diagnostic.plan.mode=Load(LoadSpec { limit: None, offset: 0 })",
        "diagnostic.plan.order_pushdown=missing_model_context",
        "diagnostic.plan.predicate_pushdown=fallback(is_null_full_scan)",
        "diagnostic.plan.distinct=false",
        "diagnostic.plan.page=None",
        "diagnostic.plan.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "is-null fallback verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn query_explain_execution_verbose_diagnostics_snapshot_for_non_strict_fallback_shape() {
    let verbose = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::NumericWiden,
        )))
        .explain_execution_verbose()
        .expect("non-strict fallback verbose explain snapshot should build");

    let diagnostics = verbose_diagnostics_lines(&verbose);
    let expected = vec![
        "diagnostic.route.execution_mode=Materialized",
        "diagnostic.route.fast_path_order=[PrimaryKey, SecondaryPrefix, IndexRange]",
        "diagnostic.route.continuation_applied=false",
        "diagnostic.route.limit=None",
        "diagnostic.route.secondary_order_pushdown=not_applicable",
        "diagnostic.route.top_n_seek=disabled",
        "diagnostic.route.index_range_limit_pushdown=disabled",
        "diagnostic.route.predicate_stage=residual_post_access",
        "diagnostic.route.projected_fields=[\"id\", \"group\", \"rank\", \"label\"]",
        "diagnostic.route.projection_pushdown=false",
        "diagnostic.route.access_choice_chosen=full_scan",
        "diagnostic.route.access_choice_chosen_reason=non_index_access",
        "diagnostic.route.access_choice_alternatives=[]",
        "diagnostic.route.access_choice_rejections=[]",
        "diagnostic.descriptor.has_top_n_seek=false",
        "diagnostic.descriptor.has_index_range_limit_pushdown=false",
        "diagnostic.descriptor.has_index_predicate_prefilter=false",
        "diagnostic.descriptor.has_residual_predicate_filter=true",
        "diagnostic.plan.mode=Load(LoadSpec { limit: None, offset: 0 })",
        "diagnostic.plan.order_pushdown=missing_model_context",
        "diagnostic.plan.predicate_pushdown=fallback(non_strict_compare_coercion)",
        "diagnostic.plan.distinct=false",
        "diagnostic.plan.page=None",
        "diagnostic.plan.consistency=Ignore",
    ]
    .into_iter()
    .map(ToOwned::to_owned)
    .collect::<Vec<_>>();

    assert_eq!(
        diagnostics, expected,
        "non-strict fallback verbose diagnostics snapshot drifted; ordering and values are part of the explain contract",
    );
}

#[test]
fn query_explain_execution_verbose_reports_equivalent_empty_contract_reason_paths() {
    let is_null_verbose = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::IsNull {
            field: "id".to_string(),
        })
        .explain_execution_verbose()
        .expect("primary-key is-null verbose explain should build");
    let strict_in_empty_verbose = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(Vec::new()),
            CoercionId::Strict,
        )))
        .explain_execution_verbose()
        .expect("strict IN [] verbose explain should build");

    let is_null_diagnostics = verbose_diagnostics_map(&is_null_verbose);
    let strict_in_empty_diagnostics = verbose_diagnostics_map(&strict_in_empty_verbose);
    assert_eq!(
        is_null_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"applied(empty_access_contract)".to_string()),
        "primary-key is-null should surface empty-contract predicate diagnostics",
    );
    assert_eq!(
        strict_in_empty_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"applied(empty_access_contract)".to_string()),
        "strict IN [] should surface empty-contract predicate diagnostics",
    );
}

#[test]
fn query_explain_execution_verbose_reports_equivalent_empty_contract_route_stage_parity() {
    let is_null_verbose = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::IsNull {
            field: "id".to_string(),
        })
        .explain_execution_verbose()
        .expect("primary-key is-null verbose explain should build");
    let strict_in_empty_verbose = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(Vec::new()),
            CoercionId::Strict,
        )))
        .explain_execution_verbose()
        .expect("strict IN [] verbose explain should build");

    let is_null_diagnostics = verbose_diagnostics_map(&is_null_verbose);
    let strict_in_empty_diagnostics = verbose_diagnostics_map(&strict_in_empty_verbose);
    assert_eq!(
        is_null_diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        strict_in_empty_diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        "equivalent empty-contract predicates should keep route predicate-stage diagnostics in parity",
    );
}

#[test]
fn query_explain_execution_verbose_reports_equivalent_in_set_route_stage_parity() {
    let in_permuted_verbose = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::In,
            Value::List(vec![Value::Uint(8), Value::Uint(7), Value::Uint(8)]),
            CoercionId::Strict,
        )))
        .explain_execution_verbose()
        .expect("permuted IN verbose explain should build");
    let in_canonical_verbose = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::In,
            Value::List(vec![Value::Uint(7), Value::Uint(8)]),
            CoercionId::Strict,
        )))
        .explain_execution_verbose()
        .expect("canonical IN verbose explain should build");

    let in_permuted_diagnostics = verbose_diagnostics_map(&in_permuted_verbose);
    let in_canonical_diagnostics = verbose_diagnostics_map(&in_canonical_verbose);
    assert_eq!(
        in_permuted_diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        in_canonical_diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        "equivalent canonical IN sets should keep route predicate-stage diagnostics in parity",
    );
}

#[test]
fn query_explain_execution_verbose_reports_equivalent_between_and_eq_parity() {
    let equivalent_between_verbose = Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Gte,
                Value::Uint(100),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Lte,
                Value::Uint(100),
                CoercionId::Strict,
            )),
        ]))
        .order_by("code")
        .order_by("id")
        .explain_execution_verbose()
        .expect("equivalent-between verbose explain should build");
    let strict_eq_verbose = Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "code",
            CompareOp::Eq,
            Value::Uint(100),
            CoercionId::Strict,
        )))
        .order_by("code")
        .order_by("id")
        .explain_execution_verbose()
        .expect("strict-eq verbose explain should build");

    let between_diagnostics = verbose_diagnostics_map(&equivalent_between_verbose);
    let eq_diagnostics = verbose_diagnostics_map(&strict_eq_verbose);
    assert_eq!(
        between_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        eq_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        "equivalent BETWEEN-style bounds and strict equality should report identical pushdown reason labels",
    );
    assert_eq!(
        between_diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        eq_diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        "equivalent BETWEEN-style bounds and strict equality should preserve route predicate-stage parity",
    );
}

#[test]
fn query_explain_execution_verbose_reports_equivalent_prefix_like_route_stage_parity() {
    let starts_with_verbose = Query::<TextPrefixParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text("foo".to_string()),
            CoercionId::Strict,
        )))
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("starts-with verbose explain should build");
    let equivalent_range_verbose = Query::<TextPrefixParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "label",
                CompareOp::Gte,
                Value::Text("foo".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "label",
                CompareOp::Lt,
                Value::Text("fop".to_string()),
                CoercionId::Strict,
            )),
        ]))
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("equivalent-range verbose explain should build");

    let starts_with_diagnostics = verbose_diagnostics_map(&starts_with_verbose);
    let equivalent_range_diagnostics = verbose_diagnostics_map(&equivalent_range_verbose);
    assert_eq!(
        starts_with_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        equivalent_range_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        "equivalent prefix-like and bounded-range forms should report identical predicate pushdown reason labels",
    );
    assert_eq!(
        starts_with_diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        equivalent_range_diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        "equivalent prefix-like and bounded-range forms should preserve route predicate-stage parity",
    );
}

#[test]
fn query_explain_execution_verbose_reports_indexed_prefix_like_strict_prefilter_stage() {
    let starts_with_verbose = Query::<TextPrefixParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text("foo".to_string()),
            CoercionId::Strict,
        )))
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("starts-with verbose explain should build");

    let diagnostics = verbose_diagnostics_map(&starts_with_verbose);
    assert_eq!(
        diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        Some(&"applied(index_range)".to_string()),
        "indexed starts-with should report index-range pushdown at plan diagnostics",
    );
    assert_eq!(
        diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        Some(&"index_prefilter(strict_all_or_none)".to_string()),
        "indexed strict starts-with should compile to strict index prefilter route stage",
    );
    assert_eq!(
        diagnostics.get("diagnostic.descriptor.has_index_predicate_prefilter"),
        Some(&"true".to_string()),
        "indexed strict starts-with should emit index prefilter descriptor node",
    );
    assert_eq!(
        diagnostics.get("diagnostic.descriptor.has_residual_predicate_filter"),
        Some(&"false".to_string()),
        "indexed strict starts-with should not require residual predicate filtering",
    );
}

#[test]
fn query_explain_execution_verbose_reports_max_unicode_prefix_like_parity() {
    let prefix = char::from_u32(0x10_FFFF).expect("valid scalar").to_string();
    let starts_with_verbose = Query::<TextPrefixParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "label",
            CompareOp::StartsWith,
            Value::Text(prefix.clone()),
            CoercionId::Strict,
        )))
        .order_by("label")
        .order_by("id")
        .explain_execution_verbose()
        .expect("max-unicode starts-with verbose explain should build");
    let equivalent_lower_bound_verbose =
        Query::<TextPrefixParityEntity>::new(MissingRowPolicy::Ignore)
            .filter(Predicate::Compare(ComparePredicate::with_coercion(
                "label",
                CompareOp::Gte,
                Value::Text(prefix),
                CoercionId::Strict,
            )))
            .order_by("label")
            .order_by("id")
            .explain_execution_verbose()
            .expect("equivalent lower-bound verbose explain should build");

    let starts_with_diagnostics = verbose_diagnostics_map(&starts_with_verbose);
    let lower_bound_diagnostics = verbose_diagnostics_map(&equivalent_lower_bound_verbose);
    assert_eq!(
        starts_with_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        lower_bound_diagnostics.get(DIAG_PLAN_PREDICATE_PUSHDOWN),
        "max-unicode prefix-like and equivalent lower-bound forms should report identical predicate pushdown reason labels",
    );
    assert_eq!(
        starts_with_diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        lower_bound_diagnostics.get(DIAG_ROUTE_PREDICATE_STAGE),
        "max-unicode prefix-like and equivalent lower-bound forms should preserve route predicate-stage parity",
    );
}

#[test]
fn fluent_load_explain_execution_surface_adapters_are_available() {
    let session = DbSession::new(DB);
    let query = session
        .load::<SimpleEntity>()
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Eq,
            Value::Ulid(Ulid::from_u128(9_201)),
            CoercionId::Strict,
        )))
        .order_by("id");
    let descriptor = query
        .explain_execution()
        .expect("fluent execution descriptor explain should build");

    let text = query
        .explain_execution_text()
        .expect("fluent execution text explain should build");
    assert!(
        text.contains("ByKeyLookup"),
        "fluent execution text surface should include root node type",
    );
    assert_eq!(
        text,
        descriptor.render_text_tree(),
        "fluent execution text surface should be canonical descriptor text rendering",
    );

    let json = query
        .explain_execution_json()
        .expect("fluent execution json explain should build");
    assert!(
        json.contains("\"node_type\":\"ByKeyLookup\""),
        "fluent execution json surface should include canonical root node type",
    );
    assert_eq!(
        json,
        descriptor.render_json_canonical(),
        "fluent execution json surface should be canonical descriptor json rendering",
    );

    let verbose = query
        .explain_execution_verbose()
        .expect("fluent execution verbose explain should build");
    assert!(
        verbose.contains("diagnostic.route.secondary_order_pushdown="),
        "fluent execution verbose surface should include diagnostics",
    );
}

#[test]
fn load_ordering_treats_missing_values_consistently_with_direction() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(902),
            opt_rank: None,
            rank: 2,
            tags: vec![2],
            label: "missing-b".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(901),
            opt_rank: None,
            rank: 1,
            tags: vec![1],
            label: "missing-a".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(903),
            opt_rank: Some(10),
            rank: 3,
            tags: vec![3],
            label: "present-10".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(904),
            opt_rank: Some(20),
            rank: 4,
            tags: vec![4],
            label: "present-20".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);

    let asc_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("opt_rank")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("ascending optional-order plan should build");
    let asc = load
        .execute(asc_plan)
        .expect("ascending optional-order query should execute");
    let asc_ids: Vec<Ulid> = asc.into_iter().map(|row| row.entity_ref().id).collect();
    assert_eq!(
        asc_ids,
        vec![
            Ulid::from_u128(901),
            Ulid::from_u128(902),
            Ulid::from_u128(903),
            Ulid::from_u128(904),
        ],
        "ascending order should treat missing as lowest and use PK tie-break within missing rows"
    );

    let desc_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by_desc("opt_rank")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("descending optional-order plan should build");
    let desc = load
        .execute(desc_plan)
        .expect("descending optional-order query should execute");
    let desc_ids: Vec<Ulid> = desc.into_iter().map(|row| row.entity_ref().id).collect();
    assert_eq!(
        desc_ids,
        vec![
            Ulid::from_u128(904),
            Ulid::from_u128(903),
            Ulid::from_u128(901),
            Ulid::from_u128(902),
        ],
        "descending order should reverse present/missing groups while preserving PK tie-break"
    );
}

#[test]
fn load_contains_filters_after_by_id_access() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    let id = Ulid::from_u128(701);
    save.insert(PhaseEntity {
        id,
        opt_rank: Some(1),
        rank: 1,
        tags: vec![2, 9],
        label: "contains".to_string(),
    })
    .expect("save should succeed");

    let contains_nine = Predicate::Compare(ComparePredicate::with_coercion(
        "tags",
        CompareOp::Contains,
        Value::Uint(9),
        CoercionId::CollectionElement,
    ));
    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let hit_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .by_id(id)
        .filter(contains_nine)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("contains hit plan should build");
    let hit = load.execute(hit_plan).expect("contains hit should execute");
    assert_eq!(hit.len(), 1, "contains predicate should match row");

    let contains_missing = Predicate::Compare(ComparePredicate::with_coercion(
        "tags",
        CompareOp::Contains,
        Value::Uint(8),
        CoercionId::CollectionElement,
    ));
    let miss_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .by_id(id)
        .filter(contains_missing)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("contains miss plan should build");
    let miss = load
        .execute(miss_plan)
        .expect("contains miss should execute");
    assert_eq!(
        miss.len(),
        0,
        "contains predicate should filter out non-matching rows after access"
    );
}

#[test]
fn load_secondary_index_missing_ok_skips_stale_keys_by_reading_primary_rows() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for (id, group, rank) in [(7101_u128, 7_u32, 10_u32), (7102, 7, 20), (7103, 7, 30)] {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(id),
            group,
            rank,
            label: format!("g{group}-r{rank}"),
        })
        .expect("seed pushdown row save should succeed");
    }

    remove_pushdown_row_data(7101);

    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::Strict,
    ));
    let explain = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("missing-ok stale-secondary explain should build");
    assert!(
        matches!(explain.access(), ExplainAccessPath::IndexPrefix { .. }),
        "group equality with rank order should plan as secondary index-prefix access",
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("rank")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("missing-ok stale-secondary load plan should build");
    let response = load
        .execute(plan)
        .expect("missing-ok stale-secondary load should succeed");
    let ids: Vec<Ulid> = response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();

    assert_eq!(
        ids,
        vec![Ulid::from_u128(7102), Ulid::from_u128(7103)],
        "Ignore must filter stale secondary keys instead of materializing missing rows",
    );
}

#[test]
fn load_secondary_index_strict_missing_row_surfaces_corruption() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PushdownParityEntity>::new(DB, false);
    for (id, group, rank) in [(7201_u128, 7_u32, 10_u32), (7202, 7, 20), (7203, 7, 30)] {
        save.insert(PushdownParityEntity {
            id: Ulid::from_u128(id),
            group,
            rank,
            label: format!("g{group}-r{rank}"),
        })
        .expect("seed pushdown row save should succeed");
    }

    remove_pushdown_row_data(7201);

    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "group",
        CompareOp::Eq,
        Value::Uint(7),
        CoercionId::Strict,
    ));
    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(MissingRowPolicy::Error)
        .filter(predicate)
        .order_by("rank")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("strict stale-secondary load plan should build");
    let err = load
        .execute(plan)
        .expect_err("strict stale-secondary load should fail on missing primary row");

    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "strict stale-secondary load must classify missing primary rows as corruption",
    );
    assert!(
        err.message.contains("missing row"),
        "strict stale-secondary failure should report missing-row corruption",
    );
}

#[test]
fn delete_limit_applies_to_filtered_rows_only() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<PhaseEntity>::new(DB, false);
    for row in [
        PhaseEntity {
            id: Ulid::from_u128(801),
            opt_rank: Some(1),
            rank: 1,
            tags: vec![1],
            label: "keep-low-1".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(802),
            opt_rank: Some(2),
            rank: 2,
            tags: vec![2],
            label: "keep-low-2".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(803),
            opt_rank: Some(100),
            rank: 100,
            tags: vec![3],
            label: "delete-first".to_string(),
        },
        PhaseEntity {
            id: Ulid::from_u128(804),
            opt_rank: Some(200),
            rank: 200,
            tags: vec![4],
            label: "delete-second".to_string(),
        },
    ] {
        save.insert(row).expect("seed row save should succeed");
    }

    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "rank",
        CompareOp::Gte,
        Value::Uint(100),
        CoercionId::NumericWiden,
    ));
    let delete = DeleteExecutor::<PhaseEntity>::new(DB, false);
    let plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .filter(predicate)
        .order_by("rank")
        .limit(1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("filtered delete plan should build");
    let deleted = delete
        .execute(plan)
        .expect("filtered delete should execute");

    assert_eq!(
        deleted.len(),
        1,
        "delete limit should remove one filtered row"
    );
    assert_eq!(
        deleted[0].entity_ref().rank,
        100,
        "delete limit should apply after filtering+ordering"
    );

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let remaining_plan = Query::<PhaseEntity>::new(MissingRowPolicy::Ignore)
        .order_by("rank")
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("remaining load plan should build");
    let remaining = load
        .execute(remaining_plan)
        .expect("remaining load should execute");
    let remaining_ranks: Vec<u64> = remaining
        .into_iter()
        .map(|row| u64::from(row.entity().rank))
        .collect();

    assert_eq!(
        remaining_ranks,
        vec![1, 2, 200],
        "only one row from the filtered window should be deleted"
    );
}

#[test]
fn delete_blocks_when_target_has_strong_referrer() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_001);
    let source_id = Ulid::from_u128(9_002);

    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");

    let source_save = SaveExecutor::<RelationSourceEntity>::new(REL_DB, false);
    source_save
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_id,
        })
        .expect("source save should succeed");

    let target_delete = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false);
    let delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let err = target_delete
        .execute(delete_plan)
        .expect_err("target delete should be blocked by strong relation");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );

    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}"
    );
    assert!(
        err.message
            .contains(&format!("source_entity={}", RelationSourceEntity::PATH)),
        "diagnostic should include source entity path: {err:?}",
    );
    assert!(
        err.message.contains("source_field=target"),
        "diagnostic should include relation field name: {err:?}",
    );
    assert!(
        err.message
            .contains(&format!("target_entity={}", RelationTargetEntity::PATH)),
        "diagnostic should include target entity path: {err:?}",
    );
    assert!(
        err.message
            .contains("action=delete source rows or retarget relation before deleting target"),
        "diagnostic should include operator action hint: {err:?}",
    );

    let target_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_data(|data_store| data_store.iter().count()))
        })
        .expect("target store access should succeed");
    let source_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationSourceStore::PATH)
                .map(|store| store.with_data(|data_store| data_store.iter().count()))
        })
        .expect("source store access should succeed");
    assert_eq!(target_rows, 1, "blocked delete must keep target row");
    assert_eq!(source_rows, 1, "blocked delete must keep source row");
}

#[test]
fn delete_target_succeeds_after_strong_referrer_is_removed() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_101);
    let source_id = Ulid::from_u128(9_102);

    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");

    let source_save = SaveExecutor::<RelationSourceEntity>::new(REL_DB, false);
    source_save
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_id,
        })
        .expect("source save should succeed");

    let source_delete = DeleteExecutor::<RelationSourceEntity>::new(REL_DB, false);
    let source_delete_plan = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source delete plan should build");
    let deleted_sources = source_delete
        .execute(source_delete_plan)
        .expect("source delete should succeed");
    assert_eq!(deleted_sources.len(), 1, "source row should be removed");

    let target_delete = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false);
    let target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let deleted_targets = target_delete
        .execute(target_delete_plan)
        .expect("target delete should succeed once referrer is removed");
    assert_eq!(deleted_targets.len(), 1, "target row should be removed");
}

#[test]
fn delete_allows_target_with_weak_single_referrer() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_111);
    let source_id = Ulid::from_u128(9_112);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");
    SaveExecutor::<WeakSingleRelationSourceEntity>::new(REL_DB, false)
        .insert(WeakSingleRelationSourceEntity {
            id: source_id,
            target: target_id,
        })
        .expect("weak source save should succeed");

    let reverse_rows_before_delete = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_delete, 0,
        "weak relation should not create reverse strong-relation index entries",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak referrer");
    assert_eq!(deleted_targets.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakSingleRelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source load plan should build");
    let remaining_source = LoadExecutor::<WeakSingleRelationSourceEntity>::new(REL_DB, false)
        .execute(source_plan)
        .expect("source load should succeed");
    assert_eq!(remaining_source.len(), 1, "weak source row should remain");
    assert_eq!(
        remaining_source[0].entity_ref().target,
        target_id,
        "weak source relation value should be preserved",
    );
}

#[test]
fn delete_allows_target_with_weak_optional_referrer() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_121);
    let source_id = Ulid::from_u128(9_122);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");
    SaveExecutor::<WeakOptionalRelationSourceEntity>::new(REL_DB, false)
        .insert(WeakOptionalRelationSourceEntity {
            id: source_id,
            target: Some(target_id),
        })
        .expect("weak optional source save should succeed");

    let reverse_rows_before_delete = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_delete, 0,
        "weak optional relation should not create reverse strong-relation index entries",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak optional referrer");
    assert_eq!(deleted_targets.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakOptionalRelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source load plan should build");
    let remaining_source = LoadExecutor::<WeakOptionalRelationSourceEntity>::new(REL_DB, false)
        .execute(source_plan)
        .expect("source load should succeed");
    assert_eq!(
        remaining_source.len(),
        1,
        "weak optional source row should remain"
    );
    assert_eq!(
        remaining_source[0].entity_ref().target,
        Some(target_id),
        "weak optional source relation value should be preserved",
    );
}

#[test]
fn delete_allows_target_with_weak_list_referrer() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_131);
    let source_id = Ulid::from_u128(9_132);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");
    SaveExecutor::<WeakListRelationSourceEntity>::new(REL_DB, false)
        .insert(WeakListRelationSourceEntity {
            id: source_id,
            targets: vec![target_id],
        })
        .expect("weak list source save should succeed");

    let reverse_rows_before_delete = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_delete, 0,
        "weak list relation should not create reverse strong-relation index entries",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak list referrer");
    assert_eq!(deleted_targets.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakListRelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source load plan should build");
    let remaining_source = LoadExecutor::<WeakListRelationSourceEntity>::new(REL_DB, false)
        .execute(source_plan)
        .expect("source load should succeed");
    assert_eq!(
        remaining_source.len(),
        1,
        "weak list source row should remain"
    );
    assert_eq!(
        remaining_source[0].entity_ref().targets,
        vec![target_id],
        "weak list source relation values should be preserved",
    );
}

#[test]
fn strong_relation_reverse_index_tracks_source_lifecycle() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_201);
    let source_id = Ulid::from_u128(9_202);

    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");

    let source_save = SaveExecutor::<RelationSourceEntity>::new(REL_DB, false);
    source_save
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_id,
        })
        .expect("source save should succeed");

    let reverse_rows_after_insert = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_insert, 1,
        "target index store should contain one reverse-relation entry after source insert",
    );

    let source_delete = DeleteExecutor::<RelationSourceEntity>::new(REL_DB, false);
    let source_delete_plan = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source delete plan should build");
    source_delete
        .execute(source_delete_plan)
        .expect("source delete should succeed");

    let reverse_rows_after_delete = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_delete, 0,
        "target index store reverse entry should be removed after source delete",
    );
}

#[test]
fn strong_relation_reverse_index_moves_on_fk_update() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_a = Ulid::from_u128(9_301);
    let target_b = Ulid::from_u128(9_302);
    let source_id = Ulid::from_u128(9_303);

    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_a })
        .expect("target A save should succeed");
    target_save
        .insert(RelationTargetEntity { id: target_b })
        .expect("target B save should succeed");

    let source_save = SaveExecutor::<RelationSourceEntity>::new(REL_DB, false);
    source_save
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_a,
        })
        .expect("source insert should succeed");

    source_save
        .replace(RelationSourceEntity {
            id: source_id,
            target: target_b,
        })
        .expect("source replace should move relation target");

    let reverse_rows_after_update = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_update, 1,
        "reverse index should remove old target entry and keep only the new one",
    );

    let old_target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_a)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target A delete plan should build");
    let deleted_a = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(old_target_delete_plan)
        .expect("old target should be deletable after relation retarget");
    assert_eq!(deleted_a.len(), 1, "old target should delete cleanly");

    let protected_target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target B delete plan should build");
    let err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(protected_target_delete_plan)
        .expect_err("new target should remain protected by strong relation");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );
}

#[test]
fn recovery_replays_reverse_relation_index_mutations() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_401);
    let source_id = Ulid::from_u128(9_402);

    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");

    let source = RelationSourceEntity {
        id: source_id,
        target: target_id,
    };
    let raw_key = DataKey::try_new::<RelationSourceEntity>(source.id)
        .expect("source data key should build")
        .to_raw()
        .expect("source data key should encode");
    let row_bytes = crate::serialize::serialize(&source).expect("source row should serialize");

    let marker = CommitMarker::new(vec![crate::db::commit::CommitRowOp::new(
        RelationSourceEntity::PATH,
        raw_key.as_bytes().to_vec(),
        None,
        Some(row_bytes),
        crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
    )])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before recovery replay",
    );

    ensure_recovered(&REL_DB).expect("recovery replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after recovery replay",
    );

    let reverse_rows_after_replay = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_replay, 1,
        "recovery replay should materialize reverse relation index entries",
    );

    let target_delete = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false);
    let delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let err = target_delete
        .execute(delete_plan)
        .expect_err("target delete should be blocked after replayed reverse index insert");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );
}

#[expect(clippy::too_many_lines)]
#[test]
fn recovery_startup_rebuild_drops_orphan_reverse_relation_entries() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    // Phase 1: seed two valid targets and two source refs to build two reverse entries.
    let target_live = Ulid::from_u128(9_410);
    let target_orphan = Ulid::from_u128(9_411);
    let source_live = Ulid::from_u128(9_412);
    let source_orphan = Ulid::from_u128(9_413);
    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_live })
        .expect("live target save should succeed");
    target_save
        .insert(RelationTargetEntity { id: target_orphan })
        .expect("orphan target save should succeed");

    let source_save = SaveExecutor::<RelationSourceEntity>::new(REL_DB, false);
    source_save
        .insert(RelationSourceEntity {
            id: source_live,
            target: target_live,
        })
        .expect("live source save should succeed");
    source_save
        .insert(RelationSourceEntity {
            id: source_orphan,
            target: target_orphan,
        })
        .expect("orphan source save should succeed");

    // Phase 2: simulate stale reverse-index drift by removing one source row directly.
    let orphan_source_key = DataKey::try_new::<RelationSourceEntity>(source_orphan)
        .expect("orphan source key should build")
        .to_raw()
        .expect("orphan source key should encode");
    REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationSourceStore::PATH).map(|store| {
                let removed =
                    store.with_data_mut(|data_store| data_store.remove(&orphan_source_key));
                assert!(
                    removed.is_some(),
                    "orphan source row should exist before direct data-store removal",
                );
            })
        })
        .expect("relation source store access should succeed");

    let reverse_rows_before_recovery = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_recovery, 2,
        "stale reverse entry should remain until startup rebuild runs",
    );

    // Phase 3: force startup recovery rebuild and assert stale reverse entry is purged.
    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("startup recovery rebuild should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after startup recovery rebuild",
    );

    let reverse_rows_after_recovery = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_recovery, 1,
        "startup rebuild should drop orphan reverse entries and keep live ones",
    );

    let delete_orphan_target = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_orphan)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("orphan target delete plan should build");
    let deleted_orphan_target = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(delete_orphan_target)
        .expect("orphan target should be deletable after startup rebuild");
    assert_eq!(
        deleted_orphan_target.len(),
        1,
        "orphan target should delete after stale reverse entry is purged",
    );

    let delete_live_target = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_live)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("live target delete plan should build");
    let err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(delete_live_target)
        .expect_err("live target should remain protected by surviving relation");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );
}

#[test]
fn recovery_startup_rebuild_restores_missing_reverse_relation_entry() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    // Phase 1: seed one valid strong relation so forward+reverse state starts consistent.
    let target_id = Ulid::from_u128(9_420);
    let source_id = Ulid::from_u128(9_421);
    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");
    SaveExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_id,
        })
        .expect("source save should succeed");

    // Phase 2: simulate partial-commit drift by removing reverse index state only.
    REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index_mut(IndexStore::clear))
        })
        .expect("target index store access should succeed");
    let reverse_rows_before_recovery = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_recovery, 0,
        "simulated partial-commit state should have missing reverse entry",
    );

    // Phase 3: force startup recovery rebuild and verify reverse symmetry is restored.
    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("startup recovery rebuild should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after startup recovery rebuild",
    );

    let reverse_rows_after_recovery = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_recovery, 1,
        "startup rebuild should restore missing reverse entry from authoritative source row",
    );

    let delete_target = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(delete_target)
        .expect_err("restored reverse entry should block target delete");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn recovery_replays_reverse_index_mixed_save_save_delete_sequence() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::from_u128(9_451);
    let source_a = Ulid::from_u128(9_452);
    let source_b = Ulid::from_u128(9_453);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target save should succeed");

    let source_a_key = DataKey::try_new::<RelationSourceEntity>(source_a)
        .expect("source A key should build")
        .to_raw()
        .expect("source A key should encode");
    let source_b_key = DataKey::try_new::<RelationSourceEntity>(source_b)
        .expect("source B key should build")
        .to_raw()
        .expect("source B key should encode");
    let source_a_row = crate::serialize::serialize(&RelationSourceEntity {
        id: source_a,
        target: target_id,
    })
    .expect("source A row should serialize");
    let source_b_row = crate::serialize::serialize(&RelationSourceEntity {
        id: source_b,
        target: target_id,
    })
    .expect("source B row should serialize");

    // Phase 1: replay first save marker.
    let save_a_marker = CommitMarker::new(vec![crate::db::commit::CommitRowOp::new(
        RelationSourceEntity::PATH,
        source_a_key.as_bytes().to_vec(),
        None,
        Some(source_a_row.clone()),
        crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
    )])
    .expect("save A marker creation should succeed");
    begin_commit(save_a_marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("save A recovery replay should succeed");

    let reverse_rows_after_save_a = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_save_a, 1,
        "first save replay should create one reverse entry",
    );

    // Phase 2: replay second save marker targeting the same target key.
    let save_b_marker = CommitMarker::new(vec![crate::db::commit::CommitRowOp::new(
        RelationSourceEntity::PATH,
        source_b_key.as_bytes().to_vec(),
        None,
        Some(source_b_row),
        crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
    )])
    .expect("save B marker creation should succeed");
    begin_commit(save_b_marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("save B recovery replay should succeed");

    let reverse_rows_after_save_b = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_save_b, 1,
        "second save replay should merge into the existing reverse entry",
    );

    // Phase 3: replay delete marker for one source row.
    let delete_a_marker = CommitMarker::new(vec![crate::db::commit::CommitRowOp::new(
        RelationSourceEntity::PATH,
        source_a_key.as_bytes().to_vec(),
        Some(source_a_row),
        None,
        crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
    )])
    .expect("delete marker creation should succeed");
    begin_commit(delete_a_marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("delete recovery replay should succeed");

    let reverse_rows_after_delete_a = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_delete_a, 1,
        "delete replay should keep reverse entry while one referrer remains",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target delete plan should build");
    let err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(target_delete_plan)
        .expect_err("target delete should remain blocked by surviving source row");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );

    let source_delete_plan = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_b)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source B delete plan should build");
    DeleteExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .execute(source_delete_plan)
        .expect("source B delete should succeed");

    let retry_target_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_id)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("retry target delete plan should build");
    let deleted_target = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(retry_target_delete_plan)
        .expect("target should delete once all referrers are removed");
    assert_eq!(deleted_target.len(), 1, "target row should be removed");
}

#[test]
fn recovery_replays_retarget_update_moves_reverse_index_membership() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_a = Ulid::from_u128(9_461);
    let target_b = Ulid::from_u128(9_462);
    let source_id = Ulid::from_u128(9_463);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_a })
        .expect("target A save should succeed");
    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_b })
        .expect("target B save should succeed");
    SaveExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_a,
        })
        .expect("source insert should succeed");

    let source_key = DataKey::try_new::<RelationSourceEntity>(source_id)
        .expect("source key should build")
        .to_raw()
        .expect("source key should encode");
    let before = crate::serialize::serialize(&RelationSourceEntity {
        id: source_id,
        target: target_a,
    })
    .expect("before row should serialize");
    let after = crate::serialize::serialize(&RelationSourceEntity {
        id: source_id,
        target: target_b,
    })
    .expect("after row should serialize");

    let marker = CommitMarker::new(vec![crate::db::commit::CommitRowOp::new(
        RelationSourceEntity::PATH,
        source_key.as_bytes().to_vec(),
        Some(before),
        Some(after),
        crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
    )])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&REL_DB).expect("recovery replay should succeed");

    let reverse_rows_after_retarget = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_retarget, 1,
        "retarget replay should keep one reverse entry mapped to the new target",
    );

    let delete_target_a = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_a)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target A delete plan should build");
    let deleted_target_a = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(delete_target_a)
        .expect("old target should be deletable after replayed retarget");
    assert_eq!(deleted_target_a.len(), 1, "old target should be removed");

    let delete_target_b = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target B delete plan should build");
    let err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(delete_target_b)
        .expect_err("new target should remain blocked by relation referrer");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );
}

#[expect(clippy::too_many_lines)]
#[test]
fn recovery_rollback_restores_reverse_index_state_on_prepare_error() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_a = Ulid::from_u128(9_471);
    let target_b = Ulid::from_u128(9_472);
    let source_id = Ulid::from_u128(9_473);

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_a })
        .expect("target A save should succeed");
    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_b })
        .expect("target B save should succeed");
    SaveExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_a,
        })
        .expect("source insert should succeed");

    let source_key = DataKey::try_new::<RelationSourceEntity>(source_id)
        .expect("source key should build")
        .to_raw()
        .expect("source key should encode");
    let source_raw_key = source_key;
    let update_before = crate::serialize::serialize(&RelationSourceEntity {
        id: source_id,
        target: target_a,
    })
    .expect("update before row should serialize");
    let update_after = crate::serialize::serialize(&RelationSourceEntity {
        id: source_id,
        target: target_b,
    })
    .expect("update after row should serialize");

    let marker = CommitMarker::new(vec![
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            source_key.as_bytes().to_vec(),
            Some(update_before),
            Some(update_after),
            crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
        ),
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            vec![7, 8, 9],
            None,
            Some(vec![1]),
            crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    let err =
        ensure_recovered(&REL_DB).expect_err("recovery should fail when a later row op is invalid");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "prepare failure should surface corruption for malformed key bytes",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Recovery,
        "malformed key bytes should surface recovery-boundary origin",
    );

    let marker_still_present = match commit_marker_present() {
        Ok(present) => present,
        Err(err) => {
            assert_eq!(
                err.class,
                crate::error::ErrorClass::Corruption,
                "invalid marker payload should fail decode as corruption",
            );
            assert_eq!(
                err.origin,
                crate::error::ErrorOrigin::Store,
                "invalid marker payload should fail at store decode boundary",
            );
            true
        }
    };
    // Clear the intentionally-bad marker to avoid contaminating later tests.
    let cleanup_marker = CommitMarker::new(Vec::new()).expect("cleanup marker should build");
    crate::db::commit::finish_commit(
        crate::db::commit::CommitGuard {
            marker: cleanup_marker,
        },
        |_| Ok(()),
    )
    .expect("marker cleanup should succeed");
    assert!(
        marker_still_present,
        "failed replay should keep the marker persisted until cleanup",
    );

    let source_after_failure = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationSourceStore::PATH)
                .map(|store| store.with_data(|data_store| data_store.get(&source_raw_key)))
        })
        .expect("source store access should succeed")
        .expect("source row should still exist after rollback");
    let source_after_failure = source_after_failure
        .try_decode::<RelationSourceEntity>()
        .expect("source row decode should succeed after rollback");
    assert_eq!(
        source_after_failure.target, target_a,
        "rollback should restore original source relation target",
    );

    let delete_target_a = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_a)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target A delete plan should build");
    let err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(delete_target_a)
        .expect_err("target A should remain protected after rollback");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected target A error after rollback: {err:?}",
    );

    let delete_target_b = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target B delete plan should build");
    let deleted_target_b = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(delete_target_b)
        .expect("target B should remain deletable after rollback");
    assert_eq!(deleted_target_b.len(), 1, "target B should be removed");
}

#[test]
#[expect(clippy::too_many_lines)]
fn recovery_partial_fk_update_preserves_reverse_index_invariants() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    // Phase 1: seed two targets and two source rows that both reference target A.
    let target_a = Ulid::from_u128(9_501);
    let target_b = Ulid::from_u128(9_502);
    let source_1 = Ulid::from_u128(9_503);
    let source_2 = Ulid::from_u128(9_504);

    let target_save = SaveExecutor::<RelationTargetEntity>::new(REL_DB, false);
    target_save
        .insert(RelationTargetEntity { id: target_a })
        .expect("target A save should succeed");
    target_save
        .insert(RelationTargetEntity { id: target_b })
        .expect("target B save should succeed");

    let source_save = SaveExecutor::<RelationSourceEntity>::new(REL_DB, false);
    source_save
        .insert(RelationSourceEntity {
            id: source_1,
            target: target_a,
        })
        .expect("source 1 save should succeed");
    source_save
        .insert(RelationSourceEntity {
            id: source_2,
            target: target_a,
        })
        .expect("source 2 save should succeed");

    let seeded_reverse_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        seeded_reverse_rows, 1,
        "initially both referrers share one reverse entry on target A",
    );

    // Phase 2: persist a marker with a partial update in one block:
    // - source 1 moves A -> B
    // - source 2 stays on A (before==after relation value)
    let source_1_key = DataKey::try_new::<RelationSourceEntity>(source_1)
        .expect("source 1 key should build")
        .to_raw()
        .expect("source 1 key should encode");
    let source_2_key = DataKey::try_new::<RelationSourceEntity>(source_2)
        .expect("source 2 key should build")
        .to_raw()
        .expect("source 2 key should encode");

    let source_1_before = crate::serialize::serialize(&RelationSourceEntity {
        id: source_1,
        target: target_a,
    })
    .expect("source 1 before row should serialize");
    let source_1_after = crate::serialize::serialize(&RelationSourceEntity {
        id: source_1,
        target: target_b,
    })
    .expect("source 1 after row should serialize");
    let source_2_same = crate::serialize::serialize(&RelationSourceEntity {
        id: source_2,
        target: target_a,
    })
    .expect("source 2 row should serialize");

    let marker = CommitMarker::new(vec![
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            source_1_key.as_bytes().to_vec(),
            Some(source_1_before),
            Some(source_1_after),
            crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
        ),
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            source_2_key.as_bytes().to_vec(),
            Some(source_2_same.clone()),
            Some(source_2_same),
            crate::db::schema::commit_schema_fingerprint_for_entity::<RelationSourceEntity>(),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before recovery replay",
    );

    // Phase 3: recovery replays row ops and reverse mutations from the marker.
    ensure_recovered(&REL_DB).expect("recovery replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after recovery replay",
    );

    let reverse_rows_after_replay = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_replay, 2,
        "partial FK update should split reverse entries across old/new targets",
    );

    let delete_target_a = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_a)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target A delete plan should build");
    let blocked_delete_err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(delete_target_a)
        .expect_err("target A should remain blocked by source 2");
    assert_eq!(
        blocked_delete_err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        blocked_delete_err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        blocked_delete_err
            .message
            .contains("delete blocked by strong relation"),
        "unexpected target A error: {blocked_delete_err:?}",
    );

    let delete_target_b = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target B delete plan should build");
    let blocked_delete_err = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(delete_target_b)
        .expect_err("target B should be blocked by moved source 1");
    assert_eq!(
        blocked_delete_err.class,
        crate::error::ErrorClass::Unsupported,
        "blocked strong-relation delete should classify as unsupported",
    );
    assert_eq!(
        blocked_delete_err.origin,
        crate::error::ErrorOrigin::Executor,
        "blocked strong-relation delete should originate from executor validation",
    );
    assert!(
        blocked_delete_err
            .message
            .contains("delete blocked by strong relation"),
        "unexpected target B error: {blocked_delete_err:?}",
    );

    // Phase 4: remove remaining refs and ensure no orphan reverse entries remain.
    let delete_source_2 = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_2)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source 2 delete plan should build");
    DeleteExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .execute(delete_source_2)
        .expect("source 2 delete should succeed");

    let retry_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_a)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target A delete plan should build");
    DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(retry_delete_plan)
        .expect("target A should delete once source 2 is gone");

    let delete_source_1 = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(source_1)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("source 1 delete plan should build");
    DeleteExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .execute(delete_source_1)
        .expect("source 1 delete should succeed");

    let retry_delete_plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(target_b)
        .plan()
        .map(crate::db::executor::ExecutablePlan::from)
        .expect("target B delete plan should build");
    DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(retry_delete_plan)
        .expect("target B should delete once source 1 is gone");

    let final_reverse_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        final_reverse_rows, 0,
        "reverse index should be empty after all source refs are removed",
    );
}
