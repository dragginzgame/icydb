//! Module: db::executor::tests::set_access
//! Covers set-based executor access paths and their result invariants.
//! Does not own: unrelated executor orchestration outside set access.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;
use crate::{
    db::{
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::explain::{
            ExplainAccessPath, ExplainExecutionNodeDescriptor, ExplainExecutionNodeType,
        },
    },
    value::Value,
};

// Recursively search the execution descriptor tree for one node type.
fn explain_execution_contains_node_type(
    descriptor: &ExplainExecutionNodeDescriptor,
    node_type: ExplainExecutionNodeType,
) -> bool {
    if descriptor.node_type() == node_type {
        return true;
    }

    descriptor
        .children()
        .iter()
        .any(|child| explain_execution_contains_node_type(child, node_type))
}

// Build one strict primary-key `IN` predicate for intersection set-access tests.
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
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
        "duplicate by_ids entries should not emit duplicate rows",
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
        matches!(explain.access(), ExplainAccessPath::Union(_)),
        "OR predicate over PK paths should plan as union access",
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("union load plan should build");
    let response = load.execute(plan).expect("union load should succeed");
    let ids: Vec<Ulid> = response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();

    assert_eq!(
        ids,
        vec![id_a, id_b],
        "union execution must keep canonical order and suppress overlapping keys",
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
        "AND predicate over key sets should plan as intersection access",
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
        "intersection execution should emit the overlap in ascending canonical order",
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
        "AND predicate over key sets should plan as intersection access",
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
        "intersection execution should emit the overlap in descending canonical order",
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
        "disjoint AND key predicates should still plan as intersection access",
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("intersection no-overlap plan should build");
    let response = load
        .execute(plan)
        .expect("intersection no-overlap load should succeed");

    assert!(
        response.is_empty(),
        "intersection with no shared keys should return no rows",
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
        "duplicate AND key predicates should still plan as intersection access",
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("intersection duplicate plan should build");
    let response = load
        .execute(plan)
        .expect("intersection duplicate load should succeed");
    let ids: Vec<Ulid> = response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();
    let unique: std::collections::BTreeSet<Ulid> = ids.iter().copied().collect();

    assert_eq!(
        ids,
        vec![Ulid::from_u128(1241), Ulid::from_u128(1243)],
        "intersection should return shared ids once in canonical order",
    );
    assert_eq!(
        unique.len(),
        ids.len(),
        "intersection execution must not emit duplicate rows",
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
        "nested intersection should preserve both composite children",
    );
    assert!(
        children
            .iter()
            .all(|child| matches!(child, ExplainAccessPath::Union(_))),
        "nested intersection children should remain union composites",
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
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
        "nested composite intersection should match overlap of union children",
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
        "overlapping AND predicate should plan as intersection access",
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let mut paged_ids = Vec::new();
    let mut cursor_bytes: Option<Vec<u8>> = None;
    loop {
        let page_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate.clone())
            .order_by_desc("id")
            .limit(2)
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("intersection desc paged plan should build");
        let page_cursor = page_plan
            .prepare_cursor(cursor_bytes.as_deref())
            .expect("intersection desc paged cursor should prepare");
        let page = load
            .execute_paged_with_cursor(page_plan, page_cursor)
            .expect("intersection desc paged load should succeed");
        let batch_ids: Vec<Ulid> = page
            .items
            .into_iter()
            .map(|row| row.entity_ref().id)
            .collect();
        paged_ids.extend(batch_ids);

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor_bytes = Some(
            next_cursor
                .encode()
                .expect("intersection desc next cursor should encode"),
        );
    }

    let full_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("intersection desc full plan should build");
    let full_response = load
        .execute(full_plan)
        .expect("intersection desc full load should succeed");
    let full_ids: Vec<Ulid> = full_response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();
    let unique: std::collections::BTreeSet<Ulid> = paged_ids.iter().copied().collect();

    assert_eq!(
        paged_ids, full_ids,
        "paged intersection DESC traversal with limit must match full execution",
    );
    assert_eq!(
        unique.len(),
        paged_ids.len(),
        "intersection DESC paged traversal must not duplicate rows",
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
        matches!(explain.access(), ExplainAccessPath::Union(_)),
        "overlapping OR predicate should plan as union access",
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let mut paged_ids = Vec::new();
    let mut cursor_bytes: Option<Vec<u8>> = None;
    loop {
        let page_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate.clone())
            .order_by_desc("id")
            .limit(2)
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("union desc paged plan should build");
        let page_cursor = page_plan
            .prepare_cursor(cursor_bytes.as_deref())
            .expect("union desc paged cursor should prepare");
        let page = load
            .execute_paged_with_cursor(page_plan, page_cursor)
            .expect("union desc paged load should succeed");
        let batch_ids: Vec<Ulid> = page
            .items
            .into_iter()
            .map(|row| row.entity_ref().id)
            .collect();
        paged_ids.extend(batch_ids);

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor_bytes = Some(
            next_cursor
                .encode()
                .expect("union desc next cursor should encode"),
        );
    }

    let full_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("union desc full plan should build");
    let full_response = load
        .execute(full_plan)
        .expect("union desc full load should succeed");
    let full_ids: Vec<Ulid> = full_response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();
    let unique: std::collections::BTreeSet<Ulid> = paged_ids.iter().copied().collect();

    assert_eq!(
        paged_ids, full_ids,
        "paged union DESC traversal with limit must match full execution",
    );
    assert_eq!(
        unique.len(),
        paged_ids.len(),
        "union DESC paged traversal must not duplicate rows",
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
        matches!(explain.access(), ExplainAccessPath::Union(_)),
        "three-child overlapping OR predicate should plan as union access",
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let mut paged_ids = Vec::new();
    let mut cursor_bytes: Option<Vec<u8>> = None;
    loop {
        let page_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
            .filter(predicate.clone())
            .order_by_desc("id")
            .limit(3)
            .plan()
            .map(crate::db::executor::PreparedExecutionPlan::from)
            .expect("three-child union desc paged plan should build");
        let page_cursor = page_plan
            .prepare_cursor(cursor_bytes.as_deref())
            .expect("three-child union desc paged cursor should prepare");
        let page = load
            .execute_paged_with_cursor(page_plan, page_cursor)
            .expect("three-child union desc paged load should succeed");
        let batch_ids: Vec<Ulid> = page
            .items
            .into_iter()
            .map(|row| row.entity_ref().id)
            .collect();
        paged_ids.extend(batch_ids);

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor_bytes = Some(
            next_cursor
                .encode()
                .expect("three-child union desc next cursor should encode"),
        );
    }

    let full_plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_by_desc("id")
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("three-child union desc full plan should build");
    let full_response = load
        .execute(full_plan)
        .expect("three-child union desc full load should succeed");
    let full_ids: Vec<Ulid> = full_response
        .into_iter()
        .map(|row| row.entity_ref().id)
        .collect();
    let unique: std::collections::BTreeSet<Ulid> = paged_ids.iter().copied().collect();

    assert_eq!(
        paged_ids, full_ids,
        "three-child paged union DESC traversal with limit must match full execution",
    );
    assert_eq!(
        unique.len(),
        paged_ids.len(),
        "three-child union DESC paged traversal must not duplicate rows",
    );
}
