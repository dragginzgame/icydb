#![expect(clippy::similar_names)]
use super::*;
use crate::db::{data::DataKey, query::explain::ExplainAccessPath};
use std::collections::BTreeSet;

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

    let plan = Query::<SingletonUnitEntity>::new(ReadConsistency::MissingOk)
        .only()
        .plan()
        .expect("singleton load plan should build");
    let response = load.execute(plan).expect("singleton load should succeed");

    assert_eq!(
        response.0.len(),
        1,
        "singleton only() should match exactly one row"
    );
    assert_eq!(
        response.0[0].1, expected,
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
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .by_ids([id_a, id_a, id_b, id_a])
        .plan()
        .expect("by_ids plan should build");
    let response = load.execute(plan).expect("by_ids load should succeed");

    let mut ids: Vec<Ulid> = response
        .0
        .into_iter()
        .map(|(_, entity)| entity.id)
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
    let explain = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("id")
        .explain()
        .expect("union explain should build");
    assert!(
        matches!(
            explain.access,
            crate::db::query::explain::ExplainAccessPath::Union(_)
        ),
        "OR predicate over PK paths should plan as union access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("id")
        .plan()
        .expect("union load plan should build");
    let response = load.execute(plan).expect("union load should succeed");
    let ids: Vec<Ulid> = response
        .0
        .into_iter()
        .map(|(_, entity)| entity.id)
        .collect();

    assert_eq!(
        ids,
        vec![id_a, id_b],
        "union execution must keep canonical order and suppress overlapping keys"
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
    let explain = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("id")
        .explain()
        .expect("intersection explain should build");
    assert!(
        matches!(explain.access, ExplainAccessPath::Intersection(_)),
        "AND predicate over key sets should plan as intersection access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("id")
        .plan()
        .expect("intersection load plan should build");
    let response = load
        .execute(plan)
        .expect("intersection load should succeed");
    let ids: Vec<Ulid> = response
        .0
        .into_iter()
        .map(|(_, entity)| entity.id)
        .collect();

    assert_eq!(
        ids,
        vec![Ulid::from_u128(1213), Ulid::from_u128(1214)],
        "intersection execution should emit the overlap in ascending canonical order"
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
    let explain = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("id")
        .explain()
        .expect("intersection DESC explain should build");
    assert!(
        matches!(explain.access, ExplainAccessPath::Intersection(_)),
        "AND predicate over key sets should plan as intersection access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("id")
        .plan()
        .expect("intersection DESC load plan should build");
    let response = load
        .execute(plan)
        .expect("intersection DESC load should succeed");
    let ids: Vec<Ulid> = response
        .0
        .into_iter()
        .map(|(_, entity)| entity.id)
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
    let explain = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("id")
        .explain()
        .expect("intersection no-overlap explain should build");
    assert!(
        matches!(explain.access, ExplainAccessPath::Intersection(_)),
        "disjoint AND key predicates should still plan as intersection access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("id")
        .plan()
        .expect("intersection no-overlap plan should build");
    let response = load
        .execute(plan)
        .expect("intersection no-overlap load should succeed");
    assert!(
        response.0.is_empty(),
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
    let explain = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("id")
        .explain()
        .expect("intersection duplicate explain should build");
    assert!(
        matches!(explain.access, ExplainAccessPath::Intersection(_)),
        "duplicate AND key predicates should still plan as intersection access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("id")
        .plan()
        .expect("intersection duplicate plan should build");
    let response = load
        .execute(plan)
        .expect("intersection duplicate load should succeed");
    let ids: Vec<Ulid> = response
        .0
        .into_iter()
        .map(|(_, entity)| entity.id)
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
    let explain = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("id")
        .explain()
        .expect("nested intersection explain should build");
    let ExplainAccessPath::Intersection(children) = explain.access else {
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
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("id")
        .plan()
        .expect("nested intersection plan should build");
    let response = load
        .execute(plan)
        .expect("nested intersection load should succeed");
    let ids: Vec<Ulid> = response
        .0
        .into_iter()
        .map(|(_, entity)| entity.id)
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
    let explain = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("id")
        .limit(2)
        .explain()
        .expect("intersection pagination explain should build");
    assert!(
        matches!(explain.access, ExplainAccessPath::Intersection(_)),
        "overlapping AND predicate should plan as intersection access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let mut paged_ids = Vec::new();
    let mut cursor: Option<CursorBoundary> = None;
    loop {
        let page_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate.clone())
            .order_by_desc("id")
            .limit(2)
            .plan()
            .expect("intersection desc paged plan should build");
        let page = load
            .execute_paged_with_cursor(page_plan, cursor.clone())
            .expect("intersection desc paged load should succeed");
        let page_ids: Vec<Ulid> = page
            .items
            .0
            .into_iter()
            .map(|(_, entity)| entity.id)
            .collect();
        paged_ids.extend(page_ids);

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        let token = ContinuationToken::decode(&next_cursor)
            .expect("intersection desc continuation should decode");
        cursor = Some(token.boundary().clone());
    }

    let full_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("id")
        .plan()
        .expect("intersection desc full plan should build");
    let full_response = load
        .execute(full_plan)
        .expect("intersection desc full load should succeed");
    let full_ids: Vec<Ulid> = full_response
        .0
        .into_iter()
        .map(|(_, entity)| entity.id)
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
    let explain = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("id")
        .limit(2)
        .explain()
        .expect("union pagination explain should build");
    assert!(
        matches!(
            explain.access,
            crate::db::query::explain::ExplainAccessPath::Union(_)
        ),
        "overlapping OR predicate should plan as union access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let mut paged_ids = Vec::new();
    let mut cursor: Option<CursorBoundary> = None;
    loop {
        let page_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate.clone())
            .order_by_desc("id")
            .limit(2)
            .plan()
            .expect("union desc paged plan should build");
        let page = load
            .execute_paged_with_cursor(page_plan, cursor.clone())
            .expect("union desc paged load should succeed");
        let page_ids: Vec<Ulid> = page
            .items
            .0
            .into_iter()
            .map(|(_, entity)| entity.id)
            .collect();
        paged_ids.extend(page_ids);

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        let token =
            ContinuationToken::decode(&next_cursor).expect("union desc continuation should decode");
        cursor = Some(token.boundary().clone());
    }

    let full_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("id")
        .plan()
        .expect("union desc full plan should build");
    let full_response = load
        .execute(full_plan)
        .expect("union desc full load should succeed");
    let full_ids: Vec<Ulid> = full_response
        .0
        .into_iter()
        .map(|(_, entity)| entity.id)
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
    let explain = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by_desc("id")
        .limit(3)
        .explain()
        .expect("three-child union pagination explain should build");
    assert!(
        matches!(
            explain.access,
            crate::db::query::explain::ExplainAccessPath::Union(_)
        ),
        "three-child overlapping OR predicate should plan as union access"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let mut paged_ids = Vec::new();
    let mut cursor: Option<CursorBoundary> = None;
    loop {
        let page_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
            .filter(predicate.clone())
            .order_by_desc("id")
            .limit(3)
            .plan()
            .expect("three-child union desc paged plan should build");
        let page = load
            .execute_paged_with_cursor(page_plan, cursor.clone())
            .expect("three-child union desc paged load should succeed");
        let page_ids: Vec<Ulid> = page
            .items
            .0
            .into_iter()
            .map(|(_, entity)| entity.id)
            .collect();
        paged_ids.extend(page_ids);

        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        let token = ContinuationToken::decode(&next_cursor)
            .expect("three-child continuation should decode");
        cursor = Some(token.boundary().clone());
    }

    let full_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by_desc("id")
        .plan()
        .expect("three-child union desc full plan should build");
    let full_response = load
        .execute(full_plan)
        .expect("three-child union desc full load should succeed");
    let full_ids: Vec<Ulid> = full_response
        .0
        .into_iter()
        .map(|(_, entity)| entity.id)
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
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .order_by("id")
        .limit(1)
        .plan()
        .expect("delete plan should build");

    let response = delete.execute(plan).expect("delete should succeed");
    assert_eq!(response.0.len(), 1, "delete limit should remove one row");
    assert_eq!(
        response.0[0].1.id,
        Ulid::from_u128(10),
        "delete limit should run after canonical ordering by id"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let remaining_plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .order_by("id")
        .plan()
        .expect("remaining load plan should build");
    let remaining = load
        .execute(remaining_plan)
        .expect("remaining load should succeed");
    let remaining_ids: Vec<Ulid> = remaining
        .0
        .into_iter()
        .map(|(_, entity)| entity.id)
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
    let match_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .by_id(id)
        .filter(equals_opt_value)
        .plan()
        .expect("optional equality plan should build");
    let match_response = load
        .execute(match_plan)
        .expect("optional equality should load");
    assert_eq!(
        match_response.0.len(),
        1,
        "filter should run after by_id access and keep matching rows"
    );

    let no_match = Predicate::Compare(ComparePredicate::with_coercion(
        "opt_rank",
        CompareOp::Eq,
        Value::Uint(99),
        CoercionId::Strict,
    ));
    let mismatch_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .by_id(id)
        .filter(no_match)
        .plan()
        .expect("mismatch plan should build");
    let mismatch_response = load
        .execute(mismatch_plan)
        .expect("mismatch predicate should execute");
    assert_eq!(
        mismatch_response.0.len(),
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
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .limit(1)
        .offset(1)
        .plan()
        .expect("in+text ordered page plan should build");
    let response = load
        .execute(plan)
        .expect("in+text ordered page should load");

    assert_eq!(
        response.0.len(),
        1,
        "ordered pagination should return one row"
    );
    assert_eq!(
        response.0[0].1.rank, 30,
        "pagination should apply to the filtered+ordered window"
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

    let asc_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("opt_rank")
        .plan()
        .expect("ascending optional-order plan should build");
    let asc = load
        .execute(asc_plan)
        .expect("ascending optional-order query should execute");
    let asc_ids: Vec<Ulid> = asc.0.into_iter().map(|(_, entity)| entity.id).collect();
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

    let desc_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by_desc("opt_rank")
        .plan()
        .expect("descending optional-order plan should build");
    let desc = load
        .execute(desc_plan)
        .expect("descending optional-order query should execute");
    let desc_ids: Vec<Ulid> = desc.0.into_iter().map(|(_, entity)| entity.id).collect();
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
    let hit_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .by_id(id)
        .filter(contains_nine)
        .plan()
        .expect("contains hit plan should build");
    let hit = load.execute(hit_plan).expect("contains hit should execute");
    assert_eq!(hit.0.len(), 1, "contains predicate should match row");

    let contains_missing = Predicate::Compare(ComparePredicate::with_coercion(
        "tags",
        CompareOp::Contains,
        Value::Uint(8),
        CoercionId::CollectionElement,
    ));
    let miss_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .by_id(id)
        .filter(contains_missing)
        .plan()
        .expect("contains miss plan should build");
    let miss = load
        .execute(miss_plan)
        .expect("contains miss should execute");
    assert_eq!(
        miss.0.len(),
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
    let explain = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate.clone())
        .order_by("rank")
        .explain()
        .expect("missing-ok stale-secondary explain should build");
    assert!(
        matches!(explain.access, ExplainAccessPath::IndexPrefix { .. }),
        "group equality with rank order should plan as secondary index-prefix access",
    );

    let load = LoadExecutor::<PushdownParityEntity>::new(DB, false);
    let plan = Query::<PushdownParityEntity>::new(ReadConsistency::MissingOk)
        .filter(predicate)
        .order_by("rank")
        .plan()
        .expect("missing-ok stale-secondary load plan should build");
    let response = load
        .execute(plan)
        .expect("missing-ok stale-secondary load should succeed");
    let ids: Vec<Ulid> = response
        .0
        .into_iter()
        .map(|(_, entity)| entity.id)
        .collect();

    assert_eq!(
        ids,
        vec![Ulid::from_u128(7102), Ulid::from_u128(7103)],
        "MissingOk must filter stale secondary keys instead of materializing missing rows",
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
    let plan = Query::<PushdownParityEntity>::new(ReadConsistency::Strict)
        .filter(predicate)
        .order_by("rank")
        .plan()
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
    let plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .filter(predicate)
        .order_by("rank")
        .limit(1)
        .plan()
        .expect("filtered delete plan should build");
    let deleted = delete
        .execute(plan)
        .expect("filtered delete should execute");

    assert_eq!(
        deleted.0.len(),
        1,
        "delete limit should remove one filtered row"
    );
    assert_eq!(
        deleted.0[0].1.rank, 100,
        "delete limit should apply after filtering+ordering"
    );

    let load = LoadExecutor::<PhaseEntity>::new(DB, false);
    let remaining_plan = Query::<PhaseEntity>::new(ReadConsistency::MissingOk)
        .order_by("rank")
        .plan()
        .expect("remaining load plan should build");
    let remaining = load
        .execute(remaining_plan)
        .expect("remaining load should execute");
    let remaining_ranks: Vec<u64> = remaining
        .0
        .into_iter()
        .map(|(_, entity)| u64::from(entity.rank))
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
    let delete_plan = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_id)
        .plan()
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
    let source_delete_plan = Query::<RelationSourceEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(source_id)
        .plan()
        .expect("source delete plan should build");
    let deleted_sources = source_delete
        .execute(source_delete_plan)
        .expect("source delete should succeed");
    assert_eq!(deleted_sources.0.len(), 1, "source row should be removed");

    let target_delete = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false);
    let target_delete_plan = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_id)
        .plan()
        .expect("target delete plan should build");
    let deleted_targets = target_delete
        .execute(target_delete_plan)
        .expect("target delete should succeed once referrer is removed");
    assert_eq!(deleted_targets.0.len(), 1, "target row should be removed");
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
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_delete, 0,
        "weak relation should not create reverse strong-relation index entries",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_id)
        .plan()
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak referrer");
    assert_eq!(deleted_targets.0.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakSingleRelationSourceEntity>::new(ReadConsistency::MissingOk)
        .by_id(source_id)
        .plan()
        .expect("source load plan should build");
    let remaining_source = LoadExecutor::<WeakSingleRelationSourceEntity>::new(REL_DB, false)
        .execute(source_plan)
        .expect("source load should succeed");
    assert_eq!(remaining_source.0.len(), 1, "weak source row should remain");
    assert_eq!(
        remaining_source.0[0].1.target, target_id,
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
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_delete, 0,
        "weak optional relation should not create reverse strong-relation index entries",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_id)
        .plan()
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak optional referrer");
    assert_eq!(deleted_targets.0.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakOptionalRelationSourceEntity>::new(ReadConsistency::MissingOk)
        .by_id(source_id)
        .plan()
        .expect("source load plan should build");
    let remaining_source = LoadExecutor::<WeakOptionalRelationSourceEntity>::new(REL_DB, false)
        .execute(source_plan)
        .expect("source load should succeed");
    assert_eq!(
        remaining_source.0.len(),
        1,
        "weak optional source row should remain"
    );
    assert_eq!(
        remaining_source.0[0].1.target,
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
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_before_delete, 0,
        "weak list relation should not create reverse strong-relation index entries",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_id)
        .plan()
        .expect("target delete plan should build");
    let deleted_targets = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(target_delete_plan)
        .expect("target delete should succeed for weak list referrer");
    assert_eq!(deleted_targets.0.len(), 1, "target row should be removed");

    let source_plan = Query::<WeakListRelationSourceEntity>::new(ReadConsistency::MissingOk)
        .by_id(source_id)
        .plan()
        .expect("source load plan should build");
    let remaining_source = LoadExecutor::<WeakListRelationSourceEntity>::new(REL_DB, false)
        .execute(source_plan)
        .expect("source load should succeed");
    assert_eq!(
        remaining_source.0.len(),
        1,
        "weak list source row should remain"
    );
    assert_eq!(
        remaining_source.0[0].1.targets,
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
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_insert, 1,
        "target index store should contain one reverse-relation entry after source insert",
    );

    let source_delete = DeleteExecutor::<RelationSourceEntity>::new(REL_DB, false);
    let source_delete_plan = Query::<RelationSourceEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(source_id)
        .plan()
        .expect("source delete plan should build");
    source_delete
        .execute(source_delete_plan)
        .expect("source delete should succeed");

    let reverse_rows_after_delete = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
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
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_update, 1,
        "reverse index should remove old target entry and keep only the new one",
    );

    let old_target_delete_plan = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_a)
        .plan()
        .expect("target A delete plan should build");
    let deleted_a = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(old_target_delete_plan)
        .expect("old target should be deletable after relation retarget");
    assert_eq!(deleted_a.0.len(), 1, "old target should delete cleanly");

    let protected_target_delete_plan =
        Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
            .delete()
            .by_id(target_b)
            .plan()
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
    )])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before recovery replay",
    );

    ensure_recovered_for_write(&REL_DB).expect("recovery replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after recovery replay",
    );

    let reverse_rows_after_replay = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_replay, 1,
        "recovery replay should materialize reverse relation index entries",
    );

    let target_delete = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false);
    let delete_plan = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_id)
        .plan()
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
    )])
    .expect("save A marker creation should succeed");
    begin_commit(save_a_marker).expect("begin_commit should persist marker");
    ensure_recovered_for_write(&REL_DB).expect("save A recovery replay should succeed");

    let reverse_rows_after_save_a = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
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
    )])
    .expect("save B marker creation should succeed");
    begin_commit(save_b_marker).expect("begin_commit should persist marker");
    ensure_recovered_for_write(&REL_DB).expect("save B recovery replay should succeed");

    let reverse_rows_after_save_b = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
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
    )])
    .expect("delete marker creation should succeed");
    begin_commit(delete_a_marker).expect("begin_commit should persist marker");
    ensure_recovered_for_write(&REL_DB).expect("delete recovery replay should succeed");

    let reverse_rows_after_delete_a = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_delete_a, 1,
        "delete replay should keep reverse entry while one referrer remains",
    );

    let target_delete_plan = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_id)
        .plan()
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

    let source_delete_plan = Query::<RelationSourceEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(source_b)
        .plan()
        .expect("source B delete plan should build");
    DeleteExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .execute(source_delete_plan)
        .expect("source B delete should succeed");

    let retry_target_delete_plan = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_id)
        .plan()
        .expect("retry target delete plan should build");
    let deleted_target = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(retry_target_delete_plan)
        .expect("target should delete once all referrers are removed");
    assert_eq!(deleted_target.0.len(), 1, "target row should be removed");
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
    )])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered_for_write(&REL_DB).expect("recovery replay should succeed");

    let reverse_rows_after_retarget = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_retarget, 1,
        "retarget replay should keep one reverse entry mapped to the new target",
    );

    let delete_target_a = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_a)
        .plan()
        .expect("target A delete plan should build");
    let deleted_target_a = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(delete_target_a)
        .expect("old target should be deletable after replayed retarget");
    assert_eq!(deleted_target_a.0.len(), 1, "old target should be removed");

    let delete_target_b = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_b)
        .plan()
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
        ),
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            vec![7, 8, 9],
            None,
            Some(vec![1]),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    let err = ensure_recovered_for_write(&REL_DB)
        .expect_err("recovery should fail when a later row op is invalid");
    assert_eq!(
        err.class,
        crate::error::ErrorClass::Corruption,
        "prepare failure should surface corruption for malformed key bytes",
    );
    assert_eq!(
        err.origin,
        crate::error::ErrorOrigin::Store,
        "malformed key bytes should surface store corruption origin",
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

    let delete_target_a = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_a)
        .plan()
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

    let delete_target_b = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_b)
        .plan()
        .expect("target B delete plan should build");
    let deleted_target_b = DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(delete_target_b)
        .expect("target B should remain deletable after rollback");
    assert_eq!(deleted_target_b.0.len(), 1, "target B should be removed");
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
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
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
        ),
        crate::db::commit::CommitRowOp::new(
            RelationSourceEntity::PATH,
            source_2_key.as_bytes().to_vec(),
            Some(source_2_same.clone()),
            Some(source_2_same),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before recovery replay",
    );

    // Phase 3: recovery replays row ops and reverse mutations from the marker.
    ensure_recovered_for_write(&REL_DB).expect("recovery replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after recovery replay",
    );

    let reverse_rows_after_replay = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        reverse_rows_after_replay, 2,
        "partial FK update should split reverse entries across old/new targets",
    );

    let delete_target_a = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_a)
        .plan()
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

    let delete_target_b = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_b)
        .plan()
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
    let delete_source_2 = Query::<RelationSourceEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(source_2)
        .plan()
        .expect("source 2 delete plan should build");
    DeleteExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .execute(delete_source_2)
        .expect("source 2 delete should succeed");

    let retry_delete_plan = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_a)
        .plan()
        .expect("target A delete plan should build");
    DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(retry_delete_plan)
        .expect("target A should delete once source 2 is gone");

    let delete_source_1 = Query::<RelationSourceEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(source_1)
        .plan()
        .expect("source 1 delete plan should build");
    DeleteExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .execute(delete_source_1)
        .expect("source 1 delete should succeed");

    let retry_delete_plan = Query::<RelationTargetEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(target_b)
        .plan()
        .expect("target B delete plan should build");
    DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .execute(retry_delete_plan)
        .expect("target B should delete once source 1 is gone");

    let final_reverse_rows = REL_DB
        .with_store_registry(|reg| {
            reg.try_get_store(RelationTargetStore::PATH)
                .map(|store| store.with_index(crate::db::index::IndexStore::len))
        })
        .expect("target index store access should succeed");
    assert_eq!(
        final_reverse_rows, 0,
        "reverse index should be empty after all source refs are removed",
    );
}
