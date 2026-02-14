use super::*;

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

    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}"
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
    let raw_key = crate::db::store::DataKey::try_new::<RelationSourceEntity>(source.id)
        .expect("source data key should build")
        .to_raw()
        .expect("source data key should encode");
    let row_bytes = crate::serialize::serialize(&source).expect("source row should serialize");

    let marker = CommitMarker::new(vec![crate::db::CommitRowOp::new(
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
    assert!(
        err.message.contains("delete blocked by strong relation"),
        "unexpected error: {err:?}",
    );
}

#[test]
#[allow(clippy::too_many_lines)]
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
    let source_1_key = crate::db::store::DataKey::try_new::<RelationSourceEntity>(source_1)
        .expect("source 1 key should build")
        .to_raw()
        .expect("source 1 key should encode");
    let source_2_key = crate::db::store::DataKey::try_new::<RelationSourceEntity>(source_2)
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
        crate::db::CommitRowOp::new(
            RelationSourceEntity::PATH,
            source_1_key.as_bytes().to_vec(),
            Some(source_1_before),
            Some(source_1_after),
        ),
        crate::db::CommitRowOp::new(
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
