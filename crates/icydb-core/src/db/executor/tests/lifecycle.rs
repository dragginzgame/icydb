use super::*;

#[test]
fn executor_save_then_delete_round_trip() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let delete = DeleteExecutor::<SimpleEntity>::new(DB, false);

    let entity = SimpleEntity {
        id: Ulid::generate(),
    };
    let saved = save.insert(entity).expect("save should succeed");

    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(saved.id().key())
        .plan()
        .expect("delete plan should build");
    let response = delete.execute(plan).expect("delete should succeed");

    assert_eq!(response.0.len(), 1);
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after delete"
    );

    DB.with_data(|reg| {
        reg.with_store(TestDataStore::PATH, |store| {
            assert!(store.is_empty(), "store should be empty after delete");
        })
        .expect("store access should succeed");
    });
}

#[test]
fn delete_replays_incomplete_commit_marker() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let delete = DeleteExecutor::<SimpleEntity>::new(DB, false);

    let entity = SimpleEntity {
        id: Ulid::generate(),
    };
    let saved = save.insert(entity).expect("save should succeed");

    let marker = CommitMarker::new(CommitKind::Save, Vec::new(), Vec::new())
        .expect("marker creation should succeed");
    let _guard = begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before delete"
    );

    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .delete()
        .by_id(saved.id().key())
        .plan()
        .expect("delete plan should build");
    let response = delete.execute(plan).expect("delete should succeed");

    assert_eq!(response.0.len(), 1);
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after delete recovery"
    );
}

#[test]
fn load_replays_incomplete_commit_marker_after_startup_recovery() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let marker = CommitMarker::new(CommitKind::Save, Vec::new(), Vec::new())
        .expect("marker creation should succeed");
    let _guard = begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before load"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(ReadConsistency::MissingOk)
        .plan()
        .expect("load plan should build");
    let response = load.execute(plan).expect("load should succeed");

    assert!(
        response.0.is_empty(),
        "empty store should still load after recovery replay"
    );
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after read recovery"
    );
}
