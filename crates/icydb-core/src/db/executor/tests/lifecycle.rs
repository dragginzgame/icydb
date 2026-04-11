//! Module: db::executor::tests::lifecycle
//! Covers executor lifecycle behavior across setup, execution, and teardown
//! seams.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;

#[test]
fn singleton_only_round_trips_through_runtime_load() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SingletonUnitEntity>::new(DB, false);
    let load = LoadExecutor::<SingletonUnitEntity>::new(DB, false);
    let expected = SingletonUnitEntity {
        id: Unit,
        label: "project".to_string(),
    };

    save.insert(expected.clone())
        .expect("singleton save should succeed");

    let plan = Query::<SingletonUnitEntity>::new(MissingRowPolicy::Ignore)
        .only()
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("singleton load plan should build");
    let response = load.execute(plan).expect("singleton load should succeed");

    assert_eq!(
        response.len(),
        1,
        "singleton only() should match exactly one row",
    );
    assert_eq!(
        response[0].entity_ref(),
        &expected,
        "loaded singleton should match inserted row",
    );
}

#[test]
fn executor_save_then_delete_round_trip() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let delete = DeleteExecutor::<SimpleEntity>::new(DB);

    let entity = SimpleEntity {
        id: Ulid::generate(),
    };
    let saved = save.insert(entity).expect("save should succeed");

    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(saved.id().key())
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("delete plan should build");
    let response = delete.execute(plan).expect("delete should succeed");

    assert_eq!(response.len(), 1);
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after delete"
    );

    DB.with_store_registry(|reg| {
        reg.try_get_store(TestDataStore::PATH)
            .map(|store| {
                store.with_data(|data_store| {
                    assert!(data_store.is_empty(), "store should be empty after delete");
                });
            })
            .expect("store access should succeed");
    });
}

#[test]
fn delete_replays_incomplete_commit_marker() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<SimpleEntity>::new(DB, false);
    let delete = DeleteExecutor::<SimpleEntity>::new(DB);

    let entity = SimpleEntity {
        id: Ulid::generate(),
    };
    let saved = save.insert(entity).expect("save should succeed");

    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    let _guard = begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before delete"
    );

    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .by_id(saved.id().key())
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("delete plan should build");
    let response = delete.execute(plan).expect("delete should succeed");

    assert_eq!(response.len(), 1);
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after delete recovery"
    );
}

#[test]
fn load_replays_incomplete_commit_marker_after_startup_recovery() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    let _guard = begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before load"
    );

    let load = LoadExecutor::<SimpleEntity>::new(DB, false);
    let plan = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .plan()
        .map(crate::db::executor::PreparedExecutionPlan::from)
        .expect("load plan should build");
    let response = load.execute(plan).expect("load should succeed");

    assert!(
        response.is_empty(),
        "empty store should still load after recovery replay"
    );
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after read recovery"
    );
}
