use super::*;
use crate::obs::sink::{MetricsEvent, MetricsSink, with_metrics_sink};
use std::cell::RefCell;

///
/// CaptureSink
///

#[derive(Default)]
struct CaptureSink {
    events: RefCell<Vec<MetricsEvent>>,
}

impl CaptureSink {
    fn into_events(self) -> Vec<MetricsEvent> {
        self.events.into_inner()
    }
}

impl MetricsSink for CaptureSink {
    fn record(&self, event: MetricsEvent) {
        self.events.borrow_mut().push(event);
    }
}

fn count_index_inserts(events: &[MetricsEvent]) -> usize {
    events.iter().fold(0usize, |acc, event| {
        let delta = match event {
            MetricsEvent::IndexDelta {
                entity_path,
                inserts,
                ..
            } if *entity_path == IndexedMetricsEntity::PATH => {
                usize::try_from(*inserts).unwrap_or(usize::MAX)
            }
            _ => 0,
        };

        acc.saturating_add(delta)
    })
}

fn count_index_removes(events: &[MetricsEvent]) -> usize {
    events.iter().fold(0usize, |acc, event| {
        let delta = match event {
            MetricsEvent::IndexDelta {
                entity_path,
                removes,
                ..
            } if *entity_path == IndexedMetricsEntity::PATH => {
                usize::try_from(*removes).unwrap_or(usize::MAX)
            }
            _ => 0,
        };

        acc.saturating_add(delta)
    })
}

fn count_reverse_index_inserts(events: &[MetricsEvent], entity_path: &'static str) -> usize {
    events.iter().fold(0usize, |acc, event| {
        let delta = match event {
            MetricsEvent::ReverseIndexDelta {
                entity_path: path,
                inserts,
                ..
            } if *path == entity_path => usize::try_from(*inserts).unwrap_or(usize::MAX),
            _ => 0,
        };

        acc.saturating_add(delta)
    })
}

fn count_reverse_index_removes(events: &[MetricsEvent], entity_path: &'static str) -> usize {
    events.iter().fold(0usize, |acc, event| {
        let delta = match event {
            MetricsEvent::ReverseIndexDelta {
                entity_path: path,
                removes,
                ..
            } if *path == entity_path => usize::try_from(*removes).unwrap_or(usize::MAX),
            _ => 0,
        };

        acc.saturating_add(delta)
    })
}

fn relation_validation_totals(
    events: &[MetricsEvent],
    entity_path: &'static str,
) -> (usize, usize) {
    events.iter().fold(
        (0usize, 0usize),
        |(lookups_acc, blocks_acc), event| match event {
            MetricsEvent::RelationValidation {
                entity_path: path,
                reverse_lookups,
                blocked_deletes,
            } if *path == entity_path => (
                lookups_acc.saturating_add(usize::try_from(*reverse_lookups).unwrap_or(usize::MAX)),
                blocks_acc.saturating_add(usize::try_from(*blocked_deletes).unwrap_or(usize::MAX)),
            ),
            _ => (lookups_acc, blocks_acc),
        },
    )
}

///
/// TESTS
///

#[test]
fn save_update_with_unchanged_index_key_emits_no_index_delta() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<IndexedMetricsEntity>::new(DB, false);
    let id = Ulid::generate();
    save.insert(IndexedMetricsEntity {
        id,
        tag: 7,
        label: "before".to_string(),
    })
    .expect("initial insert should succeed");

    let sink = CaptureSink::default();
    with_metrics_sink(&sink, || {
        save.update(IndexedMetricsEntity {
            id,
            tag: 7,
            label: "after".to_string(),
        })
        .expect("update should succeed");
    });
    let events = sink.into_events();

    assert_eq!(count_index_removes(&events), 0);
    assert_eq!(count_index_inserts(&events), 0);
}

#[test]
fn save_update_with_changed_index_key_emits_remove_and_insert() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<IndexedMetricsEntity>::new(DB, false);
    let id = Ulid::generate();
    save.insert(IndexedMetricsEntity {
        id,
        tag: 7,
        label: "before".to_string(),
    })
    .expect("initial insert should succeed");

    let sink = CaptureSink::default();
    with_metrics_sink(&sink, || {
        save.update(IndexedMetricsEntity {
            id,
            tag: 8,
            label: "after".to_string(),
        })
        .expect("update should succeed");
    });
    let events = sink.into_events();

    assert_eq!(count_index_removes(&events), 1);
    assert_eq!(count_index_inserts(&events), 1);
}

#[test]
fn delete_emits_remove_from_prepared_row_deltas() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_store();

    let save = SaveExecutor::<IndexedMetricsEntity>::new(DB, false);
    let delete = DeleteExecutor::<IndexedMetricsEntity>::new(DB, false);
    let id = Ulid::generate();
    save.insert(IndexedMetricsEntity {
        id,
        tag: 9,
        label: "before-delete".to_string(),
    })
    .expect("initial insert should succeed");

    let sink = CaptureSink::default();
    with_metrics_sink(&sink, || {
        let plan = Query::<IndexedMetricsEntity>::new(MissingRowPolicy::Ignore)
            .delete()
            .by_id(id)
            .plan()
            .expect("delete plan should build");
        delete.execute(plan).expect("delete should succeed");
    });
    let events = sink.into_events();

    assert_eq!(count_index_removes(&events), 1);
    assert_eq!(count_index_inserts(&events), 0);
}

#[test]
fn save_relation_insert_emits_reverse_index_delta() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::generate();
    let source_id = Ulid::generate();

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target insert should succeed");

    let sink = CaptureSink::default();
    with_metrics_sink(&sink, || {
        SaveExecutor::<RelationSourceEntity>::new(REL_DB, false)
            .insert(RelationSourceEntity {
                id: source_id,
                target: target_id,
            })
            .expect("source insert should succeed");
    });
    let events = sink.into_events();

    assert_eq!(
        count_reverse_index_inserts(&events, RelationSourceEntity::PATH),
        1
    );
    assert_eq!(
        count_reverse_index_removes(&events, RelationSourceEntity::PATH),
        0
    );
}

#[test]
fn delete_relation_emits_reverse_index_remove_delta() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::generate();
    let source_id = Ulid::generate();

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target insert should succeed");
    SaveExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_id,
        })
        .expect("source insert should succeed");

    let sink = CaptureSink::default();
    with_metrics_sink(&sink, || {
        let plan = Query::<RelationSourceEntity>::new(MissingRowPolicy::Ignore)
            .delete()
            .by_id(source_id)
            .plan()
            .expect("source delete plan should build");
        DeleteExecutor::<RelationSourceEntity>::new(REL_DB, false)
            .execute(plan)
            .expect("source delete should succeed");
    });
    let events = sink.into_events();

    assert_eq!(
        count_reverse_index_removes(&events, RelationSourceEntity::PATH),
        1
    );
}

#[test]
fn blocked_target_delete_emits_relation_validation_metrics() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::generate();
    let source_id = Ulid::generate();

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target insert should succeed");
    SaveExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_id,
        })
        .expect("source insert should succeed");

    let sink = CaptureSink::default();
    with_metrics_sink(&sink, || {
        let plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
            .delete()
            .by_id(target_id)
            .plan()
            .expect("target delete plan should build");
        DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
            .execute(plan)
            .expect_err("target delete should be blocked");
    });
    let events = sink.into_events();

    let (lookups, blocks) = relation_validation_totals(&events, RelationSourceEntity::PATH);
    assert!(
        lookups >= 1,
        "relation validation should perform reverse lookups"
    );
    assert_eq!(blocks, 1, "blocked delete should be counted once");
    assert_eq!(
        count_reverse_index_inserts(&events, RelationTargetEntity::PATH),
        0,
        "blocked target delete must not emit reverse-index insert deltas",
    );
    assert_eq!(
        count_reverse_index_removes(&events, RelationTargetEntity::PATH),
        0,
        "blocked target delete must not emit reverse-index remove deltas",
    );
}

#[test]
fn allowed_target_delete_emits_relation_lookup_without_block() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_id = Ulid::generate();
    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_id })
        .expect("target insert should succeed");

    let sink = CaptureSink::default();
    with_metrics_sink(&sink, || {
        let plan = Query::<RelationTargetEntity>::new(MissingRowPolicy::Ignore)
            .delete()
            .by_id(target_id)
            .plan()
            .expect("target delete plan should build");
        DeleteExecutor::<RelationTargetEntity>::new(REL_DB, false)
            .execute(plan)
            .expect("target delete should succeed");
    });
    let events = sink.into_events();

    let (lookups, blocks) = relation_validation_totals(&events, RelationSourceEntity::PATH);
    assert!(
        lookups >= 1,
        "allowed target delete should still emit reverse-lookup validation metrics",
    );
    assert_eq!(
        blocks, 0,
        "allowed target delete should not emit block counts"
    );
}

#[test]
fn save_missing_strong_relation_emits_no_reverse_index_delta() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let sink = CaptureSink::default();
    with_metrics_sink(&sink, || {
        SaveExecutor::<RelationSourceEntity>::new(REL_DB, false)
            .insert(RelationSourceEntity {
                id: Ulid::generate(),
                target: Ulid::generate(),
            })
            .expect_err("source save should fail when strong target is missing");
    });
    let events = sink.into_events();

    assert_eq!(
        count_reverse_index_inserts(&events, RelationSourceEntity::PATH),
        0,
        "failed strong-relation save must not emit reverse-index insert deltas",
    );
    assert_eq!(
        count_reverse_index_removes(&events, RelationSourceEntity::PATH),
        0,
        "failed strong-relation save must not emit reverse-index remove deltas",
    );
}

#[test]
fn save_relation_retarget_update_emits_reverse_index_remove_and_insert() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    reset_relation_stores();

    let target_a = Ulid::generate();
    let target_b = Ulid::generate();
    let source_id = Ulid::generate();

    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_a })
        .expect("target A insert should succeed");
    SaveExecutor::<RelationTargetEntity>::new(REL_DB, false)
        .insert(RelationTargetEntity { id: target_b })
        .expect("target B insert should succeed");
    SaveExecutor::<RelationSourceEntity>::new(REL_DB, false)
        .insert(RelationSourceEntity {
            id: source_id,
            target: target_a,
        })
        .expect("source insert should succeed");

    let sink = CaptureSink::default();
    with_metrics_sink(&sink, || {
        SaveExecutor::<RelationSourceEntity>::new(REL_DB, false)
            .replace(RelationSourceEntity {
                id: source_id,
                target: target_b,
            })
            .expect("source retarget update should succeed");
    });
    let events = sink.into_events();

    assert_eq!(
        count_reverse_index_removes(&events, RelationSourceEntity::PATH),
        1,
        "retarget update should emit one reverse-index remove delta",
    );
    assert_eq!(
        count_reverse_index_inserts(&events, RelationSourceEntity::PATH),
        1,
        "retarget update should emit one reverse-index insert delta",
    );
}
