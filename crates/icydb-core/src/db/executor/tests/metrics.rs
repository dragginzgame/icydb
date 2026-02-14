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
        let plan = Query::<IndexedMetricsEntity>::new(ReadConsistency::MissingOk)
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
