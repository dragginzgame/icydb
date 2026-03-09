use crate::obs::metrics::{
    EntityCounters, EntitySummary, EventOps, EventPerf, EventReport, EventState,
    report_window_start, reset_all, with_state, with_state_mut,
};
use serde::Serialize;
use serde_cbor::Value as CborValue;
use std::collections::BTreeMap;

fn to_cbor_value<T: Serialize>(value: &T) -> CborValue {
    let bytes = serde_cbor::to_vec(value).expect("test fixtures must serialize into CBOR payloads");
    serde_cbor::from_slice::<CborValue>(&bytes)
        .expect("test fixtures must deserialize into CBOR value trees")
}

fn expect_cbor_map(value: &CborValue) -> &BTreeMap<CborValue, CborValue> {
    match value {
        CborValue::Map(map) => map,
        other => panic!("expected CBOR map, got {other:?}"),
    }
}

fn map_field<'a>(map: &'a BTreeMap<CborValue, CborValue>, key: &str) -> Option<&'a CborValue> {
    map.get(&CborValue::Text(key.to_string()))
}

#[test]
fn reset_all_clears_state() {
    with_state_mut(|m| {
        m.ops.load_calls = 3;
        m.ops.index_inserts = 2;
        m.perf.save_inst_max = 9;
        m.entities.insert(
            "alpha".to_string(),
            EntityCounters {
                load_calls: 1,
                ..Default::default()
            },
        );
    });

    reset_all();

    with_state(|m| {
        assert_eq!(m.ops.load_calls, 0);
        assert_eq!(m.ops.index_inserts, 0);
        assert_eq!(m.perf.save_inst_max, 0);
        assert!(m.entities.is_empty());
    });
}

#[test]
fn report_sorts_entities_by_average_rows() {
    reset_all();
    with_state_mut(|m| {
        m.entities.insert(
            "alpha".to_string(),
            EntityCounters {
                load_calls: 2,
                rows_loaded: 6,
                ..Default::default()
            },
        );
        m.entities.insert(
            "beta".to_string(),
            EntityCounters {
                load_calls: 1,
                rows_loaded: 5,
                ..Default::default()
            },
        );
        m.entities.insert(
            "gamma".to_string(),
            EntityCounters {
                load_calls: 2,
                rows_loaded: 6,
                ..Default::default()
            },
        );
    });

    let report = report_window_start(None);
    let paths: Vec<_> = report
        .entity_counters
        .iter()
        .map(|e| e.path.as_str())
        .collect();

    // Order by avg rows per load desc, then rows_loaded desc, then path asc.
    assert_eq!(paths, ["beta", "alpha", "gamma"]);
    assert_eq!(report.entity_counters[0].avg_rows_per_load, 5.0);
    assert_eq!(report.entity_counters[1].avg_rows_per_load, 3.0);
    assert_eq!(report.entity_counters[2].avg_rows_per_load, 3.0);
}

#[test]
fn event_report_serialization_shape_is_stable() {
    let report = EventReport {
        counters: Some(EventState {
            ops: EventOps {
                load_calls: 1,
                rows_loaded: 2,
                rows_scanned: 3,
                non_atomic_partial_rows_committed: 4,
                ..Default::default()
            },
            perf: EventPerf {
                load_inst_total: 11,
                load_inst_max: 12,
                ..Default::default()
            },
            entities: BTreeMap::from([(
                "alpha".to_string(),
                EntityCounters {
                    load_calls: 5,
                    rows_loaded: 8,
                    ..Default::default()
                },
            )]),
            window_start_ms: 99,
        }),
        entity_counters: vec![EntitySummary {
            path: "alpha".to_string(),
            load_calls: 5,
            rows_loaded: 8,
            avg_rows_per_load: 1.6,
            ..Default::default()
        }],
    };

    let encoded = to_cbor_value(&report);
    let root = expect_cbor_map(&encoded);
    assert!(
        map_field(root, "counters").is_some(),
        "EventReport must keep `counters` as serialized field key",
    );
    assert!(
        map_field(root, "entity_counters").is_some(),
        "EventReport must keep `entity_counters` as serialized field key",
    );

    let counters = map_field(root, "counters").expect("counters payload should exist");
    let counters_map = expect_cbor_map(counters);
    assert!(
        map_field(counters_map, "ops").is_some(),
        "EventState must keep `ops` as serialized field key",
    );
    assert!(
        map_field(counters_map, "perf").is_some(),
        "EventState must keep `perf` as serialized field key",
    );
    assert!(
        map_field(counters_map, "entities").is_some(),
        "EventState must keep `entities` as serialized field key",
    );
    assert!(
        map_field(counters_map, "window_start_ms").is_some(),
        "EventState must keep `window_start_ms` as serialized field key",
    );
}

#[test]
fn entity_summary_serialization_shape_is_stable() {
    let encoded = to_cbor_value(&EntitySummary {
        path: "alpha".to_string(),
        load_calls: 5,
        delete_calls: 6,
        rows_loaded: 8,
        rows_scanned: 9,
        rows_deleted: 10,
        avg_rows_per_load: 1.6,
        avg_rows_scanned_per_load: 1.8,
        avg_rows_per_delete: 2.0,
        index_inserts: 11,
        index_removes: 12,
        reverse_index_inserts: 13,
        reverse_index_removes: 14,
        relation_reverse_lookups: 15,
        relation_delete_blocks: 16,
        unique_violations: 17,
        non_atomic_partial_commits: 18,
        non_atomic_partial_rows_committed: 19,
    });
    let root = expect_cbor_map(&encoded);
    assert!(
        map_field(root, "path").is_some(),
        "EntitySummary must keep `path` as serialized field key",
    );
    assert!(
        map_field(root, "avg_rows_per_load").is_some(),
        "EntitySummary must keep `avg_rows_per_load` as serialized field key",
    );
    assert!(
        map_field(root, "relation_delete_blocks").is_some(),
        "EntitySummary must keep `relation_delete_blocks` as serialized field key",
    );
    assert!(
        map_field(root, "non_atomic_partial_rows_committed").is_some(),
        "EntitySummary must keep `non_atomic_partial_rows_committed` as serialized field key",
    );
}
