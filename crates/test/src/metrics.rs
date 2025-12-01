use icydb::core::{obs::metrics, prelude::*, traits::EntityKind};

///
/// MetricsSuite
/// Verifies global and per-entity counters, index ops, unique violations,
/// and reset behavior surfaced by the metrics endpoint.
///

pub struct MetricsSuite;

impl MetricsSuite {
    pub fn test() {
        let tests: Vec<(&str, fn())> = vec![
            ("counters_basic", Self::counters_basic),
            (
                "index_counters_and_violation",
                Self::index_counters_and_violation,
            ),
            ("reset_metrics", Self::reset_metrics),
        ];

        for (name, test_fn) in tests {
            // fresh counters and data
            metrics::reset();
            crate::clear_test_data_store();

            println!("Running test: {name}");
            test_fn();
        }
    }

    // basic load/save/delete counters and rows touched
    fn counters_basic() {
        use test_design::e2e::db::SimpleEntity;

        // 3 creates → 3 save calls
        for _ in 0..3 {
            db!().insert(SimpleEntity::default()).unwrap();
        }

        // 1 load all → rows_loaded = 3
        let loaded = db!().load::<SimpleEntity>().all().unwrap();
        assert_eq!(loaded.count(), 3);

        // 1 delete one → row is deleted
        let first_key = loaded.keys()[0];
        db!().delete::<SimpleEntity>().one(first_key).unwrap();

        // Snapshot
        let stats = crate::icydb_metrics().unwrap();
        let m = stats.counters.as_ref().expect("metrics snapshot present");

        // Global counters
        assert_eq!(m.ops.save_calls, 3, "save_calls should be 3");
        assert_eq!(m.ops.load_calls, 1, "load_calls should be 1");
        assert_eq!(m.ops.delete_calls, 1, "delete_calls should be 1");
        assert_eq!(m.ops.rows_loaded, 3, "rows_loaded should be 3");
        assert_eq!(m.ops.rows_deleted, 1, "rows_deleted should be 1");

        // Per-entity counters
        let path = SimpleEntity::PATH.to_string();
        let e_ops = m
            .entities
            .get(&path)
            .expect("per-entity counters present for SimpleEntity");
        assert_eq!(e_ops.load_calls, 1);
        assert_eq!(e_ops.delete_calls, 1);
        assert_eq!(e_ops.rows_loaded, 3);
        assert_eq!(e_ops.rows_deleted, 1);

        // Derived entity_stats entry contains correct averages
        let e_stat = stats
            .entity_counters
            .iter()
            .find(|e| e.path == path)
            .expect("entity_stats contains SimpleEntity");
        assert!((e_stat.avg_rows_per_load - 3.0).abs() < f64::EPSILON);
        assert!((e_stat.avg_rows_per_delete - 1.0).abs() < f64::EPSILON);
    }

    // index insert/remove and unique violation are counted
    fn index_counters_and_violation() {
        use test_design::e2e::db::Index;

        // Insert e1, e2 (each has 2 indexes) → index_inserts += 4
        let e1 = Index::new(1, 10);
        let id1 = db!().insert(e1).unwrap().key();

        let e2 = Index::new(1, 20);
        db!().insert(e2).unwrap();

        // Attempt conflicting unique y=10 → should fail and count unique_violation
        let e3_conflict = Index::new(2, 10);
        let err = db!()
            .insert(e3_conflict)
            .expect_err("expected unique violation");
        let _ = err; // just ensure it errored

        // Delete e1 → index_removes += 2
        db!().delete::<Index>().one(id1).unwrap();

        // Retry e3 with y=10 now free → success → index_inserts += 2
        let e3_ok = Index::new(2, 10);
        db!().insert(e3_ok).unwrap();

        // Snapshot
        let stats = crate::icydb_metrics().unwrap();
        let m = stats.counters.as_ref().unwrap();

        // Save calls include the failed attempt
        assert_eq!(m.ops.save_calls, 4, "save_calls counts failed attempt");
        // 3 successful inserts, 2 indexes each
        assert_eq!(
            m.ops.index_inserts, 6,
            "index_inserts across 3 successful saves"
        );
        assert_eq!(
            m.ops.index_removes, 2,
            "index_removes for one deleted entity"
        );
        assert_eq!(m.ops.unique_violations, 1, "one unique violation recorded");

        // Per-entity also reflects the same
        let path = Index::PATH.to_string();
        let e_ops = m
            .entities
            .get(&path)
            .expect("per-entity counters present for Index");
        assert_eq!(e_ops.index_inserts, 6);
        assert_eq!(e_ops.index_removes, 2);
        assert_eq!(e_ops.unique_violations, 1);
    }

    // verify reset clears counters via the endpoint as well
    fn reset_metrics() {
        use test_design::e2e::db::SimpleEntity;

        // Bump something
        db!().insert(SimpleEntity::default()).unwrap();
        let before = crate::icydb_metrics().unwrap();
        assert!(before.counters.as_ref().unwrap().ops.save_calls > 0);

        // Endpoint reset
        crate::icydb_metrics_reset().unwrap();

        let after = crate::icydb_metrics().unwrap();
        let m = after.counters.as_ref().unwrap();
        assert_eq!(m.ops.save_calls, 0);
        assert_eq!(m.ops.load_calls, 0);
        assert_eq!(m.ops.delete_calls, 0);
        assert_eq!(m.ops.rows_loaded, 0);
        assert_eq!(m.ops.rows_deleted, 0);
    }
}
