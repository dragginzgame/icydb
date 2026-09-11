//! Matched, explicitly built audit actors; never a wall-clock benchmark.

use std::{env, fs, path::Path};

use icydb_testing_integration::install_prebuilt_fixture_canister;

type Samples = Result<Vec<(u64, u64, String)>, u16>;
type TypedSamples = Result<Vec<(u64, u64, String, u64)>, u16>;

#[test]
#[ignore = "requires matched prebuilt actors; see 0.257-terminal-qualification.md"]
fn typed_explain_matches_sql_reports_and_instruction_gate() {
    let directory = env::var("ICYDB_EXPLAIN_WASM_DIR").expect("set the qualified Wasm directory");
    let load = |name: &str| fs::read(Path::new(&directory).join(name)).unwrap();
    let typed = install_prebuilt_fixture_canister("one_entity_typed_query", load("typed.wasm"));
    let sql = install_prebuilt_fixture_canister("one_entity_typed_query", load("sql-explain.wasm"));
    let mixed = install_prebuilt_fixture_canister("one_entity_typed_query", load("mixed.wasm"));
    let mut instruction_misses = Vec::new();
    for kind in 0..3_u8 {
        let typed_samples: TypedSamples = typed
            .query_candid("measure_typed_explain", (kind,))
            .unwrap();
        let sql_samples: Samples = sql.query_candid("measure_sql_explain", (kind,)).unwrap();
        let mixed_samples: TypedSamples = mixed
            .query_candid("measure_typed_explain", (kind,))
            .unwrap();
        let typed_samples = typed_samples.expect("typed explain succeeds");
        let sql_samples = sql_samples.expect("SQL explain succeeds");
        let mixed_samples = mixed_samples.expect("mixed actor typed explain succeeds");
        assert_eq!(typed_samples.len(), 3);
        assert_eq!(sql_samples.len(), 3);
        assert_eq!(mixed_samples.len(), 3);
        for (call, ((typed, sql), mixed)) in typed_samples
            .iter()
            .zip(&sql_samples)
            .zip(&mixed_samples)
            .enumerate()
        {
            println!(
                "kind={kind} call={call} typed_plan={} typed_render={} sql_total={} mixed_plan={} mixed_render={} report_bytes={} typed_binding={}",
                typed.0,
                typed.1,
                sql.0,
                mixed.0,
                mixed.1,
                typed.2.len(),
                typed.3
            );
            assert_eq!(typed.2, sql.2, "same logical report");
            assert_eq!(
                typed.2, mixed.2,
                "retaining SQL does not change typed diagnostics"
            );
            if typed.0 > sql.0 + sql.0 / 20 {
                instruction_misses.push((kind, call));
            }
        }
    }
    let rows: Result<u32, u16> = mixed.query_candid("retained_sql_read", ()).unwrap();
    assert_eq!(rows.unwrap(), 0);
    assert!(
        instruction_misses.is_empty(),
        "frozen 5% instruction ceiling missed: {instruction_misses:?}"
    );
}

#[test]
#[ignore = "requires matched typed-explain and exact-key audit actors"]
fn typed_explain_before_after_instruction_comparison() {
    let directory = env::var("ICYDB_EXPLAIN_WASM_DIR").expect("set the qualified Wasm directory");
    let load = |name: &str| fs::read(Path::new(&directory).join(name)).unwrap();
    let before = install_prebuilt_fixture_canister("one_entity_typed_query", load("before.wasm"));
    let after = install_prebuilt_fixture_canister("one_entity_typed_query", load("after.wasm"));
    for kind in 0..3_u8 {
        let before: TypedSamples = before
            .query_candid("measure_typed_explain", (kind,))
            .unwrap();
        let after: TypedSamples = after
            .query_candid("measure_typed_explain", (kind,))
            .unwrap();
        let before = before.unwrap();
        let after = after.unwrap();
        assert_eq!(before.len(), 3);
        assert_eq!(after.len(), 3);
        for (call, (before, after)) in before.iter().zip(&after).enumerate() {
            println!(
                "kind={kind} call={call} before_plan={} after_plan={} before_binding={} after_binding={} before_render={} after_render={}",
                before.0, after.0, before.3, after.3, before.1, after.1
            );
            assert_eq!(
                before.2, after.2,
                "typed preparation does not change the report"
            );
            assert!(
                after.0 <= before.0,
                "total planning instructions do not increase"
            );
        }
    }
    for items in [1_u16, 16] {
        type ExactSample = ((u16, u16, u32, u64),);
        let baseline: ExactSample = before
            .query_candid("measure_exact_key_batch", (items, true))
            .unwrap();
        let candidate: ExactSample = after
            .query_candid("measure_exact_key_batch", (items, true))
            .unwrap();
        assert_eq!((baseline.0.0, baseline.0.1, baseline.0.2), (items, 0, 0));
        assert_eq!((candidate.0.0, candidate.0.1, candidate.0.2), (items, 0, 0));
        println!(
            "exact_keys={items} before={} after={}",
            baseline.0.3, candidate.0.3
        );
        assert!(
            candidate.0.3 <= baseline.0.3,
            "exact-key instructions do not increase"
        );
    }
}
