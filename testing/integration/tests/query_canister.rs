use ic_testkit::pic::StandaloneCanisterFixture;
use icydb_testing_integration::{install_fixture_canister, install_prebuilt_fixture_canister};
use std::{env, fs};

fn audited_typed_query_row_count(
    fixture: &StandaloneCanisterFixture,
    method: &str,
) -> Result<u32, u16> {
    fixture
        .query_candid(method, ())
        .expect("typed query endpoint response should decode")
}

#[test]
fn query_only_typed_and_dynamic_canisters_execute_without_sql() {
    let dynamic_fixture = install_fixture_canister("one_entity_dynamic_query");
    let dynamic_row_count: u32 = dynamic_fixture
        .query_candid("query_one_entity_dynamic_rows", ())
        .expect("dynamic query endpoint response should decode");
    assert_eq!(dynamic_row_count, 0);

    let audited_fixture = install_fixture_canister("one_entity_typed_query");
    let audited_row_count =
        audited_typed_query_row_count(&audited_fixture, "query_one_entity_typed_rows")
            .unwrap_or_else(|error| panic!("typed schema/query initialization failed: {error}"));
    assert_eq!(audited_row_count, 0);

    let ten_entity_fixture = install_fixture_canister("ten_entity_typed_query");
    let ten_entity_row_count: u32 = ten_entity_fixture
        .query_candid("query_ten_entity_typed_rows", ())
        .expect("ten-entity typed query endpoint response should decode");
    assert_eq!(ten_entity_row_count, 0);
}

#[test]
#[ignore = "the 0.221 measurement handoff supplies one exact prebuilt Wasm subject"]
fn repeated_dynamic_point_query_instruction_measurement() {
    let wasm_path = env::var("ICYDB_0221_POINT_QUERY_WASM")
        .expect("ICYDB_0221_POINT_QUERY_WASM should name the measured Wasm");
    let wasm = fs::read(&wasm_path).expect("measured point-query Wasm should read");
    let fixture = install_prebuilt_fixture_canister("one_entity_dynamic_query", wasm);
    for (shape, method) in [
        ("point", "measure_repeated_point_queries"),
        ("scan", "measure_repeated_scan_queries"),
    ] {
        let ((executions, failures, rows, local_instructions),): ((u16, u16, u32, u64),) = fixture
            .query_candid(method, (1_000_u16,))
            .expect("repeated dynamic-query measurement should decode");

        assert_eq!(executions, 1_000);
        assert_eq!(failures, 0);
        assert_eq!(rows, 0);
        assert!(local_instructions > 0);
        println!(
            "icydb_0221_repeated_queries wasm={wasm_path} shape={shape} executions={executions} local_instructions={local_instructions}",
        );
    }
}

#[test]
#[ignore = "the 0.221 native exact-key handoff supplies one prebuilt Wasm subject"]
fn native_exact_key_batch_instruction_measurement() {
    let wasm_path = env::var("ICYDB_0221_EXACT_KEY_WASM")
        .expect("ICYDB_0221_EXACT_KEY_WASM should name the measured Wasm");
    let wasm = fs::read(&wasm_path).expect("measured exact-key Wasm should read");
    let fixture = install_prebuilt_fixture_canister("one_entity_typed_query", wasm);
    for (shape, method, distinct) in [
        ("dynamic_distinct", "measure_dynamic_key_loop", true),
        ("exact_distinct", "measure_exact_key_batch", true),
        ("dynamic_duplicate", "measure_dynamic_key_loop", false),
        ("exact_duplicate", "measure_exact_key_batch", false),
    ] {
        let ((items, failures, rows, local_instructions),): ((u16, u16, u32, u64),) = fixture
            .query_candid(method, (1_000_u16, distinct))
            .expect("exact-key instruction measurement should decode");

        assert_eq!(items, 1_000);
        assert_eq!(failures, 0);
        assert_eq!(rows, 0);
        assert!(local_instructions > 0);
        println!(
            "icydb_0221_exact_keys wasm={wasm_path} shape={shape} items={items} local_instructions={local_instructions}",
        );
    }
}
