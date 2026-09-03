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
fn reachable_entity_audit_pair_exercises_the_shared_operation_mix() {
    for (canister, entity) in [
        ("one_entity_reachable_operations", 0_u8),
        ("ten_entity_reachable_operations", 9_u8),
    ] {
        let fixture = install_fixture_canister(canister);
        for operation in 0_u8..=7 {
            let method = if operation <= 1 {
                "exercise_reachable_entity_read"
            } else {
                "exercise_reachable_entity_write"
            };
            let selected_entity = if operation >= 6 { 0 } else { entity };
            let response = if operation <= 1 {
                fixture.query_candid(method, (selected_entity, operation))
            } else {
                fixture.update_candid(method, (selected_entity, operation))
            };
            let ((succeeded, local_instructions),): ((u32, u64),) =
                response.expect("reachable generated operation response should decode");
            println!(
                "icydb_0250_reachable_entity canister={canister} entity={selected_entity} operation={operation} succeeded={succeeded} local_instructions={local_instructions}",
            );
            assert!(local_instructions > 0);
            if matches!(operation, 0 | 1 | 2 | 4 | 6 | 7) {
                assert_eq!(succeeded, 1);
            }
        }
    }
}

#[test]
#[ignore = "requires one exact prebuilt point-query Wasm subject"]
fn repeated_dynamic_point_query_instruction_measurement() {
    let wasm_path = env::var("ICYDB_POINT_QUERY_WASM")
        .expect("ICYDB_POINT_QUERY_WASM should name the measured Wasm");
    let wasm = fs::read(&wasm_path).expect("measured point-query Wasm should read");
    let fixture = install_prebuilt_fixture_canister("one_entity_dynamic_query", wasm);
    for (shape, method) in [
        ("point", "measure_repeated_point_queries"),
        ("point_distinct", "measure_parameterized_point_queries"),
        ("scan", "measure_repeated_scan_queries"),
    ] {
        let ((executions, failures, rows, local_instructions),): ((u16, u16, u32, u64),) = fixture
            .query_candid(method, (200_u16,))
            .expect("repeated dynamic-query measurement should decode");

        assert_eq!(executions, 200);
        assert_eq!(failures, 0);
        assert_eq!(rows, 0);
        assert!(local_instructions > 0);
        println!(
            "icydb_repeated_queries wasm={wasm_path} shape={shape} executions={executions} local_instructions={local_instructions}",
        );
    }
}

#[test]
#[ignore = "requires one exact prebuilt native exact-key Wasm subject"]
fn native_exact_key_batch_instruction_measurement() {
    let wasm_path = env::var("ICYDB_EXACT_KEY_WASM")
        .expect("ICYDB_EXACT_KEY_WASM should name the measured Wasm");
    let wasm = fs::read(&wasm_path).expect("measured exact-key Wasm should read");
    let fixture = install_prebuilt_fixture_canister("one_entity_typed_query", wasm);
    for (shape, method, distinct, requested_items) in [
        (
            "dynamic_distinct",
            "measure_dynamic_key_loop",
            true,
            200_u16,
        ),
        ("exact_distinct", "measure_exact_key_batch", true, 1_000_u16),
        (
            "dynamic_duplicate",
            "measure_dynamic_key_loop",
            false,
            200_u16,
        ),
        (
            "exact_duplicate",
            "measure_exact_key_batch",
            false,
            1_000_u16,
        ),
    ] {
        let ((items, failures, rows, local_instructions),): ((u16, u16, u32, u64),) = fixture
            .query_candid(method, (requested_items, distinct))
            .expect("exact-key instruction measurement should decode");

        assert_eq!(items, requested_items);
        assert_eq!(failures, 0);
        assert_eq!(rows, 0);
        assert!(local_instructions > 0);
        println!(
            "icydb_0221_exact_keys wasm={wasm_path} shape={shape} items={items} local_instructions={local_instructions}",
        );
    }
}
