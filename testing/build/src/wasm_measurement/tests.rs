use super::*;

#[test]
fn current_measurement_contract_is_complete_and_fail_closed() {
    validate_wasm_measurement_contract().expect("current measurement contract should validate");
    assert_eq!(MAX_SCHEMA_ENTITY_SCALE_RAW_BYTES_PER_ENTITY, 2_048);
    assert_eq!(MAX_REACHABLE_ENTITY_SCALE_RAW_BYTES_PER_ENTITY, 8_192);
    assert_eq!(
        WASM_MEASUREMENT_COMPARISONS
            .iter()
            .find(|comparison| comparison.id == "schema_entity_scale")
            .map(|comparison| comparison.disposition),
        Some(WasmComparisonDisposition::Attributable)
    );
}

#[test]
fn schema_entity_scale_guard_uses_final_raw_bytes_and_rounds_up() {
    let exact_limit = MAX_SCHEMA_ENTITY_SCALE_RAW_BYTES_PER_ENTITY * ENTITY_SCALE_ADDED_ENTITIES;
    assert_eq!(
        validate_schema_entity_scale_raw_wasm(1_000_000, 1_000_000 + exact_limit),
        Ok(MAX_SCHEMA_ENTITY_SCALE_RAW_BYTES_PER_ENTITY)
    );
    assert_eq!(entity_scale_raw_bytes_per_added_entity(10_000, 10_001), 1);
    assert_eq!(entity_scale_raw_bytes_per_added_entity(10_001, 10_000), 0);

    let exceeded = validate_schema_entity_scale_raw_wasm(1_000_000, 1_000_000 + exact_limit + 1)
        .expect_err("one byte above the aggregate ceiling should fail");
    assert_eq!(
        exceeded,
        EntityScaleWasmBudgetExceeded {
            maximum_raw_bytes_per_entity: MAX_SCHEMA_ENTITY_SCALE_RAW_BYTES_PER_ENTITY,
            observed_raw_bytes_per_entity: MAX_SCHEMA_ENTITY_SCALE_RAW_BYTES_PER_ENTITY + 1,
        }
    );
}

#[test]
fn reachable_entity_scale_guard_uses_its_distinct_fail_closed_ceiling() {
    assert_eq!(
        validate_reachable_entity_scale_raw_wasm(2_871_318, 2_911_506),
        Ok(4_466)
    );

    let exact_limit = MAX_REACHABLE_ENTITY_SCALE_RAW_BYTES_PER_ENTITY * ENTITY_SCALE_ADDED_ENTITIES;
    assert_eq!(
        validate_reachable_entity_scale_raw_wasm(1_000_000, 1_000_000 + exact_limit),
        Ok(MAX_REACHABLE_ENTITY_SCALE_RAW_BYTES_PER_ENTITY)
    );
    let exceeded = validate_reachable_entity_scale_raw_wasm(1_000_000, 1_000_000 + exact_limit + 1)
        .expect_err("one byte above the reachable aggregate ceiling should fail");
    assert_eq!(
        exceeded,
        EntityScaleWasmBudgetExceeded {
            maximum_raw_bytes_per_entity: MAX_REACHABLE_ENTITY_SCALE_RAW_BYTES_PER_ENTITY,
            observed_raw_bytes_per_entity: MAX_REACHABLE_ENTITY_SCALE_RAW_BYTES_PER_ENTITY + 1,
        }
    );
}
