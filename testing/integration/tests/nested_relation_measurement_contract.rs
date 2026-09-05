use icydb_testing_integration::{
    install_fixture_canister,
    nested_relation_contract::{
        DIRECT_INSERT_CALIBRATION_ROWS, DIRECT_REPLACE_CALIBRATION_ROWS,
        IC_UPDATE_INSTRUCTION_LIMIT, MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES,
        MAX_NESTED_RELATION_PATH_STEPS, REQUIRED_INSTRUCTION_HEADROOM,
    },
};
use std::time::Duration;

const ADMITTED_CALIBRATION_INSTRUCTIONS: u64 =
    IC_UPDATE_INSTRUCTION_LIMIT - REQUIRED_INSTRUCTION_HEADROOM;

struct RelationCalibrationResult {
    rows: u16,
    target_insert_instructions: u64,
    replacement_target_insert_instructions: u64,
    source_insert_instructions: u64,
    source_replace_instructions: u64,
    source_delete_instructions: u64,
    target_delete_instructions: u64,
    replacement_target_delete_instructions: u64,
    target_miss_instructions: u64,
    target_restrict_instructions: u64,
}

fn drain_convergence(fixture: &ic_testkit::pic::StandaloneCanisterFixture) {
    fixture.pocket_ic().advance_time(Duration::from_secs(1));
    for _ in 0..16 {
        fixture.pocket_ic().tick();
    }
}

fn measure_phase(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    base: i32,
    count: u16,
    phase: u8,
) -> u64 {
    let measured: Result<u64, icydb::Error> = fixture
        .update_candid("measure_relation_calibration", (base, count, phase))
        .expect("stable relation-tree calibration phase should decode");
    let instructions = measured.unwrap_or_else(|error| {
        panic!("stable relation-tree calibration phase {phase} at base {base} failed: {error:?}")
    });
    drain_convergence(fixture);
    instructions
}

fn call_phase(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
    base: i32,
    count: u16,
    phase: u8,
) -> Result<u64, icydb::Error> {
    fixture
        .update_candid("measure_relation_calibration", (base, count, phase))
        .expect("stable relation-tree boundary phase should decode")
}

fn prove_commit_work_boundaries(fixture: &ic_testkit::pic::StandaloneCanisterFixture) {
    let _replacement_boundary_targets =
        measure_phase(fixture, 100_000, DIRECT_REPLACE_CALIBRATION_ROWS + 1, 0);
    let mixed_boundary = call_phase(fixture, 0, DIRECT_REPLACE_CALIBRATION_ROWS + 1, 2)
        .expect_err("the first row above the two-delta mixed boundary should reject");
    assert_eq!(
        mixed_boundary.code(),
        icydb::ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED,
    );
    drain_convergence(fixture);

    let _lookup_boundary_targets =
        measure_phase(fixture, 200_000, DIRECT_INSERT_CALIBRATION_ROWS + 1, 0);
    let lookup_boundary = call_phase(fixture, 200_000, DIRECT_INSERT_CALIBRATION_ROWS + 1, 1)
        .expect_err("the first row above the one-delta lookup boundary should reject");
    assert_eq!(
        lookup_boundary.code(),
        icydb::ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED,
    );
    drain_convergence(fixture);
}

fn measure_stable_tree_matrix(
    fixture: &ic_testkit::pic::StandaloneCanisterFixture,
) -> RelationCalibrationResult {
    RelationCalibrationResult {
        rows: DIRECT_REPLACE_CALIBRATION_ROWS,
        target_insert_instructions: measure_phase(
            fixture,
            20_000,
            DIRECT_REPLACE_CALIBRATION_ROWS,
            0,
        ),
        replacement_target_insert_instructions: measure_phase(
            fixture,
            120_000,
            DIRECT_REPLACE_CALIBRATION_ROWS,
            0,
        ),
        source_insert_instructions: measure_phase(
            fixture,
            20_000,
            DIRECT_REPLACE_CALIBRATION_ROWS,
            1,
        ),
        source_replace_instructions: measure_phase(
            fixture,
            20_000,
            DIRECT_REPLACE_CALIBRATION_ROWS,
            2,
        ),
        source_delete_instructions: measure_phase(
            fixture,
            20_000,
            DIRECT_REPLACE_CALIBRATION_ROWS,
            3,
        ),
        target_delete_instructions: measure_phase(
            fixture,
            20_000,
            DIRECT_REPLACE_CALIBRATION_ROWS,
            4,
        ),
        replacement_target_delete_instructions: measure_phase(
            fixture,
            120_000,
            DIRECT_REPLACE_CALIBRATION_ROWS,
            4,
        ),
        target_miss_instructions: measure_phase(fixture, 20_000, 1, 5),
        target_restrict_instructions: measure_phase(fixture, 20_000, 1, 6),
    }
}

#[test]
fn controlled_actors_exercise_the_frozen_direct_relation_flow() {
    for (actor, restricts_target_delete) in [
        ("nested_relation_none", false),
        ("nested_relation_direct", true),
        ("nested_relation_shallow", true),
        ("nested_relation_repeated", false),
    ] {
        let fixture = install_fixture_canister(actor);
        for operation in 0_u8..=6 {
            let ((succeeded, instructions),): ((bool, u64),) = fixture
                .update_candid("exercise_relation_flow", (operation,))
                .expect("controlled relation operation should decode");
            assert!(instructions > 0);
            assert_eq!(
                succeeded,
                operation != 3 || restricts_target_delete,
                "unexpected operation outcome for {actor} operation {operation}",
            );
            println!(
                "icydb_0253_relation_flow actor={actor} operation={operation} instructions={instructions} succeeded={succeeded}",
            );
        }
    }
}

#[test]
fn shallow_actor_exercises_every_single_valued_nested_relation_flow() {
    let fixture = install_fixture_canister("nested_relation_shallow");
    for operation in 0_u8..=5 {
        let ((succeeded, instructions),): ((bool, u64),) = fixture
            .update_candid("exercise_shallow_relation_flow", (operation,))
            .expect("shallow nested relation operation should decode");
        assert!(
            succeeded,
            "shallow nested relation operation {operation} failed"
        );
        assert!(instructions > 0);
        println!(
            "icydb_0253_shallow_relation_flow operation={operation} instructions={instructions}",
        );
    }
}

#[test]
fn repeated_actor_exercises_every_collection_nested_relation_flow() {
    let fixture = install_fixture_canister("nested_relation_repeated");
    for operation in 0_u8..=5 {
        let ((succeeded, instructions),): ((bool, u64),) = fixture
            .update_candid("exercise_repeated_relation_flow", (operation,))
            .expect("repeated nested relation operation should decode");
        assert!(
            succeeded,
            "repeated nested relation operation {operation} failed"
        );
        assert!(instructions > 0);
        println!(
            "icydb_0253_repeated_relation_flow operation={operation} instructions={instructions}",
        );
    }
}

#[test]
fn repeated_actor_charges_raw_occurrences_before_deduplication() {
    let fixture = install_fixture_canister("nested_relation_repeated");
    let exact: Result<(), icydb::Error> = fixture
        .update_candid(
            "commit_repeated_relation_boundary",
            (
                40_000_i32,
                u16::try_from(MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES)
                    .expect("reference limit fits u16"),
            ),
        )
        .expect("exact repeated relation boundary should decode");
    exact.expect("exact raw-occurrence boundary should commit");

    let over: Result<(), icydb::Error> = fixture
        .update_candid(
            "commit_repeated_relation_boundary",
            (
                40_001_i32,
                u16::try_from(MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES + 1)
                    .expect("first rejected reference count fits u16"),
            ),
        )
        .expect("over-limit repeated relation boundary should decode");
    assert_eq!(
        over.expect_err("first raw occurrence above the limit should reject")
            .code(),
        icydb::ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED,
    );
}

#[test]
fn direct_relation_stable_tree_calibration_retains_required_message_headroom() {
    let fixture = install_fixture_canister("nested_relation_direct");
    let ((traversal_checksum, traversal_instructions),): ((u64, u64),) = fixture
        .query_candid(
            "measure_relation_traversal",
            (
                u8::try_from(MAX_NESTED_RELATION_PATH_STEPS)
                    .expect("frozen path-step limit fits u8"),
                u16::try_from(MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES)
                    .expect("frozen reference limit fits u16"),
            ),
        )
        .expect("nested traversal calibration should decode");
    assert_ne!(traversal_checksum, 0);
    assert!(traversal_instructions <= ADMITTED_CALIBRATION_INSTRUCTIONS);
    let prefill_target_instructions = measure_phase(&fixture, 0, DIRECT_INSERT_CALIBRATION_ROWS, 0);
    let prefill_source_instructions = measure_phase(&fixture, 0, DIRECT_INSERT_CALIBRATION_ROWS, 1);
    prove_commit_work_boundaries(&fixture);
    let measured = measure_stable_tree_matrix(&fixture);

    assert_eq!(measured.rows, DIRECT_REPLACE_CALIBRATION_ROWS);
    let successful_batch_maximum = [
        measured.target_insert_instructions,
        measured.replacement_target_insert_instructions,
        measured.source_insert_instructions,
        measured.source_replace_instructions,
        measured.source_delete_instructions,
        measured.target_delete_instructions,
        measured.replacement_target_delete_instructions,
    ]
    .into_iter()
    .max()
    .expect("calibration matrix is non-empty");
    assert!(
        successful_batch_maximum <= ADMITTED_CALIBRATION_INSTRUCTIONS,
        "calibrated stable-tree batch must retain at least 25% IC update headroom",
    );
    assert!(measured.target_miss_instructions > 0);
    assert!(measured.target_restrict_instructions > 0);

    println!(
        "icydb_0253_relation_calibration traversal_work={} traversal_instructions={} prefill_rows={} prefill_target={} prefill_source={} rows={} target_insert={} replacement_target_insert={} source_insert={} source_replace={} source_delete={} target_delete={} replacement_target_delete={} target_miss={} target_restrict={} admitted={} headroom={}",
        MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES * MAX_NESTED_RELATION_PATH_STEPS,
        traversal_instructions,
        DIRECT_INSERT_CALIBRATION_ROWS,
        prefill_target_instructions,
        prefill_source_instructions,
        measured.rows,
        measured.target_insert_instructions,
        measured.replacement_target_insert_instructions,
        measured.source_insert_instructions,
        measured.source_replace_instructions,
        measured.source_delete_instructions,
        measured.target_delete_instructions,
        measured.replacement_target_delete_instructions,
        measured.target_miss_instructions,
        measured.target_restrict_instructions,
        ADMITTED_CALIBRATION_INSTRUCTIONS,
        REQUIRED_INSTRUCTION_HEADROOM,
    );
}
