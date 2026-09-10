//! Controller-only allocation reporting through the generated snapshot endpoint.

use candid::Principal;
use icydb::{
    Error, ErrorOrigin,
    db::{MemoryAllocationBinding, StorageReport},
    diagnostic::RuntimeBoundaryCode,
};
use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildProfile, build_fixture_canister_wasm_bytes_with_options,
    install_prebuilt_fixture_canister,
};

#[test]
fn generated_snapshot_reports_allocation_totals_without_mutation() {
    let wasm = build_fixture_canister_wasm_bytes_with_options(
        "sql",
        CanisterBuildOptions {
            build_profile: CanisterBuildProfile::Production,
            ..CanisterBuildOptions::default()
        },
    );
    let fixture = install_prebuilt_fixture_canister("sql", wasm);
    let before = fixture.pocket_ic().get_stable_memory(fixture.canister_id());
    let response: Result<StorageReport, Error> =
        fixture.query_candid("icydb_snapshot", ()).unwrap();
    let report = response.unwrap();
    let allocations = report.memory_allocations().unwrap();
    assert_eq!(allocations.bucket_size_pages, 128);
    assert_eq!(allocations.memories.len(), 255);
    assert_eq!(allocations.physical_extent.bytes, before.len() as u64);
    assert_eq!(
        allocations.physical_extent.bytes,
        allocations.manager_metadata_bytes
            + allocations.allocated_bucket_bytes
            + allocations.unmanaged_bytes
    );
    assert_eq!(
        allocations.allocated_bucket_bytes,
        allocations.virtual_extent.bytes + allocations.bucket_slack_bytes
    );
    assert!(matches!(
        allocations.memories[0].binding,
        MemoryAllocationBinding::Ledger { .. }
    ));
    assert!(allocations.memories.iter().any(|slot| matches!(
        slot.binding,
        MemoryAllocationBinding::Current { .. }
    ) && slot.allocated_bytes > 0));
    assert!(
        allocations
            .memories
            .iter()
            .all(|slot| slot.payload_bytes.is_none())
    );
    assert_eq!(allocations.metadata_bytes_read, 34_848);

    let repeated: Result<StorageReport, Error> =
        fixture.query_candid("icydb_snapshot", ()).unwrap();
    assert_eq!(repeated.unwrap().memory_allocations(), Some(allocations));
    let denied: Result<StorageReport, Error> = fixture
        .query_candid_as(
            Principal::self_authenticating([42; 32]),
            "icydb_snapshot",
            (),
        )
        .unwrap();
    assert_eq!(
        denied.expect_err("allocation diagnostics require the operational controller"),
        Error::from_runtime_boundary(
            RuntimeBoundaryCode::OperationalSurfaceControllerRequired,
            ErrorOrigin::Interface,
        ),
    );
    assert_eq!(
        fixture.pocket_ic().get_stable_memory(fixture.canister_id()),
        before
    );
}
