use super::*;
use crate::testing::run_isolated;
use ic_storage::{
    AllocationGrants, GenericAllocationPolicy, MemoryAuthority, MemoryRequest, MemoryRuntime,
    RuntimeOpenError, SealedDeclarationSnapshot, StableKey, StorageConfig, VectorMemory,
};

fn declarations(keys: &[&str]) -> SealedDeclarationSnapshot {
    SealedDeclarationSnapshot::new(
        keys.iter()
            .map(|key| MemoryRequest::new("diagnostics", key).unwrap())
            .collect(),
    )
    .unwrap()
}

fn grants() -> AllocationGrants {
    AllocationGrants::new(vec![
        MemoryAuthority::new("diagnostics", "diagnostics.").unwrap(),
    ])
    .unwrap()
}

#[test]
fn directory_report_observes_logical_extents_without_acquiring_leases() {
    let mut runtime =
        MemoryRuntime::new_with_config(VectorMemory::default(), StorageConfig::default()).unwrap();
    runtime
        .bootstrap(
            &declarations(&["diagnostics.rows.v1", "diagnostics.index.v1"]),
            &grants(),
            &GenericAllocationPolicy,
        )
        .unwrap();
    let key = StableKey::parse("diagnostics.rows.v1").unwrap();
    let memory = runtime.open_memory(&key).unwrap();
    memory.grow(2).unwrap();
    let report = MemoryAllocations::from_runtime(&runtime).unwrap();
    assert_eq!(
        report.current_generation,
        runtime.committed_generation().unwrap()
    );
    assert_eq!(
        report.virtual_extent,
        MemoryExtent {
            wasm_pages: 2,
            bytes: 131_072
        }
    );
    assert_eq!(report.memories.len(), 2);
    let row = report
        .memories
        .iter()
        .find(|entry| entry.stable_key == key.as_str())
        .unwrap();
    assert_eq!(
        row.allocation_id,
        runtime.allocation_id(&key).unwrap().get()
    );
    assert_eq!(row.virtual_extent, report.virtual_extent);
    assert!(matches!(
        runtime.open_memory(&key),
        Err(RuntimeOpenError::AlreadyOpen { .. })
    ));
}

#[test]
fn retained_directory_labels_do_not_grant_opening_authority() {
    let backing = VectorMemory::default();
    let key = StableKey::parse("diagnostics.rows.v1").unwrap();
    let before = {
        let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
        runtime
            .bootstrap(
                &declarations(&[key.as_str()]),
                &grants(),
                &GenericAllocationPolicy,
            )
            .unwrap();
        runtime.open_memory(&key).unwrap().grow(1).unwrap();
        MemoryAllocations::from_runtime(&runtime).unwrap()
    };
    let runtime = MemoryRuntime::new(backing).unwrap();
    assert!(MemoryAllocations::from_runtime(&runtime).is_err());
    let retained = runtime.memory_allocations().unwrap();
    assert_eq!(retained.len(), before.memories.len());
    assert_eq!(retained[0].id.get(), before.memories[0].allocation_id);
    assert_eq!(
        retained[0].pages,
        before.memories[0].virtual_extent.wasm_pages
    );
    assert!(matches!(
        runtime.open_memory(&key),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
}

#[test]
fn unbound_host_has_no_directory_report() {
    run_isolated(|| assert!(collect_memory_allocations().unwrap().is_none()));
}
