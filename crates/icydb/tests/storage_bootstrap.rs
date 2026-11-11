//! Current Storage installation and host-adoption contracts through the facade.

use ic_storage::{
    AllocationGrants, GenericAllocationPolicy, Memory, MemoryAuthority, MemoryRequest,
    MemoryRuntime, RuntimeConstructionError, RuntimeOpenError, SealedDeclarationSnapshot,
    StableKey, StorageConfig, VectorMemory,
};
use icydb::db::{bind_memory_runtime, ensure_default_storage_runtime, memory_runtime};
use icydb_core::db::{StoreAllocationIdentity, open_memory};
use std::rc::Rc;

const OWNER: &str = "icydb_storage_test";
const PREFIX: &str = "icydb.storage_test.";
const KEY: &str = "icydb.storage_test.commit.control.v1";

fn requirements() -> SealedDeclarationSnapshot {
    SealedDeclarationSnapshot::new(
        ["commit.control", "startup.control", "integrity.progress"]
            .into_iter()
            .map(|role| MemoryRequest::new(OWNER, &format!("{PREFIX}{role}.v1")).unwrap())
            .collect(),
    )
    .unwrap()
}

fn grants() -> Result<AllocationGrants, ic_storage::AdmissionError> {
    AllocationGrants::new(vec![MemoryAuthority::new(OWNER, PREFIX)?])
}

#[test]
fn cold_bootstrap_and_warm_adoption_preserve_one_runtime() {
    std::thread::spawn(|| {
        assert!(matches!(
            open_memory(KEY),
            Err(RuntimeOpenError::NotBootstrapped)
        ));
        ensure_default_storage_runtime(OWNER, &requirements(), StorageConfig::default(), grants)
            .unwrap();
        let runtime = memory_runtime().unwrap();
        let before = runtime.memory_allocations().unwrap();
        ensure_default_storage_runtime(OWNER, &requirements(), StorageConfig::default(), || {
            panic!("warm adoption must not request new grants")
        })
        .unwrap();
        assert!(Rc::ptr_eq(&runtime, &memory_runtime().unwrap()));
        assert_eq!(runtime.memory_allocations().unwrap(), before);
        let memory = open_memory(KEY).unwrap();
        assert!(matches!(
            open_memory(KEY),
            Err(RuntimeOpenError::AlreadyOpen { .. })
        ));
        drop(memory);
        drop(open_memory(KEY).unwrap());
    })
    .join()
    .unwrap();
}

#[test]
fn existing_foreign_and_previous_format_images_are_rejected_unchanged() {
    for header in [
        b"MGR".as_slice(),
        b"BTR\x02",
        b"ICMEMORY",
        b"ICROOTR1",
        b"ICBUCK01",
        b"unknown",
    ] {
        let backing = VectorMemory::default();
        assert_eq!(backing.grow(1), 0);
        backing.write(0, header);
        let before = backing.borrow().clone();
        assert!(matches!(
            MemoryRuntime::new(backing.clone()),
            Err(RuntimeConstructionError::Directory(_))
        ));
        assert_eq!(*backing.borrow(), before);
    }
}

#[test]
fn allocation_identity_preserves_values_above_the_previous_slot_width() {
    std::thread::spawn(|| {
        let mut requests = (0..300)
            .map(|id| MemoryRequest::new("host", &format!("a.host.slot_{id:03}.v1")).unwrap())
            .collect::<Vec<_>>();
        requests.extend(requirements().requests().iter().cloned());
        let requests = SealedDeclarationSnapshot::new(requests).unwrap();
        let grants = AllocationGrants::new(vec![
            MemoryAuthority::new("host", "a.").unwrap(),
            MemoryAuthority::new(OWNER, PREFIX).unwrap(),
        ])
        .unwrap();
        let mut runtime = MemoryRuntime::new(VectorMemory::default()).unwrap();
        runtime
            .bootstrap(&requests, &grants, &GenericAllocationPolicy)
            .unwrap();
        let key = StableKey::parse(KEY).unwrap();
        let id = runtime.allocation_id(&key).unwrap().get();
        assert!(id > u64::from(u8::MAX));
        bind_memory_runtime(Rc::new(runtime)).unwrap();
        let identity = StoreAllocationIdentity::from_committed_key(KEY).unwrap();
        assert_eq!(identity.memory_id(), id);
        drop(open_memory(KEY).unwrap());
    })
    .join()
    .unwrap();
}

#[test]
fn current_backing_cold_reopen_preserves_bound_allocation_and_bytes() {
    std::thread::spawn(|| {
        let backing = VectorMemory::default();
        let key = StableKey::parse(KEY).unwrap();
        let allocation_id = {
            let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
            runtime
                .bootstrap(
                    &requirements(),
                    &grants().unwrap(),
                    &GenericAllocationPolicy,
                )
                .unwrap();
            let memory = runtime.open_memory(&key).unwrap();
            memory.grow(1).unwrap();
            memory.write(0, b"retained");
            runtime.allocation_id(&key).unwrap()
        };
        // Reopen the same-format backing after dropping every runtime and lease.
        let before = backing.borrow().clone();
        let mut reopened = MemoryRuntime::new(backing.clone()).unwrap();
        reopened
            .bootstrap(
                &requirements(),
                &grants().unwrap(),
                &GenericAllocationPolicy,
            )
            .unwrap();
        assert_eq!(reopened.allocation_id(&key).unwrap(), allocation_id);
        bind_memory_runtime(Rc::new(reopened)).unwrap();
        let memory = open_memory(KEY).unwrap();
        let mut bytes = [0; 8];
        memory.read(0, &mut bytes);
        assert_eq!(&bytes, b"retained");
        assert_eq!(*backing.borrow(), before);
    })
    .join()
    .unwrap();
}
