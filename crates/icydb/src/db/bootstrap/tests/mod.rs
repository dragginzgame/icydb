mod public_failures;
use super::*;
use ic_storage::{GenericAllocationPolicy, MemoryAuthority, MemoryRequest, VectorMemory};

const OWNER: &str = "icydb_bootstrap_test";
const PREFIX: &str = "icydb.bootstrap_test.";

fn requirements() -> SealedDeclarationSnapshot {
    SealedDeclarationSnapshot::new(
        ["commit.control", "startup.control", "integrity.progress"]
            .into_iter()
            .map(|role| MemoryRequest::new(OWNER, &format!("{PREFIX}{role}.v1")).unwrap())
            .collect(),
    )
    .unwrap()
}

fn grants() -> Result<AllocationGrants, AdmissionError> {
    AllocationGrants::new(vec![MemoryAuthority::new(OWNER, PREFIX)?])
}

#[test]
fn adopts_runtime_bound_during_grants_callback() {
    std::thread::spawn(|| {
        ensure_default_storage_runtime(OWNER, &requirements(), StorageConfig::default(), || {
            ensure_default_storage_runtime(
                OWNER,
                &requirements(),
                StorageConfig::default(),
                grants,
            )
            .unwrap();
            grants()
        })
        .unwrap();
        assert_eq!(memory_runtime().unwrap().committed_generation().unwrap(), 1);
    })
    .join()
    .unwrap();
}

#[test]
fn adopts_host_policy_without_rebootstrap_or_regrant() {
    std::thread::spawn(|| {
        let mut runtime = MemoryRuntime::new(VectorMemory::default()).unwrap();
        runtime.bootstrap(&requirements(), &grants().unwrap(), &GenericAllocationPolicy).unwrap();
        let before = runtime.memory_allocations().unwrap();
        let runtime = Rc::new(runtime);
        bind_memory_runtime(runtime.clone()).unwrap();
        ensure_default_storage_runtime(OWNER, &requirements(), StorageConfig::default(), || panic!("warm adoption must not call provider")).unwrap();
        assert!(Rc::ptr_eq(&memory_runtime().unwrap(), &runtime));
        assert_eq!(runtime.memory_allocations().unwrap(), before);
        let error = ensure_default_storage_runtime("unknown", &requirements(), StorageConfig::default(), grants).unwrap_err();
        assert!(matches!(error, DatabaseBootstrapError::Adoption(cause) if matches!(cause.as_ref(), RuntimeOpenError::NotAdmitted(_))));
        assert_eq!(runtime.memory_allocations().unwrap(), before);
    }).join().unwrap();
}
