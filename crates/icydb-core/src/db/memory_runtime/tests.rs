use crate::db::{MemoryRuntimeBindingError, bind_memory_runtime, memory_runtime};
use crate::testing::{run_isolated, test_memory_runtime};
use ic_memory::{DefaultMemoryImpl, MemoryManagerConfig, MemoryRuntime};
use std::rc::Rc;

#[test]
fn binding_requires_bootstrap_and_preserves_one_runtime_identity() {
    run_isolated(|| {
        assert!(memory_runtime().is_none());
        let unbootstrapped = Rc::new(MemoryRuntime::new(DefaultMemoryImpl::default()).unwrap());
        assert_eq!(
            bind_memory_runtime(unbootstrapped),
            Err(MemoryRuntimeBindingError::NotBootstrapped)
        );
        assert!(memory_runtime().is_none());

        let runtime = Rc::new(test_memory_runtime(
            DefaultMemoryImpl::default(),
            MemoryManagerConfig::default(),
        ));
        bind_memory_runtime(Rc::clone(&runtime)).unwrap();
        bind_memory_runtime(Rc::clone(&runtime)).unwrap();
        assert!(Rc::ptr_eq(&memory_runtime().unwrap(), &runtime));

        let other = Rc::new(test_memory_runtime(
            DefaultMemoryImpl::default(),
            MemoryManagerConfig::default(),
        ));
        assert_eq!(
            bind_memory_runtime(other),
            Err(MemoryRuntimeBindingError::AlreadyBound)
        );
        assert!(Rc::ptr_eq(&memory_runtime().unwrap(), &runtime));
    });
}

#[test]
fn descriptor_admission_preserves_runtime_authority_and_exclusive_leases() {
    use crate::db::{StoreAllocationIdentity, open_memory};
    use ic_memory::RuntimeOpenError;

    run_isolated(|| {
        // Missing authority wins even when a manual descriptor is malformed.
        assert!(matches!(
            open_memory("INVALID"),
            Err(RuntimeOpenError::NotBootstrapped)
        ));
        let runtime = Rc::new(test_memory_runtime(
            DefaultMemoryImpl::default(),
            MemoryManagerConfig::default(),
        ));
        bind_memory_runtime(Rc::clone(&runtime)).unwrap();
        let before = runtime.memory_allocation_summary().unwrap();
        let committed = runtime.committed_allocations().unwrap().to_vec();
        for descriptor in ["INVALID", "icydb.core_tests.unknown.v1"] {
            assert!(matches!(
                open_memory(descriptor),
                Err(RuntimeOpenError::StableKeyNotCommitted(_))
            ));
            assert!(matches!(
                StoreAllocationIdentity::from_committed_key(descriptor),
                Err(RuntimeOpenError::StableKeyNotCommitted(_))
            ));
        }
        assert_eq!(runtime.memory_allocation_summary().unwrap(), before);
        assert_eq!(runtime.committed_allocations().unwrap(), &committed);
        let descriptor = "icydb.core_tests.slot_177.v1";
        let allocation = StoreAllocationIdentity::from_committed_key(descriptor).unwrap();
        let memory = open_memory(descriptor).unwrap();
        assert!(matches!(
            open_memory(descriptor),
            Err(RuntimeOpenError::AlreadyOpen { id }) if id == allocation.memory_id()
        ));
        drop(memory);
        drop(open_memory(descriptor).unwrap());
    });
}
