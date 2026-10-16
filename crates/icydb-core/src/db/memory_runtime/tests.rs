use crate::db::{MemoryRuntimeBindingError, bind_memory_runtime, memory_runtime};
use crate::testing::{run_isolated, test_memory_runtime};
use ic_memory::{MemoryManagerConfig, MemoryRuntime, ic_stable_structures::DefaultMemoryImpl};
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
