//! Module: testing
//! Responsibility: shared crate-local test helpers and stable fixture constants.
//! Does not own: production runtime behavior or public testing APIs.
//! Boundary: internal-only support surface for `icydb-core` tests.

mod entity_tags;

use ic_memory::ic_stable_structures::DefaultMemoryImpl;
use ic_memory::{
    AllocationPolicy, AllocationSlotDescriptor, MemoryManagerConfig, MemoryRuntime, PolicyIdentity,
    PolicyIdentityError, RuntimeBootstrapPolicy, RuntimeMemory, SealedDeclarationSnapshot,
    StableKey, register_static_memory_manager_declaration, sealed_declaration_snapshot,
};
use std::{convert::Infallible, sync::OnceLock};

pub(crate) use entity_tags::*;

pub(crate) const RESERVED_INTERNAL_MEMORY_ID: u8 = u8::MAX;

/// Return a validated test memory id.
///
/// Memory id `255` is reserved by stable-structures internals and must never
/// be used by application or test memory allocations.
#[must_use]
pub(crate) const fn test_memory_id(id: u8) -> u8 {
    assert!(
        id != RESERVED_INTERNAL_MEMORY_ID,
        "memory id 255 is reserved for stable-structures internals",
    );
    id
}

/// Shared test-only stable memory allocation for in-memory stores.
pub(crate) fn test_memory(id: u8) -> RuntimeMemory<DefaultMemoryImpl> {
    test_memory_with_backing(id, DefaultMemoryImpl::default())
}

/// Open an isolated fixture through committed runtime authority, retaining the
/// caller's backing handle when a test needs to inspect physical growth.
pub(crate) fn test_memory_with_backing(
    id: u8,
    backing: DefaultMemoryImpl,
) -> RuntimeMemory<DefaultMemoryImpl> {
    let id = test_memory_id(id);
    test_memory_runtime(backing, MemoryManagerConfig::default())
        .open_memory(&format!("icydb.core_tests.slot_{id}.v1"), id)
        .expect("test memory should open")
}

/// Bootstrap an isolated fixture runtime with an explicit bucket size.
pub(crate) fn test_memory_runtime(
    backing: DefaultMemoryImpl,
    config: MemoryManagerConfig,
) -> MemoryRuntime<DefaultMemoryImpl> {
    static DECLARATIONS: OnceLock<SealedDeclarationSnapshot> = OnceLock::new();
    let declarations = DECLARATIONS.get_or_init(|| {
        // Seal one shared fixture layout before parallel tests create their
        // independent runtimes. Only opened slots allocate store memory.
        for id in 10..=254 {
            register_static_memory_manager_declaration(
                id,
                "icydb.core-tests",
                "TestMemory",
                format!("icydb.core_tests.slot_{id}.v1"),
            )
            .expect("test allocation should register");
        }
        sealed_declaration_snapshot().expect("test declarations should seal")
    });
    let mut runtime =
        MemoryRuntime::new_with_config(backing, config).expect("test runtime should initialize");
    runtime
        .bootstrap(declarations, &TestMemoryPolicy)
        .expect("test runtime should bootstrap");
    runtime
}

struct TestMemoryPolicy;

impl AllocationPolicy for TestMemoryPolicy {
    type Error = Infallible;

    fn validate_key(&self, _: &StableKey) -> Result<(), Self::Error> {
        Ok(())
    }

    fn validate_slot(
        &self,
        _: &StableKey,
        _: &AllocationSlotDescriptor,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn validate_reserved_slot(
        &self,
        _: &StableKey,
        _: &AllocationSlotDescriptor,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl RuntimeBootstrapPolicy for TestMemoryPolicy {
    fn runtime_bootstrap_identity(&self) -> Result<PolicyIdentity, PolicyIdentityError> {
        PolicyIdentity::new("icydb.core-tests", 1)
    }
}
