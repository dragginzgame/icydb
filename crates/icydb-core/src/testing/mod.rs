//! Module: testing
//! Responsibility: shared crate-local test helpers and stable fixture constants.
//! Does not own: production runtime behavior or public testing APIs.
//! Boundary: internal-only support surface for `icydb-core` tests.

mod entity_tags;

use ic_memory::ic_stable_structures::{DefaultMemoryImpl, Memory};
use ic_memory::{
    MemoryAllocationPool, MemoryAuthority, MemoryManagerConfig, MemoryRequest, MemoryRuntime,
    RuntimeMemory, SchemaMetadata, SealedDeclarationSnapshot,
};
use std::sync::OnceLock;

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
        .open_memory(&format!("icydb.core_tests.slot_{id:03}.v1"))
        .expect("test memory should open")
}

/// Bootstrap an isolated fixture runtime with an explicit bucket size.
pub(crate) fn test_memory_runtime<M: Memory>(
    backing: M,
    config: MemoryManagerConfig,
) -> MemoryRuntime<M> {
    static DECLARATIONS: OnceLock<SealedDeclarationSnapshot> = OnceLock::new();
    let declarations = DECLARATIONS.get_or_init(|| {
        let requests: Vec<_> = (10..=254)
            .map(|id| {
                MemoryRequest::new(
                    "icydb.core-tests",
                    &format!("icydb.core_tests.slot_{id:03}.v1"),
                    SchemaMetadata::default(),
                )
                .expect("test request should validate")
            })
            .collect();
        SealedDeclarationSnapshot::new(&requests).expect("test requests should seal")
    });
    let pool = MemoryAllocationPool::new(
        vec![MemoryAuthority::new("icydb.core-tests", "icydb.core_tests.").unwrap()],
        vec![],
    )
    .unwrap();
    let mut runtime =
        MemoryRuntime::new_with_config(backing, config).expect("test runtime should initialize");
    runtime
        .bootstrap(declarations, &pool, &ic_memory::GenericAllocationPolicy)
        .expect("test runtime should bootstrap");
    runtime
}
