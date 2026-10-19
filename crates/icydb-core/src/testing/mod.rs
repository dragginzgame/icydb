//! Module: testing
//! Responsibility: shared crate-local test helpers and stable fixture constants.
//! Does not own: production runtime behavior or public testing APIs.
//! Boundary: internal-only support surface for `icydb-core` tests.

mod entity_tags;

use ic_memory::{
    DefaultMemoryImpl, Memory, MemoryAllocationPool, MemoryAuthority, MemoryManagerConfig,
    MemoryRequest, MemoryRuntime, RuntimeMemory, SealedDeclarationSnapshot, VectorMemory,
};
use std::sync::OnceLock;

pub(crate) use entity_tags::*;

pub(crate) const RESERVED_INTERNAL_MEMORY_ID: u8 = u8::MAX;

/// Test backing that refuses growth above a fixed physical extent.
///
/// Only injects backing exhaustion into IcyDB's frame-write admission tests;
/// allocation and collection behavior still belongs to Memory and Storage.
pub(crate) struct GrowthLimitedMemory {
    pub(crate) backing: VectorMemory,
    pub(crate) maximum_pages: u64,
}

impl Memory for GrowthLimitedMemory {
    fn grow(&self, pages: u64) -> i64 {
        if self
            .size()
            .checked_add(pages)
            .is_none_or(|total| total > self.maximum_pages)
        {
            return -1;
        }

        self.backing.grow(pages)
    }

    fn read(&self, offset: u64, dst: &mut [u8]) {
        self.backing.read(offset, dst);
    }

    fn size(&self) -> u64 {
        self.backing.size()
    }

    fn write(&self, offset: u64, src: &[u8]) {
        self.backing.write(offset, src);
    }
}

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

/// One fixture runtime that can reopen an allocation after its lease is dropped.
pub(crate) struct TestMemoryOwner {
    runtime: MemoryRuntime<DefaultMemoryImpl>,
    key: String,
}

impl TestMemoryOwner {
    /// Open the fixture's sole lease; an overlapping collection is a test error.
    pub(crate) fn open(&self) -> RuntimeMemory<DefaultMemoryImpl> {
        self.runtime
            .open_memory(&self.key)
            .expect("prior fixture lease must be released before reopening")
    }

    /// Observe virtual pages through runtime diagnostics without aliasing a tree.
    pub(crate) fn size(&self) -> u64 {
        let id = self
            .runtime
            .memory_id(&self.key)
            .expect("fixture key is committed");
        self.runtime
            .memory_allocations()
            .expect("fixture diagnostics should succeed")
            .memories[usize::from(id)]
        .virtual_extent
        .wasm_pages
    }
}

/// Retain the runtime owner when a fixture needs reopen or live page observations.
pub(crate) fn test_memory_owner(id: u8) -> TestMemoryOwner {
    TestMemoryOwner {
        runtime: test_memory_runtime(DefaultMemoryImpl::default(), MemoryManagerConfig::default()),
        key: format!("icydb.core_tests.slot_{:03}.v1", test_memory_id(id)),
    }
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
                )
                .expect("test request should validate")
            })
            .collect();
        SealedDeclarationSnapshot::new(requests).expect("test requests should seal")
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

/// Run a native fixture with fresh thread-local state and return its result.
///
/// Keep each fixture's schema and data setup in the caller. Forward the original
/// panic payload so a child assertion retains its diagnostic on the test thread.
///
/// # Panics
/// Propagates a fixture panic or failure to create its native thread.
pub(crate) fn run_isolated<T: Send + 'static>(fixture: impl FnOnce() -> T + Send + 'static) -> T {
    match std::thread::spawn(fixture).join() {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

#[cfg(test)]
mod isolation_tests {
    use crate::testing::run_isolated;
    use std::cell::Cell;

    thread_local! {
        static FIXTURE_VALUE: Cell<u32> = const { Cell::new(0) };
    }

    #[test]
    fn isolated_fixtures_preserve_thread_local_boundaries_and_return_values() {
        FIXTURE_VALUE.set(99);
        assert_eq!(
            run_isolated(|| {
                assert_eq!(FIXTURE_VALUE.get(), 0);
                FIXTURE_VALUE.set(1);
                7
            }),
            7
        );
        assert_eq!(FIXTURE_VALUE.get(), 99);
        run_isolated(|| assert_eq!(FIXTURE_VALUE.get(), 0));
        FIXTURE_VALUE.set(0);
    }

    #[test]
    fn isolated_fixture_preserves_the_original_panic_payload() {
        let failure = std::panic::catch_unwind(|| {
            run_isolated(|| std::panic::panic_any(17_u32));
        })
        .unwrap_err();
        assert_eq!(failure.downcast_ref::<u32>(), Some(&17));
    }
}
