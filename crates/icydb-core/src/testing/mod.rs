//! Module: testing
//! Responsibility: shared crate-local test helpers and stable fixture constants.
//! Does not own: production runtime behavior or public testing APIs.
//! Boundary: internal-only support surface for `icydb-core` tests.

mod entity_tags;

use ic_storage::{
    AllocationGrants, DefaultMemoryImpl, Memory, MemoryAuthority, MemoryRequest, MemoryRuntime,
    RuntimeMemory, SealedDeclarationSnapshot, StorageConfig, VectorMemory,
};
use std::sync::OnceLock;

pub(crate) use entity_tags::*;

/// Test backing that refuses growth above a fixed physical extent.
///
/// Only injects backing exhaustion into IcyDB's frame-write admission tests;
/// allocation and collection behavior still belongs to Memory and Storage.
pub(crate) struct GrowthLimitedMemory {
    pub(crate) backing: VectorMemory,
    pub(crate) maximum_pages: std::rc::Rc<std::cell::Cell<u64>>,
}

impl Memory for GrowthLimitedMemory {
    fn grow(&self, pages: u64) -> i64 {
        if self
            .size()
            .checked_add(pages)
            .is_none_or(|total| total > self.maximum_pages.get())
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

impl ic_storage::MemoryBacking for GrowthLimitedMemory {
    fn backing_identity(&self) -> ic_storage::BackingIdentity {
        ic_storage::MemoryBacking::backing_identity(&self.backing)
    }
}

/// One fixture runtime that can reopen an allocation after its lease is dropped.
pub(crate) struct TestMemoryOwner {
    runtime: MemoryRuntime<DefaultMemoryImpl>,
    key: ic_storage::StableKey,
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
        self.runtime
            .memory_allocations()
            .expect("fixture directory should validate")
            .into_iter()
            .find(|entry| entry.stable_key == self.key)
            .expect("fixture key is committed")
            .pages
    }
}

/// Retain the runtime owner when a fixture needs reopen or live page observations.
pub(crate) fn test_memory_owner(id: u64) -> TestMemoryOwner {
    TestMemoryOwner {
        runtime: test_memory_runtime(DefaultMemoryImpl::default(), StorageConfig::default()),
        key: ic_storage::StableKey::parse(format!("icydb.core_tests.slot_{id:03}.v1")).unwrap(),
    }
}

/// Check each store's real constructor preserves all bytes on invalid headers.
pub(crate) fn assert_retained_store_header_preserved<T>(
    open: impl Fn(RuntimeMemory<DefaultMemoryImpl>) -> Result<T, crate::error::InternalError>,
) {
    // The predecessor map framing must fail without resetting retained bytes.
    for header in [b"BAD".as_slice(), &[0, 0, 0], b"BTR\xff", b"ICMAP001"] {
        let owner = test_memory_owner(177);
        drop(open(owner.open()).unwrap());
        // Valid retained storage must still reopen through the same constructor.
        drop(open(owner.open()).unwrap());
        let memory = owner.open();
        memory.write(0, header);
        let pages = memory.size();
        let mut before = vec![0; usize::try_from(pages * 65_536).unwrap()];
        memory.read(0, &mut before);
        drop(memory);
        assert!(open(owner.open()).is_err());
        let memory = owner.open();
        let mut after = vec![0; before.len()];
        memory.read(0, &mut after);
        assert_eq!(after, before);
        assert_eq!(memory.size(), pages);
    }
}

/// Shared test-only stable memory allocation for in-memory stores.
pub(crate) fn test_memory(id: u64) -> RuntimeMemory<DefaultMemoryImpl> {
    test_memory_with_backing(id, DefaultMemoryImpl::default())
}

/// Open an isolated fixture through committed runtime authority, retaining the
/// caller's backing handle when a test needs to inspect physical growth.
pub(crate) fn test_memory_with_backing(
    id: u64,
    backing: DefaultMemoryImpl,
) -> RuntimeMemory<DefaultMemoryImpl> {
    test_memory_runtime(backing, StorageConfig::default())
        .open_memory(
            &ic_storage::StableKey::parse(format!("icydb.core_tests.slot_{id:03}.v1")).unwrap(),
        )
        .expect("test memory should open")
}

/// Bootstrap an isolated fixture runtime with an explicit storage limits.
pub(crate) fn test_memory_runtime<M: ic_storage::MemoryBacking>(
    backing: M,
    config: StorageConfig,
) -> MemoryRuntime<M> {
    static DECLARATIONS: OnceLock<SealedDeclarationSnapshot> = OnceLock::new();
    let declarations = DECLARATIONS.get_or_init(|| {
        let requests: Vec<_> = (1..=254)
            .map(|id| {
                MemoryRequest::new(
                    "icydb_core_tests",
                    &format!("icydb.core_tests.slot_{id:03}.v1"),
                )
                .expect("test request should validate")
            })
            .collect();
        SealedDeclarationSnapshot::new(requests).expect("test requests should seal")
    });
    let pool = AllocationGrants::new(vec![
        MemoryAuthority::new("icydb_core_tests", "icydb.core_tests.").unwrap(),
    ])
    .unwrap();
    let mut runtime =
        MemoryRuntime::new_with_config(backing, config).expect("test runtime should initialize");
    runtime
        .bootstrap(declarations, &pool, &ic_storage::GenericAllocationPolicy)
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
mod isolation_tests;

/// Distinguishes deliberate visitor cancellation from storage failures in fixtures.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum VisitorTestError {
    Callback(u8),
    Storage,
}

impl From<crate::error::InternalError> for VisitorTestError {
    fn from(_: crate::error::InternalError) -> Self {
        Self::Storage
    }
}
