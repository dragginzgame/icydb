//! Archived production-runtime probe for 0.258 dependency readiness.

use ic_memory::{
    GenericRangePolicy, MemoryManagerAuthorityRecord, MemoryManagerIdRange,
    MemoryManagerRangeMode, MemoryRequest, MemoryRuntime, RuntimeOpenError, SchemaMetadata,
    SealedDeclarationSnapshot, StableKey, StaticMemoryRangeDeclaration,
    ic_stable_structures::{Memory, VectorMemory},
};

fn snapshot(owner: &str, keys: &[&str]) -> SealedDeclarationSnapshot {
    let grant = StaticMemoryRangeDeclaration::new(
        MemoryManagerAuthorityRecord::new(
            MemoryManagerIdRange::new(100, 110).unwrap(),
            owner,
            MemoryManagerRangeMode::Allowed,
            None,
        )
        .unwrap(),
    )
    .unwrap();
    let requests = keys
        .iter()
        .map(|key| MemoryRequest::new(owner, key, SchemaMetadata::default()).unwrap())
        .collect::<Vec<_>>();
    SealedDeclarationSnapshot::new(&[], &[grant], &requests).unwrap()
}

fn main() {
    const CONTROL: &str = "icydb.app.canister.commit.control.v1";
    const JOURNAL: &str = "icydb.app.store.removed.journal.v1";
    const NEW_CONTROL: &str = "icydb.typo.canister.commit.control.v1";
    let backing = VectorMemory::default();
    let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
    runtime
        .bootstrap(&snapshot("icydb.app", &[CONTROL, JOURNAL]), &GenericRangePolicy)
        .unwrap();
    let journal = runtime.open_memory_by_key(JOURNAL).unwrap();
    assert_eq!(journal.grow(1), 0);
    journal.write(0, b"debt");
    drop(journal);
    drop(runtime);

    let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
    runtime
        .bootstrap(&snapshot("icydb.app", &[CONTROL]), &GenericRangePolicy)
        .unwrap();
    assert!(matches!(
        runtime.open_memory_by_key(JOURNAL),
        Err(RuntimeOpenError::StableKeyNotCommitted(_))
    ));
    println!("PASS: an omitted journal cannot open after current declarations commit");
    drop(runtime);

    let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
    runtime
        .bootstrap(&snapshot("icydb.app", &[CONTROL, JOURNAL]), &GenericRangePolicy)
        .unwrap();
    let mut bytes = [0; 4];
    runtime.open_memory_by_key(JOURNAL).unwrap().read(0, &mut bytes);
    assert_eq!(&bytes, b"debt");
    println!("PASS: explicit pre-bootstrap journal declaration preserves the stored marker");
    drop(runtime);

    let mut runtime = MemoryRuntime::new(backing).unwrap();
    runtime
        .bootstrap(&snapshot("icydb.typo", &[NEW_CONTROL]), &GenericRangePolicy)
        .unwrap();
    let key = StableKey::parse(NEW_CONTROL).unwrap();
    let slot = runtime.committed_allocations().unwrap().slot_for(&key).unwrap();
    assert_eq!(slot.memory_manager_id().unwrap(), 102);
    assert_eq!(runtime.open_memory_by_key(NEW_CONTROL).unwrap().size(), 0);
    println!("PASS: a changed namespace with a new host grant receives fresh slot 102");
}
