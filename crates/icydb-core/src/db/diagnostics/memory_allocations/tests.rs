use super::*;
use crate::{
    db::{
        DataStore, IndexStore, JournalTailStore, SchemaStore, StorageReport,
        data::{DecodedDataStoreKey, RawRow},
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
    },
    testing::test_memory_runtime,
    types::EntityTag,
};
use ic_memory::ic_stable_structures::VectorMemory;
use ic_memory::{MemoryManagerConfig, MemoryRuntime};

fn assert_accounting(report: &MemoryAllocations) {
    assert_eq!(report.memories.len(), 255);
    assert_eq!(
        report.physical_extent.bytes,
        report.manager_metadata_bytes + report.allocated_bucket_bytes + report.unmanaged_bytes
    );
    assert_eq!(
        report.allocated_bucket_bytes,
        report.virtual_extent.bytes + report.bucket_slack_bytes
    );
    assert_eq!(
        report.allocated_bucket_bytes,
        report.known_binding_bytes + report.unknown_binding_bytes
    );
    assert_eq!(
        report.allocated_bucket_bytes,
        report
            .memories
            .iter()
            .map(|slot| slot.allocated_bytes)
            .sum::<u64>()
    );
    assert!(
        report
            .memories
            .iter()
            .all(|slot| slot.payload_bytes.is_none())
    );
}

fn bucket_trial(pages: u16) -> (MemoryAllocations, MemoryAllocations) {
    let backing = VectorMemory::default();
    let runtime = test_memory_runtime(backing.clone(), MemoryManagerConfig::new(pages).unwrap());
    let open = |id| {
        runtime
            .open_memory(&format!("icydb.core_tests.slot_{id}.v1"), id)
            .unwrap()
    };
    let mut data = DataStore::init_journaled(open(100));
    let index = IndexStore::init_journaled(open(101));
    let schema = SchemaStore::init_journaled(open(102));
    let journal = JournalTailStore::init(open(103));
    let empty: MemoryAllocations = runtime.memory_allocations().unwrap().into();

    // Cross the 16-page bucket boundary with canonical row writes. This is a
    // storage/accounting fixture, not an end-to-end transaction cost model.
    for id in 0..2048 {
        let key = DecodedDataStoreKey::new(
            EntityTag::new(1),
            &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(id)),
        )
        .to_raw()
        .unwrap();
        data.fold_recovered_journal_put(key, RawRow::try_new(vec![7; 1024]).unwrap())
            .unwrap();
    }
    let before = backing.borrow().clone();
    let populated: MemoryAllocations = runtime.memory_allocations().unwrap().into();
    assert_eq!(*backing.borrow(), before);
    assert_eq!(populated.current_generation, empty.current_generation);
    assert_eq!(populated.bucket_size_pages, pages);
    assert_accounting(&empty);
    assert_accounting(&populated);

    // The current Candid shape preserves ledger, known and unknown rows along
    // with unavailable payload occupancy; the DTO is never allocation authority.
    let report = StorageReport {
        memory_allocations: Some(populated.clone()),
        ..StorageReport::default()
    };
    let bytes = candid::encode_one(report).unwrap();
    let decoded: StorageReport = candid::decode_one(&bytes).unwrap();
    assert_eq!(decoded.memory_allocations(), Some(&populated));
    assert!(matches!(
        populated.memories[0].binding,
        MemoryAllocationBinding::Ledger { .. }
    ));
    assert!(matches!(
        populated.memories[100].binding,
        MemoryAllocationBinding::Current { .. }
    ));

    drop((data, index, schema, journal, runtime));
    // Reopening without bootstrap has physical allocations but no current
    // application bindings. Preserve those bytes as unknown, not free.
    let reopened = MemoryRuntime::new(backing.clone()).unwrap();
    let unknown: MemoryAllocations = reopened.memory_allocations().unwrap().into();
    assert_accounting(&unknown);
    assert!(unknown.unknown_binding_bytes > 0);
    assert_eq!(unknown.physical_extent, populated.physical_extent);
    assert_eq!(unknown.bucket_size_pages, pages);
    assert!(matches!(
        unknown.memories[100].binding,
        MemoryAllocationBinding::Unknown
    ));
    drop(reopened);
    let other_pages = if pages == 16 { 128 } else { 16 };
    assert!(matches!(
        MemoryRuntime::new_with_config(
            backing.clone(),
            MemoryManagerConfig::new(other_pages).unwrap()
        ),
        Err(ic_memory::RuntimeConstructionError::BucketSizeMismatch { persisted, requested })
            if persisted == pages && requested == other_pages
    ));
    assert_eq!(*backing.borrow(), before);

    for (phase, report) in [("empty", &empty), ("populated", &populated)] {
        println!(
            "memory_bucket_trial pages={pages} phase={phase} physical_bytes={} virtual_bytes={} bucket_slack_bytes={} table_capacity_bytes={}",
            report.physical_extent.bytes,
            report.virtual_extent.bytes,
            report.bucket_slack_bytes,
            report.maximum_bucket_bytes
        );
    }
    (empty, populated)
}

#[test]
fn fresh_bucket_trial_preserves_accounting_and_current_candid_shape() {
    let (default_empty, default_populated) = bucket_trial(128);
    let (small_empty, small_populated) = bucket_trial(16);
    assert_eq!(default_empty.virtual_extent, small_empty.virtual_extent);
    assert_eq!(
        default_populated.virtual_extent,
        small_populated.virtual_extent
    );
    assert!(small_empty.physical_extent.bytes < default_empty.physical_extent.bytes);
    assert!(small_populated.physical_extent.bytes < default_populated.physical_extent.bytes);
    assert!(small_populated.memories[100].allocated_buckets > 1);
}

#[test]
fn absent_default_runtime_diagnostics_do_not_construct_memory() {
    assert!(collect_memory_allocations().unwrap().is_none());
    assert!(collect_memory_allocations().unwrap().is_none());
}
