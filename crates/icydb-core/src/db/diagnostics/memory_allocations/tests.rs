use super::*;
use crate::{
    db::{
        DataStore, IndexStore, JournalTailStore, SchemaStore, StorageReport,
        data::{DecodedDataStoreKey, RawRow},
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
    },
    testing::{run_isolated, test_memory_runtime},
    types::EntityTag,
};
use ic_memory::{Memory, MemoryManagerConfig, MemoryRuntime, VectorMemory};

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
}

// Numeric observers must retain detailed accounting without materializing ID rows.
fn assert_numeric_summary<M: ic_memory::Memory>(
    runtime: &MemoryRuntime<M>,
    detailed: &MemoryAllocations,
) {
    let summary = runtime.memory_allocation_summary().unwrap();
    assert_eq!(summary.current_generation, detailed.current_generation);
    assert_eq!(
        summary.physical_extent.bytes(),
        detailed.physical_extent.bytes
    );
    assert_eq!(
        summary.virtual_extent.bytes(),
        detailed.virtual_extent.bytes
    );
    assert_eq!(
        summary.allocated_bucket_bytes(),
        detailed.allocated_bucket_bytes
    );
    assert_eq!(summary.bucket_slack_bytes(), detailed.bucket_slack_bytes);
    assert_eq!(
        summary
            .unknown_binding
            .allocated_bytes(summary.bucket_size_pages),
        detailed.unknown_binding_bytes
    );
    assert_eq!(
        summary
            .current_binding
            .allocated_bytes(summary.bucket_size_pages)
            + summary
                .ledger_binding
                .allocated_bytes(summary.bucket_size_pages),
        detailed.known_binding_bytes
    );
    assert_eq!(
        summary
            .current_binding
            .bucket_slack_bytes(summary.bucket_size_pages)
            + summary
                .ledger_binding
                .bucket_slack_bytes(summary.bucket_size_pages)
            + summary
                .unknown_binding
                .bucket_slack_bytes(summary.bucket_size_pages),
        detailed.bucket_slack_bytes
    );
    assert_eq!(
        ic_memory::MemoryAllocationSummary::METADATA_BYTES_READ,
        detailed.metadata_bytes_read
    );
    assert!(detailed.metadata_bytes_read <= 34_848);
    assert_eq!(
        usize::from(ic_memory::MemoryAllocationSummary::MEMORIES_MEASURED),
        detailed.memories.len()
    );
}

fn bucket_trial(pages: u16) -> (MemoryAllocations, MemoryAllocations) {
    let backing = VectorMemory::default();
    let runtime = test_memory_runtime(backing.clone(), MemoryManagerConfig::new(pages).unwrap());
    let open = |id| {
        runtime
            .open_memory(&format!("icydb.core_tests.slot_{id:03}.v1"))
            .unwrap()
    };
    let mut data = DataStore::init_journaled(open(100));
    let index = IndexStore::init_journaled(open(101));
    let schema = SchemaStore::init_journaled(open(102));
    let journal = JournalTailStore::init(open(103));
    let empty: MemoryAllocations = MemoryAllocations::from_runtime(&runtime).unwrap();

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
    let populated: MemoryAllocations = MemoryAllocations::from_runtime(&runtime).unwrap();
    assert_eq!(*backing.borrow(), before);
    assert_eq!(populated.current_generation, empty.current_generation);
    assert_eq!(populated.bucket_size_pages, pages);
    assert_numeric_summary(&runtime, &populated);
    assert_accounting(&empty);
    assert_accounting(&populated);

    // The current Wire shape preserves ledger, known and unknown rows along
    // with unavailable payload occupancy; the DTO is never allocation authority.
    let report = StorageReport {
        memory_allocations: Some(populated.clone()),
        ..StorageReport::default()
    };
    let bytes = icydb_schema::wire::encode_one(report).unwrap();
    let decoded: StorageReport = icydb_schema::wire::decode_one(&bytes).unwrap();
    assert_eq!(decoded.memory_allocations(), Some(&populated));
    assert!(matches!(
        populated.memories[0].binding,
        MemoryAllocationBinding::Ledger { .. }
    ));
    assert_eq!(
        populated.memories[100].binding,
        MemoryAllocationBinding::Current {
            stable_key: "icydb.core_tests.slot_100.v1".to_string(),
            owner: "icydb.core-tests".to_string(),
        }
    );
    assert_eq!(populated.memories[100].pool_eligible, Some(true));
    assert_eq!(populated.memories[0].pool_eligible, Some(false));

    drop((data, index, schema, journal, runtime));
    // Reopening without bootstrap has physical allocations but no current
    // application bindings. Preserve those bytes as unknown, not free.
    let reopened = MemoryRuntime::new(backing.clone()).unwrap();
    let unknown: MemoryAllocations = MemoryAllocations::from_runtime(&reopened).unwrap();
    assert_numeric_summary(&reopened, &unknown);
    assert_accounting(&unknown);
    assert!(unknown.unknown_binding_bytes > 0);
    assert_eq!(unknown.physical_extent, populated.physical_extent);
    assert_eq!(unknown.bucket_size_pages, pages);
    assert!(matches!(
        unknown.memories[100].binding,
        MemoryAllocationBinding::Unknown
    ));
    assert_eq!(unknown.memories[100].pool_eligible, None);
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
fn fresh_bucket_trial_preserves_accounting_and_current_wire_shape() {
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

#[test]
fn bound_runtime_diagnostics_distinguish_corruption_from_absence() {
    run_isolated(|| {
        assert!(collect_memory_allocations().unwrap().is_none());
        let backing = VectorMemory::default();
        let runtime = test_memory_runtime(backing.clone(), MemoryManagerConfig::new(1).unwrap());
        crate::db::bind_memory_runtime(std::rc::Rc::new(runtime)).unwrap();
        assert!(collect_memory_allocations().unwrap().is_some());
        backing.write(0, b"BAD");
        assert!(collect_memory_allocations().is_err());
    });
}
