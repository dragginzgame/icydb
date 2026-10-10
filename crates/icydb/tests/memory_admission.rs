//! Host-policy integration for logical allocations using production ledger recovery.
//! These tests do not substitute for generated database retirement/debt checks.

use std::{cell::Cell, rc::Rc};

use ic_memory::{
    AllocationPolicy, BootstrapAdmission, BootstrapAdmissionError, GenericAllocationPolicy,
    MemoryAllocationPool, MemoryAuthority, MemoryManagerConfig, MemoryManagerIdRange,
    MemoryManagerSlot, MemoryRequest, MemoryResolutionError, MemoryRuntime, PolicyIdentity,
    PolicyIdentityError, RuntimeAdoptionError, RuntimeBootstrapError, RuntimeBootstrapPolicy,
    RuntimeConstructionError, RuntimeOpenError, SchemaMetadata, SealedDeclarationSnapshot,
    StableKey,
    ic_stable_structures::{Memory, VectorMemory},
};
use icydb::db::{MemoryBootstrapAdmissionError, prepare_memory_bootstrap};

#[derive(Default)]
struct HostPolicy {
    calls: Cell<usize>,
    select_historical_controls: bool,
}

impl AllocationPolicy for HostPolicy {
    type Error = MemoryBootstrapAdmissionError;

    fn validate_key(&self, _: &StableKey) -> Result<(), Self::Error> {
        Ok(())
    }

    fn validate_slot(&self, _: &StableKey, _: &MemoryManagerSlot) -> Result<(), Self::Error> {
        Ok(())
    }

    fn validate_reserved_slot(
        &self,
        _: &StableKey,
        _: &MemoryManagerSlot,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl RuntimeBootstrapPolicy for HostPolicy {
    fn prepare_bootstrap(&self, admission: &mut BootstrapAdmission<'_>) -> Result<(), Self::Error> {
        self.calls.set(self.calls.get() + 1);
        if self.select_historical_controls {
            // Simulate an earlier host participant selecting old controls.
            // Those selections must not replace original current declarations.
            for request in requests("main", &[]) {
                admission
                    .include_historical(request.authority(), request.stable_key().as_str())
                    .unwrap();
            }
        }
        prepare_memory_bootstrap(admission)
    }

    fn runtime_bootstrap_identity(&self) -> Result<PolicyIdentity, PolicyIdentityError> {
        let name = if self.select_historical_controls {
            "icydb.tests.select_controls_then_admit"
        } else {
            "icydb.tests.logical_admission"
        };
        PolicyIdentity::new(name, 1)
    }
}

fn runtime(backing: &VectorMemory) -> MemoryRuntime<VectorMemory> {
    MemoryRuntime::new_with_config(backing.clone(), MemoryManagerConfig::new(1).unwrap()).unwrap()
}

fn request(authority: &str, key: &str) -> MemoryRequest {
    MemoryRequest::new(authority, key, SchemaMetadata::default()).unwrap()
}

fn requests(namespace: &str, stores: &[&str]) -> Vec<MemoryRequest> {
    let authority = format!("icydb.{namespace}");
    let mut requests: Vec<_> = ["commit.control", "startup.control", "integrity.progress"]
        .map(|role| request(&authority, &format!("icydb.{namespace}.{role}.v1")))
        .into();
    for store in stores {
        for role in ["data", "index", "schema", "journal"] {
            requests.push(request(
                &authority,
                &format!("icydb.{namespace}.store.{store}.{role}.v1"),
            ));
        }
    }
    requests
}

fn pool() -> MemoryAllocationPool {
    MemoryAllocationPool::new(
        ["icydb.main", "icydb.main_extra", "icydb.other", "foreign"]
            .into_iter()
            .map(|owner| MemoryAuthority::new(owner, format!("{owner}.")).unwrap())
            .collect(),
        vec![],
    )
    .unwrap()
}

fn snapshot(requests: &[MemoryRequest]) -> SealedDeclarationSnapshot {
    SealedDeclarationSnapshot::new(requests).unwrap()
}

// Refuse capacity before growing the backing, then permit retry on the same runtime.
#[derive(Clone, Default)]
struct LimitedMemory {
    bytes: VectorMemory,
    limit: Rc<Cell<u64>>,
}

impl Memory for LimitedMemory {
    fn size(&self) -> u64 {
        self.bytes.size()
    }

    fn grow(&self, pages: u64) -> i64 {
        if self
            .size()
            .checked_add(pages)
            .is_none_or(|end| end > self.limit.get())
        {
            return -1;
        }
        self.bytes.grow(pages)
    }

    fn read(&self, offset: u64, dst: &mut [u8]) {
        self.bytes.read(offset, dst);
    }

    fn write(&self, offset: u64, src: &[u8]) {
        self.bytes.write(offset, src);
    }
}

#[test]
fn typed_ledger_and_application_growth_refusal_preserve_authority_and_retry() {
    use ic_memory::RuntimeGrowError;
    use icydb::db::DatabaseBootstrapError;

    let backing = LimitedMemory::default();
    backing.limit.set(1);
    let mut runtime =
        MemoryRuntime::new_with_config(backing.clone(), MemoryManagerConfig::new(1).unwrap())
            .unwrap();
    let declarations = snapshot(&requests("main", &["transfers"]));
    let policy = HostPolicy::default();
    let before = backing.bytes.borrow().clone();
    let error = runtime
        .bootstrap(&declarations, &pool(), &policy)
        .unwrap_err();
    assert!(matches!(
        &error,
        RuntimeBootstrapError::LedgerGrowth(RuntimeGrowError::BackingRefused { .. })
    ));
    let facade_error = DatabaseBootstrapError::from(error);
    assert!(
        matches!(facade_error, DatabaseBootstrapError::Bootstrap(cause)
        if matches!(cause.as_ref(), RuntimeBootstrapError::LedgerGrowth(
            RuntimeGrowError::BackingRefused { .. })))
    );
    assert!(!runtime.is_bootstrapped());
    assert!(matches!(
        runtime.open_memory("icydb.main.store.transfers.data.v1"),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    assert_eq!(*backing.bytes.borrow(), before);

    backing.limit.set(32);
    let committed = runtime
        .bootstrap(&declarations, &pool(), &policy)
        .unwrap()
        .clone();
    assert_eq!(committed.generation(), 1);
    let memory = runtime
        .open_memory("icydb.main.store.transfers.data.v1")
        .unwrap();
    let before = backing.bytes.borrow().clone();
    let summary = runtime.memory_allocation_summary().unwrap();
    backing.limit.set(backing.size());
    assert!(matches!(
        memory.grow(1),
        Err(RuntimeGrowError::BackingRefused { .. })
    ));
    assert_eq!(memory.size(), 0);
    assert_eq!(*backing.bytes.borrow(), before);
    assert_eq!(runtime.memory_allocation_summary().unwrap(), summary);
    assert_eq!(runtime.committed_allocations().unwrap(), &committed);

    backing.limit.set(32);
    let retry_memory = memory.clone();
    assert_eq!(retry_memory.grow(1), Ok(0));
    assert_eq!(memory.size(), 1);
    retry_memory.write(0, b"retained");
    let before = backing.bytes.borrow().clone();
    assert!(matches!(
        memory.grow(u64::from(summary.bucket_capacity)),
        Err(RuntimeGrowError::BucketExhausted { .. })
    ));
    assert_eq!(
        memory.grow(u64::MAX),
        Err(RuntimeGrowError::ArithmeticOverflow)
    );
    assert_eq!(*backing.bytes.borrow(), before);
    let mut retained = [0; 8];
    memory.read(0, &mut retained);
    assert_eq!(&retained, b"retained");
}

#[test]
fn host_adoption_checks_authority_metadata_without_effects() {
    const KEY: &str = "icydb.main.commit.control.v1";
    let backing = VectorMemory::default();
    let mut runtime = runtime(&backing);
    let policy = HostPolicy::default();
    let declarations = snapshot(&requests("main", &[]));
    let committed = runtime
        .bootstrap(&declarations, &pool(), &policy)
        .unwrap()
        .clone();
    let before = backing.borrow().clone();
    let summary = runtime.memory_allocation_summary().unwrap();
    runtime
        .verify_authority(&declarations, "icydb.main")
        .unwrap();
    let foreign = snapshot(&[request("foreign", KEY)]);
    assert!(matches!(
        runtime.verify_authority(&foreign, "foreign"),
        Err(RuntimeAdoptionError::AuthorityMismatch { .. })
    ));
    let metadata =
        snapshot(&[
            MemoryRequest::new("icydb.main", KEY, SchemaMetadata::new(Some(1)).unwrap()).unwrap(),
        ]);
    assert!(matches!(
        runtime.verify_authority(&metadata, "icydb.main"),
        Err(RuntimeAdoptionError::DeclarationMetadataMismatch { .. })
    ));
    assert!(matches!(
        runtime.verify_authority(&declarations, "absent"),
        Err(RuntimeAdoptionError::UnknownAuthority { .. })
    ));
    assert_eq!(policy.calls.get(), 1);
    assert_eq!(*backing.borrow(), before);
    assert_eq!(runtime.memory_allocation_summary().unwrap(), summary);
    assert_eq!(runtime.committed_allocations().unwrap(), &committed);
}

#[test]
fn fresh_warm_reordered_and_extended_declarations_preserve_existing_assignments() {
    let backing = VectorMemory::default();
    let policy = HostPolicy::default();
    let original = snapshot(&requests("main", &["transfers"]));
    let mut first = runtime(&backing);
    let committed = first.bootstrap(&original, &pool(), &policy).unwrap();
    let previous = committed.declarations().to_vec();
    let generation = committed.generation();
    let before = backing.borrow().clone();
    assert_eq!(
        first
            .bootstrap(&original, &pool(), &policy)
            .unwrap()
            .generation(),
        generation
    );
    assert_eq!(policy.calls.get(), 1);
    assert_eq!(*backing.borrow(), before);
    drop(first);

    let mut reordered = requests("main", &["transfers", "accounts"]);
    reordered.extend(requests("other", &[]));
    reordered.reverse();
    let mut second = runtime(&backing);
    let committed = second
        .bootstrap(&snapshot(&reordered), &pool(), &policy)
        .unwrap();
    for allocation in &previous {
        assert_eq!(
            committed.slot_for(allocation.stable_key()),
            Some(allocation.slot())
        );
    }
    assert_eq!(policy.calls.get(), 2);
}

#[test]
fn store_replacement_selects_only_old_journal_and_preserves_its_debt_bytes() {
    const JOURNAL: &str = "icydb.main.store.old.journal.v1";
    let backing = VectorMemory::default();
    let policy = HostPolicy::default();
    let mut first = runtime(&backing);
    first
        .bootstrap(&snapshot(&requests("main", &["old"])), &pool(), &policy)
        .unwrap();
    let journal = first.open_memory(JOURNAL).unwrap();
    assert_eq!(journal.grow(1), Ok(0));
    journal.write(0, b"debt");
    let old_slot = first
        .committed_allocations()
        .unwrap()
        .slot_for(&StableKey::parse(JOURNAL).unwrap())
        .cloned()
        .unwrap();
    drop(journal);
    drop(first);

    let mut second = runtime(&backing);
    assert!(matches!(
        second.open_memory(JOURNAL),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    let committed = second
        .bootstrap(&snapshot(&requests("main", &["new"])), &pool(), &policy)
        .unwrap();
    assert_eq!(committed.generation(), 2);
    assert_eq!(
        committed.slot_for(&StableKey::parse(JOURNAL).unwrap()),
        Some(&old_slot)
    );
    let mut debt = [0; 4];
    second.open_memory(JOURNAL).unwrap().read(0, &mut debt);
    assert_eq!(&debt, b"debt");
    assert_eq!(
        second
            .open_memory("icydb.main.store.new.data.v1")
            .unwrap()
            .size(),
        0
    );
    for role in ["data", "index", "schema"] {
        assert!(matches!(
            second.open_memory(&format!("icydb.main.store.old.{role}.v1")),
            Err(RuntimeOpenError::StableKeyNotCommitted { .. })
        ));
    }
    // This succeeds only at the allocation boundary. The preserved debt must
    // still prevent database retirement in the later convergence owner.
}

#[test]
fn selection_covers_all_namespaces_without_opening_other_consumers_history() {
    const FOREIGN: &str = "foreign.main.store.old.journal.v1";
    let backing = VectorMemory::default();
    let policy = HostPolicy::default();
    let mut original = requests("main", &["old"]);
    original.extend(requests("main_extra", &["old"]));
    original.push(request("foreign", FOREIGN));
    runtime(&backing)
        .bootstrap(&snapshot(&original), &pool(), &policy)
        .unwrap();

    let mut current = requests("main", &[]);
    current.extend(requests("main_extra", &[]));
    let mut recovered = runtime(&backing);
    recovered
        .bootstrap(&snapshot(&current), &pool(), &policy)
        .unwrap();
    for namespace in ["main", "main_extra"] {
        assert!(
            recovered
                .open_memory(&format!("icydb.{namespace}.store.old.journal.v1"))
                .is_ok()
        );
    }
    assert!(matches!(
        recovered.open_memory(FOREIGN),
        Err(RuntimeOpenError::StableKeyNotCommitted { .. })
    ));
}

#[test]
fn replacing_namespace_with_a_new_valid_grant_rejects_without_committing_and_can_retry() {
    let backing = VectorMemory::default();
    let policy = HostPolicy::default();
    let original = snapshot(&requests("main", &["transfers"]));
    let generation = runtime(&backing)
        .bootstrap(&original, &pool(), &policy)
        .unwrap()
        .generation();
    let before = backing.borrow().clone();
    let mut recovered = runtime(&backing);
    assert!(matches!(
        recovered.bootstrap(&snapshot(&requests("other", &["transfers"])), &pool(), &policy),
        Err(RuntimeBootstrapError::AdmissionPolicy(MemoryBootstrapAdmissionError::NamespaceRemoved(namespace)))
            if namespace == "main"
    ));
    assert_eq!(*backing.borrow(), before);
    assert!(matches!(
        recovered.committed_allocations(),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    assert_eq!(
        recovered
            .bootstrap(&original, &pool(), &policy)
            .unwrap()
            .generation(),
        generation + 1
    );
}

#[test]
fn incomplete_current_controls_and_store_quartets_reject() {
    let complete = requests("main", &["transfers"]);
    for missing in 0..complete.len() {
        let mut current = complete.clone();
        current.remove(missing);
        assert!(matches!(
            runtime(&VectorMemory::default()).bootstrap(
                &snapshot(&current),
                &pool(),
                &HostPolicy::default()
            ),
            Err(RuntimeBootstrapError::AdmissionPolicy(
                MemoryBootstrapAdmissionError::IncompleteRoles { .. }
            ))
        ));
    }
    assert!(matches!(
        runtime(&VectorMemory::default()).bootstrap(
            &snapshot(&complete[3..]),
            &pool(),
            &HostPolicy::default()
        ),
        Err(RuntimeBootstrapError::AdmissionPolicy(
            MemoryBootstrapAdmissionError::IncompleteRoles { store: None, .. }
        ))
    ));
}

#[test]
fn unsupported_roles_reject_in_current_requests_and_recovered_history() {
    for key in [
        "icydb.main.store.transfers.audit.v1",
        "icydb.main.store.transfers.journal.extra.v1",
        "icydb.main.commit.progress.v1",
    ] {
        let mut current = requests("main", &[]);
        current.push(request("icydb.main", key));
        assert!(matches!(
            runtime(&VectorMemory::default()).bootstrap(
                &snapshot(&current),
                &pool(),
                &HostPolicy::default()
            ),
            Err(RuntimeBootstrapError::AdmissionPolicy(
                MemoryBootstrapAdmissionError::UnsupportedKey(_)
            ))
        ));

        let backing = VectorMemory::default();
        runtime(&backing)
            .bootstrap(&snapshot(&current), &pool(), &GenericAllocationPolicy)
            .unwrap();
        let before = backing.borrow().clone();
        assert!(matches!(
            runtime(&backing).bootstrap(
                &snapshot(&requests("main", &[])),
                &pool(),
                &HostPolicy::default()
            ),
            Err(RuntimeBootstrapError::AdmissionPolicy(
                MemoryBootstrapAdmissionError::UnsupportedKey(_)
            ))
        ));
        assert_eq!(*backing.borrow(), before);
    }
}

#[test]
fn namespace_authority_and_requests_coexist_with_foreign_claims() {
    let mut current = requests("main", &[]);
    current[0] = request("foreign", current[0].stable_key().as_str());
    assert!(matches!(
        runtime(&VectorMemory::default()).bootstrap(
            &snapshot(&current),
            &pool(),
            &HostPolicy::default()
        ),
        Err(RuntimeBootstrapError::AdmissionPolicy(
            MemoryBootstrapAdmissionError::InvalidDeclaration(_)
        ))
    ));
    let mut current = requests("main", &[]);
    current.push(request("foreign", "foreign.control.v1"));
    assert!(
        runtime(&VectorMemory::default())
            .bootstrap(&snapshot(&current), &pool(), &HostPolicy::default())
            .is_ok()
    );
}

#[test]
fn revoked_historical_journal_grant_is_not_bypassed() {
    let backing = VectorMemory::default();
    let policy = HostPolicy::default();
    let mut first = runtime(&backing);
    first
        .bootstrap(&snapshot(&requests("main", &["old"])), &pool(), &policy)
        .unwrap();
    let id = first.memory_id("icydb.main.store.old.journal.v1").unwrap();
    drop(first);
    let before = backing.borrow().clone();
    let revoked = MemoryAllocationPool::new(
        pool().authorities().to_vec(),
        vec![MemoryManagerIdRange::new(id, id).unwrap()],
    )
    .unwrap();
    assert!(matches!(
        runtime(&backing).bootstrap(&snapshot(&requests("main", &[])), &revoked, &policy),
        Err(RuntimeBootstrapError::Admission(
            BootstrapAdmissionError::Pool(_)
        ))
    ));
    assert_eq!(*backing.borrow(), before);
}

#[test]
fn another_host_participant_cannot_replace_original_namespace_declarations() {
    let backing = VectorMemory::default();
    runtime(&backing)
        .bootstrap(
            &snapshot(&requests("main", &[])),
            &pool(),
            &HostPolicy::default(),
        )
        .unwrap();
    let before = backing.borrow().clone();
    let policy = HostPolicy {
        select_historical_controls: true,
        ..HostPolicy::default()
    };
    assert!(matches!(
        runtime(&backing).bootstrap(&snapshot(&requests("other", &[])), &pool(), &policy),
        Err(RuntimeBootstrapError::AdmissionPolicy(
            MemoryBootstrapAdmissionError::NamespaceRemoved(namespace)
        )) if namespace == "main"
    ));
    assert_eq!(*backing.borrow(), before);
}

#[test]
fn exhausted_host_pool_preserves_ledger_and_expansion_preserves_existing_slots() {
    let backing = VectorMemory::default();
    let policy = HostPolicy::default();
    let original = snapshot(&requests("main", &["first"]));
    let cramped = MemoryAllocationPool::new(
        pool().authorities().to_vec(),
        vec![MemoryManagerIdRange::new(17, 254).unwrap()],
    )
    .unwrap();
    let previous = runtime(&backing)
        .bootstrap(&original, &cramped, &policy)
        .unwrap()
        .declarations()
        .to_vec();
    let before = backing.borrow().clone();
    let current = snapshot(&requests("main", &["first", "second"]));
    let mut recovered = runtime(&backing);
    assert!(matches!(
        recovered.bootstrap(&current, &cramped, &policy),
        Err(RuntimeBootstrapError::Resolution(
            MemoryResolutionError::Exhausted { .. }
        ))
    ));
    assert!(matches!(
        recovered.committed_allocations(),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    assert_eq!(*backing.borrow(), before);
    let committed = recovered.bootstrap(&current, &pool(), &policy).unwrap();
    for allocation in previous {
        assert_eq!(
            committed.slot_for(allocation.stable_key()),
            Some(allocation.slot())
        );
    }
}

#[test]
fn host_committed_without_historical_journal_cannot_be_repaired_by_warm_admission() {
    const JOURNAL: &str = "icydb.main.store.old.journal.v1";
    let backing = VectorMemory::default();
    let policy = HostPolicy::default();
    runtime(&backing)
        .bootstrap(&snapshot(&requests("main", &["old"])), &pool(), &policy)
        .unwrap();
    let current = snapshot(&requests("main", &[]));
    let mut host = runtime(&backing);
    let generation = host
        .bootstrap(&current, &pool(), &GenericAllocationPolicy)
        .unwrap()
        .generation();
    let before = backing.borrow().clone();
    let calls = policy.calls.get();
    assert!(matches!(
        host.bootstrap(&current, &pool(), &policy),
        Err(RuntimeBootstrapError::PolicyIdentityMismatch { .. })
    ));
    assert_eq!(policy.calls.get(), calls);
    assert_eq!(
        host.committed_allocations().unwrap().generation(),
        generation
    );
    assert!(matches!(
        host.open_memory(JOURNAL),
        Err(RuntimeOpenError::StableKeyNotCommitted(_))
    ));
    assert_eq!(*backing.borrow(), before);
}

#[test]
fn persisted_bucket_profile_mismatch_rejects_without_resizing() {
    let backing = VectorMemory::default();
    runtime(&backing)
        .bootstrap(
            &snapshot(&requests("main", &[])),
            &pool(),
            &HostPolicy::default(),
        )
        .unwrap();
    let before = backing.borrow().clone();
    assert!(matches!(
        MemoryRuntime::new_with_config(backing.clone(), MemoryManagerConfig::new(4).unwrap()),
        Err(RuntimeConstructionError::BucketSizeMismatch {
            persisted: 1,
            requested: 4
        })
    ));
    assert_eq!(*backing.borrow(), before);
}

#[test]
fn cold_reopen_with_a_wider_host_pool_preserves_keys_ids_and_payloads() {
    const KEY: &str = "icydb.main.store.transfers.journal.v1";
    let backing = VectorMemory::default();
    let policy = HostPolicy::default();
    let declarations = snapshot(&requests("main", &["transfers"]));
    let original_pool = MemoryAllocationPool::new(
        pool().authorities().to_vec(),
        vec![MemoryManagerIdRange::new(10, 99).unwrap()],
    )
    .unwrap();
    let mut original = runtime(&backing);
    let committed = original
        .bootstrap(&declarations, &original_pool, &policy)
        .unwrap()
        .clone();
    let original_id = original.memory_id(KEY).unwrap();
    assert!(original_id >= 100);
    let memory = original.open_memory(KEY).unwrap();
    memory.grow(1).unwrap();
    memory.write(0, b"retained-journal");
    drop(memory);
    drop(original);
    let mut reopened = runtime(&backing);
    let current = reopened.bootstrap(&declarations, &pool(), &policy).unwrap();
    for allocation in committed.declarations() {
        assert_eq!(
            current.slot_for(allocation.stable_key()),
            Some(allocation.slot())
        );
    }
    assert_eq!(reopened.memory_id(KEY).unwrap(), original_id);
    let mut bytes = [0; 16];
    reopened.open_memory(KEY).unwrap().read(0, &mut bytes);
    assert_eq!(&bytes, b"retained-journal");
}
