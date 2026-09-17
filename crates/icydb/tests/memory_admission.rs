//! Host-policy integration for logical allocations using production ledger recovery.
//! These tests do not substitute for generated database retirement/debt checks.

use std::cell::Cell;

use ic_memory::{
    AllocationDeclaration, AllocationPolicy, AllocationSlotDescriptor, BootstrapAdmission,
    BootstrapAdmissionError, GenericRangePolicy, MemoryManagerAuthorityRecord, MemoryManagerConfig,
    MemoryManagerIdRange, MemoryManagerRangeMode, MemoryRequest, MemoryResolutionError,
    MemoryRuntime, PolicyIdentity, PolicyIdentityError, RuntimeBootstrapError,
    RuntimeBootstrapPolicy, RuntimeConstructionError, RuntimeOpenError, SchemaMetadata,
    SealedDeclarationSnapshot, StableKey, StaticMemoryDeclaration, StaticMemoryRangeDeclaration,
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

fn grant(authority: &str, start: u8, end: u8) -> StaticMemoryRangeDeclaration {
    StaticMemoryRangeDeclaration::new(
        MemoryManagerAuthorityRecord::new(
            MemoryManagerIdRange::new(start, end).unwrap(),
            authority,
            MemoryManagerRangeMode::Allowed,
            None,
        )
        .unwrap(),
    )
    .unwrap()
}

fn grants() -> Vec<StaticMemoryRangeDeclaration> {
    vec![
        grant("icydb.main", 100, 139),
        grant("icydb.main_extra", 140, 179),
        grant("icydb.other", 180, 219),
        grant("foreign", 220, 239),
    ]
}

fn snapshot(requests: &[MemoryRequest]) -> SealedDeclarationSnapshot {
    SealedDeclarationSnapshot::new(&[], &grants(), requests).unwrap()
}

fn fixed(authority: &str, key: &str, id: u8) -> StaticMemoryDeclaration {
    StaticMemoryDeclaration::new(
        authority,
        AllocationDeclaration::new(
            key,
            AllocationSlotDescriptor::memory_manager(id).unwrap(),
            None,
            SchemaMetadata::default(),
        )
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn fresh_warm_reordered_and_extended_declarations_preserve_existing_assignments() {
    let backing = VectorMemory::default();
    let policy = HostPolicy::default();
    let original = snapshot(&requests("main", &["transfers"]));
    let mut first = runtime(&backing);
    let committed = first.bootstrap(&original, &policy).unwrap();
    let previous = committed.declarations().to_vec();
    let generation = committed.generation();
    let before = backing.borrow().clone();
    assert_eq!(
        first.bootstrap(&original, &policy).unwrap().generation(),
        generation
    );
    assert_eq!(policy.calls.get(), 1);
    assert_eq!(*backing.borrow(), before);
    drop(first);

    let mut reordered = requests("main", &["transfers", "accounts"]);
    reordered.extend(requests("other", &[]));
    reordered.reverse();
    let mut second = runtime(&backing);
    let committed = second.bootstrap(&snapshot(&reordered), &policy).unwrap();
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
        .bootstrap(&snapshot(&requests("main", &["old"])), &policy)
        .unwrap();
    let journal = first.open_memory_by_key(JOURNAL).unwrap();
    assert_eq!(journal.grow(1), 0);
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
        second.open_memory_by_key(JOURNAL),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    let committed = second
        .bootstrap(&snapshot(&requests("main", &["new"])), &policy)
        .unwrap();
    assert_eq!(committed.generation(), 2);
    assert_eq!(
        committed.slot_for(&StableKey::parse(JOURNAL).unwrap()),
        Some(&old_slot)
    );
    let mut debt = [0; 4];
    second
        .open_memory_by_key(JOURNAL)
        .unwrap()
        .read(0, &mut debt);
    assert_eq!(&debt, b"debt");
    assert_eq!(
        second
            .open_memory_by_key("icydb.main.store.new.data.v1")
            .unwrap()
            .size(),
        0
    );
    for role in ["data", "index", "schema"] {
        assert!(matches!(
            second.open_memory_by_key(&format!("icydb.main.store.old.{role}.v1")),
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
        .bootstrap(&snapshot(&original), &policy)
        .unwrap();

    let mut current = requests("main", &[]);
    current.extend(requests("main_extra", &[]));
    let mut recovered = runtime(&backing);
    recovered.bootstrap(&snapshot(&current), &policy).unwrap();
    for namespace in ["main", "main_extra"] {
        assert!(
            recovered
                .open_memory_by_key(&format!("icydb.{namespace}.store.old.journal.v1"))
                .is_ok()
        );
    }
    assert!(matches!(
        recovered.open_memory_by_key(FOREIGN),
        Err(RuntimeOpenError::StableKeyNotCommitted { .. })
    ));
}

#[test]
fn replacing_namespace_with_a_new_valid_grant_rejects_without_committing_and_can_retry() {
    let backing = VectorMemory::default();
    let policy = HostPolicy::default();
    let original = snapshot(&requests("main", &["transfers"]));
    let generation = runtime(&backing)
        .bootstrap(&original, &policy)
        .unwrap()
        .generation();
    let before = backing.borrow().clone();
    let mut recovered = runtime(&backing);
    assert!(matches!(
        recovered.bootstrap(&snapshot(&requests("other", &["transfers"])), &policy),
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
            .bootstrap(&original, &policy)
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
            runtime(&VectorMemory::default())
                .bootstrap(&snapshot(&current), &HostPolicy::default()),
            Err(RuntimeBootstrapError::AdmissionPolicy(
                MemoryBootstrapAdmissionError::IncompleteRoles { .. }
            ))
        ));
    }
    assert!(matches!(
        runtime(&VectorMemory::default())
            .bootstrap(&snapshot(&complete[3..]), &HostPolicy::default()),
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
            runtime(&VectorMemory::default())
                .bootstrap(&snapshot(&current), &HostPolicy::default()),
            Err(RuntimeBootstrapError::AdmissionPolicy(
                MemoryBootstrapAdmissionError::UnsupportedKey(_)
            ))
        ));

        let backing = VectorMemory::default();
        runtime(&backing)
            .bootstrap(&snapshot(&current), &GenericRangePolicy)
            .unwrap();
        let before = backing.borrow().clone();
        assert!(matches!(
            runtime(&backing).bootstrap(&snapshot(&requests("main", &[])), &HostPolicy::default()),
            Err(RuntimeBootstrapError::AdmissionPolicy(
                MemoryBootstrapAdmissionError::UnsupportedKey(_)
            ))
        ));
        assert_eq!(*backing.borrow(), before);
    }
}

#[test]
fn namespace_authority_and_logical_request_contract_coexist_with_foreign_fixed_claims() {
    let mut current = requests("main", &[]);
    current[0] = request("foreign", current[0].stable_key().as_str());
    assert!(matches!(
        runtime(&VectorMemory::default()).bootstrap(&snapshot(&current), &HostPolicy::default()),
        Err(RuntimeBootstrapError::AdmissionPolicy(
            MemoryBootstrapAdmissionError::InvalidDeclaration(_)
        ))
    ));

    let current = requests("main", &[]);
    let control = fixed("icydb.main", current[0].stable_key().as_str(), 100);
    let declarations =
        SealedDeclarationSnapshot::new(&[control], &grants(), &current[1..]).unwrap();
    assert!(matches!(
        runtime(&VectorMemory::default()).bootstrap(&declarations, &HostPolicy::default()),
        Err(RuntimeBootstrapError::AdmissionPolicy(
            MemoryBootstrapAdmissionError::InvalidDeclaration(_)
        ))
    ));

    let foreign = fixed("foreign", "foreign.control.v1", 230);
    let declarations = SealedDeclarationSnapshot::new(&[foreign], &grants(), &current).unwrap();
    assert!(
        runtime(&VectorMemory::default())
            .bootstrap(&declarations, &HostPolicy::default())
            .is_ok()
    );
}

#[test]
fn revoked_historical_journal_grant_is_not_bypassed() {
    let backing = VectorMemory::default();
    let policy = HostPolicy::default();
    runtime(&backing)
        .bootstrap(&snapshot(&requests("main", &["old"])), &policy)
        .unwrap();
    let before = backing.borrow().clone();
    let revoked = SealedDeclarationSnapshot::new(
        &[],
        &[grant("icydb.main", 130, 139)],
        &requests("main", &[]),
    )
    .unwrap();
    assert!(matches!(
        runtime(&backing).bootstrap(&revoked, &policy),
        Err(RuntimeBootstrapError::Admission(
            BootstrapAdmissionError::Range { .. }
        ))
    ));
    assert_eq!(*backing.borrow(), before);
}

#[test]
fn another_host_participant_cannot_replace_original_namespace_declarations() {
    let backing = VectorMemory::default();
    runtime(&backing)
        .bootstrap(&snapshot(&requests("main", &[])), &HostPolicy::default())
        .unwrap();
    let before = backing.borrow().clone();
    let policy = HostPolicy {
        select_historical_controls: true,
        ..HostPolicy::default()
    };
    assert!(matches!(
        runtime(&backing).bootstrap(&snapshot(&requests("other", &[])), &policy),
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
    let original = SealedDeclarationSnapshot::new(
        &[],
        &[grant("icydb.main", 100, 106)],
        &requests("main", &["first"]),
    )
    .unwrap();
    let previous = runtime(&backing)
        .bootstrap(&original, &policy)
        .unwrap()
        .declarations()
        .to_vec();
    let before = backing.borrow().clone();
    let current = requests("main", &["first", "second"]);
    let cramped =
        SealedDeclarationSnapshot::new(&[], &[grant("icydb.main", 100, 106)], &current).unwrap();
    let mut recovered = runtime(&backing);
    assert!(matches!(
        recovered.bootstrap(&cramped, &policy),
        Err(RuntimeBootstrapError::Resolution(
            MemoryResolutionError::Exhausted { .. }
        ))
    ));
    assert!(matches!(
        recovered.committed_allocations(),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    assert_eq!(*backing.borrow(), before);
    let expanded =
        SealedDeclarationSnapshot::new(&[], &[grant("icydb.main", 100, 110)], &current).unwrap();
    let committed = recovered.bootstrap(&expanded, &policy).unwrap();
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
        .bootstrap(&snapshot(&requests("main", &["old"])), &policy)
        .unwrap();
    let current = snapshot(&requests("main", &[]));
    let mut host = runtime(&backing);
    let generation = host
        .bootstrap(&current, &GenericRangePolicy)
        .unwrap()
        .generation();
    let before = backing.borrow().clone();
    let calls = policy.calls.get();
    assert!(matches!(
        host.bootstrap(&current, &policy),
        Err(RuntimeBootstrapError::PolicyIdentityMismatch { .. })
    ));
    assert_eq!(policy.calls.get(), calls);
    assert_eq!(
        host.committed_allocations().unwrap().generation(),
        generation
    );
    assert!(matches!(
        host.open_memory_by_key(JOURNAL),
        Err(RuntimeOpenError::StableKeyNotCommitted(_))
    ));
    assert_eq!(*backing.borrow(), before);
}

#[test]
fn persisted_bucket_profile_mismatch_rejects_without_resizing() {
    let backing = VectorMemory::default();
    runtime(&backing)
        .bootstrap(&snapshot(&requests("main", &[])), &HostPolicy::default())
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
