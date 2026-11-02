//! IcyDB role admission over Storage's current directory and namespace grants.

use ic_storage::{
    AdmissionError, AllocationGrants, BootstrapAdmission, MemoryAuthority, MemoryRequest,
    MemoryRuntime, PolicyIdentity, RuntimeBootstrapError, RuntimeBootstrapPolicy, RuntimeOpenError,
    SealedDeclarationSnapshot, StableKey, VectorMemory,
};
use icydb::db::{MemoryBootstrapAdmissionError, prepare_memory_bootstrap};

struct HostPolicy<'a>(&'a SealedDeclarationSnapshot);

impl RuntimeBootstrapPolicy for HostPolicy<'_> {
    type Error = MemoryBootstrapAdmissionError;

    fn validate_allocation(&self, _: &StableKey) -> Result<(), Self::Error> {
        Ok(())
    }
    fn prepare_bootstrap(&self, admission: &mut BootstrapAdmission<'_>) -> Result<(), Self::Error> {
        prepare_memory_bootstrap(self.0, admission)
    }
    fn runtime_bootstrap_identity(&self) -> PolicyIdentity {
        PolicyIdentity::new([64; 32])
    }
}

fn requests(namespace: &str, stores: &[&str]) -> Vec<MemoryRequest> {
    let owner = format!("icydb_{namespace}");
    let mut requests = ["commit.control", "startup.control", "integrity.progress"]
        .into_iter()
        .map(|role| MemoryRequest::new(&owner, &format!("icydb.{namespace}.{role}.v1")).unwrap())
        .collect::<Vec<_>>();
    for store in stores {
        for role in ["data", "index", "schema", "journal"] {
            requests.push(
                MemoryRequest::new(
                    &owner,
                    &format!("icydb.{namespace}.store.{store}.{role}.v1"),
                )
                .unwrap(),
            );
        }
    }
    requests
}

fn grants(namespaces: &[&str]) -> AllocationGrants {
    AllocationGrants::new(
        namespaces
            .iter()
            .map(|namespace| {
                MemoryAuthority::new(format!("icydb_{namespace}"), format!("icydb.{namespace}."))
                    .unwrap()
            })
            .collect(),
    )
    .unwrap()
}

fn bootstrap(
    runtime: &mut MemoryRuntime<VectorMemory>,
    requests: Vec<MemoryRequest>,
    grants: &AllocationGrants,
) -> Result<u64, RuntimeBootstrapError<MemoryBootstrapAdmissionError>> {
    let source = SealedDeclarationSnapshot::new(requests).unwrap();
    runtime.bootstrap(&source, grants, &HostPolicy(&source))
}

fn key(name: &str) -> StableKey {
    StableKey::parse(name).unwrap()
}

#[test]
fn same_contract_restart_preserves_ids_and_journal_bytes() {
    let backing = VectorMemory::default();
    let journal = key("icydb.main.store.original.journal.v1");
    let id = {
        let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
        bootstrap(
            &mut runtime,
            requests("main", &["original"]),
            &grants(&["main"]),
        )
        .unwrap();
        let memory = runtime.open_memory(&journal).unwrap();
        memory.grow(1).unwrap();
        memory.write(0, b"journal debt");
        runtime.allocation_id(&journal).unwrap()
    };
    let mut runtime = MemoryRuntime::new(backing).unwrap();
    bootstrap(
        &mut runtime,
        requests("main", &["replacement"]),
        &grants(&["main"]),
    )
    .unwrap();
    assert_eq!(runtime.allocation_id(&journal), Some(id));
    let memory = runtime.open_memory(&journal).unwrap();
    let mut bytes = [0; 12];
    memory.read(0, &mut bytes);
    assert_eq!(&bytes, b"journal debt");
    assert!(matches!(
        runtime.open_memory(&key("icydb.main.store.original.data.v1")),
        Err(RuntimeOpenError::NotAdmitted(_))
    ));
}

#[test]
fn incomplete_controls_and_store_roles_reject_without_committing() {
    for missing in ["commit.control.v1", "store.rows.index.v1"] {
        let backing = VectorMemory::default();
        let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
        let before = backing.borrow().clone();
        let declarations = requests("main", &["rows"])
            .into_iter()
            .filter(|request| !request.stable_key().as_str().ends_with(missing))
            .collect();
        assert!(matches!(
            bootstrap(&mut runtime, declarations, &grants(&["main"])),
            Err(RuntimeBootstrapError::Policy(
                MemoryBootstrapAdmissionError::IncompleteRoles { .. }
            ))
        ));
        assert_eq!(*backing.borrow(), before);
        assert!(!runtime.is_bootstrapped());
    }
}

#[test]
fn removed_historical_namespace_rejects_and_valid_retry_succeeds() {
    let backing = VectorMemory::default();
    {
        let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
        bootstrap(
            &mut runtime,
            requests("main", &["rows"]),
            &grants(&["main"]),
        )
        .unwrap();
    }
    let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
    let before = backing.borrow().clone();
    assert!(matches!(
        bootstrap(
            &mut runtime,
            requests("replacement", &[]),
            &grants(&["replacement"])
        ),
        Err(RuntimeBootstrapError::Policy(
            MemoryBootstrapAdmissionError::NamespaceRemoved(_)
        ))
    ));
    assert_eq!(*backing.borrow(), before);
    bootstrap(&mut runtime, requests("main", &[]), &grants(&["main"])).unwrap();
}

#[test]
fn invalid_role_and_namespace_authority_reject() {
    for invalid_role in [true, false] {
        let backing = VectorMemory::default();
        let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
        let before = backing.borrow().clone();
        let mut declarations = requests("main", &[]);
        let granted = if invalid_role {
            declarations.push(MemoryRequest::new("icydb_main", "icydb.main.unknown.v1").unwrap());
            grants(&["main"])
        } else {
            declarations[0] = MemoryRequest::new("wrong", "icydb.main.commit.control.v1").unwrap();
            AllocationGrants::new(vec![
                MemoryAuthority::new("wrong", "icydb.main.commit.").unwrap(),
                MemoryAuthority::new("icydb_main", "icydb.main.startup.").unwrap(),
                MemoryAuthority::new("icydb_main", "icydb.main.integrity.").unwrap(),
            ])
            .unwrap()
        };
        let error = bootstrap(&mut runtime, declarations, &granted).unwrap_err();
        if invalid_role {
            assert!(matches!(
                error,
                RuntimeBootstrapError::Policy(MemoryBootstrapAdmissionError::UnsupportedKey(_))
            ));
        } else {
            assert!(matches!(
                error,
                RuntimeBootstrapError::Policy(MemoryBootstrapAdmissionError::InvalidDeclaration(_))
            ));
        }
        assert_eq!(*backing.borrow(), before);
    }
}

#[test]
fn revoked_grant_cannot_admit_a_historical_journal() {
    let backing = VectorMemory::default();
    {
        let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
        bootstrap(&mut runtime, requests("main", &["old"]), &grants(&["main"])).unwrap();
    }
    let source = requests("main", &[]);
    let current_only = AllocationGrants::new(
        ["commit.", "startup.", "integrity."]
            .into_iter()
            .map(|role| MemoryAuthority::new("icydb_main", format!("icydb.main.{role}")).unwrap())
            .collect(),
    )
    .unwrap();
    let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
    let before = backing.borrow().clone();
    assert!(matches!(
        bootstrap(&mut runtime, source, &current_only),
        Err(
            RuntimeBootstrapError::Admission(AdmissionError::Unauthorized)
                | RuntimeBootstrapError::Policy(MemoryBootstrapAdmissionError::HistoricalJournal(
                    AdmissionError::Unauthorized
                ))
        )
    ));
    assert_eq!(*backing.borrow(), before);
}

#[test]
fn foreign_namespace_and_multiple_databases_share_one_host_directory() {
    let mut declarations = requests("one", &["rows"]);
    declarations.extend(requests("two", &[]));
    declarations.push(MemoryRequest::new("foreign", "foreign.control.v1").unwrap());
    let grants = AllocationGrants::new(vec![
        MemoryAuthority::new("icydb_one", "icydb.one.").unwrap(),
        MemoryAuthority::new("icydb_two", "icydb.two.").unwrap(),
        MemoryAuthority::new("foreign", "foreign.").unwrap(),
    ])
    .unwrap();
    let mut runtime = MemoryRuntime::new(VectorMemory::default()).unwrap();
    bootstrap(&mut runtime, declarations, &grants).unwrap();
    drop(runtime.open_memory(&key("foreign.control.v1")).unwrap());
    drop(
        runtime
            .open_memory(&key("icydb.two.commit.control.v1"))
            .unwrap(),
    );
}
