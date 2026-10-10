//! Rejected real bootstraps preserve their cause through the public startup boundary.

use super::*;
use crate::{Error, db::startup::__startup_bootstrap_failure};
use ic_memory::{
    MemoryAuthority, MemoryManagerIdRange, MemoryRuntime, SealedDeclarationSnapshot,
    ic_stable_structures::{Memory, VectorMemory},
};

const AUTHORITY: &str = "icydb.public_failure";
const CONTROLS: &[&str] = &["commit.control", "startup.control", "integrity.progress"];
const STORE: &[&str] = &[
    "store.main.data",
    "store.main.index",
    "store.main.schema",
    "store.main.journal",
];

fn pool(owner: &str, excluded: bool) -> MemoryAllocationPool {
    MemoryAllocationPool::new(
        vec![MemoryAuthority::new(owner, format!("{AUTHORITY}.")).unwrap()],
        if excluded {
            vec![MemoryManagerIdRange::new(10, 254).unwrap()]
        } else {
            vec![]
        },
    )
    .unwrap()
}

fn snapshot(owner: &str, roles: &[&str]) -> SealedDeclarationSnapshot {
    let requests: Vec<_> = roles
        .iter()
        .map(|role| {
            MemoryRequest::new(
                owner,
                &format!("{AUTHORITY}.{role}.v1"),
                SchemaMetadata::default(),
            )
            .unwrap()
        })
        .collect();
    SealedDeclarationSnapshot::new(&requests).unwrap()
}

fn reject_fresh(
    snapshot: &SealedDeclarationSnapshot,
    pool: &MemoryAllocationPool,
) -> DatabaseBootstrapError {
    let backing = VectorMemory::default();
    let mut runtime =
        MemoryRuntime::new_with_config(backing.clone(), MemoryManagerConfig::new(1).unwrap())
            .unwrap();
    let error = runtime
        .bootstrap(snapshot, pool, &DatabaseMemoryPolicy)
        .unwrap_err();
    assert!(matches!(
        runtime.committed_allocations(),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    assert!(matches!(
        runtime.open_memory(&format!("{AUTHORITY}.commit.control.v1")),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    assert!(backing.size() > 0);
    error.into()
}

fn reject_recovered(roles: &[&str], owner: &str) -> DatabaseBootstrapError {
    let backing = VectorMemory::default();
    let all: Vec<_> = CONTROLS.iter().chain(STORE).copied().collect();
    let mut original =
        MemoryRuntime::new_with_config(backing.clone(), MemoryManagerConfig::new(1).unwrap())
            .unwrap();
    original
        .bootstrap(
            &snapshot(AUTHORITY, &all),
            &pool(AUTHORITY, false),
            &DatabaseMemoryPolicy,
        )
        .unwrap();
    let before = backing.borrow().clone();
    drop(original);
    let mut runtime =
        MemoryRuntime::new_with_config(backing.clone(), MemoryManagerConfig::new(1).unwrap())
            .unwrap();
    let error = runtime
        .bootstrap(
            &snapshot(AUTHORITY, roles),
            &pool(owner, false),
            &DatabaseMemoryPolicy,
        )
        .unwrap_err();
    assert!(matches!(
        runtime.committed_allocations(),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    assert_eq!(*backing.borrow(), before);
    error.into()
}

fn snapshot_mismatch() -> DatabaseBootstrapError {
    let mut runtime = MemoryRuntime::new(VectorMemory::default()).unwrap();
    let declared = snapshot(AUTHORITY, CONTROLS);
    runtime
        .bootstrap(&declared, &pool(AUTHORITY, false), &DatabaseMemoryPolicy)
        .unwrap();
    let before = runtime.committed_allocations().unwrap().clone();
    let all: Vec<_> = CONTROLS.iter().chain(STORE).copied().collect();
    let error = runtime
        .bootstrap(
            &snapshot(AUTHORITY, &all),
            &pool(AUTHORITY, false),
            &DatabaseMemoryPolicy,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RuntimeBootstrapError::DeclarationSnapshotMismatch
    ));
    assert_eq!(runtime.committed_allocations().unwrap(), &before);
    error.into()
}

#[test]
fn memory_admission_causes_reach_public_startup_boundary() {
    let cases = [
        reject_fresh(&snapshot(AUTHORITY, CONTROLS), &pool(AUTHORITY, true)),
        reject_fresh(
            &snapshot(AUTHORITY, &CONTROLS[..1]),
            &pool(AUTHORITY, false),
        ),
        reject_fresh(
            &snapshot("wrong.authority", CONTROLS),
            &pool(AUTHORITY, false),
        ),
        reject_recovered(&[], AUTHORITY),
        reject_recovered(CONTROLS, "host.revoked"),
        snapshot_mismatch(),
    ];
    let codes = [
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MEMORY_ALLOCATION_RESOLUTION_FAILED,
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MEMORY_ALLOCATION_ROLES_INCOMPLETE,
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_INVALID,
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MEMORY_NAMESPACE_REMOVED,
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MEMORY_HISTORICAL_JOURNAL_UNAVAILABLE,
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_SNAPSHOT_MISMATCH,
    ];
    for (index, cause) in cases.into_iter().enumerate() {
        let DatabaseBootstrapError::Bootstrap(typed) = &cause else {
            panic!("bootstrap cause expected")
        };
        let expected_cause = matches!(
            (index, typed.as_ref()),
            (
                0,
                RuntimeBootstrapError::Resolution(
                    ic_memory::MemoryResolutionError::Exhausted { .. }
                ),
            ) | (
                1,
                RuntimeBootstrapError::AdmissionPolicy(
                    MemoryBootstrapAdmissionError::IncompleteRoles { .. },
                ),
            ) | (
                2,
                RuntimeBootstrapError::AdmissionPolicy(
                    MemoryBootstrapAdmissionError::InvalidDeclaration(_),
                ),
            ) | (
                3,
                RuntimeBootstrapError::AdmissionPolicy(
                    MemoryBootstrapAdmissionError::NamespaceRemoved(_),
                ),
            ) | (
                4,
                RuntimeBootstrapError::Admission(ic_memory::BootstrapAdmissionError::Pool(_)),
            ) | (5, RuntimeBootstrapError::DeclarationSnapshotMismatch)
        );
        assert!(expected_cause, "case {index}: {typed:?}");
        let direct = Error::from(cause.clone());
        let startup = __startup_bootstrap_failure(cause);
        assert_eq!(startup.error(), &direct);
        assert_eq!(direct.code(), codes[index]);
        assert_eq!(direct.origin(), crate::ErrorOrigin::Runtime);
        assert_eq!(direct.class(), codes[index].class());
        assert_eq!(
            icydb_diagnostic_code::validate_known_diagnostic_fact_schema(
                direct.code(),
                &direct.core_facts().unwrap()
            ),
            Ok(())
        );
        assert_eq!(startup.diagnostic(), direct.diagnostic());
        assert_eq!(startup.facts(), direct.facts());
        let bytes = candid::encode_one(&startup).unwrap();
        let decoded: crate::db::StartupFailure = candid::decode_one(&bytes).unwrap();
        assert_eq!(decoded, startup);
    }
}

#[test]
fn memory_admission_store_roles_reject_before_open() {
    let roles: Vec<_> = CONTROLS.iter().chain(&STORE[..1]).copied().collect();
    let error = reject_fresh(&snapshot(AUTHORITY, &roles), &pool(AUTHORITY, false));
    let public = Error::from(error.clone());
    assert_eq!(
        public.code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MEMORY_ALLOCATION_ROLES_INCOMPLETE
    );
    assert_eq!(
        public.core_facts().unwrap(),
        [(icydb_diagnostic_code::DiagnosticFactTag::ExpectedCount, 4)]
    );
    assert_eq!(__startup_bootstrap_failure(error).error(), &public);
}
