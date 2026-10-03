//! Rejected real bootstraps preserve their cause through the public startup boundary.

use super::*;
use crate::{Error, db::startup::__startup_bootstrap_failure};
use ic_memory::{
    MemoryManagerAuthorityRecord, MemoryManagerIdRange, MemoryRuntime, SealedDeclarationSnapshot,
    StaticMemoryRangeDeclaration,
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

fn snapshot(
    owner: &str,
    range_owner: &str,
    roles: &[&str],
    mode: MemoryManagerRangeMode,
) -> SealedDeclarationSnapshot {
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
    let grant = StaticMemoryRangeDeclaration::new(
        MemoryManagerAuthorityRecord::new(
            MemoryManagerIdRange::new(100, 110).unwrap(),
            range_owner,
            mode,
            None,
        )
        .unwrap(),
    )
    .unwrap();
    SealedDeclarationSnapshot::new(&[], &[grant], &requests).unwrap()
}

fn reject_fresh(snapshot: &SealedDeclarationSnapshot) -> DatabaseBootstrapError {
    let backing = VectorMemory::default();
    let mut runtime =
        MemoryRuntime::new_with_config(backing.clone(), MemoryManagerConfig::new(1).unwrap())
            .unwrap();
    let error = runtime
        .bootstrap(snapshot, &DatabaseMemoryPolicy)
        .unwrap_err();
    assert!(matches!(
        runtime.committed_allocations(),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    assert!(matches!(
        runtime.open_memory_by_key(&format!("{AUTHORITY}.commit.control.v1")),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    // A rejection cannot publish allocation authority; no application payload exists.
    assert!(backing.size() > 0);
    error.into()
}

fn reject_recovered(roles: &[&str], range_owner: &str) -> DatabaseBootstrapError {
    let backing = VectorMemory::default();
    let all: Vec<_> = CONTROLS.iter().chain(STORE).copied().collect();
    let mut original =
        MemoryRuntime::new_with_config(backing.clone(), MemoryManagerConfig::new(1).unwrap())
            .unwrap();
    original
        .bootstrap(
            &snapshot(AUTHORITY, AUTHORITY, &all, MemoryManagerRangeMode::Allowed),
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
            &snapshot(
                AUTHORITY,
                range_owner,
                roles,
                MemoryManagerRangeMode::Allowed,
            ),
            &DatabaseMemoryPolicy,
        )
        .unwrap_err();
    assert!(matches!(
        runtime.committed_allocations(),
        Err(RuntimeOpenError::NotBootstrapped)
    ));
    assert_eq!(
        *backing.borrow(),
        before,
        "rejected recovered admission cannot commit memory changes"
    );
    error.into()
}

fn snapshot_mismatch() -> DatabaseBootstrapError {
    let mut runtime = MemoryRuntime::new(VectorMemory::default()).unwrap();
    let declared = snapshot(
        AUTHORITY,
        AUTHORITY,
        CONTROLS,
        MemoryManagerRangeMode::Allowed,
    );
    runtime.bootstrap(&declared, &DatabaseMemoryPolicy).unwrap();
    let before = runtime.committed_allocations().unwrap().clone();
    let changed = snapshot(
        AUTHORITY,
        AUTHORITY,
        CONTROLS,
        MemoryManagerRangeMode::Reserved,
    );
    let error = runtime
        .bootstrap(&changed, &DatabaseMemoryPolicy)
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
        reject_fresh(&snapshot(
            AUTHORITY,
            AUTHORITY,
            CONTROLS,
            MemoryManagerRangeMode::Reserved,
        )),
        reject_fresh(&snapshot(
            AUTHORITY,
            AUTHORITY,
            &CONTROLS[..1],
            MemoryManagerRangeMode::Allowed,
        )),
        reject_fresh(&snapshot(
            "wrong.authority",
            AUTHORITY,
            CONTROLS,
            MemoryManagerRangeMode::Allowed,
        )),
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
                RuntimeBootstrapError::Admission(ic_memory::BootstrapAdmissionError::Range { .. }),
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
fn memory_admission_store_roles_and_fixed_declarations_reject_before_open() {
    let roles: Vec<_> = CONTROLS.iter().chain(&STORE[..1]).copied().collect();
    let error = reject_fresh(&snapshot(
        AUTHORITY,
        AUTHORITY,
        &roles,
        MemoryManagerRangeMode::Allowed,
    ));
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

    let fixed = ic_memory::StaticMemoryDeclaration::new(
        AUTHORITY,
        ic_memory::AllocationDeclaration::memory_manager_unlabeled(
            format!("{AUTHORITY}.commit.control.v1"),
            100,
        )
        .unwrap(),
    )
    .unwrap();
    let grant = StaticMemoryRangeDeclaration::new(
        MemoryManagerAuthorityRecord::new(
            MemoryManagerIdRange::new(100, 110).unwrap(),
            AUTHORITY,
            MemoryManagerRangeMode::Allowed,
            None,
        )
        .unwrap(),
    )
    .unwrap();
    let declared = SealedDeclarationSnapshot::new(&[fixed], &[grant], &[]).unwrap();
    let error = reject_fresh(&declared);
    let public = Error::from(error.clone());
    assert_eq!(
        public.code(),
        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_INVALID
    );
    assert_eq!(__startup_bootstrap_failure(error).error(), &public);
}
