//! Isolate generated startup pool failures in child processes, without runtime reset hooks.

use runtime_api::{
    self as icydb, ErrorCode,
    db::{StartupFailure, with_request_execution},
};

const CASE_ENV: &str = "ICYDB_FACADE_MEMORY_FAILURE_CASE";

pub(super) fn extend_pool(
    grants: &mut Vec<ic_memory::MemoryAuthority>,
    exclusions: &mut Vec<ic_memory::MemoryManagerIdRange>,
) {
    let Ok(case) = std::env::var(CASE_ENV) else {
        return;
    };
    if case == "missing_grant" {
        grants.clear();
    }
    if case == "excluded_pool" {
        exclusions.push(ic_memory::MemoryManagerIdRange::new(10, 254).unwrap());
    }
}

#[test]
fn generated_memory_admission_errors_match_startup_and_db() {
    if let Ok(case) = std::env::var(CASE_ENV) {
        if case == "fresh_allowed" {
            crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
            assert_eq!(
                crate::startup_state().unwrap(),
                icydb::db::DatabaseStartupState::Ready
            );
            assert!(with_request_execution(|| icydb::db!()).is_ok());
            return;
        }
        let failure = crate::startup_state().unwrap_err();
        let code = match case.as_str() {
            "excluded_pool" | "missing_grant" => {
                ErrorCode::RUNTIME_BOUNDARY_MEMORY_ALLOCATION_RESOLUTION_FAILED
            }
            _ => panic!("unknown fixture case"),
        };
        assert_eq!(failure.error().code(), code);
        assert_eq!(
            failure.kind(),
            icydb::db::StartupFailureKind::DatabaseControl
        );
        for _ in 0..2 {
            let access = with_request_execution(|| icydb::db!()).err().unwrap();
            assert_eq!(&access, failure.error());
            assert_eq!(crate::startup_state().unwrap_err(), failure);
            assert_eq!(failure.diagnostic(), access.diagnostic());
            assert_eq!(failure.facts(), access.facts());
        }
        let wire = candid::encode_one(&failure).unwrap();
        assert_eq!(
            candid::decode_one::<StartupFailure>(&wire).unwrap(),
            failure
        );
        assert!(matches!(
            ic_memory::committed_allocations(),
            Err(ic_memory::RuntimeOpenError::NotBootstrapped)
        ));
        return;
    }
    for case in ["excluded_pool", "missing_grant", "fresh_allowed"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "startup_admission::generated_memory_admission_errors_match_startup_and_db",
                "--nocapture",
            ])
            .env(CASE_ENV, case)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{case}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn component_requests_are_complete_before_open_and_adopt_a_composed_host() {
    std::thread::spawn(|| {
        let requests = crate::icydb_memory_requests().unwrap();
        let mut keys: Vec<_> = requests
            .iter()
            .map(|request| request.stable_key().as_str())
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "icydb.facade_only.commit.control.v1",
                "icydb.facade_only.integrity.progress.v1",
                "icydb.facade_only.startup.control.v1",
                "icydb.facade_only.store.main.data.v1",
                "icydb.facade_only.store.main.index.v1",
                "icydb.facade_only.store.main.journal.v1",
                "icydb.facade_only.store.main.schema.v1",
            ]
        );
        assert!(
            requests
                .iter()
                .all(|request| request.authority() == "icydb.facade_only")
        );
        assert!(matches!(
            ic_memory::committed_allocations(),
            Err(ic_memory::RuntimeOpenError::NotBootstrapped)
        ));
        let mut composed = requests;
        composed.push(
            ic_memory::MemoryRequest::new(
                "host",
                "host.control.v1",
                ic_memory::SchemaMetadata::default(),
            )
            .unwrap(),
        );
        let snapshot = ic_memory::SealedDeclarationSnapshot::new(composed).unwrap();
        let pool = ic_memory::MemoryAllocationPool::new(
            vec![
                ic_memory::MemoryAuthority::new("icydb.facade_only", "icydb.facade_only.").unwrap(),
                ic_memory::MemoryAuthority::new("host", "host.").unwrap(),
            ],
            vec![],
        )
        .unwrap();
        let committed = ic_memory::bootstrap_default_memory_manager(&snapshot, &pool).unwrap();
        crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
        assert_eq!(
            crate::startup_state().unwrap(),
            icydb::db::DatabaseStartupState::Ready
        );
        assert!(with_request_execution(|| icydb::db!()).is_ok());
        assert_eq!(ic_memory::committed_allocations().unwrap(), committed);
        assert!(ic_memory::open_default_memory_manager_memory("host.control.v1").is_ok());
    })
    .join()
    .unwrap();
}
