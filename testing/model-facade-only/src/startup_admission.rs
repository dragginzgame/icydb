//! Isolate generated startup grant failures in child processes, without runtime reset hooks.

use runtime_api::{
    self as icydb, ErrorCode,
    db::{StartupFailure, with_request_execution},
};

const CASE_ENV: &str = "ICYDB_FACADE_MEMORY_FAILURE_CASE";

pub(super) fn adjust_grants(grants: &mut Vec<ic_storage::MemoryAuthority>) {
    let Ok(case) = std::env::var(CASE_ENV) else {
        return;
    };
    if case == "missing_grant" {
        grants.clear();
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
            "missing_grant" => ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_INVALID,
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
        let wire = runtime_api::wire::encode_one(&failure).unwrap();
        assert_eq!(
            runtime_api::wire::decode_one::<StartupFailure>(&wire).unwrap(),
            failure
        );
        assert!(icydb::db::memory_runtime().is_none());
        return;
    }
    for case in ["missing_grant", "fresh_allowed"] {
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
                .all(|request| request.authority() == "icydb_facade_only")
        );
        assert!(icydb::db::memory_runtime().is_none());
        let mut composed = requests;
        let host_request = ic_storage::MemoryRequest::new("host", "host.control.v1").unwrap();
        let host_key = host_request.stable_key().clone();
        composed.push(host_request);
        let snapshot = ic_storage::SealedDeclarationSnapshot::new(composed).unwrap();
        let pool = ic_storage::AllocationGrants::new(vec![
            ic_storage::MemoryAuthority::new("icydb_facade_only", "icydb.facade_only.").unwrap(),
            ic_storage::MemoryAuthority::new("host", "host.").unwrap(),
        ])
        .unwrap();
        let mut runtime =
            ic_storage::MemoryRuntime::new(ic_storage::DefaultMemoryImpl::default()).unwrap();
        let committed = runtime
            .bootstrap(&snapshot, &pool, &ic_storage::GenericAllocationPolicy)
            .unwrap();
        let runtime = std::rc::Rc::new(runtime);
        icydb::db::bind_memory_runtime(std::rc::Rc::clone(&runtime)).unwrap();
        crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
        assert_eq!(
            crate::startup_state().unwrap(),
            icydb::db::DatabaseStartupState::Ready
        );
        assert!(with_request_execution(|| icydb::db!()).is_ok());
        assert_eq!(runtime.committed_generation().unwrap(), committed);
        assert!(runtime.open_memory(&host_key).is_ok());
    })
    .join()
    .unwrap();
}
