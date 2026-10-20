//! Isolate generated startup pool failures in child processes, without runtime reset hooks.

use runtime_api::{
    self as icydb, ErrorCode,
    db::{StartupFailure, with_request_execution},
};

const CASE_ENV: &str = "ICYDB_FACADE_MEMORY_FAILURE_CASE";

pub(super) fn extend_pool(grants: &mut Vec<ic_memory::MemoryAuthority>, exclusions: &mut Vec<u8>) {
    let Ok(case) = std::env::var(CASE_ENV) else {
        return;
    };
    if case == "missing_grant" {
        grants.clear();
    }
    if case == "excluded_pool" {
        exclusions.extend(10..=254);
    }
}

#[test]
fn generated_memory_admission_errors_match_startup_and_db() {
    if let Ok(case) = std::env::var(CASE_ENV) {
        if case == "fresh_allowed" {
            crate::bootstrap_standalone_memory(crate::icydb_memory_pool).unwrap();
            crate::__icydb_generated::__drive_native_database_for_tests().unwrap();
            assert_eq!(
                crate::startup_state().unwrap(),
                icydb::db::DatabaseStartupState::Ready
            );
            assert!(with_request_execution(|| icydb::db!()).is_ok());
            return;
        }
        let cause = crate::bootstrap_standalone_memory(crate::icydb_memory_pool).unwrap_err();
        let failure = icydb::db::__startup_bootstrap_failure(cause);
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
        // Failed standalone bootstrap publishes no authority. Database access
        // independently refuses the missing host binding without retrying bootstrap.
        assert!(with_request_execution(|| icydb::db!()).is_err());
        assert!(crate::startup_state().is_err());
        let wire = runtime_api::wire::encode_one(&failure).unwrap();
        assert_eq!(
            runtime_api::wire::decode_one::<StartupFailure>(&wire).unwrap(),
            failure
        );
        assert!(icydb::db::memory_runtime().is_none());
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
        assert!(icydb::db::memory_runtime().is_none());
        let mut composed = requests;
        composed.push(ic_memory::MemoryRequest::new("host", "host.control.v1").unwrap());
        let snapshot = ic_memory::SealedDeclarationSnapshot::new(composed).unwrap();
        let pool = ic_memory::MemoryAllocationPool::new(
            vec![
                ic_memory::MemoryAuthority::new("icydb.facade_only", "icydb.facade_only.").unwrap(),
                ic_memory::MemoryAuthority::new("host", "host.").unwrap(),
            ],
            vec![],
        )
        .unwrap();
        let mut runtime =
            ic_memory::MemoryRuntime::new(ic_memory::DefaultMemoryImpl::default()).unwrap();
        let committed = runtime
            .bootstrap(&snapshot, &pool, &ic_memory::GenericAllocationPolicy)
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
        assert!(runtime.open_memory("host.control.v1").is_ok());
    })
    .join()
    .unwrap();
}
