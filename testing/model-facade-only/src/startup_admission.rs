//! Isolate linked-registry failures in child test processes, without runtime reset hooks.

use runtime_api::{
    self as icydb, ErrorCode,
    db::{StartupFailure, with_request_execution},
};

// Exercise the re-export's actual omitted-mode default; do not alter its expansion.
icydb::ic_memory_range!(authority = "icydb.facade_probe", start = 200, end = 210);
icydb::ic_memory_range!(
    authority = "icydb.allowed_probe",
    start = 220,
    end = 230,
    mode = Allowed
);

const CASE_ENV: &str = "ICYDB_FACADE_MEMORY_FAILURE_CASE";

fn register_case(case: &str) {
    let namespace = match case {
        "missing_grant" => "ungranted_probe",
        "fresh_allowed" => "allowed_probe",
        _ => "facade_probe",
    };
    let owner = if case == "invalid_declaration" {
        "wrong.authority".into()
    } else {
        format!("icydb.{namespace}")
    };
    let controls = ["commit.control", "startup.control", "integrity.progress"];
    let roles = if case == "incomplete_roles" {
        &controls[..1]
    } else {
        &controls[..]
    };
    for role in roles {
        ic_memory::register_memory_request(
            ic_memory::MemoryRequest::new(
                &owner,
                &format!("icydb.{namespace}.{role}.v1"),
                ic_memory::SchemaMetadata::default(),
            )
            .unwrap(),
        )
        .unwrap();
    }
}

#[test]
fn generated_memory_admission_errors_match_startup_and_db() {
    if let Ok(case) = std::env::var(CASE_ENV) {
        register_case(&case);
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
            "reserved_default" | "missing_grant" => {
                ErrorCode::RUNTIME_BOUNDARY_MEMORY_ALLOCATION_RESOLUTION_FAILED
            }
            "incomplete_roles" => ErrorCode::RUNTIME_BOUNDARY_MEMORY_ALLOCATION_ROLES_INCOMPLETE,
            "invalid_declaration" => ErrorCode::RUNTIME_BOUNDARY_MEMORY_DECLARATION_INVALID,
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
    for case in [
        "reserved_default",
        "missing_grant",
        "incomplete_roles",
        "invalid_declaration",
        "fresh_allowed",
    ] {
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
