//! Current admission failures survive the public startup and Wire boundaries.
use super::*;
use crate::{Error, db::startup::__startup_bootstrap_failure};
use icydb_diagnostic_code::{DiagnosticFactTag, ErrorCode};

#[test]
fn incomplete_roles_reject_without_writes_and_preserve_public_cause() {
    for roles in [
        vec!["commit.control"],
        vec![
            "commit.control",
            "startup.control",
            "integrity.progress",
            "store.main.data",
        ],
    ] {
        let requests = SealedDeclarationSnapshot::new(
            roles
                .iter()
                .map(|role| MemoryRequest::new(OWNER, &format!("{PREFIX}{role}.v1")).unwrap())
                .collect(),
        )
        .unwrap();
        let backing = VectorMemory::default();
        let mut runtime = MemoryRuntime::new(backing.clone()).unwrap();
        let before = backing.borrow().clone();
        let cause = runtime
            .bootstrap(
                &requests,
                &grants().unwrap(),
                &DatabaseMemoryPolicy(&requests),
            )
            .unwrap_err();
        assert!(matches!(
            &cause,
            RuntimeBootstrapError::Policy(MemoryBootstrapAdmissionError::IncompleteRoles { .. })
        ));
        assert_eq!(*backing.borrow(), before);
        let cause = DatabaseBootstrapError::from(cause);
        let direct = Error::from(cause.clone());
        let startup = __startup_bootstrap_failure(cause);
        assert_eq!(startup.error(), &direct);
        assert_eq!(
            direct.code(),
            ErrorCode::RUNTIME_BOUNDARY_MEMORY_ALLOCATION_ROLES_INCOMPLETE
        );
        assert_eq!(
            direct.core_facts().unwrap(),
            [(
                DiagnosticFactTag::ExpectedCount,
                if roles.len() == 1 { 3 } else { 4 }
            )]
        );
        let bytes = icydb_schema::wire::encode_one(&startup).unwrap();
        let decoded: crate::db::StartupFailure = icydb_schema::wire::decode_one(&bytes).unwrap();
        assert_eq!(decoded, startup);
    }
}
