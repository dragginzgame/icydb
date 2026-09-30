//! Admission replaces boot keys before scalar or grouped cursors can be used.

use super::*;
use crate::{
    db::{
        commit::{cursor_authentication_key, forget_recovered_domain_for_tests},
        count,
        database_format::{
            DatabaseFormatObservation, forget_database_format_admission_for_tests,
            observe_database_format, set_boot_entropy_for_tests,
        },
    },
    error::InternalError,
};
use icydb_diagnostic_code::ErrorCode;

#[test]
fn cursor_entropy_pending_leaves_fresh_database_uninitialized() {
    set_boot_entropy_for_tests(None);
    let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
    let error = session.db.drive_startup_recovery_page().unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::recovery_pending().diagnostic_code()
    );
    assert_eq!(
        observe_database_format(&STORE_REGISTRY).unwrap(),
        DatabaseFormatObservation::Uninitialized
    );
    assert!(cursor_authentication_key().is_err());
    set_boot_entropy_for_tests(Some([0x17; 32]));
    assert!(session.db.drive_startup_recovery_page().unwrap());
    assert_eq!(
        observe_database_format(&STORE_REGISTRY).unwrap(),
        DatabaseFormatObservation::Current
    );
    assert_ne!(cursor_authentication_key().unwrap(), [0; 32]);
}

#[test]
fn cursor_entropy_new_boot_rejects_old_scalar_and_grouped_tokens() {
    let session = initialize();
    seed_rows(&session);
    let scalar = DynamicQuery::new(ENTITY_NAME)
        .select(["id"])
        .order_by(asc("id"));
    let scalar_cursor = session
        .execute_trusted_live_page(&scalar, None)
        .unwrap()
        .continuation
        .unwrap();
    let grouped = DynamicQuery::new(ENTITY_NAME)
        .group_by("rare")
        .aggregate(count())
        .order_by(asc("rare"))
        .grouped_limits(4, 16 * 1024)
        .limit(1);
    let grouped_cursor = session
        .execute_trusted_dynamic_grouped_query(&grouped)
        .unwrap()
        .next_cursor
        .unwrap();
    let key = cursor_authentication_key().unwrap();
    let incarnation = database_incarnation_id().unwrap();

    forget_database_format_admission_for_tests();
    forget_recovered_domain_for_tests(&session.db).unwrap();
    set_boot_entropy_for_tests(None);
    assert!(cursor_authentication_key().is_err());
    let error = session.db.drive_startup_recovery_page().unwrap_err();
    assert_eq!(
        error.diagnostic_code(),
        InternalError::recovery_pending().diagnostic_code()
    );
    set_boot_entropy_for_tests(Some([0x27; 32]));
    assert!(session.db.drive_startup_recovery_page().unwrap());
    assert_eq!(database_incarnation_id().unwrap(), incarnation);
    assert_ne!(cursor_authentication_key().unwrap(), key);
    let error = session
        .execute_trusted_live_page(&scalar, Some(&scalar_cursor))
        .unwrap_err();
    assert_eq!(
        error.diagnostic().error_code(),
        ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR
    );
    let error = session
        .execute_trusted_dynamic_grouped_query(&grouped.clone().cursor(&grouped_cursor))
        .unwrap_err();
    assert_eq!(
        error.diagnostic().error_code(),
        ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR
    );

    let fresh_scalar = session
        .execute_trusted_live_page(&scalar, None)
        .unwrap()
        .continuation
        .unwrap();
    assert!(
        session
            .execute_trusted_live_page(&scalar, Some(&fresh_scalar))
            .is_ok()
    );
    let fresh_grouped = session
        .execute_trusted_dynamic_grouped_query(&grouped)
        .unwrap()
        .next_cursor
        .unwrap();
    assert!(
        session
            .execute_trusted_dynamic_grouped_query(&grouped.cursor(&fresh_grouped))
            .is_ok()
    );
    assert_eq!(
        projection_rows(&session, "SELECT COUNT(*) FROM PlannerRow"),
        vec![vec![OutputValue::nat64(12)]]
    );
    let current = cursor_authentication_key().unwrap();
    assert!(session.db.drive_startup_recovery_page().unwrap());
    assert_eq!(cursor_authentication_key().unwrap(), current);
}
