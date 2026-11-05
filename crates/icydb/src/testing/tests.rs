use super::{DatabaseStartupState, Poll, startup_poll};
use crate::{Error, ErrorCode, db::DatabaseBootstrapError};

#[test]
fn readiness_uses_the_typed_state_and_preserves_terminal_diagnostics() {
    assert_eq!(
        startup_poll::<Error>(Ok(DatabaseStartupState::Ready)),
        Poll::Ready(Ok(()))
    );
    assert_eq!(
        startup_poll::<Error>(Ok(DatabaseStartupState::Recovering)),
        Poll::Pending
    );
    let failure = crate::db::__startup_bootstrap_failure(DatabaseBootstrapError::from(
        ic_storage::RuntimeConstructionError::Directory(ic_storage::DirectoryError::Invalid(
            "test",
        )),
    ));
    assert_eq!(
        startup_poll(Err(failure.clone())),
        Poll::Ready(Err(failure))
    );
}

#[test]
fn observation_errors_are_never_reclassified_as_pending() {
    let error = crate::db::__startup_recovery_pending();
    assert_eq!(
        error.code(),
        ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING
    );
    assert_eq!(startup_poll(Err(error.clone())), Poll::Ready(Err(error)));
}
