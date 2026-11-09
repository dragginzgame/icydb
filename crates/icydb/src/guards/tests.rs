use std::cell::Cell;

use super::*;

thread_local! {
    static GUARD_CALLS: Cell<u8> = const { Cell::new(0) };
    static OBSERVED_CONTEXT: Cell<Option<ReadAuthorizationContext>> = const { Cell::new(None) };
}

fn principal(seed: u8) -> Principal {
    Principal::self_authenticating([seed; 32])
}

fn counting_allow(context: ReadAuthorizationContext) -> ReadAuthorizationDecision {
    assert_eq!(context.surface, ReadAuthorizationSurface::Sql);
    GUARD_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    ReadAuthorizationDecision::Allow
}

fn counting_deny(context: ReadAuthorizationContext) -> ReadAuthorizationDecision {
    assert_eq!(context.surface, ReadAuthorizationSurface::Sql);
    GUARD_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    ReadAuthorizationDecision::Deny
}

fn counting_schema_allow(context: ReadAuthorizationContext) -> ReadAuthorizationDecision {
    assert_eq!(context.surface, ReadAuthorizationSurface::Schema);
    GUARD_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    ReadAuthorizationDecision::Allow
}

fn take_guard_calls() -> u8 {
    GUARD_CALLS.with(|calls| calls.replace(0))
}

#[test]
fn allowlist_is_bounded_exact_and_fail_closed() {
    let reader = principal(1);
    let outsider = principal(2);
    let reader_context = ReadAuthorizationContext {
        caller: reader,
        surface: ReadAuthorizationSurface::Sql,
    };

    assert_eq!(
        allowlist(reader_context, &[]),
        ReadAuthorizationDecision::Deny
    );
    assert_eq!(
        allowlist(reader_context, &[outsider, reader, reader]),
        ReadAuthorizationDecision::Allow,
    );
    assert_eq!(
        allowlist(
            ReadAuthorizationContext {
                caller: outsider,
                surface: ReadAuthorizationSurface::Sql,
            },
            &[reader],
        ),
        ReadAuthorizationDecision::Deny,
    );
    assert_eq!(
        allowlist(
            ReadAuthorizationContext {
                caller: Principal::anonymous(),
                surface: ReadAuthorizationSurface::Sql,
            },
            &[Principal::anonymous()],
        ),
        ReadAuthorizationDecision::Deny,
    );

    let maximum = [reader; MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS];
    assert_eq!(
        allowlist(reader_context, &maximum),
        ReadAuthorizationDecision::Allow,
    );
    let over_capacity = [reader; MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS + 1];
    assert_eq!(
        allowlist(reader_context, &over_capacity),
        ReadAuthorizationDecision::Deny,
    );
}

#[test]
fn surface_allowlist_preserves_bounded_admission_and_denies_other_surfaces() {
    let reader = principal(1);
    let outsider = principal(2);
    for granted in [
        ReadAuthorizationSurface::Sql,
        ReadAuthorizationSurface::Schema,
    ] {
        for observed in [
            ReadAuthorizationSurface::Sql,
            ReadAuthorizationSurface::Schema,
        ] {
            for caller in [reader, outsider, Principal::anonymous()] {
                for readers in [
                    vec![],
                    vec![reader],
                    vec![Principal::anonymous()],
                    vec![reader; MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS],
                    vec![reader; MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS + 1],
                ] {
                    let context = ReadAuthorizationContext {
                        caller,
                        surface: observed,
                    };
                    let expected = if granted == observed {
                        allowlist(context, &readers)
                    } else {
                        ReadAuthorizationDecision::Deny
                    };
                    assert_eq!(allowlist_for_surface(context, granted, &readers), expected);
                }
            }
        }
    }
    let schema_guard =
        |context| allowlist_for_surface(context, ReadAuthorizationSurface::Schema, &[principal(1)]);
    assert_eq!(authorize_schema_read(reader, schema_guard), Ok(()));
    assert_eq!(
        authorize_sql_read(reader, schema_guard).unwrap_err().code(),
        crate::ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED,
    );
}

#[test]
fn sql_authorization_rejects_anonymous_without_calling_application_code() {
    take_guard_calls();

    let error = authorize_sql_read(Principal::anonymous(), counting_allow)
        .expect_err("anonymous SQL read should be denied");

    assert_eq!(
        error.code(),
        crate::ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED,
    );
    assert_eq!(take_guard_calls(), 0);
}

#[test]
fn sql_authorization_invokes_one_guard_once_and_preserves_its_decision() {
    take_guard_calls();
    let caller = principal(3);

    assert_eq!(authorize_sql_read(caller, counting_allow), Ok(()));
    assert_eq!(take_guard_calls(), 1);

    let error = authorize_sql_read(caller, counting_deny)
        .expect_err("application denial should be returned");
    assert_eq!(
        error.code(),
        crate::ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED,
    );
    assert_eq!(take_guard_calls(), 1);
}

#[test]
fn schema_authorization_uses_schema_context_and_schema_denial() {
    take_guard_calls();
    let caller = principal(11);
    assert_eq!(authorize_schema_read(caller, counting_schema_allow), Ok(()));
    assert_eq!(take_guard_calls(), 1);

    let error = authorize_schema_read(Principal::anonymous(), counting_schema_allow)
        .expect_err("anonymous schema reads must fail before the guard");
    assert_eq!(
        error.code(),
        crate::ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_POLICY_DENIED,
    );
    assert_eq!(take_guard_calls(), 0);
}

#[test]
fn caller_bytes_preserve_all_canonical_lengths_on_both_surfaces() {
    fn record(context: ReadAuthorizationContext) -> ReadAuthorizationDecision {
        OBSERVED_CONTEXT.with(|observed| observed.set(Some(context)));
        ReadAuthorizationDecision::Allow
    }

    let bytes = [2; Principal::MAX_LENGTH_IN_BYTES];
    for length in 0..=bytes.len() {
        let caller = &bytes[..length];
        let expected = Principal::from_slice(caller);
        assert_eq!(authorize_sql_read(caller, record), Ok(()));
        assert_eq!(
            OBSERVED_CONTEXT.with(Cell::take),
            Some(ReadAuthorizationContext {
                caller: expected,
                surface: ReadAuthorizationSurface::Sql,
            }),
        );
        assert_eq!(authorize_schema_read(caller, record), Ok(()));
        assert_eq!(
            OBSERVED_CONTEXT.with(Cell::take),
            Some(ReadAuthorizationContext {
                caller: expected,
                surface: ReadAuthorizationSurface::Schema,
            }),
        );
    }
}

#[test]
fn invalid_caller_bytes_deny_before_guard_invocation() {
    take_guard_calls();
    let invalid = [0; Principal::MAX_LENGTH_IN_BYTES + 1];
    let anonymous = Principal::anonymous();
    for caller in [invalid.as_slice(), anonymous.as_slice()] {
        assert_eq!(
            authorize_sql_read(caller, counting_allow)
                .expect_err("invalid or anonymous SQL caller must be denied")
                .code(),
            crate::ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED,
        );
        assert_eq!(
            authorize_schema_read(caller, counting_schema_allow)
                .expect_err("invalid or anonymous schema caller must be denied")
                .code(),
            crate::ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_POLICY_DENIED,
        );
    }
    assert_eq!(take_guard_calls(), 0);
}
