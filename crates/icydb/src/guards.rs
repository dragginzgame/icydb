//! Bounded application-supplied read-authorization contracts.
//!
//! Application identity, roles, sessions, policy storage, and audit remain
//! application concerns. This module owns only synchronous caller/surface decisions
//! before IcyDB request construction.

use ic_platform::Principal;

use crate::{Error, ErrorOrigin, diagnostic::RuntimeBoundaryCode};

/// Generated read surface presented to an application authorization guard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadAuthorizationSurface {
    /// The complete admitted generated SQL query lane.
    Sql,
    /// The dedicated generated accepted-schema query lane.
    Schema,
}

/// Bounded context supplied to an application read-authorization guard.
///
/// Authorization runs before SQL parsing. An allowed SQL invocation grants the
/// complete SQL surface across all entities; this context cannot enforce entity,
/// field, statement or row restrictions. Use a narrower application endpoint
/// when callers must receive only a selected entity or projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadAuthorizationContext {
    /// Principal observed at the generated endpoint boundary.
    pub caller: Principal,
    /// Generated read surface being authorized.
    pub surface: ReadAuthorizationSurface,
}

/// Complete application decision for one generated read invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadAuthorizationDecision {
    /// Continue into normal IcyDB startup admission and read execution.
    Allow,
    /// Reject this invocation without constructing an IcyDB request.
    Deny,
}

/// Exact synchronous application guard accepted by generated read endpoints.
pub type ReadAuthorizationGuard = fn(ReadAuthorizationContext) -> ReadAuthorizationDecision;

/// Maximum principal count accepted by [`allowlist`].
pub const MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS: usize = 64;

/// Decide one read invocation against a fixed bounded principal allowlist.
///
/// Anonymous callers, empty or missing matches, and lists above the fixed
/// maximum deny. Matching uses exact principal equality and performs no
/// allocation, logging, mutation, database entry, timer work, or remote call.
///
/// This helper checks the principal only: it does not restrict `context.surface`.
/// Reusing it for SQL and schema grants both surfaces to matching readers.
/// Use [`allowlist_for_surface`] to grant just one surface.
#[must_use]
pub fn allowlist(
    context: ReadAuthorizationContext,
    readers: &[Principal],
) -> ReadAuthorizationDecision {
    if context.caller == Principal::anonymous()
        || readers.len() > MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS
    {
        return ReadAuthorizationDecision::Deny;
    }

    if readers.contains(&context.caller) {
        ReadAuthorizationDecision::Allow
    } else {
        ReadAuthorizationDecision::Deny
    }
}

/// Decide one read invocation for a single surface and bounded principal list.
///
/// A mismatched surface denies before principal admission. Matching surfaces
/// use [`allowlist`], including its anonymous and capacity refusals. SQL access
/// still covers every entity and admitted statement, not a per-entity subset.
#[must_use]
pub fn allowlist_for_surface(
    context: ReadAuthorizationContext,
    surface: ReadAuthorizationSurface,
    readers: &[Principal],
) -> ReadAuthorizationDecision {
    if context.surface != surface {
        return ReadAuthorizationDecision::Deny;
    }
    allowlist(context, readers)
}

/// Apply the canonical generated SQL read-authorization boundary.
///
/// This is public only for generated-code and evidence-canister plumbing.
/// Transport principals are normalized through Platform's owned conversion
/// before constructing the application authorization context.
#[doc(hidden)]
pub fn authorize_sql_read(
    caller: impl Into<Principal>,
    guard: ReadAuthorizationGuard,
) -> Result<(), Error> {
    authorize_read(
        caller.into(),
        ReadAuthorizationSurface::Sql,
        guard,
        RuntimeBoundaryCode::SqlSurfacePolicyDenied,
    )
}

/// Apply the canonical generated accepted-schema read-authorization boundary.
///
/// This is public only for generated-code and evidence-canister plumbing.
/// Transport principals are normalized through Platform's owned conversion
/// before constructing the application authorization context.
#[doc(hidden)]
pub fn authorize_schema_read(
    caller: impl Into<Principal>,
    guard: ReadAuthorizationGuard,
) -> Result<(), Error> {
    authorize_read(
        caller.into(),
        ReadAuthorizationSurface::Schema,
        guard,
        RuntimeBoundaryCode::SchemaSurfacePolicyDenied,
    )
}

fn authorize_read(
    caller: Principal,
    surface: ReadAuthorizationSurface,
    guard: ReadAuthorizationGuard,
    denied_boundary: RuntimeBoundaryCode,
) -> Result<(), Error> {
    if caller == Principal::anonymous() {
        return Err(policy_denied(denied_boundary));
    }

    let context = ReadAuthorizationContext { caller, surface };
    match guard(context) {
        ReadAuthorizationDecision::Allow => Ok(()),
        ReadAuthorizationDecision::Deny => Err(policy_denied(denied_boundary)),
    }
}

const fn policy_denied(boundary: RuntimeBoundaryCode) -> Error {
    Error::from_runtime_boundary(boundary, ErrorOrigin::Interface)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use ic_platform::principal::ExternalPrincipal;

    thread_local! {
        static GUARD_CALLS: Cell<u8> = const { Cell::new(0) };
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
        let schema_guard = |context| {
            allowlist_for_surface(context, ReadAuthorizationSurface::Schema, &[principal(1)])
        };
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
    fn external_callers_preserve_identity_and_authorization() {
        let caller = principal(7);
        let external = ExternalPrincipal::from(caller);
        let sql_guard = |context: ReadAuthorizationContext| {
            assert_eq!(context.caller, principal(7));
            assert_eq!(context.surface, ReadAuthorizationSurface::Sql);
            ReadAuthorizationDecision::Allow
        };
        let schema_guard = |context: ReadAuthorizationContext| {
            assert_eq!(context.caller, principal(7));
            assert_eq!(context.surface, ReadAuthorizationSurface::Schema);
            ReadAuthorizationDecision::Allow
        };
        assert_eq!(authorize_sql_read(external, sql_guard), Ok(()));
        assert_eq!(authorize_schema_read(external, schema_guard), Ok(()));
        assert_eq!(
            authorize_sql_read(ExternalPrincipal::anonymous(), sql_guard)
                .expect_err("anonymous transport caller must be denied before its guard")
                .code(),
            crate::ErrorCode::RUNTIME_BOUNDARY_SQL_SURFACE_POLICY_DENIED,
        );
        assert_eq!(
            authorize_schema_read(ExternalPrincipal::anonymous(), schema_guard)
                .expect_err("anonymous transport caller must be denied before its guard")
                .code(),
            crate::ErrorCode::RUNTIME_BOUNDARY_SCHEMA_SURFACE_POLICY_DENIED,
        );
    }
}
