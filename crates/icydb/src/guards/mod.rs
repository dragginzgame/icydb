//! Bounded application-supplied read-authorization contracts.
//!
//! Application identity, roles, sessions, policy storage, and audit remain
//! application concerns. This module owns only synchronous caller/surface decisions
//! before IcyDB request construction.

use ic_platform_types::Principal;

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
/// Transport principal bytes are checked and normalized through Platform before
/// constructing the application context. This boundary does not require the
/// transport and database to select the same Platform package identity.
#[doc(hidden)]
pub fn authorize_sql_read(
    caller: impl AsRef<[u8]>,
    guard: ReadAuthorizationGuard,
) -> Result<(), Error> {
    authorize_read(
        caller.as_ref(),
        ReadAuthorizationSurface::Sql,
        guard,
        RuntimeBoundaryCode::SqlSurfacePolicyDenied,
    )
}

/// Apply the canonical generated accepted-schema read-authorization boundary.
///
/// This is public only for generated-code and evidence-canister plumbing.
/// Transport principal bytes are checked and normalized through Platform before
/// constructing the application context. This boundary does not require the
/// transport and database to select the same Platform package identity.
#[doc(hidden)]
pub fn authorize_schema_read(
    caller: impl AsRef<[u8]>,
    guard: ReadAuthorizationGuard,
) -> Result<(), Error> {
    authorize_read(
        caller.as_ref(),
        ReadAuthorizationSurface::Schema,
        guard,
        RuntimeBoundaryCode::SchemaSurfacePolicyDenied,
    )
}

fn authorize_read(
    caller: &[u8],
    surface: ReadAuthorizationSurface,
    guard: ReadAuthorizationGuard,
    denied_boundary: RuntimeBoundaryCode,
) -> Result<(), Error> {
    // A principal's canonical bytes, rather than a transport crate's Rust type,
    // are the identity boundary. Refuse invalid bytes before application code.
    let caller = Principal::try_from_slice(caller).map_err(|_| policy_denied(denied_boundary))?;
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
mod tests;
