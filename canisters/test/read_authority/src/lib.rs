//! Framework-neutral SQL and schema read-authority evidence canister.

use std::cell::Cell;

use candid::Principal;
#[cfg(feature = "guarded-reads")]
use icydb::{
    ReadAuthorizationContext, ReadAuthorizationDecision, ReadAuthorizationSurface,
    guards::allowlist,
};

const ADMIN_SEED: u8 = 41;
const READER_SEED: u8 = 42;

icydb::start!();

icydb::endpoints! {
    #[cfg(all(feature = "sql", not(feature = "guarded-reads")))]
    icydb_sql_query(introspection = true);
    #[cfg(feature = "guarded-reads")]
    icydb_sql_query(
        introspection = true,
        authorization = guard(sql_read_guard),
    );

    #[cfg(not(feature = "guarded-reads"))]
    icydb_schema(authorization = controller);
    #[cfg(feature = "guarded-reads")]
    icydb_schema(authorization = guard(schema_read_guard));
}

thread_local! {
    static SQL_READER_ENABLED: Cell<bool> = const { Cell::new(false) };
    static SCHEMA_READER_ENABLED: Cell<bool> = const { Cell::new(false) };
}

fn principal(seed: u8) -> Principal {
    Principal::self_authenticating([seed; 32])
}

#[cfg(feature = "guarded-reads")]
fn sql_read_guard(context: ReadAuthorizationContext) -> ReadAuthorizationDecision {
    if context.surface != ReadAuthorizationSurface::Sql || !SQL_READER_ENABLED.with(Cell::get) {
        return ReadAuthorizationDecision::Deny;
    }

    allowlist(context, &[principal(READER_SEED)])
}

#[cfg(feature = "guarded-reads")]
fn schema_read_guard(context: ReadAuthorizationContext) -> ReadAuthorizationDecision {
    if context.surface != ReadAuthorizationSurface::Schema || !SCHEMA_READER_ENABLED.with(Cell::get)
    {
        return ReadAuthorizationDecision::Deny;
    }

    allowlist(context, &[principal(READER_SEED)])
}

/// Replace the application-owned SQL reader policy immediately.
#[ic_cdk::update]
fn set_sql_reader_enabled(enabled: bool) -> bool {
    if ic_cdk::api::msg_caller() != principal(ADMIN_SEED) {
        return false;
    }

    SQL_READER_ENABLED.with(|current| current.set(enabled));
    true
}

/// Replace the application-owned schema reader policy immediately.
#[ic_cdk::update]
fn set_schema_reader_enabled(enabled: bool) -> bool {
    if ic_cdk::api::msg_caller() != principal(ADMIN_SEED) {
        return false;
    }

    SCHEMA_READER_ENABLED.with(|current| current.set(enabled));
    true
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
