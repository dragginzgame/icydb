//! Framework-neutral SQL and schema read-authority evidence canister.

use std::cell::Cell;

use ic_platform::Principal;
#[cfg(feature = "guarded-reads")]
use icydb::{
    ReadAuthorizationContext, ReadAuthorizationDecision, ReadAuthorizationSurface,
    guards::allowlist_for_surface,
};

const ADMIN_SEED: u8 = 41;
#[cfg(feature = "guarded-reads")]
const READER_SEED: u8 = 42;

// The final host owns grants and exclusions; components request permanent keys.
fn icydb_memory_pool()
-> Result<icydb::db::MemoryAllocationPool, icydb::db::MemoryAllocationPoolError> {
    icydb::db::MemoryAllocationPool::new(
        vec![icydb::db::MemoryAuthority::new(
            "icydb.one_simple",
            "icydb.one_simple.",
        )?],
        vec![],
    )
}

canic_icydb::start!(icydb);

canic_icydb::mount! {
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
    if !SQL_READER_ENABLED.with(Cell::get) {
        return ReadAuthorizationDecision::Deny;
    }

    allowlist_for_surface(
        context,
        ReadAuthorizationSurface::Sql,
        &[principal(READER_SEED)],
    )
}

#[cfg(feature = "guarded-reads")]
fn schema_read_guard(context: ReadAuthorizationContext) -> ReadAuthorizationDecision {
    if !SCHEMA_READER_ENABLED.with(Cell::get) {
        return ReadAuthorizationDecision::Deny;
    }

    allowlist_for_surface(
        context,
        ReadAuthorizationSurface::Schema,
        &[principal(READER_SEED)],
    )
}

/// Replace the application-owned SQL reader policy immediately.

fn set_sql_reader_enabled(enabled: bool) -> bool {
    if ic_platform::cdk::api::msg_caller() != principal(ADMIN_SEED) {
        return false;
    }

    SQL_READER_ENABLED.with(|current| current.set(enabled));
    true
}
icydb_testing_canic_adapter::export_wire!(update, "set_sql_reader_enabled", SetSqlReaderEnabled, set_sql_reader_enabled, (enabled: bool), bool);

/// Replace the application-owned schema reader policy immediately.

fn set_schema_reader_enabled(enabled: bool) -> bool {
    if ic_platform::cdk::api::msg_caller() != principal(ADMIN_SEED) {
        return false;
    }

    SCHEMA_READER_ENABLED.with(|current| current.set(enabled));
    true
}
icydb_testing_canic_adapter::export_wire!(update, "set_schema_reader_enabled", SetSchemaReaderEnabled, set_schema_reader_enabled, (enabled: bool), bool);
