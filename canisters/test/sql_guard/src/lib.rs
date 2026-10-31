//! One-entity generated SQL authorization evidence canister.

// Exercise generated handlers and exported wrappers under downstream lint policy.
#![deny(clippy::needless_pass_by_value)]

// IC counter 1 preserves the call-context sample boundary through Platform.
// Native system calls provide no IC measurement evidence.
#[cfg(feature = "guarded-sql-query")]
use ic_platform::ic0::performance_counter;

#[cfg(feature = "guarded-sql-query")]
use std::cell::Cell;

#[cfg(feature = "guarded-sql-query")]
use ic_platform::Principal;
#[cfg(feature = "guarded-sql-query")]
use icydb::{
    ReadAuthorizationContext, ReadAuthorizationDecision, ReadAuthorizationSurface,
    guards::{MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS, allowlist},
};

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
    #[cfg(all(feature = "sql", not(feature = "guarded-sql-query")))]
    icydb_sql_query(introspection = true);
    #[cfg(feature = "guarded-sql-query")]
    icydb_sql_query(
        introspection = true,
        authorization = guard(sql_read_guard),
    );
}

#[cfg(feature = "guarded-sql-query")]
thread_local! {
    static GUARD_CALLED: Cell<bool> = const { Cell::new(false) };
}

#[cfg(feature = "guarded-sql-query")]
fn principal(seed: u8) -> Principal {
    Principal::self_authenticating([seed; 32])
}

#[cfg(feature = "guarded-sql-query")]
fn sql_read_guard(context: ReadAuthorizationContext) -> ReadAuthorizationDecision {
    if context.caller == Principal::anonymous() {
        ic_platform::ic0::trap(b"anonymous caller reached the application SQL guard");
    }
    if context.caller == principal(44) {
        ic_platform::ic0::trap(b"intentional application SQL guard trap");
    }
    if context.surface != ReadAuthorizationSurface::Sql {
        return ReadAuthorizationDecision::Deny;
    }

    let first_call = GUARD_CALLED.with(|called| !called.replace(true));
    if !first_call {
        return ReadAuthorizationDecision::Deny;
    }

    allowlist(context, &[principal(42)])
}

#[derive(serde::Deserialize, serde::Serialize)]
struct ReadAuthorizationCostResult {
    caller_instructions: u64,
    helper_instructions: u64,
    guard_instructions: u64,
    authorization_instructions: u64,
    wrapper_instructions: u64,
    query_instructions: u64,
    helper_allowed: bool,
}

#[cfg(feature = "guarded-sql-query")]
fn maximum_allowlist_guard(context: ReadAuthorizationContext) -> ReadAuthorizationDecision {
    let mut readers = [Principal::anonymous(); MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS];
    readers[MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS - 1] = context.caller;
    allowlist(context, &readers)
}

/// Measure the canonical local authorization seam without SQL execution.

#[cfg_attr(
    not(feature = "guarded-sql-query"),
    allow(clippy::missing_const_for_fn)
)]
#[allow(clippy::unnecessary_wraps)]
fn read_authorization_cost() -> Result<ReadAuthorizationCostResult, icydb::Error> {
    #[cfg(feature = "guarded-sql-query")]
    {
        let start = performance_counter(1);
        let caller = ic_platform::cdk::api::msg_caller();
        let caller_instructions = performance_counter(1).saturating_sub(start);

        let context = ReadAuthorizationContext {
            caller,
            surface: ReadAuthorizationSurface::Sql,
        };
        let mut readers = [Principal::anonymous(); MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS];
        readers[MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS - 1] = caller;
        let start = performance_counter(1);
        let helper_decision = allowlist(context, &readers);
        let helper_instructions = performance_counter(1).saturating_sub(start);

        let start = performance_counter(1);
        let guard_decision = maximum_allowlist_guard(context);
        let guard_instructions = performance_counter(1).saturating_sub(start);

        let start = performance_counter(1);
        icydb::__macro::authorize_sql_read(caller, maximum_allowlist_guard)?;
        let authorization_instructions = performance_counter(1).saturating_sub(start);
        let wrapper_instructions = authorization_instructions.saturating_sub(guard_instructions);

        let start = performance_counter(1);
        let query_result = icydb::db::with_request_execution(|| {
            crate::__icydb_generated::endpoint_handlers::sql_query::<true>(
                "SHOW ENTITIES".to_string(),
            )
        });
        let query_instructions = performance_counter(1).saturating_sub(start);
        query_result?;

        Ok(ReadAuthorizationCostResult {
            caller_instructions,
            helper_instructions,
            guard_instructions,
            authorization_instructions,
            wrapper_instructions,
            query_instructions,
            helper_allowed: helper_decision == ReadAuthorizationDecision::Allow
                && guard_decision == ReadAuthorizationDecision::Allow,
        })
    }

    #[cfg(not(feature = "guarded-sql-query"))]
    Ok(ReadAuthorizationCostResult {
        caller_instructions: 0,
        helper_instructions: 0,
        guard_instructions: 0,
        authorization_instructions: 0,
        wrapper_instructions: 0,
        query_instructions: 0,
        helper_allowed: false,
    })
}
icydb_testing_canic_adapter::export_wire!(query, "read_authorization_cost", ReadAuthorizationCost, read_authorization_cost, (), Result<ReadAuthorizationCostResult, icydb::Error>);
