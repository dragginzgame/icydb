//! One-entity generated SQL authorization evidence canister.

#[cfg(feature = "guarded-sql-query")]
use std::cell::Cell;

use candid::CandidType;
#[cfg(feature = "guarded-sql-query")]
use candid::Principal;
use ic_cdk::query;
#[cfg(feature = "guarded-sql-query")]
use icydb::{
    ReadAuthorizationContext, ReadAuthorizationDecision, ReadAuthorizationSurface,
    guards::{MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS, allowlist},
};

icydb::start!();

icydb::endpoints! {
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
        ic_cdk::trap("anonymous caller reached the application SQL guard");
    }
    if context.caller == principal(44) {
        ic_cdk::trap("intentional application SQL guard trap");
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

#[derive(CandidType)]
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
#[query]
#[cfg_attr(
    not(feature = "guarded-sql-query"),
    allow(clippy::missing_const_for_fn)
)]
#[allow(clippy::unnecessary_wraps)]
fn read_authorization_cost() -> Result<ReadAuthorizationCostResult, icydb::Error> {
    #[cfg(feature = "guarded-sql-query")]
    {
        let start = ic_cdk::api::performance_counter(1);
        let caller = ic_cdk::api::msg_caller();
        let caller_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        let context = ReadAuthorizationContext {
            caller,
            surface: ReadAuthorizationSurface::Sql,
        };
        let mut readers = [Principal::anonymous(); MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS];
        readers[MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS - 1] = caller;
        let start = ic_cdk::api::performance_counter(1);
        let helper_decision = allowlist(context, &readers);
        let helper_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        let start = ic_cdk::api::performance_counter(1);
        let guard_decision = maximum_allowlist_guard(context);
        let guard_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        let start = ic_cdk::api::performance_counter(1);
        icydb::__macro::authorize_sql_read(caller, maximum_allowlist_guard)?;
        let authorization_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        let wrapper_instructions = authorization_instructions.saturating_sub(guard_instructions);

        let start = ic_cdk::api::performance_counter(1);
        let query_result = icydb::db::with_request_execution(|| {
            crate::__icydb_generated::endpoint_handlers::sql_query::<true>(
                "SHOW ENTITIES".to_string(),
            )
        });
        let query_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
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

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
