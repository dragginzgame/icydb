//! One-entity generated schema authorization evidence canister.

#[cfg(feature = "guarded-schema")]
use std::cell::Cell;

use candid::CandidType;
#[cfg(feature = "guarded-schema")]
use candid::Principal;
use ic_cdk::query;
#[cfg(feature = "guarded-schema")]
use icydb::{
    ReadAuthorizationContext, ReadAuthorizationDecision, ReadAuthorizationSurface,
    guards::{MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS, allowlist},
};

icydb::start!();

icydb::endpoints! {
    #[cfg(not(feature = "guarded-schema"))]
    icydb_schema(authorization = controller);
    #[cfg(feature = "guarded-schema")]
    icydb_schema(authorization = guard(schema_read_guard));
}

#[cfg(feature = "guarded-schema")]
thread_local! {
    static GUARD_CALLED: Cell<bool> = const { Cell::new(false) };
}

#[cfg(feature = "guarded-schema")]
fn principal(seed: u8) -> Principal {
    Principal::self_authenticating([seed; 32])
}

#[cfg(feature = "guarded-schema")]
fn schema_read_guard(context: ReadAuthorizationContext) -> ReadAuthorizationDecision {
    if context.caller == Principal::anonymous() {
        ic_cdk::trap("anonymous caller reached the application schema guard");
    }
    if context.caller == principal(44) {
        ic_cdk::trap("intentional application schema guard trap");
    }
    if context.surface != ReadAuthorizationSurface::Schema {
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
    schema_instructions: u64,
    helper_allowed: bool,
}

#[cfg(feature = "guarded-schema")]
fn maximum_allowlist_guard(context: ReadAuthorizationContext) -> ReadAuthorizationDecision {
    let mut readers = [Principal::anonymous(); MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS];
    readers[MAX_READ_AUTHORIZATION_ALLOWLIST_PRINCIPALS - 1] = context.caller;
    allowlist(context, &readers)
}

/// Measure the canonical local schema authorization seam without altering it.
#[query]
#[cfg_attr(not(feature = "guarded-schema"), allow(clippy::missing_const_for_fn))]
#[allow(clippy::unnecessary_wraps)]
fn read_authorization_cost() -> Result<ReadAuthorizationCostResult, icydb::Error> {
    #[cfg(feature = "guarded-schema")]
    {
        let start = ic_cdk::api::performance_counter(1);
        let caller = ic_cdk::api::msg_caller();
        let caller_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);

        let context = ReadAuthorizationContext {
            caller,
            surface: ReadAuthorizationSurface::Schema,
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
        icydb::__macro::authorize_schema_read(caller, maximum_allowlist_guard)?;
        let authorization_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        let wrapper_instructions = authorization_instructions.saturating_sub(guard_instructions);

        let start = ic_cdk::api::performance_counter(1);
        let schema_result = icydb::db::with_request_execution(|| {
            crate::__icydb_generated::endpoint_handlers::schema()
        });
        let schema_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
        schema_result?;

        Ok(ReadAuthorizationCostResult {
            caller_instructions,
            helper_instructions,
            guard_instructions,
            authorization_instructions,
            wrapper_instructions,
            schema_instructions,
            helper_allowed: helper_decision == ReadAuthorizationDecision::Allow
                && guard_decision == ReadAuthorizationDecision::Allow,
        })
    }

    #[cfg(not(feature = "guarded-schema"))]
    Ok(ReadAuthorizationCostResult {
        caller_instructions: 0,
        helper_instructions: 0,
        guard_instructions: 0,
        authorization_instructions: 0,
        wrapper_instructions: 0,
        schema_instructions: 0,
        helper_allowed: false,
    })
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
