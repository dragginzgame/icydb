//! One-entity generated schema authorization evidence canister.

// IC counter 1 preserves the call-context sample boundary through Platform.
// Native system calls provide no IC measurement evidence.
#[cfg(feature = "guarded-schema")]
use ic_platform::ic0::performance_counter;

#[cfg(feature = "guarded-schema")]
use std::cell::Cell;

#[cfg(feature = "guarded-schema")]
use ic_platform::Principal;
#[cfg(feature = "guarded-schema")]
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
        ic_platform::ic0::trap(b"anonymous caller reached the application schema guard");
    }
    if context.caller == principal(44) {
        ic_platform::ic0::trap(b"intentional application schema guard trap");
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

#[derive(serde::Deserialize, serde::Serialize)]
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

#[cfg_attr(not(feature = "guarded-schema"), allow(clippy::missing_const_for_fn))]
#[allow(clippy::unnecessary_wraps)]
fn read_authorization_cost() -> Result<ReadAuthorizationCostResult, icydb::Error> {
    #[cfg(feature = "guarded-schema")]
    {
        let start = performance_counter(1);
        let caller = ic_platform::cdk::api::msg_caller();
        let caller_instructions = performance_counter(1).saturating_sub(start);

        let context = ReadAuthorizationContext {
            caller,
            surface: ReadAuthorizationSurface::Schema,
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
        icydb::__macro::authorize_schema_read(caller, maximum_allowlist_guard)?;
        let authorization_instructions = performance_counter(1).saturating_sub(start);
        let wrapper_instructions = authorization_instructions.saturating_sub(guard_instructions);

        let start = performance_counter(1);
        let schema_result = icydb::db::with_request_execution(|| {
            crate::__icydb_generated::endpoint_handlers::schema()
        });
        let schema_instructions = performance_counter(1).saturating_sub(start);
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
icydb_testing_canic_adapter::export_wire!(query, "read_authorization_cost", ReadAuthorizationCost, read_authorization_cost, (), Result<ReadAuthorizationCostResult, icydb::Error>);
