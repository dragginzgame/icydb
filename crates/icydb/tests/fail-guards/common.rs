mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn sql_query<const INTROSPECTION: bool>(
            _: String,
        ) -> Result<icydb::db::sql::SqlQueryResult, icydb::Error> {
            let _ = INTROSPECTION;
            unimplemented!()
        }
    }
}

mod policy {
    pub(crate) fn guard(
        _: icydb::ReadAuthorizationContext,
    ) -> icydb::ReadAuthorizationDecision {
        icydb::ReadAuthorizationDecision::Allow
    }
}

struct Policy;

trait GuardPolicy {
    fn guard(_: icydb::ReadAuthorizationContext) -> icydb::ReadAuthorizationDecision;
}

impl GuardPolicy for Policy {
    fn guard(_: icydb::ReadAuthorizationContext) -> icydb::ReadAuthorizationDecision {
        icydb::ReadAuthorizationDecision::Allow
    }
}

unsafe fn unsafe_guard(
    _: icydb::ReadAuthorizationContext,
) -> icydb::ReadAuthorizationDecision {
    icydb::ReadAuthorizationDecision::Allow
}

async fn async_guard(
    _: icydb::ReadAuthorizationContext,
) -> icydb::ReadAuthorizationDecision {
    icydb::ReadAuthorizationDecision::Allow
}

fn reference_guard(
    _: &icydb::ReadAuthorizationContext,
) -> icydb::ReadAuthorizationDecision {
    icydb::ReadAuthorizationDecision::Allow
}

fn result_guard(
    _: icydb::ReadAuthorizationContext,
) -> Result<icydb::ReadAuthorizationDecision, ()> {
    Ok(icydb::ReadAuthorizationDecision::Allow)
}

extern "C" fn abi_guard(
    _: icydb::ReadAuthorizationContext,
) -> icydb::ReadAuthorizationDecision {
    icydb::ReadAuthorizationDecision::Allow
}
