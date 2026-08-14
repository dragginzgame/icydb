#![allow(clippy::missing_const_for_fn, clippy::unnecessary_wraps, dead_code)]

extern crate self as guard_application;

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn sql_query<const INTROSPECTION: bool>(
            _: String,
        ) -> Result<icydb::db::sql::SqlQueryPerfResult, icydb::Error> {
            let _ = INTROSPECTION;
            unimplemented!()
        }

        pub(crate) fn schema(
        ) -> Result<Vec<icydb::db::EntitySchemaDescription>, icydb::Error> {
            unimplemented!()
        }
    }
}

fn bare(_: icydb::ReadAuthorizationContext) -> icydb::ReadAuthorizationDecision {
    icydb::ReadAuthorizationDecision::Allow
}

mod policy {
    pub(crate) fn guard(
        _: icydb::ReadAuthorizationContext,
    ) -> icydb::ReadAuthorizationDecision {
        icydb::ReadAuthorizationDecision::Allow
    }
}

mod r#type {
    pub(crate) fn r#match(
        _: icydb::ReadAuthorizationContext,
    ) -> icydb::ReadAuthorizationDecision {
        icydb::ReadAuthorizationDecision::Allow
    }
}

struct Policy;

impl Policy {
    fn guard(_: icydb::ReadAuthorizationContext) -> icydb::ReadAuthorizationDecision {
        icydb::ReadAuthorizationDecision::Allow
    }
}

const NAMED_GUARD: icydb::ReadAuthorizationGuard = bare;
use policy::guard as reexported_guard;

fn inferred<T: Into<icydb::ReadAuthorizationContext>>(
    _: T,
) -> icydb::ReadAuthorizationDecision {
    icydb::ReadAuthorizationDecision::Allow
}

const _: icydb::ReadAuthorizationGuard = bare;
const _: icydb::ReadAuthorizationGuard = policy::guard;
const _: icydb::ReadAuthorizationGuard = crate::policy::guard;
const _: icydb::ReadAuthorizationGuard = self::policy::guard;
const _: icydb::ReadAuthorizationGuard = ::guard_application::policy::guard;
const _: icydb::ReadAuthorizationGuard = r#type::r#match;
const _: icydb::ReadAuthorizationGuard = Policy::guard;
const _: icydb::ReadAuthorizationGuard = NAMED_GUARD;
const _: icydb::ReadAuthorizationGuard = reexported_guard;
const _: icydb::ReadAuthorizationGuard = inferred;
const _: icydb::ReadAuthorizationGuard = inferred::<icydb::ReadAuthorizationContext>;

icydb::endpoints! {
    // Route every accepted Rust path form through the production macro grammar.
    // The declarations are disabled to avoid duplicate exported methods; the
    // exact function-pointer coercions above remain active and type-checked.
    #[cfg(any())]
    icydb_sql_query(introspection = true, authorization = guard(bare));
    #[cfg(any())]
    icydb_sql_query(introspection = true, authorization = guard(policy::guard));
    #[cfg(any())]
    icydb_sql_query(introspection = true, authorization = guard(crate::policy::guard));
    #[cfg(any())]
    icydb_sql_query(introspection = true, authorization = guard(self::policy::guard));
    #[cfg(any())]
    icydb_sql_query(
        introspection = true,
        authorization = guard(::guard_application::policy::guard),
    );
    #[cfg(any())]
    icydb_sql_query(introspection = true, authorization = guard(r#type::r#match));
    #[cfg(any())]
    icydb_sql_query(introspection = true, authorization = guard(Policy::guard));
    #[cfg(any())]
    icydb_sql_query(introspection = true, authorization = guard(NAMED_GUARD));
    #[cfg(any())]
    icydb_sql_query(introspection = true, authorization = guard(reexported_guard));
    #[cfg(any())]
    icydb_sql_query(introspection = true, authorization = guard(inferred));
    icydb_sql_query(
        introspection = true,
        authorization = guard(inferred::<icydb::ReadAuthorizationContext>),
    );
    icydb_schema(authorization = guard(policy::guard));
}

fn main() {}
