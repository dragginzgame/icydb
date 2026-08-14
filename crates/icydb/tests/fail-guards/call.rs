include!("common.rs");

icydb::endpoints! {
    icydb_sql_query(
        introspection = true,
        authorization = guard(policy::guard(icydb::ReadAuthorizationContext {
            caller: candid::Principal::anonymous(),
            surface: icydb::ReadAuthorizationSurface::Sql,
        })),
    );
}

fn main() {}
