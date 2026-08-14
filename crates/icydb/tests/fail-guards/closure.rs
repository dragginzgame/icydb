include!("common.rs");

icydb::endpoints! {
    icydb_sql_query(
        introspection = true,
        authorization = guard(|_| icydb::ReadAuthorizationDecision::Allow),
    );
}

fn main() {}
