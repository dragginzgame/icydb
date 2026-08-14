include!("common.rs");

icydb::endpoints! {
    icydb_sql_query(introspection = true, authorization = guard(super::policy::guard));
}

fn main() {}
