include!("common.rs");

icydb::endpoints! {
    icydb_sql_query(introspection = true, authorization = guard(async_guard));
}

fn main() {}
