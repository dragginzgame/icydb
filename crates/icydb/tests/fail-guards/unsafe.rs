include!("common.rs");

icydb::endpoints! {
    icydb_sql_query(introspection = true, authorization = guard(unsafe_guard));
}

fn main() {}
