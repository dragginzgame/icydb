include!("common.rs");

icydb::endpoints! {
    icydb_sql_query(introspection = true, authorization = guard(result_guard));
}

fn main() {}
