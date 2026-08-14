include!("common.rs");

icydb::endpoints! {
    icydb_sql_query(introspection = true, authorization = guard(reference_guard));
}

fn main() {}
