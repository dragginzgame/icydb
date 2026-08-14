include!("common.rs");

icydb::endpoints! {
    icydb_sql_query(introspection = true, authorization = guard(abi_guard));
}

fn main() {}
