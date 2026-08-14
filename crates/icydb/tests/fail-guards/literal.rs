include!("common.rs");

icydb::endpoints! {
    icydb_sql_query(introspection = true, authorization = guard(0));
}

fn main() {}
