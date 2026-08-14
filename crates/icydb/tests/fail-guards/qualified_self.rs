include!("common.rs");

icydb::endpoints! {
    icydb_sql_query(
        introspection = true,
        authorization = guard(<Policy as GuardPolicy>::guard),
    );
}

fn main() {}
