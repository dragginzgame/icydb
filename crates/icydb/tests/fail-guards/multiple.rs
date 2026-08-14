include!("common.rs");

icydb::endpoints! {
    icydb_sql_query(
        introspection = true,
        authorization = guard(policy::guard, policy::guard),
    );
}

fn main() {}
