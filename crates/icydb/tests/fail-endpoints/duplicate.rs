mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn schema(
        ) -> Result<Vec<icydb::db::EntitySchemaDescription>, icydb::Error> {
            unreachable!()
        }
    }
}

icydb::endpoints! {
    icydb_schema(authorization = public);
    icydb_schema(authorization = public);
}

fn main() {}
