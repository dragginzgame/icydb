use icydb as __icydb_facade;
use canic_icydb::__private::cdk as __icydb_host_sdk;

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn schema(
        ) -> Result<Vec<icydb::db::EntitySchemaDescription>, icydb::Error> {
            unreachable!()
        }
    }
}

canic_icydb::endpoints! {
    icydb_schema(authorization = public);
}

#[canic_icydb::__private::cdk::query(crate = "canic_icydb::__private::cdk", name = "icydb_schema", manual_reply = true, hidden = true)]
fn handwritten_icydb_schema() {}

fn main() {}
