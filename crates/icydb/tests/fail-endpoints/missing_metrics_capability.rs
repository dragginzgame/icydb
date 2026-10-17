use icydb as __icydb_facade;
use canic_icydb::__private::cdk as __icydb_host_sdk;

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();
}

mod public_query {
    canic_icydb::endpoints! {
        icydb_metrics(authorization = public);
    }
}

mod controller_query {
    canic_icydb::endpoints! {
        icydb_metrics(authorization = controller);
    }
}

mod reset {
    canic_icydb::endpoints! {
        icydb_metrics_reset;
    }
}

fn main() {}
