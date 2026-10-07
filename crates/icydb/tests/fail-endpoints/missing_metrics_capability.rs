mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();
}

mod public_query {
    icydb::endpoints! {
        icydb_metrics(authorization = public);
    }
}

mod controller_query {
    icydb::endpoints! {
        icydb_metrics(authorization = controller);
    }
}

mod reset {
    icydb::endpoints! {
        icydb_metrics_reset;
    }
}

fn main() {}
