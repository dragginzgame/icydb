mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn metrics() -> Result<icydb::metrics::MetricsReport, icydb::Error> {
            Ok(icydb::metrics::MetricsReport::default())
        }
    }
}

icydb::endpoints! {
    icydb_metrics(authorization = public);
    icydb_metrics(authorization = public);
}

fn main() {}
