mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn metrics(
            _: Option<u64>,
        ) -> Result<icydb::metrics::CompactMetricsReport, icydb::Error> {
            Ok(icydb::metrics::CompactMetricsReport::default())
        }
    }
}

icydb::endpoints! {
    icydb_metrics(authorization = public);
    icydb_metrics(authorization = public);
}

fn main() {}
