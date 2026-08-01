mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn metrics(
            _: Option<u64>,
        ) -> Result<icydb::metrics::CompactMetricsReport, icydb::Error> {
            Ok(icydb::metrics::CompactMetricsReport::default())
        }

        pub(crate) fn metrics_reset() -> Result<(), icydb::Error> {
            Ok(())
        }
    }

    pub(crate) mod endpoint_authorization {
        pub(crate) fn require_operational_controller() -> Result<(), icydb::Error> {
            Ok(())
        }
    }
}

icydb::endpoints! {
    icydb_metrics(authorization = public);
    icydb_metrics_reset;
}

fn main() {}
