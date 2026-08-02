mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn metrics(
            _: Option<u64>,
        ) -> Result<icydb::metrics::CompactMetricsReport, icydb::Error> {
            unreachable!()
        }
    }
}

icydb::endpoints! {
    icydb_metrics(authorization = public);
}

#[icydb::__reexports::ic_cdk::query(name = "icydb_metrics")]
fn handwritten_icydb_metrics() {}

fn main() {}
