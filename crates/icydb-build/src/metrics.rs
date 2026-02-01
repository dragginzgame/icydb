use crate::ActorBuilder;
use proc_macro2::TokenStream;
use quote::quote;

/// Render the metrics/snapshot endpoints for a canister actor.
#[must_use]
pub fn generate(_builder: &ActorBuilder) -> TokenStream {
    quote! {
        /// Storage snapshot (live view).
        /// Includes data/index store stats and per-entity breakdown by store.
        #[::icydb::__reexports::canic_cdk::query]
        pub fn icydb_snapshot() -> Result<::icydb::obs::snapshot::StorageReport, ::icydb::Error> {
            Ok(::icydb::obs::snapshot::storage_report(&DB, &[])?)
        }

        /// Ephemeral event report since the internal `since_ms` (counters + per-entity summaries).
        #[::icydb::__reexports::canic_cdk::query]
        pub fn icydb_metrics() -> Result<::icydb::obs::EventReport, ::icydb::Error> {
            Ok(::icydb::obs::sink::metrics_report())
        }

        /// Reset ephemeral event state and refresh `since_ms`.
        #[::icydb::__reexports::canic_cdk::update]
        pub fn icydb_metrics_reset() -> Result<(), ::icydb::Error> {
            ::icydb::obs::sink::metrics_reset_all();

            Ok(())
        }

    }
}
