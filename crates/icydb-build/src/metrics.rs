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
        pub fn icydb_snapshot() -> Result<::icydb::obs::StorageReport, ::icydb::Error> {
            Ok(::icydb::obs::storage_report(&DB, &[])?)
        }

        /// Ephemeral event report with optional `window_start_ms` window-start filter.
        /// If `window_start_ms` is newer than the current in-memory window start,
        /// returns an empty report.
        #[::icydb::__reexports::canic_cdk::query]
        pub fn icydb_metrics(window_start_ms: Option<u64>) -> Result<::icydb::obs::EventReport, ::icydb::Error> {
            Ok(::icydb::obs::metrics_report(window_start_ms))
        }

        /// Reset ephemeral event state and refresh `window_start_ms`.
        #[::icydb::__reexports::canic_cdk::update]
        pub fn icydb_metrics_reset() -> Result<(), ::icydb::Error> {
            ::icydb::obs::metrics_reset_all();

            Ok(())
        }

    }
}
