use crate::ActorBuilder;
use proc_macro2::TokenStream;
use quote::quote;

/// Render the metrics/snapshot endpoints for a canister actor.
#[must_use]
pub fn generate(_builder: &ActorBuilder) -> TokenStream {
    quote! {
        #[::icydb::__reexports::canic_cdk::query]
        pub fn icydb_snapshot() -> Result<::icydb::db::StorageReport, ::icydb::Error> {
            ::icydb::__macro::execute_generated_storage_report(&db())
        }

        #[::icydb::__reexports::canic_cdk::query]
        pub fn icydb_metrics(window_start_ms: Option<u64>) -> Result<::icydb::metrics::EventReport, ::icydb::Error> {
            Ok(::icydb::metrics::metrics_report(window_start_ms))
        }

        #[::icydb::__reexports::canic_cdk::update]
        pub fn icydb_metrics_reset() -> Result<(), ::icydb::Error> {
            ::icydb::metrics::metrics_reset_all();

            Ok(())
        }
    }
}
