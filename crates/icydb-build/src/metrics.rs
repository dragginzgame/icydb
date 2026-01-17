use crate::ActorBuilder;
use proc_macro2::TokenStream;
use quote::quote;

// generate
#[must_use]
/// Render the metrics/snapshot endpoints for a canister actor.
pub fn generate(builder: &ActorBuilder) -> TokenStream {
    // Build (ENTITY_NAME, PATH) mapping for all entities
    let mut pairs: Vec<TokenStream> = Vec::new();
    for (entity_path, _) in builder.get_entities() {
        let entity_ident: syn::Path = syn::parse_str(&entity_path)
            .unwrap_or_else(|_| panic!("invalid entity path: {entity_path}"));
        pairs.push(quote! { (#entity_ident::ENTITY_NAME, #entity_ident::PATH) });
    }

    quote! {
        const ICYDB_ENTITY_NAME_PATH: &[(&str, &str)] = &[
            #(#pairs),*
        ];

        /// Storage snapshot (live view).
        /// Includes data/index store stats and per-entity breakdown by store.
        #[::icydb::__reexports::canic_cdk::query]
        pub fn icydb_snapshot() -> Result<::icydb::obs::snapshot::StorageReport, ::icydb::Error> {
            Ok(::icydb::obs::snapshot::storage_report(&DB, ICYDB_ENTITY_NAME_PATH))
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
