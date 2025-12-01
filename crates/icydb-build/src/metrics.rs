use crate::ActorBuilder;
use proc_macro2::TokenStream;
use quote::quote;

// generate
#[must_use]
pub fn generate(builder: &ActorBuilder) -> TokenStream {
    // Build (ENTITY_ID, PATH) mapping for all entities
    let mut pairs: Vec<TokenStream> = Vec::new();
    for (entity_path, _) in builder.get_entities() {
        let entity_ident: syn::Path = syn::parse_str(&entity_path)
            .unwrap_or_else(|_| panic!("invalid entity path: {entity_path}"));
        pairs.push(quote! { (#entity_ident::ENTITY_ID, #entity_ident::PATH) });
    }

    quote! {
        const ICYDB_ENTITY_ID_PATH: &[(u64, &str)] = &[
            #(#pairs),*
        ];

        /// Storage snapshot (live view).
        /// Includes data/index store stats and per-entity breakdown by store.
        #[::icydb::core::export::canic::cdk::query]
        pub fn icydb_snapshot() -> Result<::icydb::core::obs::snapshot::StorageReport, ::icydb::core::Error> {
            Ok(::icydb::core::obs::snapshot::storage_report(&DB, ICYDB_ENTITY_ID_PATH))
        }

        /// Ephemeral event report since the internal `since_ms` (counters + per-entity summaries).
        #[::icydb::core::export::canic::cdk::query]
        pub fn icydb_metrics() -> Result<::icydb::core::obs::metrics::EventReport, ::icydb::core::Error> {
            Ok(::icydb::core::obs::metrics::report())
        }

        /// Reset ephemeral event state and refresh `since_ms`.
        #[::icydb::core::export::canic::cdk::update]
        pub fn icydb_metrics_reset() -> Result<(), ::icydb::core::Error> {
            ::icydb::core::obs::metrics::reset_all();

            Ok(())
        }

    }
}
