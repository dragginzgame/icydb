use crate::ActorBuilder;
use proc_macro2::TokenStream;
use quote::quote;
use syn::parse_str;

/// Render the metrics/snapshot endpoints for a canister actor.
#[must_use]
pub fn generate(builder: &ActorBuilder) -> TokenStream {
    let sql_test_endpoint = sql_test_endpoint(builder);

    quote! {
        /// Storage snapshot (live view).
        /// Includes data/index store stats and per-entity breakdown by store.
        #[::icydb::__reexports::canic_cdk::query]
        pub fn icydb_snapshot() -> Result<::icydb::db::StorageReport, ::icydb::Error> {
            Ok(db().storage_report(&[])?)
        }

        /// Ephemeral event report with optional `window_start_ms` window-start filter.
        /// If `window_start_ms` is newer than the current in-memory window start,
        /// returns an empty report.
        #[::icydb::__reexports::canic_cdk::query]
        pub fn icydb_metrics(window_start_ms: Option<u64>) -> Result<::icydb::metrics::EventReport, ::icydb::Error> {
            Ok(::icydb::metrics::metrics_report(window_start_ms))
        }

        /// Reset ephemeral event state and refresh `window_start_ms`.
        #[::icydb::__reexports::canic_cdk::update]
        pub fn icydb_metrics_reset() -> Result<(), ::icydb::Error> {
            ::icydb::metrics::metrics_reset_all();

            Ok(())
        }

        #sql_test_endpoint

    }
}

fn sql_test_endpoint(builder: &ActorBuilder) -> TokenStream {
    // Keep this endpoint scoped to the generated test canister surface only.
    if builder.canister.def().ident() != "TestCanister" {
        return quote! {};
    }

    let canister_path: syn::Path = parse_str(&builder.canister.def().path())
        .unwrap_or_else(|_| panic!("invalid canister path: {}", builder.canister.def().path()));

    let mut dispatch_arms = quote! {};
    let mut supported_names = quote! {};

    for (entity_path, entity) in builder.get_entities() {
        let entity_ty: syn::Path = parse_str(&entity_path)
            .unwrap_or_else(|_| panic!("invalid entity path: {entity_path}"));
        let entity_path_lit = entity_path.as_str();
        let entity_ident_lit = entity.def().ident();
        let entity_resolved_name_lit = entity.resolved_name();

        dispatch_arms.extend(quote! {
            #entity_path_lit | #entity_ident_lit | #entity_resolved_name_lit => {
                icydb_sql_execute_for_entity::<#entity_ty>(sql.as_str())
            }
        });

        supported_names.extend(quote! {
            #entity_path_lit.to_string(),
            #entity_ident_lit.to_string(),
            #entity_resolved_name_lit.to_string(),
        });
    }

    quote! {
        // Execute one SQL query for one concrete entity and render deterministic text output.
        fn icydb_sql_execute_for_entity<E>(sql: &str) -> Result<String, ::icydb::Error>
        where
            E: ::icydb::traits::EntityKind<Canister = #canister_path>
                + ::icydb::traits::EntityValue,
        {
            let sql_trimmed = sql.trim();
            if sql_trimmed.is_empty() {
                return Err(::icydb::Error::new(
                    ::icydb::error::ErrorKind::Runtime(
                        ::icydb::error::RuntimeErrorKind::Unsupported
                    ),
                    ::icydb::error::ErrorOrigin::Query,
                    "sql endpoint requires a non-empty query string",
                ));
            }

            if sql_trimmed.to_ascii_uppercase().starts_with("EXPLAIN ") {
                return Ok(db().explain_sql::<E>(sql_trimmed)?);
            }

            use ::std::fmt::Write as _;

            let projection = db().execute_sql_projection::<E>(sql_trimmed)?;
            let mut out = String::new();
            writeln!(&mut out, "surface=projection")
                .expect("writing projection surface header should not fail");
            writeln!(&mut out, "row_count={}", projection.count())
                .expect("writing projection row-count should not fail");

            for (row_index, row) in projection.into_iter().enumerate() {
                writeln!(&mut out, "row[{row_index}]={:?}", row.values())
                    .expect("writing projection row should not fail");
            }

            Ok(out)
        }

        /// Return the entity names accepted by the `sql(...)` test endpoint.
        #[::icydb::__reexports::canic_cdk::query]
        pub fn icydb_sql_entities() -> Vec<String> {
            vec![#supported_names]
        }

        /// Test-only SQL endpoint for the generated test canister.
        ///
        /// This endpoint intentionally returns text output for quick manual
        /// verification, and is restricted to the test canister actor surface.
        #[::icydb::__reexports::canic_cdk::query]
        pub fn sql(entity: String, sql: String) -> Result<String, ::icydb::Error> {
            let entity_name = entity.trim();

            match entity_name {
                #dispatch_arms
                _ => {
                    let supported = icydb_sql_entities().join(", ");
                    Err(::icydb::Error::new(
                        ::icydb::error::ErrorKind::Runtime(
                            ::icydb::error::RuntimeErrorKind::Unsupported
                        ),
                        ::icydb::error::ErrorOrigin::Query,
                        format!(
                            "unknown entity '{entity_name}' for sql endpoint; supported: {supported}",
                        ),
                    ))
                }
            }
        }
    }
}
