use crate::ActorBuilder;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Path, parse_str};

/// Generate all query dispatch glue for the current canister.
///
/// This emits the `dispatch_entity`, `dispatch_load`, `dispatch_save`,
/// and `dispatch_delete` entrypoints that route untyped interface calls
/// (path + query payloads) into strongly-typed database operations.
#[must_use]
pub fn generate(builder: &ActorBuilder) -> TokenStream {
    generate_dispatch(builder)
}

// generate_dispatch
//
// Build the actual match arms and dispatch functions.
// Each entity results in one match arm that constructs an `EntityDispatch`
// value containing closures for load/save/delete on that entity.
fn generate_dispatch(builder: &ActorBuilder) -> TokenStream {
    let entities = builder.get_entities();

    // Generate match arms of the form:
    //
    //   "my.entity.Path" => Ok(EntityDispatch { ... }),
    //
    // Each arm embeds typed closures that call db!().load::<Ty>(), etc.
    let arms = entities.iter().map(|(entity_path, _)| {
        // Parse the fully-qualified Rust type (e.g. "my_canister::types::User")
        let ty: Path =
            parse_str(entity_path).unwrap_or_else(|_| panic!("Invalid path: {entity_path}"));

        quote! {
            #entity_path => Ok(::icydb::core::interface::query::EntityDispatch {
                // Static identity for this entity type
                entity_id: #ty::ENTITY_ID,
                path: #ty::PATH,

                // Load closure: executes the LoadQuery on this entity type.
                load_keys: |query: ::icydb::core::db::query::LoadQuery| -> Result<Vec<::icydb::core::Key>, ::icydb::core::runtime_error::RuntimeError> {
                    db!().load::<#ty>().execute(query).map(|res| res.keys())
                },

                // Save closure: executes a SaveQuery and returns the resulting key.
                save_key: |query: ::icydb::core::db::query::SaveQuery| -> Result<::icydb::core::Key, ::icydb::core::runtime_error::RuntimeError> {
                    db!().save::<#ty>().execute(query).map(|res| res.key())
                },

                // Delete closure: executes DeleteQuery and returns all removed keys.
                delete_keys: |query: ::icydb::core::db::query::DeleteQuery| -> Result<Vec<::icydb::core::Key>, ::icydb::core::runtime_error::RuntimeError> {
                    db!().delete::<#ty>().execute(query).map(|res| res.keys())
                },
            }),
        }
    });

    quote! {
        /// Resolve a path string into an `EntityDispatch` containing typed
        /// closures for the underlying entity. The caller is expected to have
        /// already passed authentication/authorization checks.
        pub(crate) fn dispatch_entity(
            path: &str,
        ) -> Result<::icydb::core::interface::query::EntityDispatch, ::icydb::core::interface::query::QueryError> {
            match path {
                #(#arms)*

                // Unknown entity path
                _ => Err(::icydb::core::interface::query::QueryError::EntityNotFound(path.to_string())),
            }
        }

        /// High-level load dispatcher:
        /// resolves the entity and invokes its typed `load_keys` closure.
        #[allow(dead_code)]
        pub(crate) fn dispatch_load(
            path: &str,
            query: ::icydb::core::db::query::LoadQuery,
        ) -> Result<Vec<::icydb::core::Key>, ::icydb::core::runtime_error::RuntimeError> {
            let dispatch = dispatch_entity(path)?;
            (dispatch.load_keys)(query)
        }

        /// High-level save dispatcher:
        /// resolves the entity and invokes its typed `save_key` closure.
        #[allow(dead_code)]
        pub(crate) fn dispatch_save(
            path: &str,
            query: ::icydb::core::db::query::SaveQuery,
        ) -> Result<::icydb::core::Key, ::icydb::core::runtime_error::RuntimeError> {
            let dispatch = dispatch_entity(path)?;
            (dispatch.save_key)(query)
        }

        /// High-level delete dispatcher:
        /// resolves the entity and invokes its typed `delete_keys` closure.
        #[allow(dead_code)]
        pub(crate) fn dispatch_delete(
            path: &str,
            query: ::icydb::core::db::query::DeleteQuery,
        ) -> Result<Vec<::icydb::core::Key>, ::icydb::core::runtime_error::RuntimeError> {
            let dispatch = dispatch_entity(path)?;
            (dispatch.delete_keys)(query)
        }
    }
}
