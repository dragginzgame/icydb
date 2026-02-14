use crate::ActorBuilder;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse_str;

#[must_use]
pub fn generate(builder: &ActorBuilder) -> TokenStream {
    let mut tokens = quote!();
    tokens.extend(stores(builder));
    tokens
}

fn stores(builder: &ActorBuilder) -> TokenStream {
    let mut data_defs = quote!();
    let mut index_defs = quote!();
    let mut store_inits = quote!();
    let stores = builder.get_stores();
    // -------------------------
    // Store registry
    // -------------------------

    for (store_path, store) in &stores {
        let data_cell_ident = format_ident!("{}_DATA", store.ident);
        let index_cell_ident = format_ident!("{}_INDEX", store.ident);
        let data_memory_id = store.data_memory_id;
        let index_memory_id = store.index_memory_id;
        let store_path_lit = store_path.as_str();

        data_defs.extend(quote! {
            ::icydb::__reexports::canic_memory::eager_static! {
                static #data_cell_ident: ::std::cell::RefCell<
                    ::icydb::__internal::core::db::store::DataStore
                > = ::std::cell::RefCell::new(
                    ::icydb::__internal::core::db::store::DataStore::init(
                        ::icydb::__reexports::canic_memory::ic_memory!(
                            ::icydb::__internal::core::db::store::DataStore,
                            #data_memory_id
                        )
                    )
                );
            }
        });

        index_defs.extend(quote! {
            ::icydb::__reexports::canic_memory::eager_static! {
                static #index_cell_ident: ::std::cell::RefCell<
                    ::icydb::__internal::core::db::index::IndexStore
                > = ::std::cell::RefCell::new(
                    ::icydb::__internal::core::db::index::IndexStore::init(
                        ::icydb::__reexports::canic_memory::ic_memory!(
                            ::icydb::__internal::core::db::index::IndexStore,
                            #index_memory_id
                        )
                    )
                );
            }
        });

        store_inits.extend(quote! {
            reg.register_store(#store_path_lit, &#data_cell_ident, &#index_cell_ident);
        });
    }

    // -------------------------
    // Canister + DB wiring
    // -------------------------

    let canister = &builder.canister;
    let canister_path: syn::Path = parse_str(&canister.def.path())
        .unwrap_or_else(|_| panic!("invalid canister path: {}", canister.def.path()));

    let relation_validators = strong_relation_delete_validators(builder, &canister_path);
    let memory_min = canister.memory_min;
    let memory_max = canister.memory_max;

    quote! {
        #data_defs
        #index_defs
        #relation_validators
        thread_local! {
            #[allow(unused_mut)]
            #[allow(clippy::let_and_return)]
            static STORE_REGISTRY:
                ::icydb::__internal::core::db::store::StoreRegistry =
            {
                let mut reg =
                    ::icydb::__internal::core::db::store::StoreRegistry::new();
                #store_inits
                reg
            };
        }

        static DB: ::icydb::__internal::core::db::Db<#canister_path> =
            ::icydb::__internal::core::db::Db::<#canister_path>::new_with_relations(
                &STORE_REGISTRY,
                STRONG_RELATION_DELETE_VALIDATORS
            );

        // reserve the ic memory range
        ::icydb::__reexports::canic_memory::eager_init!({
            ::icydb::__reexports::canic_memory::ic_memory_range!(
                #memory_min,
                #memory_max
            );
        });

        /// Global accessor (fat handle) for this canister’s DB.
        /// This is the **only** API applications should use.
        #[must_use]
        pub const fn db() -> ::icydb::db::DbSession<#canister_path> {
            ::icydb::db::DbSession::new(DB)
        }
    }
}

fn strong_relation_delete_validators(
    builder: &ActorBuilder,
    canister_path: &syn::Path,
) -> TokenStream {
    let mut validator_defs = quote!();
    let mut validator_inits = quote!();

    for (i, (entity_path, _)) in builder.get_entities().into_iter().enumerate() {
        let source_path: syn::Path = parse_str(&entity_path)
            .unwrap_or_else(|_| panic!("invalid entity path: {entity_path}"));
        let fn_ident = format_ident!("__icydb_delete_relation_validator_{i}");

        validator_defs.extend(quote! {
            fn #fn_ident(
                db: &::icydb::__internal::core::db::Db<#canister_path>,
                target_path: &str,
                deleted_target_keys: &::std::collections::BTreeSet<
                    ::icydb::__internal::core::db::store::RawDataKey
                >,
            ) -> ::std::result::Result<(), ::icydb::__internal::core::error::InternalError> {
                ::icydb::__internal::core::db::validate_delete_strong_relations_for_source::<#source_path>(
                    db,
                    target_path,
                    deleted_target_keys,
                )
            }
        });
        validator_inits.extend(quote! {
            ::icydb::__internal::core::db::StrongRelationDeleteValidator::<#canister_path>::new(
                #fn_ident
            ),
        });
    }

    quote! {
        #validator_defs

        static STRONG_RELATION_DELETE_VALIDATORS: &[
            ::icydb::__internal::core::db::StrongRelationDeleteValidator<#canister_path>
        ] = &[
            #validator_inits
        ];
    }
}
