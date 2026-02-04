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
    let mut data_inits = quote!();
    let mut index_inits = quote!();
    // -------------------------
    // Data stores
    // -------------------------

    for (store_path, store) in builder.get_data_stores() {
        let cell_ident = format_ident!("{}", store.ident);
        let memory_id = store.memory_id;
        let store_path_lit = store_path;

        data_defs.extend(quote! {
            ::icydb::__reexports::canic_memory::eager_static! {
                static #cell_ident: ::std::cell::RefCell<
                    ::icydb::__internal::core::db::store::DataStore
                > = ::std::cell::RefCell::new(
                    ::icydb::__internal::core::db::store::DataStore::init(
                        ::icydb::__reexports::canic_memory::ic_memory!(
                            ::icydb::__internal::core::db::store::DataStore,
                            #memory_id
                        )
                    )
                );
            }
        });

        data_inits.extend(quote! {
            reg.register(#store_path_lit, &#cell_ident);
        });
    }

    // -------------------------
    // Index stores
    // -------------------------

    for (store_path, store) in builder.get_index_stores() {
        let cell_ident = format_ident!("{}", store.ident);
        let entry_memory_id = store.entry_memory_id;
        let fingerprint_memory_id = store.fingerprint_memory_id;
        let store_path_lit = store_path;

        index_defs.extend(quote! {
            ::icydb::__reexports::canic_memory::eager_static! {
                static #cell_ident: ::std::cell::RefCell<
                    ::icydb::__internal::core::db::index::IndexStore
                > = ::std::cell::RefCell::new(
                    ::icydb::__internal::core::db::index::IndexStore::init(
                        ::icydb::__reexports::canic_memory::ic_memory!(
                            ::icydb::__internal::core::db::index::IndexStore,
                            #entry_memory_id
                        ),
                        ::icydb::__reexports::canic_memory::ic_memory!(
                            ::icydb::__internal::core::db::index::IndexStore,
                            #fingerprint_memory_id
                        )
                    )
                );
            }
        });

        index_inits.extend(quote! {
            reg.register(#store_path_lit, &#cell_ident);
        });
    }

    // -------------------------
    // Canister + DB wiring
    // -------------------------

    let canister = &builder.canister;
    let canister_path: syn::Path = parse_str(&canister.def.path())
        .unwrap_or_else(|_| panic!("invalid canister path: {}", canister.def.path()));

    let memory_min = canister.memory_min;
    let memory_max = canister.memory_max;

    quote! {
        #data_defs
        #index_defs
        thread_local! {
            #[allow(unused_mut)]
            #[allow(clippy::let_and_return)]
            static DATA_REGISTRY:
                ::icydb::__internal::core::db::store::DataStoreRegistry =
            {
                let mut reg =
                    ::icydb::__internal::core::db::store::DataStoreRegistry::new();
                #data_inits
                reg
            };

            #[allow(unused_mut)]
            #[allow(clippy::let_and_return)]
            static INDEX_REGISTRY:
                ::icydb::__internal::core::db::index::IndexStoreRegistry =
            {
                let mut reg =
                    ::icydb::__internal::core::db::index::IndexStoreRegistry::new();
                #index_inits
                reg
            };
        }

        static DB: ::icydb::__internal::core::db::Db<#canister_path> =
            ::icydb::__internal::core::db::Db::<#canister_path>::new(
                &DATA_REGISTRY,
                &INDEX_REGISTRY
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
