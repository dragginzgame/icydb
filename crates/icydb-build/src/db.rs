use crate::ActorBuilder;
use icydb_schema::types::StoreType;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::parse_str;

///
/// Entry point for codegen.
/// Expands into:
/// - thread-local Data/Index store definitions
/// - registry wiring
/// - a global `db()` accessor
///
#[must_use]
pub fn generate(builder: &ActorBuilder) -> TokenStream {
    let mut tokens = quote!();

    // Generate store definitions + registries
    tokens.extend(stores(builder));

    tokens
}

///
/// Generate thread-local DataStores and IndexStores for this canister,
/// along with their registries and the `db()` accessor.
///
fn stores(builder: &ActorBuilder) -> TokenStream {
    let mut data_defs = quote!();
    let mut index_defs = quote!();
    let mut data_inits = quote!();
    let mut index_inits = quote!();

    for (store_path, store) in builder.get_stores() {
        let cell_ident = format_ident!("{}", &store.ident);
        let memory_id = store.memory_id;
        let store_path_lit = store_path;

        if matches!(store.ty, StoreType::Index) {
            // Index store
            index_defs.extend(quote! {
                ::canic::eager_static! {
                    static #cell_ident: ::std::cell::RefCell<::icydb::core::db::store::IndexStore> =
                        ::std::cell::RefCell::new(::icydb::core::db::store::IndexStore::init(
                            ::canic::ic_memory!(::icydb::core::db::store::IndexStore, #memory_id)
                        ));
                }
            });

            index_inits.extend(quote! {
                reg.register(#store_path_lit, &#cell_ident);
            });
        } else {
            // Data store
            data_defs.extend(quote! {
                ::canic::eager_static! {
                    static #cell_ident: ::std::cell::RefCell<::icydb::core::db::store::DataStore> =
                        ::std::cell::RefCell::new(::icydb::core::db::store::DataStore::init(
                            ::canic::ic_memory!(::icydb::core::db::store::DataStore, #memory_id)
                        ));
                }
            });

            data_inits.extend(quote! {
                reg.register(#store_path_lit, &#cell_ident);
            });
        }
    }

    let canister = &builder.canister;
    let canister_path: syn::Path = parse_str(&canister.def.path())
        .unwrap_or_else(|_| panic!("invalid canister path: {}", builder.canister.def.path()));
    let memory_min = canister.memory_min;
    let memory_max = canister.memory_max;

    quote! {
        #data_defs
        #index_defs

        thread_local! {
            // registries
            #[allow(unused_mut)]
            #[allow(clippy::let_and_return)]
            static DATA_REGISTRY: ::icydb::core::db::store::DataStoreRegistry = {
                let mut reg = ::icydb::core::db::store::DataStoreRegistry::new();
                #data_inits
                reg
            };

            #[allow(unused_mut)]
            #[allow(clippy::let_and_return)]
            static INDEX_REGISTRY: ::icydb::core::db::store::IndexStoreRegistry = {
                let mut reg = ::icydb::core::db::store::IndexStoreRegistry::new();
                #index_inits
                reg
            };
        }

        static DB: ::icydb::core::db::Db<#canister_path> =
            ::icydb::core::db::Db::<#canister_path>::new(&DATA_REGISTRY, &INDEX_REGISTRY);

        // reserve the ic memory range
        ::canic::eager_init!({
            ::canic::ic_memory_range!(#memory_min, #memory_max);
        });

        /// Global accessor (fat handle) for this canister’s DB
        #[must_use]
        pub const fn db() -> ::icydb::core::db::DbSession<#canister_path> {
            ::icydb::core::db::DbSession::new(DB)
        }
    }
}
