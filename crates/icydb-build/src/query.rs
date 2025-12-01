use crate::ActorBuilder;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Path, parse_str};

// generate
#[must_use]
pub fn generate(builder: &ActorBuilder) -> TokenStream {
    let mut tokens = quote!();

    tokens.extend(generate_query("icydb_query_load", builder, QueryKind::Load));
    tokens.extend(generate_query("icydb_query_save", builder, QueryKind::Save));
    tokens.extend(generate_query(
        "icydb_query_delete",
        builder,
        QueryKind::Delete,
    ));

    tokens
}

enum QueryKind {
    Load,
    Save,
    Delete,
}

// generate_query
fn generate_query(name: &str, builder: &ActorBuilder, kind: QueryKind) -> TokenStream {
    let entities = builder.get_entities();

    let match_arms = if entities.is_empty() {
        quote! {
            Err(::icydb::core::interface::query::QueryError::EntityNotFound(path))?
        }
    } else {
        let arms = entities.iter().map(|(entity_path, _)| {
            let ty: Path =
                parse_str(entity_path).unwrap_or_else(|_| panic!("Invalid path: {entity_path}"));

            match kind {
                QueryKind::Load => quote! {
                    #entity_path => db!().load::<#ty>().execute(query)?.keys(),
                },
                QueryKind::Delete => quote! {
                    #entity_path => db!().delete::<#ty>().execute(query)?.keys(),
                },
                QueryKind::Save => quote! {
                    #entity_path => db!().save::<#ty>().execute(query)?.key(),
                },
            }
        });

        quote! {
            let res = match path.as_str() {
                #(#arms)*
                _ => Err(::icydb::core::interface::query::QueryError::EntityNotFound(path))?,
            };

            Ok(res)
        }
    };

    // generate the fn
    let fn_name = quote::format_ident!("{name}");
    let fn_sig = match kind {
        QueryKind::Load => quote! {
            #[::icydb::core::export::canic::cdk::query]
            pub fn #fn_name(
                path: String,
                query: ::icydb::core::db::query::LoadQuery,
            ) -> Result<Vec<::icydb::core::Key>, ::icydb::core::Error>
        },

        QueryKind::Save => quote! {
            #[::icydb::core::export::canic::cdk::update]
            pub fn #fn_name(
                path: String,
                query: ::icydb::core::db::query::SaveQuery,
            ) -> Result<::icydb::core::Key, ::icydb::core::Error>
        },

        QueryKind::Delete => quote! {
           #[::icydb::core::export::canic::cdk::update]
            pub fn #fn_name(
                path: String,
                query: ::icydb::core::db::query::DeleteQuery,
            ) -> Result<Vec<::icydb::core::Key>, ::icydb::core::Error>
        },
    };

    quote! {
        #[allow(unused_variables)]
        #fn_sig {
            #match_arms
        }
    }
}
