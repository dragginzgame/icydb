mod r#gen;
mod helper;
mod imp;
mod node;
mod trait_kind;
mod types;
mod view;

use crate::node::Def;
use darling::{Error as DarlingError, FromMeta, ast::NestedMeta};
use proc_macro2::Span;
use quote::quote;
use syn::{Attribute, ItemStruct, LitStr, Visibility, parse_macro_input};

///
/// Prelude
///
/// INTERNAL prelude for proc-macro and schema code generation.
/// Pulls in crate helpers, core traits, schema types, and proc-macro essentials.
/// Not exposed outside this crate.
///

mod prelude {
    pub use crate::{
        r#gen::{Imp, Implementor},
        helper::{
            as_tokens, quote_one, quote_option, quote_slice, split_idents, to_path, to_str_lit,
        },
        node::*,
        trait_kind::{TraitBuilder, TraitKind, TraitSet},
        types::TraitStrategy,
    };
    pub use icydb_paths::paths;
    pub use icydb_schema::types::{Cardinality, Primitive, StoreType};

    // proc-macro essentials
    pub use darling::FromMeta;
    pub use proc_macro2::{Span, TokenStream};
    pub use quote::{ToTokens, format_ident, quote};
    pub use serde::{Deserialize, Serialize};
    pub use syn::{Ident, ItemStruct, LitStr, Path};
}

///
/// Node Macros
///

macro_rules! macro_node {
    ($fn_name:ident, $node_type:ty, $gen_type:path) => {
        #[doc = concat!(
            "Schema macro for `",
            stringify!($fn_name),
            "` nodes; validates the annotated public struct and expands to generated code."
        )]
        #[proc_macro_attribute]
        pub fn $fn_name(
            args: proc_macro::TokenStream,
            input: proc_macro::TokenStream,
        ) -> proc_macro::TokenStream {
            match NestedMeta::parse_meta_list(args.into()) {
                Ok(args) => {
                    let item = parse_macro_input!(input as ItemStruct);
                    let comments = extract_comments(&item.attrs);

                    // validate
                    if !matches!(item.vis, Visibility::Public(_)) {
                        return proc_macro::TokenStream::from(
                            DarlingError::custom("expected public visibility").write_errors(),
                        );
                    }

                    // build def
                    let debug = item.attrs.iter().any(|attr| attr.path().is_ident("debug"));
                    let mut node = <$node_type>::from_list(&args).unwrap();
                    node.def = Def::new(item, comments);

                    // instantiate the generator
                    let generator = $gen_type(&node);
                    let q = quote!(#generator);

                    if debug {
                        quote! { compile_error!(stringify! { #q }); }
                    } else {
                        q
                    }
                    .into()
                }
                Err(e) => proc_macro::TokenStream::from(DarlingError::from(e).write_errors()),
            }
        }
    };
}

macro_node!(canister, node::Canister, r#gen::CanisterGen);
macro_node!(entity, node::Entity, r#gen::EntityGen);
macro_node!(enum_, node::Enum, r#gen::EnumGen);
macro_node!(list, node::List, r#gen::ListGen);
macro_node!(map, node::Map, r#gen::MapGen);
macro_node!(newtype, node::Newtype, r#gen::NewtypeGen);
macro_node!(record, node::Record, r#gen::RecordGen);
macro_node!(sanitizer, node::Sanitizer, r#gen::SanitizerGen);
macro_node!(set, node::Set, r#gen::SetGen);
macro_node!(store, node::Store, r#gen::StoreGen);
macro_node!(tuple, node::Tuple, r#gen::TupleGen);
macro_node!(validator, node::Validator, r#gen::ValidatorGen);

///
/// Helper Functions
///

/// Extracts and joins `///` doc comments from a list of attributes into a single `LitStr`.
///
/// Strips leading spaces from each doc line, trims surrounding newlines,
/// and returns `None` if no doc comments are found.
fn extract_comments(attrs: &[Attribute]) -> Option<LitStr> {
    let lines: Vec<String> = attrs
        .iter()
        .filter_map(|attr| match &attr.meta {
            syn::Meta::NameValue(meta) if meta.path.is_ident("doc") => {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(lit_str),
                    ..
                }) = &meta.value
                {
                    let value = lit_str.value();
                    Some(value.strip_prefix(' ').unwrap_or(&value).to_string())
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();

    let cleaned = lines.join("\n").trim_matches('\n').to_string();

    if cleaned.is_empty() {
        None
    } else {
        Some(LitStr::new(&cleaned, Span::call_site()))
    }
}
