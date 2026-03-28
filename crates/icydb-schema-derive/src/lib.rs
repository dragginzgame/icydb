mod r#gen;
mod helper;
mod imp;
mod node;
mod trait_kind;
mod types;
mod validate;

use crate::node::{Def, ValidateNode};
use darling::{Error as DarlingError, FromMeta, ast::NestedMeta};
use quote::quote;
use syn::{ItemStruct, Visibility, parse_macro_input};

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
        helper::{quote_one, quote_option, quote_slice, split_idents, to_path, to_str_lit},
        node::*,
        trait_kind::{TraitBuilder, TraitKind},
        types::TraitStrategy,
    };
    pub use icydb_schema::{
        MAX_ENTITY_NAME_LEN, MAX_FIELD_NAME_LEN, MAX_INDEX_FIELDS, MAX_INDEX_NAME_LEN,
        types::{Cardinality, Primitive},
    };

    // proc-macro essentials
    pub use darling::{Error as DarlingError, FromMeta};
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

                    // validate
                    if !matches!(item.vis, Visibility::Public(_)) {
                        return proc_macro::TokenStream::from(
                            DarlingError::custom("expected public visibility").write_errors(),
                        );
                    }

                    // build def
                    let debug = item.attrs.iter().any(|attr| attr.path().is_ident("debug"));
                    let mut node = match <$node_type>::from_list(&args) {
                        Ok(node) => node,
                        Err(err) => return proc_macro::TokenStream::from(err.write_errors()),
                    };
                    node.def = Def::new(item);
                    if let Err(err) = node.validate() {
                        return proc_macro::TokenStream::from(err.write_errors());
                    }

                    // fatal schema errors
                    let fatal_errors = node.fatal_errors();
                    if !fatal_errors.is_empty() {
                        let tokens: proc_macro2::TokenStream = fatal_errors
                            .into_iter()
                            .map(|err| err.to_compile_error())
                            .collect();

                        return tokens.into();
                    }

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
