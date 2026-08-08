//! Module: lib
//! Responsibility: application-model declarations plus thin runtime entry attributes.
//! Does not own: runtime schema semantics.
//! Boundary: macro input to generated tokens.

#![doc = include_str!("../README.md")]

mod authoring_types;
mod case;
mod crate_path;
mod r#gen;
mod helper;
mod helper_deref;
mod helper_display;
mod helper_inner;
mod helper_newtype;
mod helper_ops;
mod imp;
mod node;
mod predicate;
mod request_execution;
mod trait_kind;
mod types;
mod validate;

use crate::{
    crate_path::{CratePathOverrides, rewrite_generated_paths},
    node::{Def, ValidateNode},
};
use darling::{Error as DarlingError, FromMeta, ast::NestedMeta};
use quote::quote;
use syn::{ItemStruct, Visibility, parse_macro_input};

/// Install one aggregate IcyDB execution root for a sync or async entry point.
///
/// Place this outside the framework export attribute so IC-CDK, Canic,
/// lifecycle, and timer handlers all use the same boundary implementation.
#[proc_macro_attribute]
pub fn request_execution(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    request_execution::expand_request_execution(args.into(), input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Declare a synchronous unit test with the production request boundary.
#[proc_macro_attribute]
pub fn test(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    request_execution::expand_test(args.into(), input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Derive `Deref` for a one-field application wrapper.
#[proc_macro_derive(Deref)]
pub fn derive_deref(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_deref::derive_deref(input.into()).into()
}

/// Derive `DerefMut` for a one-field application wrapper.
#[proc_macro_derive(DerefMut)]
pub fn derive_deref_mut(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_deref::derive_deref_mut(input.into()).into()
}

///
/// Prelude
///
/// Internal proc-macro prelude shared by node parsing and generator code.
/// Keeps proc-macro essentials and schema helpers in one crate-local bundle.
/// This is not part of the external derive surface.
///

mod prelude {
    pub use crate::authoring_types::{Cardinality, Primitive};
    pub use crate::{
        r#gen::{Imp, Implementor},
        helper::{quote_one, quote_option, quote_slice, to_path, to_str_lit},
        node::*,
        trait_kind::{
            ApplicationTypeKind, TraitBuilder, TraitKind, TraitSet, application_type_trait_set,
            generated_node_trait_set,
        },
        types::TraitStrategy,
    };

    pub const MAX_FIELD_NAME_LEN: usize = 64;

    // proc-macro essentials
    pub use darling::{Error as DarlingError, FromMeta};
    pub use proc_macro2::{Span, TokenStream};
    pub use quote::{ToTokens, format_ident, quote};
    pub use serde::Deserialize;
    pub use syn::{Ident, ItemStruct, LitStr, Path};
}

///
/// Node Macros
///

macro_rules! macro_node {
    ($fn_name:ident, $node_type:ty, $gen_type:path $(, $configure:expr)?) => {
        #[proc_macro_attribute]
        pub fn $fn_name(
            args: proc_macro::TokenStream,
            input: proc_macro::TokenStream,
        ) -> proc_macro::TokenStream {
            match NestedMeta::parse_meta_list(args.into()) {
                Ok(mut args) => {
                    let crate_paths = match CratePathOverrides::extract(&mut args) {
                        Ok(crate_paths) => crate_paths,
                        Err(err) => return err.write_errors().into(),
                    };
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
                    $(($configure)(&mut node, CratePathOverrides::has_icydb_runtime());)?
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
                    let q = match rewrite_generated_paths(quote!(#generator), &crate_paths) {
                        Ok(tokens) => tokens,
                        Err(err) => return err.write_errors().into(),
                    };

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
macro_node!(
    entity,
    node::Entity,
    r#gen::EntityGen,
    |entity: &mut node::Entity, has_runtime| entity.emit_runtime_adapters = has_runtime
);
macro_node!(enum_, node::Enum, r#gen::EnumGen);
macro_node!(list, node::List, r#gen::ListGen);
macro_node!(map, node::Map, r#gen::MapGen);
macro_node!(newtype, node::Newtype, r#gen::NewtypeGen);
macro_node!(record, node::Record, r#gen::RecordGen);
macro_node!(normalizer, node::Normalizer, r#gen::NormalizerGen);
macro_node!(set, node::Set, r#gen::SetGen);
macro_node!(store, node::Store, r#gen::StoreGen);
macro_node!(tuple, node::Tuple, r#gen::TupleGen);
macro_node!(validator, node::Validator, r#gen::ValidatorGen);

/// Derive addition for one-field application wrappers.
#[proc_macro_derive(Add)]
pub fn derive_add(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_add(input.into()))
}

/// Derive additive assignment for one-field application wrappers.
#[proc_macro_derive(AddAssign)]
pub fn derive_add_assign(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_add_assign(input.into()))
}

/// Derive subtraction for one-field application wrappers.
#[proc_macro_derive(Sub)]
pub fn derive_sub(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_sub(input.into()))
}

/// Derive subtractive assignment for one-field application wrappers.
#[proc_macro_derive(SubAssign)]
pub fn derive_sub_assign(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_sub_assign(input.into()))
}

/// Derive multiplication for one-field application wrappers.
#[proc_macro_derive(Mul)]
pub fn derive_mul(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_mul(input.into()))
}

/// Derive multiplicative assignment for one-field application wrappers.
#[proc_macro_derive(MulAssign)]
pub fn derive_mul_assign(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_mul_assign(input.into()))
}

/// Derive division for one-field application wrappers.
#[proc_macro_derive(Div)]
pub fn derive_div(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_div(input.into()))
}

/// Derive divisive assignment for one-field application wrappers.
#[proc_macro_derive(DivAssign)]
pub fn derive_div_assign(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_div_assign(input.into()))
}

/// Derive remainder for one-field application wrappers.
#[proc_macro_derive(Rem)]
pub fn derive_rem(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_rem(input.into()))
}

/// Derive remainder assignment for one-field application wrappers.
#[proc_macro_derive(RemAssign)]
pub fn derive_rem_assign(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_rem_assign(input.into()))
}

/// Derive negation for one-field application wrappers.
#[proc_macro_derive(Neg)]
pub fn derive_neg(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_neg(input.into()))
}

/// Derive iterator multiplication for one-field application wrappers.
#[proc_macro_derive(Product)]
pub fn derive_product(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_product(input.into()))
}

/// Derive iterator summation for one-field application wrappers.
#[proc_macro_derive(Sum)]
pub fn derive_sum(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_ops::derive_sum(input.into()))
}

/// Derive display forwarding for one-field application wrappers.
#[proc_macro_derive(Display)]
pub fn derive_display(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_display::derive_display(input.into()))
}

/// Derive borrowed and consuming inner access for one-field wrappers.
#[proc_macro_derive(Inner)]
pub fn derive_inner(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    finish_helper_derive(helper_inner::derive_inner(input.into()))
}

fn finish_helper_derive(tokens: proc_macro2::TokenStream) -> proc_macro::TokenStream {
    match rewrite_generated_paths(tokens, &CratePathOverrides::default()) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.write_errors().into(),
    }
}
