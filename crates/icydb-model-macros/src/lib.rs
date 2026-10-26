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
    crate_path::CratePathOverrides,
    node::{Def, HasDef, ValidateNode},
};
use darling::{Error as DarlingError, FromMeta, ast::NestedMeta};
use quote::quote;
use syn::{ItemStruct, Visibility, parse_macro_input};

/// Install one aggregate IcyDB execution root for a sync or async entry point.
///
/// Place this outside the framework export attribute so IC-CDK, Canic,
/// lifecycle, and timer handlers all use the same boundary implementation.
///
/// Async functions bind their request root when first polled, not when the
/// future is constructed. Nested calls reuse the active root. Keep a started
/// future within that request; do not share it across independent request roots
/// through a singleflight or shared-future cache. Share completed application
/// data instead, leaving each request responsible for its own database work.
///
/// # Panics
///
/// An async function panics when its started future is polled beneath a
/// different active request root. This refuses to charge one request's work
/// to another request's counters. See `icydb::db::with_request_execution_async`.
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
///
/// The entire test body shares one aggregate request budget. Nested
/// `icydb::db::with_request_execution` calls reuse that budget; they do not
/// start separate simulated messages or reset counters.
///
/// For a test that simulates multiple IC messages, use plain `#[test]` and
/// wrap each message separately in `icydb::db::with_request_execution`.
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
        crate_path::CratePaths,
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
    ($fn_name:ident, $node_type:ty $(, $configure:expr)?) => {
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

                    // Hook macros preserve the authored struct, including its fields and generics.
                    if !matches!(stringify!($fn_name), "validator" | "normalizer")
                        && let Err(error) = Def::validate_declaration(&item) {
                        return error.write_errors().into();
                    }

                    // build def
                    let debug = item.attrs.iter().any(|attr| attr.path().is_ident("debug"));
                    let mut node = match <$node_type>::from_list(&args) {
                        Ok(node) => node,
                        Err(err) => return proc_macro::TokenStream::from(err.write_errors()),
                    };
                    let paths = match crate_paths.resolve() {
                        Ok(paths) => paths,
                        Err(err) => return err.write_errors().into(),
                    };
                    node.def = Def::new(item, paths);
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

                    // Emit only framework-owned references through the resolved paths.
                    let q = node.generate_tokens(node.def().paths());

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

macro_node!(canister, node::Canister);
macro_node!(
    entity,
    node::Entity,
    |entity: &mut node::Entity, has_runtime| entity.emit_runtime_adapters = has_runtime
);
macro_node!(enum_, node::Enum);
macro_node!(list, node::List);
macro_node!(map, node::Map);
macro_node!(newtype, node::Newtype);
macro_node!(
    record,
    node::Record,
    |record: &mut node::Record, has_runtime| record.emit_runtime_references = has_runtime
);
macro_node!(normalizer, node::Normalizer);
macro_node!(set, node::Set);
macro_node!(store, node::Store);
macro_node!(tuple, node::Tuple);
macro_node!(validator, node::Validator);

/// Derive addition for one-field application wrappers.
#[proc_macro_derive(Add)]
pub fn derive_add(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_add(input.into()).into()
}

/// Derive additive assignment for one-field application wrappers.
#[proc_macro_derive(AddAssign)]
pub fn derive_add_assign(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_add_assign(input.into()).into()
}

/// Derive subtraction for one-field application wrappers.
#[proc_macro_derive(Sub)]
pub fn derive_sub(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_sub(input.into()).into()
}

/// Derive subtractive assignment for one-field application wrappers.
#[proc_macro_derive(SubAssign)]
pub fn derive_sub_assign(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_sub_assign(input.into()).into()
}

/// Derive multiplication for one-field application wrappers.
#[proc_macro_derive(Mul)]
pub fn derive_mul(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_mul(input.into()).into()
}

/// Derive multiplicative assignment for one-field application wrappers.
#[proc_macro_derive(MulAssign)]
pub fn derive_mul_assign(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_mul_assign(input.into()).into()
}

/// Derive division for one-field application wrappers.
#[proc_macro_derive(Div)]
pub fn derive_div(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_div(input.into()).into()
}

/// Derive divisive assignment for one-field application wrappers.
#[proc_macro_derive(DivAssign)]
pub fn derive_div_assign(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_div_assign(input.into()).into()
}

/// Derive remainder for one-field application wrappers.
#[proc_macro_derive(Rem)]
pub fn derive_rem(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_rem(input.into()).into()
}

/// Derive remainder assignment for one-field application wrappers.
#[proc_macro_derive(RemAssign)]
pub fn derive_rem_assign(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_rem_assign(input.into()).into()
}

/// Derive negation for one-field application wrappers.
#[proc_macro_derive(Neg)]
pub fn derive_neg(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_neg(input.into()).into()
}

/// Derive iterator multiplication for one-field application wrappers.
#[proc_macro_derive(Product)]
pub fn derive_product(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_product(input.into()).into()
}

/// Derive iterator summation for one-field application wrappers.
#[proc_macro_derive(Sum)]
pub fn derive_sum(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_ops::derive_sum(input.into()).into()
}

/// Derive display forwarding for one-field application wrappers.
#[proc_macro_derive(Display)]
pub fn derive_display(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_display::derive_display(input.into()).into()
}

/// Derive borrowed and consuming inner access for one-field wrappers.
#[proc_macro_derive(Inner)]
pub fn derive_inner(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    helper_inner::derive_inner(input.into()).into()
}
