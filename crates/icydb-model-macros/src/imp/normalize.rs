//! Module: imp::normalize
//! Responsibility: generated implementation tokens.
//! Does not own: runtime trait semantics.
//! Boundary: parsed nodes to impl tokens.

use crate::{imp::field_walk::field_walk_bindings, prelude::*};

/// ---------------------------------------------------------------------------
/// NormalizeAuto
/// ---------------------------------------------------------------------------

pub struct NormalizeAutoTrait;

/// Each node type can emit normalizer code for its *own value only*.
/// Traversal into children is handled by the visitor.

pub trait NormalizeAutoFn {
    fn self_tokens(_: &Self) -> TokenStream {
        quote!()
    }
}

macro_rules! impl_normalize_auto {
    ($($ty:ty),* $(,)?) => {
        $(impl Imp<$ty> for NormalizeAutoTrait {
            fn strategy(node: &$ty) -> Option<TraitStrategy> {
                let self_tokens = NormalizeAutoFn::self_tokens(node);

                let tokens = Implementor::new(node.def(), TraitKind::NormalizeAuto)
                    .add_tokens(self_tokens)
                    .to_token_stream();

                Some(TraitStrategy::from_impl(tokens))
            }
        })*
    };
}

impl_normalize_auto!(Enum, List, Map, Newtype, Set);

/// ---------------------------------------------------------------------------
/// Entity / Record
/// ---------------------------------------------------------------------------
/// Apply field-level normalizers directly to owned fields.
/// Do NOT recurse.
impl Imp<Entity> for NormalizeAutoTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        Some(field_list_normalize_strategy(node.def(), &node.fields))
    }
}

impl Imp<Record> for NormalizeAutoTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(field_list_normalize_strategy(node.def(), &node.fields))
    }
}

/// ---------------------------------------------------------------------------
/// Enum
/// ---------------------------------------------------------------------------
/// No direct normalization for enum selection.
/// Payload normalization occurs when payload node is visited.
impl NormalizeAutoFn for Enum {}

/// ---------------------------------------------------------------------------
/// Newtype
/// ---------------------------------------------------------------------------
/// Apply normalizers attached to the newtype itself / its inner value.
impl NormalizeAutoFn for Newtype {
    fn self_tokens(node: &Self) -> TokenStream {
        fn_wrap_normalize_self(newtype_normalizers(node))
    }
}

/// ---------------------------------------------------------------------------
/// List / Set / Map
/// ---------------------------------------------------------------------------
/// IMPORTANT:
/// - Do NOT iterate items here
/// - List items and map values are normalized via traversal
/// - Set items and map keys are not visited in mutable traversal
/// - Only container-level normalizers belong here
impl NormalizeAutoFn for List {
    fn self_tokens(node: &Self) -> TokenStream {
        container_self_tokens(&node.ty.normalizers)
    }
}

impl NormalizeAutoFn for Set {
    fn self_tokens(node: &Self) -> TokenStream {
        container_self_tokens(&node.ty.normalizers)
    }
}

impl NormalizeAutoFn for Map {
    fn self_tokens(node: &Self) -> TokenStream {
        container_self_tokens(&node.ty.normalizers)
    }
}

/// ---------------------------------------------------------------------------
/// Helpers
/// ---------------------------------------------------------------------------

/// Emit normalizer calls.
/// Errors are recorded via VisitorContext.
fn generate_normalizers(
    normalizers: &[TypeNormalizer],
    target: TokenStream,
    seg: Option<TokenStream>,
) -> Vec<TokenStream> {
    normalizers
        .iter()
        .map(|normalizer| {
            let ctor = normalizer.quote_constructor();
            let callback_type = &normalizer.path;
            match &seg {
                None => quote! {{
                    let mut __callback_ctx = ::icydb_model::visitor::CallbackContext::new(
                        ctx,
                        ::icydb_model::visitor::CallbackIdentity::new(
                            ::icydb_model::visitor::CallbackKind::Normalizer,
                            ::core::any::type_name::<#callback_type>(),
                        ),
                    );
                    if let Err(msg) = ::icydb_model::visitor::Normalizer::normalize_with_context(
                        &(#ctor),
                        &mut #target,
                        &mut __callback_ctx,
                    ) {
                        ::icydb_model::visitor::VisitorContext::add_issue(
                            &mut __callback_ctx,
                            ::icydb_model::visitor::Issue::from(msg),
                        );
                    }
                }},
                Some(seg) => quote! {{
                    let mut __field_ctx =
                        ::icydb_model::visitor::ScopedContext::new(ctx, #seg);
                    let mut __callback_ctx = ::icydb_model::visitor::CallbackContext::new(
                        &mut __field_ctx,
                        ::icydb_model::visitor::CallbackIdentity::new(
                            ::icydb_model::visitor::CallbackKind::Normalizer,
                            ::core::any::type_name::<#callback_type>(),
                        ),
                    );
                    if let Err(msg) = ::icydb_model::visitor::Normalizer::normalize_with_context(
                        &(#ctor),
                        &mut #target,
                        &mut __callback_ctx,
                    ) {
                        ::icydb_model::visitor::VisitorContext::add_issue(
                            &mut __callback_ctx,
                            ::icydb_model::visitor::Issue::from(msg),
                        );
                    }
                }},
            }
        })
        .collect()
}

/// Normalizers attached to the container itself (not items).
fn container_normalizers(
    normalizers: &[TypeNormalizer],
    target: TokenStream,
) -> Option<TokenStream> {
    let stmts = generate_normalizers(normalizers, target, None);
    if stmts.is_empty() {
        None
    } else {
        Some(quote! { #(#stmts)* })
    }
}

/// List, set, and map containers share the same direct self-normalizer shape.
fn container_self_tokens(normalizers: &[TypeNormalizer]) -> TokenStream {
    fn_wrap_normalize_self(container_normalizers(normalizers, quote!(self.0)))
}

/// Field-level normalizers for Entity / Record.
/// Applies directly to owned fields.
fn field_list(def: &Def, fields: &FieldList) -> TokenStream {
    let bindings = field_walk_bindings(fields);
    let field_table_ident = format_ident!("__NORMALIZE_FIELDS");

    let normalize_helpers = fields
        .iter()
        .zip(bindings.iter())
        .filter_map(|(field, binding)| {
            let stmts = generate_normalizers(
                &field.value.item.normalizers,
                binding.member_mut_from(quote!(node)),
                Some(binding.path_segment()),
            );
            let fn_ident = binding.normalize_fn_ident();

            if stmts.is_empty() {
                None
            } else {
                Some(quote! {
                    fn #fn_ident(
                        node: &mut Self,
                        ctx: &mut dyn ::icydb_model::visitor::VisitorContext,
                    ) {
                        #(#stmts)*
                    }
                })
            }
        });

    let descriptors = bindings
        .iter()
        .zip(fields.iter())
        .filter_map(|(binding, field)| {
            if field.value.item.normalizers.is_empty() {
                None
            } else {
                let normalize_fn = binding.normalize_fn_ident();

                Some(quote! {
                    ::icydb_model::visitor::NormalizeFieldDescriptor::new(Self::#normalize_fn)
                })
            }
        });

    let inherent_tokens = Implementor::new(def, TraitKind::Inherent)
        .set_tokens(quote! {
            #(#normalize_helpers)*

            const #field_table_ident: &'static [::icydb_model::visitor::NormalizeFieldDescriptor<Self>] =
                &[#(#descriptors),*];
        })
        .to_token_stream();

    let trait_tokens = Implementor::new(def, TraitKind::NormalizeAuto)
        .add_tokens(fn_wrap_normalize_self(Some(quote! {
            ::icydb_model::visitor::drive_normalize_fields(self, ctx, Self::#field_table_ident);
        })))
        .to_token_stream();

    quote! {
        #inherent_tokens
        #trait_tokens
    }
}

/// Entity and record normalize generation share the same field-driven strategy.
fn field_list_normalize_strategy(def: &Def, fields: &FieldList) -> TraitStrategy {
    TraitStrategy::from_impl(field_list(def, fields))
}

/// Normalizers for a newtype’s inner value (`self.0`).
fn newtype_normalizers(node: &Newtype) -> Option<TokenStream> {
    let target = quote!(self.0);

    let mut stmts = Vec::new();
    stmts.extend(generate_normalizers(
        &node.ty.normalizers,
        target.clone(),
        None,
    ));
    stmts.extend(generate_normalizers(&node.item.normalizers, target, None));

    if stmts.is_empty() {
        None
    } else {
        Some(quote! { #(#stmts)* })
    }
}

/// Emit `fn normalize_self(&mut self, ctx: &mut dyn VisitorContext)`
/// only if there is something to do.
fn fn_wrap_normalize_self(inner: Option<TokenStream>) -> TokenStream {
    match inner {
        None => quote!(),
        Some(inner) => quote! {
            fn normalize_self(
                &mut self,
                ctx: &mut dyn ::icydb_model::visitor::VisitorContext
            ) {
                #inner
            }
        },
    }
}
