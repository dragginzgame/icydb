use crate::{imp::field_walk::field_walk_bindings, prelude::*};

/// ---------------------------------------------------------------------------
/// SanitizeAuto
/// ---------------------------------------------------------------------------

pub struct SanitizeAutoTrait;

/// Each node type can emit sanitizer code for its *own value only*.
/// Traversal into children is handled by the visitor.
pub trait SanitizeAutoFn {
    fn self_tokens(_: &Self) -> TokenStream {
        quote!()
    }
}

macro_rules! impl_sanitize_auto {
    ($($ty:ty),* $(,)?) => {
        $(impl Imp<$ty> for SanitizeAutoTrait {
            fn strategy(node: &$ty) -> Option<TraitStrategy> {
                let self_tokens = SanitizeAutoFn::self_tokens(node);

                let tokens = Implementor::new(node.def(), TraitKind::SanitizeAuto)
                    .add_tokens(self_tokens)
                    .to_token_stream();

                Some(TraitStrategy::from_impl(tokens))
            }
        })*
    };
}

impl_sanitize_auto!(Enum, List, Map, Newtype, Set);

/// ---------------------------------------------------------------------------
/// Entity / Record
/// ---------------------------------------------------------------------------
/// Apply field-level sanitizers directly to owned fields.
/// Do NOT recurse.
impl Imp<Entity> for SanitizeAutoTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        Some(field_list_sanitize_strategy(node.def(), &node.fields))
    }
}

impl Imp<Record> for SanitizeAutoTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(field_list_sanitize_strategy(node.def(), &node.fields))
    }
}

/// ---------------------------------------------------------------------------
/// Enum
/// ---------------------------------------------------------------------------
/// No direct sanitization for enum selection.
/// Payload sanitization occurs when payload node is visited.
impl SanitizeAutoFn for Enum {}

/// ---------------------------------------------------------------------------
/// Newtype
/// ---------------------------------------------------------------------------
/// Apply sanitizers attached to the newtype itself / its inner value.
impl SanitizeAutoFn for Newtype {
    fn self_tokens(node: &Self) -> TokenStream {
        fn_wrap_sanitize_self(newtype_sanitizers(node))
    }
}

/// ---------------------------------------------------------------------------
/// List / Set / Map
/// ---------------------------------------------------------------------------
/// IMPORTANT:
/// - Do NOT iterate items here
/// - List items and map values are sanitized via traversal
/// - Set items and map keys are not visited in mutable traversal
/// - Only container-level sanitizers belong here
impl SanitizeAutoFn for List {
    fn self_tokens(node: &Self) -> TokenStream {
        container_self_tokens(&node.ty.sanitizers)
    }
}

impl SanitizeAutoFn for Set {
    fn self_tokens(node: &Self) -> TokenStream {
        container_self_tokens(&node.ty.sanitizers)
    }
}

impl SanitizeAutoFn for Map {
    fn self_tokens(node: &Self) -> TokenStream {
        container_self_tokens(&node.ty.sanitizers)
    }
}

/// ---------------------------------------------------------------------------
/// Helpers
/// ---------------------------------------------------------------------------

/// Emit sanitizer calls.
/// Errors are recorded via VisitorContext.
fn generate_sanitizers(
    sanitizers: &[TypeSanitizer],
    target: TokenStream,
    seg: Option<TokenStream>,
) -> Vec<TokenStream> {
    sanitizers
        .iter()
        .map(|sanitizer| {
            let ctor = sanitizer.quote_constructor();
            match &seg {
                None => quote! {
                    if let Err(msg) = #ctor.sanitize_with_context(&mut #target, ctx) {
                        ctx.issue(msg);
                    }
                },
                Some(seg) => quote! {
                    if let Err(msg) = #ctor.sanitize_with_context(&mut #target, ctx) {
                        ctx.issue_at(#seg, msg);
                    }
                },
            }
        })
        .collect()
}

/// Sanitizers attached to the container itself (not items).
fn container_sanitizers(sanitizers: &[TypeSanitizer], target: TokenStream) -> Option<TokenStream> {
    let stmts = generate_sanitizers(sanitizers, target, None);
    if stmts.is_empty() {
        None
    } else {
        Some(quote! { #(#stmts)* })
    }
}

/// List, set, and map containers share the same direct self-sanitizer shape.
fn container_self_tokens(sanitizers: &[TypeSanitizer]) -> TokenStream {
    fn_wrap_sanitize_self(container_sanitizers(sanitizers, quote!(self.0)))
}

/// Field-level sanitizers for Entity / Record.
/// Applies directly to owned fields.
fn field_list(def: &Def, fields: &FieldList) -> TokenStream {
    let bindings = field_walk_bindings(fields);
    let field_table_ident = format_ident!("__SANITIZE_FIELDS");

    let sanitize_helpers = fields
        .iter()
        .zip(bindings.iter())
        .filter_map(|(field, binding)| {
            let stmts = generate_sanitizers(
                &field.value.item.sanitizers,
                binding.member_mut_from(quote!(node)),
                Some(binding.path_segment()),
            );
            let fn_ident = binding.sanitize_fn_ident();

            if stmts.is_empty() {
                None
            } else {
                Some(quote! {
                    fn #fn_ident(
                        node: &mut Self,
                        ctx: &mut dyn ::icydb::visitor::VisitorContext,
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
            if field.value.item.sanitizers.is_empty() {
                None
            } else {
                let sanitize_fn = binding.sanitize_fn_ident();

                Some(quote! {
                    ::icydb::visitor::SanitizeFieldDescriptor::new(Self::#sanitize_fn)
                })
            }
        });

    let inherent_tokens = Implementor::new(def, TraitKind::Inherent)
        .set_tokens(quote! {
            #(#sanitize_helpers)*

            const #field_table_ident: &'static [::icydb::visitor::SanitizeFieldDescriptor<Self>] =
                &[#(#descriptors),*];
        })
        .to_token_stream();

    let trait_tokens = Implementor::new(def, TraitKind::SanitizeAuto)
        .add_tokens(fn_wrap_sanitize_self(Some(quote! {
            ::icydb::visitor::drive_sanitize_fields(self, ctx, Self::#field_table_ident);
        })))
        .to_token_stream();

    quote! {
        #inherent_tokens
        #trait_tokens
    }
}

/// Entity and record sanitize generation share the same field-driven strategy.
fn field_list_sanitize_strategy(def: &Def, fields: &FieldList) -> TraitStrategy {
    TraitStrategy::from_impl(field_list(def, fields))
}

/// Sanitizers for a newtype’s inner value (`self.0`).
fn newtype_sanitizers(node: &Newtype) -> Option<TokenStream> {
    let target = quote!(self.0);

    let mut stmts = Vec::new();
    stmts.extend(generate_sanitizers(
        &node.ty.sanitizers,
        target.clone(),
        None,
    ));
    stmts.extend(generate_sanitizers(&node.item.sanitizers, target, None));

    if stmts.is_empty() {
        None
    } else {
        Some(quote! { #(#stmts)* })
    }
}

/// Emit `fn sanitize_self(&mut self, ctx: &mut dyn VisitorContext)`
/// only if there is something to do.
fn fn_wrap_sanitize_self(inner: Option<TokenStream>) -> TokenStream {
    match inner {
        None => quote!(),
        Some(inner) => quote! {
            fn sanitize_self(
                &mut self,
                ctx: &mut dyn ::icydb::visitor::VisitorContext
            ) {
                #inner
            }
        },
    }
}
