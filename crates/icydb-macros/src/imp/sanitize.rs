use crate::prelude::*;

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

impl_sanitize_auto!(Entity, Enum, List, Map, Newtype, Record, Set);

/// ---------------------------------------------------------------------------
/// Entity / Record
/// ---------------------------------------------------------------------------
/// Apply field-level sanitizers directly to owned fields.
/// Do NOT recurse.
impl SanitizeAutoFn for Entity {
    fn self_tokens(node: &Self) -> TokenStream {
        fn_wrap_sanitize_self(field_list(&node.fields))
    }
}

impl SanitizeAutoFn for Record {
    fn self_tokens(node: &Self) -> TokenStream {
        fn_wrap_sanitize_self(field_list(&node.fields))
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
        fn_wrap_sanitize_self(container_sanitizers(&node.ty.sanitizers, quote!(self.0)))
    }
}

impl SanitizeAutoFn for Set {
    fn self_tokens(node: &Self) -> TokenStream {
        fn_wrap_sanitize_self(container_sanitizers(&node.ty.sanitizers, quote!(self.0)))
    }
}

impl SanitizeAutoFn for Map {
    fn self_tokens(node: &Self) -> TokenStream {
        fn_wrap_sanitize_self(container_sanitizers(&node.ty.sanitizers, quote!(self.0)))
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
                    if let Err(msg) = #ctor.sanitize(&mut #target) {
                        ctx.issue(msg);
                    }
                },
                Some(seg) => quote! {
                    if let Err(msg) = #ctor.sanitize(&mut #target) {
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

/// Field-level sanitizers for Entity / Record.
/// Applies directly to owned fields.
fn field_list(fields: &FieldList) -> Option<TokenStream> {
    let rules: Vec<TokenStream> = fields
        .iter()
        .filter_map(|field| {
            let field_ident = &field.ident;
            let target = quote!(self.#field_ident);
            let seg = quote!(::icydb::visitor::PathSegment::Field(
                stringify!(#field_ident)
            ));

            let stmts = generate_sanitizers(&field.value.item.sanitizers, target, Some(seg));

            if stmts.is_empty() {
                None
            } else {
                Some(quote! { #(#stmts)* })
            }
        })
        .collect();

    if rules.is_empty() {
        None
    } else {
        Some(quote! { #(#rules)* })
    }
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
