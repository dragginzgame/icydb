use crate::prelude::*;

///
/// SanitizeAuto
///

pub struct SanitizeAutoTrait;

/// Small helper trait: each node type can say how to emit sanitizer code.
pub trait SanitizeAutoFn {
    fn self_tokens(_: &Self) -> TokenStream {
        quote!()
    }

    fn child_tokens(_: &Self) -> TokenStream {
        quote!()
    }
}

macro_rules! impl_sanitize_auto {
    ($ty:ty) => {
        impl Imp<$ty> for SanitizeAutoTrait {
            fn strategy(node: &$ty) -> Option<TraitStrategy> {
                let self_tokens = SanitizeAutoFn::self_tokens(node);
                let child_tokens = SanitizeAutoFn::child_tokens(node);

                let tokens = Implementor::new(node.def(), TraitKind::SanitizeAuto)
                    .add_tokens(self_tokens)
                    .add_tokens(child_tokens)
                    .to_token_stream();

                Some(TraitStrategy::from_impl(tokens))
            }
        }
    };
}

impl_sanitize_auto!(Entity);
impl_sanitize_auto!(Enum);
impl_sanitize_auto!(List);
impl_sanitize_auto!(Map);
impl_sanitize_auto!(Newtype);
impl_sanitize_auto!(Record);
impl_sanitize_auto!(Set);

impl SanitizeAutoFn for Entity {
    fn child_tokens(node: &Self) -> TokenStream {
        fn_wrap(field_list(&node.fields))
    }
}

impl SanitizeAutoFn for Enum {}

impl SanitizeAutoFn for List {
    fn child_tokens(node: &Self) -> TokenStream {
        if node.item.sanitizers.is_empty() {
            // no sanitizers → rely on blanket impl
            quote!()
        } else {
            let stmts =
                generate_sanitizers(&node.item.sanitizers, quote!(self.0[i]), quote!(self.0[i]));

            fn_wrap(Some(quote! {
                for i in 0..self.0.len() {
                    #(#stmts)*
                }
            }))
        }
    }
}

impl SanitizeAutoFn for Map {}

impl SanitizeAutoFn for Newtype {
    fn child_tokens(node: &Self) -> TokenStream {
        fn_wrap(newtype_sanitizers(node))
    }
}

impl SanitizeAutoFn for Record {
    fn child_tokens(node: &Self) -> TokenStream {
        fn_wrap(field_list(&node.fields))
    }
}

impl SanitizeAutoFn for Set {}

///
/// Helpers
///

/// Emit sanitizer calls from a list of TypeSanitizers
fn generate_sanitizers(
    sanitizers: &[TypeSanitizer],
    lhs: TokenStream,
    rhs: TokenStream,
) -> Vec<TokenStream> {
    sanitizers
        .iter()
        .map(|sanitizer| {
            let constructor = sanitizer.quote_constructor();
            quote! {
                #lhs = #constructor.sanitize(#rhs);
            }
        })
        .collect()
}

/// Collect sanitizers for all fields in a record/entity
fn field_list(fields: &FieldList) -> Option<TokenStream> {
    let rules: Vec<TokenStream> = fields
        .iter()
        .filter_map(|field| {
            let field_ident = &field.ident;
            let lhs = quote!(self.#field_ident);
            let rhs = quote!(self.#field_ident.clone());
            let rules = generate_sanitizers(&field.value.item.sanitizers, lhs, rhs);
            if rules.is_empty() {
                None
            } else {
                Some(quote! { #(#rules)* })
            }
        })
        .collect();

    if rules.is_empty() {
        None
    } else {
        Some(quote! { #(#rules)* })
    }
}

/// Build sanitizer block for a newtype’s inner value
fn newtype_sanitizers(node: &Newtype) -> Option<TokenStream> {
    let mut stmts = Vec::new();

    let lhs = quote!(self.0);
    let rhs = quote!(self.0.clone());
    stmts.extend(generate_sanitizers(
        &node.ty.sanitizers,
        lhs.clone(),
        rhs.clone(),
    ));
    stmts.extend(generate_sanitizers(&node.item.sanitizers, lhs, rhs));

    if stmts.is_empty() {
        None
    } else {
        Some(quote! { #(#stmts)* })
    }
}

/// Only emits sanitize_children if inner is not empty
fn fn_wrap(inner: Option<TokenStream>) -> TokenStream {
    match inner {
        None => quote!(),
        Some(inner) => quote! {
            fn sanitize_children(&mut self) {
                #inner
            }
        },
    }
}
