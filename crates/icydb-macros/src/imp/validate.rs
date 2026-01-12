use crate::prelude::*;
use quote::format_ident;

///
/// ---------------------------------------------------------------------------
/// ValidateAuto
/// ---------------------------------------------------------------------------
///

pub struct ValidateAutoTrait;

pub trait ValidateAutoFn {
    /// Emit schema-defined validation for this node.
    fn self_tokens(_: &Self) -> TokenStream {
        quote!()
    }
}

/// Blanket impl macro – keeps local logic out of macro scope.
macro_rules! impl_validate_auto {
    ($($ty:ty),* $(,)?) => {
        $(impl Imp<$ty> for ValidateAutoTrait {
            fn strategy(node: &$ty) -> Option<TraitStrategy> {
                let self_tokens = ValidateAutoFn::self_tokens(node);

                let tokens = Implementor::new(node.def(), TraitKind::ValidateAuto)
                    .add_tokens(self_tokens)
                    .to_token_stream();

                Some(TraitStrategy::from_impl(tokens))
            }
        })*
    };
}

impl_validate_auto!(Entity, Enum, List, Map, Newtype, Record, Set);

///
/// ---------------------------------------------------------------------------
/// Entity / Record
/// ---------------------------------------------------------------------------
///

impl ValidateAutoFn for Entity {
    fn self_tokens(node: &Self) -> TokenStream {
        wrap_validate_self_fn(field_list(&node.fields))
    }
}

impl ValidateAutoFn for Record {
    fn self_tokens(node: &Self) -> TokenStream {
        wrap_validate_self_fn(field_list(&node.fields))
    }
}

///
/// ---------------------------------------------------------------------------
/// Enum
/// ---------------------------------------------------------------------------
/// Any variants marked `unspecified` are invalid if selected.
///

impl ValidateAutoFn for Enum {
    fn self_tokens(node: &Self) -> TokenStream {
        let invalid_arms: TokenStream = node
            .variants
            .iter()
            .filter(|v| v.unspecified)
            .map(|v| {
                let ident = v.effective_ident();
                let ident_str = format!("{ident}");
                quote! {
                    Self::#ident => {
                        ctx.issue(format!("unspecified variant: {}", #ident_str));
                    }
                }
            })
            .collect();

        if invalid_arms.is_empty() {
            quote!()
        } else {
            wrap_validate_self_fn(Some(quote! {
                match self {
                    #invalid_arms
                    _ => {}
                }
            }))
        }
    }
}

///
/// ---------------------------------------------------------------------------
/// List
/// ---------------------------------------------------------------------------
///

impl ValidateAutoFn for List {
    fn self_tokens(node: &Self) -> TokenStream {
        let list_rules =
            generate_validators_inner(&node.ty.validators, quote!(&self.0), quote!(ctx));

        let item_rules =
            generate_validators_inner(&node.item.validators, quote!(item), quote!(&mut item_ctx))
                .map(|block| {
                    let item_ident = format_ident!("__item");
                    quote! {
                        for (i, #item_ident) in self.0.iter().enumerate() {
                            let item = #item_ident;
                            let mut item_ctx = ::icydb::visitor::ScopedContext::new(
                                ctx,
                                ::icydb::visitor::PathSegment::Index(i),
                            );
                            #block
                        }
                    }
                });

        wrap_validate_self_fn(merge_rules(list_rules, item_rules))
    }
}

///
/// ---------------------------------------------------------------------------
/// Map
/// ---------------------------------------------------------------------------
///

impl ValidateAutoFn for Map {
    fn self_tokens(node: &Self) -> TokenStream {
        let map_rules =
            generate_validators_inner(&node.ty.validators, quote!(&self.0), quote!(ctx));

        let key_rules =
            generate_validators_inner(&node.key.validators, quote!(k), quote!(&mut entry_ctx));

        let value_rules =
            generate_value_validation_inner(&node.value, quote!(v), quote!(&mut entry_ctx));

        let entry_rules = match (key_rules, value_rules) {
            (None, None) => None,
            (k, v) => {
                let k = k.unwrap_or_default();
                let v = v.unwrap_or_default();

                Some(quote! {
                    for (i, (k, v)) in self.0.iter().enumerate() {
                        let mut entry_ctx = ::icydb::visitor::ScopedContext::new(
                            ctx,
                            ::icydb::visitor::PathSegment::Index(i),
                        );
                        #k
                        #v
                    }
                })
            }
        };

        wrap_validate_self_fn(merge_rules(map_rules, entry_rules))
    }
}

///
/// ---------------------------------------------------------------------------
/// Newtype
/// ---------------------------------------------------------------------------
///

impl ValidateAutoFn for Newtype {
    fn self_tokens(node: &Self) -> TokenStream {
        let type_rules =
            generate_validators_inner(&node.ty.validators, quote!(&self.0), quote!(ctx));
        let item_rules =
            generate_validators_inner(&node.item.validators, quote!(&self.0), quote!(ctx));

        wrap_validate_self_fn(merge_rules(type_rules, item_rules))
    }
}

///
/// ---------------------------------------------------------------------------
/// Set
/// ---------------------------------------------------------------------------
///

impl ValidateAutoFn for Set {
    fn self_tokens(node: &Self) -> TokenStream {
        let set_rules =
            generate_validators_inner(&node.ty.validators, quote!(&self.0), quote!(ctx));

        let item_rules =
            generate_validators_inner(&node.item.validators, quote!(item), quote!(&mut item_ctx))
                .map(|block| {
                    let item_ident = format_ident!("__item");
                    quote! {
                        for (i, #item_ident) in self.0.iter().enumerate() {
                            let item = #item_ident;
                            let mut item_ctx = ::icydb::visitor::ScopedContext::new(
                                ctx,
                                ::icydb::visitor::PathSegment::Index(i),
                            );
                            #block
                        }
                    }
                });

        wrap_validate_self_fn(merge_rules(set_rules, item_rules))
    }
}

///
/// ---------------------------------------------------------------------------
/// Helper functions
/// ---------------------------------------------------------------------------
///

fn merge_rules(a: Option<TokenStream>, b: Option<TokenStream>) -> Option<TokenStream> {
    match (a, b) {
        (None, None) => None,
        (x, None) => x,
        (None, y) => y,
        (Some(x), Some(y)) => Some(quote! { #x #y }),
    }
}

/// Field-level validators for Records / Entities
fn field_list(fields: &FieldList) -> Option<TokenStream> {
    let validations: Vec<_> = fields
        .iter()
        .filter_map(|field| {
            let field_ident = &field.ident;
            generate_field_value_validation_inner(
                &field.value,
                quote!(&self.#field_ident),
                quote!(::icydb::visitor::PathSegment::Field(
                    stringify!(#field_ident)
                )),
            )
        })
        .collect();

    if validations.is_empty() {
        None
    } else {
        Some(quote! { #(#validations)* })
    }
}

/// Generate validator expressions for a list of validators.
/// Validators emit issues directly via `VisitorContext`.
fn generate_validators_inner(
    validators: &[TypeValidator],
    var_expr: TokenStream,
    ctx_expr: TokenStream,
) -> Option<TokenStream> {
    if validators.is_empty() {
        return None;
    }

    let exprs: Vec<TokenStream> = validators
        .iter()
        .map(|validator| {
            let ctor = validator.quote_constructor(); // yields "...::new(args...)"
            quote! {
                (#ctor).validate(#var_expr, #ctx_expr);
            }
        })
        .collect();

    Some(quote!(#(#exprs)*))
}

/// Wrap inner tokens into `fn validate_self`
fn wrap_validate_self_fn(inner: Option<TokenStream>) -> TokenStream {
    match inner {
        None => quote!(),
        Some(inner) => quote! {
            fn validate_self(&self, ctx: &mut dyn ::icydb::visitor::VisitorContext) {
                #inner
            }
        },
    }
}

/// Applies cardinality (One / Opt / Many)
fn cardinality_wrapper(
    card: Cardinality,
    rules: Vec<TokenStream>,
    var_expr: TokenStream,
) -> Option<TokenStream> {
    if rules.is_empty() {
        return None;
    }

    let body = quote! { #(#rules)* };

    let tokens = match card {
        Cardinality::One => quote! {
            let v = #var_expr;
            #body
        },
        Cardinality::Opt => quote! {
            if let Some(v) = #var_expr {
                #body
            }
        },
        Cardinality::Many => {
            let item = format_ident!("__item");
            quote! {
                for #item in #var_expr {
                    let v = #item;
                    #body
                }
            }
        }
    };

    Some(tokens)
}

/// Value-level validation (no path manipulation)
fn generate_value_validation_inner(
    value: &Value,
    var_expr: TokenStream,
    ctx_expr: TokenStream,
) -> Option<TokenStream> {
    let rules: Vec<TokenStream> = value
        .item
        .validators
        .iter()
        .map(|validator| {
            let ctor = validator.quote_constructor(); // "...::new(args...)"
            quote! {
                (#ctor).validate(v, #ctx_expr);
            }
        })
        .collect();

    cardinality_wrapper(value.cardinality(), rules, var_expr)
}

/// Field-level value validation
fn generate_field_value_validation_inner(
    value: &Value,
    var_expr: TokenStream,
    seg: TokenStream,
) -> Option<TokenStream> {
    let rules: Vec<TokenStream> = value
        .item
        .validators
        .iter()
        .map(|validator| {
            let ctor = validator.quote_constructor(); // "...::new(args...)"
            quote! {
                (#ctor).validate(v, &mut __field_ctx);
            }
        })
        .collect();

    let body = cardinality_wrapper(value.cardinality(), rules, var_expr)?;

    Some(quote! {{
        let mut __field_ctx =
            ::icydb::visitor::ScopedContext::new(ctx, #seg);
        #body
    }})
}
