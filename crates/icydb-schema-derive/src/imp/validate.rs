use crate::{imp::field_walk::field_walk_bindings, prelude::*};
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

impl_validate_auto!(Enum, List, Map, Newtype, Set);

///
/// ---------------------------------------------------------------------------
/// Entity / Record
/// ---------------------------------------------------------------------------
///

impl Imp<Entity> for ValidateAutoTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        Some(TraitStrategy::from_impl(field_list(
            node.def(),
            &node.fields,
        )))
    }
}

impl Imp<Record> for ValidateAutoTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(TraitStrategy::from_impl(field_list(
            node.def(),
            &node.fields,
        )))
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
                    _ => {
                        // NOTE: Only unspecified variants emit diagnostics.
                    }
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
                        use ::icydb::traits::Collection;

                        for (i, #item_ident) in self.iter().enumerate() {
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
                // NOTE: Missing key/value rules are treated as empty blocks.
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
                        use ::icydb::traits::Collection;

                        for (i, #item_ident) in self.iter().enumerate() {
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
fn field_list(def: &Def, fields: &FieldList) -> TokenStream {
    let bindings = field_walk_bindings(fields);
    let field_table_ident = format_ident!("__VALIDATE_FIELDS");

    let validate_helpers = fields
        .iter()
        .zip(bindings.iter())
        .filter_map(|(field, binding)| {
            let validation = generate_field_value_validation_inner(
                &field.value,
                binding.member_ref_from(quote!(node)),
                binding.path_segment(),
            )?;
            let fn_ident = binding.validate_fn_ident();

            Some(quote! {
                fn #fn_ident(
                    node: &Self,
                    ctx: &mut dyn ::icydb::visitor::VisitorContext,
                ) {
                    #validation
                }
            })
        });

    let descriptors = bindings
        .iter()
        .zip(fields.iter())
        .filter_map(|(binding, field)| {
            if field.value.item.validators.is_empty() {
                None
            } else {
                let validate_fn = binding.validate_fn_ident();

                Some(quote! {
                    ::icydb::visitor::ValidateFieldDescriptor::new(Self::#validate_fn)
                })
            }
        });

    let inherent_tokens = Implementor::new(def, TraitKind::Inherent)
        .set_tokens(quote! {
            #(#validate_helpers)*

            const #field_table_ident: &'static [::icydb::visitor::ValidateFieldDescriptor<Self>] =
                &[#(#descriptors),*];
        })
        .to_token_stream();

    let trait_tokens = Implementor::new(def, TraitKind::ValidateAuto)
        .set_tokens(wrap_validate_self_fn(Some(quote! {
            ::icydb::visitor::drive_validate_fields(self, ctx, Self::#field_table_ident);
        })))
        .to_token_stream();

    quote! {
        #inherent_tokens
        #trait_tokens
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
