use crate::prelude::*;
use quote::format_ident;

/// ---------------------------------------------------------------------------
/// ValidateAuto
/// ---------------------------------------------------------------------------
///
/// Generates auto-validation logic for schema nodes.
/// Each node type (`Entity`, `Enum`, `List`, etc.) implements
/// [`ValidateAutoFn`], which returns one or both token streams:
/// - `self_tokens`: validation logic for the node itself
/// - `child_tokens`: validation logic for its child values
///
/// The macro [`impl_validate_auto!`] wires these up into a concrete
/// implementation of [`TraitKind::ValidateAuto`].
///

pub struct ValidateAutoTrait;

pub trait ValidateAutoFn {
    fn self_tokens(_: &Self) -> TokenStream {
        quote!()
    }

    fn child_tokens(_: &Self) -> TokenStream {
        quote!()
    }
}

/// Blanket impl macro – keeps local logic out of macro scope.
macro_rules! impl_validate_auto {
    ($($ty:ty),* $(,)?) => {
        $(impl Imp<$ty> for ValidateAutoTrait {
            fn strategy(node: &$ty) -> Option<TraitStrategy> {
                let self_tokens = ValidateAutoFn::self_tokens(node);
                let child_tokens = ValidateAutoFn::child_tokens(node);

                // Combine both token sets into a single impl body
                let tokens = quote! {
                    #self_tokens
                    #child_tokens
                };

                let tokens = Implementor::new(node.def(), TraitKind::ValidateAuto)
                    .add_tokens(tokens)
                    .to_token_stream();

                Some(TraitStrategy::from_impl(tokens))
            }
        })*
    };
}

impl_validate_auto!(Entity, Enum, List, Map, Newtype, Record, Set);

/// ---------------------------------------------------------------------------
/// Entity
/// ---------------------------------------------------------------------------

impl ValidateAutoFn for Entity {
    fn child_tokens(node: &Self) -> TokenStream {
        wrap_validate_fn(field_list(&node.fields))
    }
}

/// ---------------------------------------------------------------------------
/// Enum
/// ---------------------------------------------------------------------------
///
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
                    Self::#ident => Err(format!("unspecified variant: {}", #ident_str).into()),
                }
            })
            .collect();

        let inner = if invalid_arms.is_empty() {
            quote!(Ok(()))
        } else {
            quote! {
                match self {
                    #invalid_arms
                    _ => Ok(()),
                }
            }
        };

        // quote
        let ep = paths().error;
        quote! {
            fn validate_self(&self) -> ::std::result::Result<(), #ep::ErrorTree> {
                #inner
            }
        }
    }
}

/// ---------------------------------------------------------------------------
/// List
/// ---------------------------------------------------------------------------

impl ValidateAutoFn for List {
    fn child_tokens(node: &Self) -> TokenStream {
        let list_rules = generate_validators_inner(&node.ty.validators, quote!(&self.0));
        let item_rules = generate_validators_inner(&node.item.validators, quote!(v)).map(|block| {
            let item = format_ident!("__item");
            quote! {
                for #item in &self.0 {
                    let v = #item;
                    #block
                }
            }
        });

        wrap_validate_fn(merge_rules(list_rules, item_rules))
    }
}

/// ---------------------------------------------------------------------------
/// Map
/// ---------------------------------------------------------------------------

impl ValidateAutoFn for Map {
    fn child_tokens(node: &Self) -> TokenStream {
        let map_rules = generate_validators_inner(&node.ty.validators, quote!(&self.0));
        let key_rules = generate_validators_inner(&node.key.validators, quote!(k));
        let value_rules = generate_value_validation_inner(&node.value, quote!(v));

        let entry_rules = match (key_rules, value_rules) {
            (None, None) => None,
            (k, v) => {
                let k = k.unwrap_or_default();
                let v = v.unwrap_or_default();
                Some(quote! {
                    for (k, v) in &self.0 {
                        #k
                        #v
                    }
                })
            }
        };

        wrap_validate_fn(merge_rules(map_rules, entry_rules))
    }
}

/// ---------------------------------------------------------------------------
/// Newtype
/// ---------------------------------------------------------------------------

impl ValidateAutoFn for Newtype {
    fn child_tokens(node: &Self) -> TokenStream {
        let type_rules = generate_validators_inner(&node.ty.validators, quote!(&self.0));
        let item_rules = generate_validators_inner(&node.item.validators, quote!(&self.0));

        wrap_validate_fn(merge_rules(type_rules, item_rules))
    }
}

/// ---------------------------------------------------------------------------
/// Record
/// ---------------------------------------------------------------------------

impl ValidateAutoFn for Record {
    fn child_tokens(node: &Self) -> TokenStream {
        wrap_validate_fn(field_list(&node.fields))
    }
}

/// ---------------------------------------------------------------------------
/// Set
/// ---------------------------------------------------------------------------

impl ValidateAutoFn for Set {
    fn child_tokens(node: &Self) -> TokenStream {
        let set_rules = generate_validators_inner(&node.ty.validators, quote!(&self.0));
        let item_rules = generate_validators_inner(&node.item.validators, quote!(v)).map(|block| {
            let item = format_ident!("__item");
            quote! {
                for #item in &self.0 {
                    let v = #item;
                    #block
                }
            }
        });

        wrap_validate_fn(merge_rules(set_rules, item_rules))
    }
}

/// ---------------------------------------------------------------------------
/// Helper functions
/// ---------------------------------------------------------------------------

/// Merge two optional token blocks into one, preserving `None` as `None`.
fn merge_rules(a: Option<TokenStream>, b: Option<TokenStream>) -> Option<TokenStream> {
    match (a, b) {
        (None, None) => None,
        (x, None) => x,
        (None, y) => y,
        (Some(x), Some(y)) => Some(quote! { #x #y }),
    }
}

/// Field-level validator list for Records / Entities
fn field_list(fields: &FieldList) -> Option<TokenStream> {
    let validations: Vec<_> = fields
        .iter()
        .filter_map(|field| {
            let field_ident = &field.ident;
            let field_key = quote_one(&field.ident, to_str_lit);
            generate_field_value_validation_inner(
                &field.value,
                quote!(&self.#field_ident),
                &field_key,
            )
        })
        .collect();

    if validations.is_empty() {
        None
    } else {
        Some(quote! { #(#validations)* })
    }
}

/// Generate validator expressions for a list of validators on a variable.
fn generate_validators(validators: &[TypeValidator], var_expr: TokenStream) -> Vec<TokenStream> {
    validators
        .iter()
        .map(|validator| {
            let constructor = validator.quote_constructor();
            quote!(errs.add_result(#constructor.validate(#var_expr));)
        })
        .collect()
}

/// Combine multiple validator expressions into one block.
/// This no longer emits `let v = &self.0;`, removing the dead code.
fn generate_validators_inner(
    validators: &[TypeValidator],
    var_expr: TokenStream,
) -> Option<TokenStream> {
    if validators.is_empty() {
        return None;
    }
    let exprs = generate_validators(validators, var_expr);

    Some(quote!(#(#exprs)*))
}

/// Wraps validation code in a standard `fn validate_children()`
/// method body if `inner` is present.
fn wrap_validate_fn(inner: Option<TokenStream>) -> TokenStream {
    let ep = paths().error;

    match inner {
        None => quote!(),
        Some(inner) => quote! {
            fn validate_children(&self) -> ::std::result::Result<(), #ep::ErrorTree> {
                let mut errs = #ep::ErrorTree::new();
                #inner
                errs.result()
            }
        },
    }
}

/// Applies cardinality (One/Opt/Many) to a set of rule expressions.
fn cardinality_wrapper(
    card: Cardinality,
    rules: Vec<TokenStream>,
    var_expr: TokenStream,
) -> Option<TokenStream> {
    if rules.is_empty() {
        return None;
    }
    let body = quote! { #(#rules;)* };
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

/// Generates validation logic for a `Value` including its cardinality.
fn generate_value_validation_inner(value: &Value, var_expr: TokenStream) -> Option<TokenStream> {
    let rules = generate_validators(&value.item.validators, quote!(v));
    cardinality_wrapper(value.cardinality(), rules, var_expr)
}

/// Field-level value validation, adds errors under field key.
fn generate_field_value_validation_inner(
    value: &Value,
    var_expr: TokenStream,
    field_key: &TokenStream,
) -> Option<TokenStream> {
    let rules = value
        .item
        .validators
        .iter()
        .map(|validator| {
            let ctor = validator.quote_constructor();
            quote! {
                if let Err(err) = #ctor.validate(v) {
                    errs.add_for(#field_key, err);
                }
            }
        })
        .collect();

    cardinality_wrapper(value.cardinality(), rules, var_expr)
}
