use crate::prelude::*;
use quote::format_ident;

/// ---------------------------------------------------------------------------
/// ValidateAuto
/// ---------------------------------------------------------------------------

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

/// ---------------------------------------------------------------------------
/// Entity
/// ---------------------------------------------------------------------------

impl ValidateAutoFn for Entity {
    fn self_tokens(node: &Self) -> TokenStream {
        wrap_validate_self_fn(field_list(&node.fields))
    }
}

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
                        ctx.add_issue(format!("unspecified variant: {}", #ident_str));
                    }
                }
            })
            .collect();

        let inner = if invalid_arms.is_empty() {
            quote!()
        } else {
            quote! {
                match self {
                    #invalid_arms
                    _ => {}
                }
            }
        };

        wrap_validate_self_fn(Some(inner))
    }
}

/// ---------------------------------------------------------------------------
/// List
/// ---------------------------------------------------------------------------

impl ValidateAutoFn for List {
    fn self_tokens(node: &Self) -> TokenStream {
        // Validators on the list itself (e.g. length constraints)
        let list_rules = generate_validators_inner(&node.ty.validators, quote!(&self.0), None);

        // Validators on items; attach at [i]
        let item_rules = generate_validators_inner(
            &node.item.validators,
            quote!(item),
            Some(quote!(::crate::visitor::PathSegment::Index(i))),
        )
        .map(|block| {
            let item_ident = format_ident!("__item");
            quote! {
                for (i, #item_ident) in self.0.iter().enumerate() {
                    let item = #item_ident;
                    #block
                }
            }
        });

        wrap_validate_self_fn(merge_rules(list_rules, item_rules))
    }
}

/// ---------------------------------------------------------------------------
/// Map
/// ---------------------------------------------------------------------------

impl ValidateAutoFn for Map {
    fn self_tokens(node: &Self) -> TokenStream {
        // Validators on the map itself
        let map_rules = generate_validators_inner(&node.ty.validators, quote!(&self.0), None);

        // Key/value validators; attach at "key" / "value"
        let key_rules = generate_validators_inner(
            &node.key.validators,
            quote!(k),
            Some(quote!(::crate::visitor::PathSegment::Field("key"))),
        );

        let value_rules = generate_value_validation_inner(
            &node.value,
            quote!(v),
            Some(quote!(::crate::visitor::PathSegment::Field("value"))),
        );

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

        wrap_validate_self_fn(merge_rules(map_rules, entry_rules))
    }
}

/// ---------------------------------------------------------------------------
/// Newtype
/// ---------------------------------------------------------------------------

impl ValidateAutoFn for Newtype {
    fn self_tokens(node: &Self) -> TokenStream {
        let type_rules = generate_validators_inner(&node.ty.validators, quote!(&self.0), None);
        let item_rules = generate_validators_inner(&node.item.validators, quote!(&self.0), None);

        wrap_validate_self_fn(merge_rules(type_rules, item_rules))
    }
}

/// ---------------------------------------------------------------------------
/// Record
/// ---------------------------------------------------------------------------

impl ValidateAutoFn for Record {
    fn self_tokens(node: &Self) -> TokenStream {
        wrap_validate_self_fn(field_list(&node.fields))
    }
}

/// ---------------------------------------------------------------------------
/// Set
/// ---------------------------------------------------------------------------

impl ValidateAutoFn for Set {
    fn self_tokens(node: &Self) -> TokenStream {
        // Validators on the set itself
        let set_rules = generate_validators_inner(&node.ty.validators, quote!(&self.0), None);

        // Validators on items; attach at [i] (iteration order is stable only for Vec,
        // but using indices here still provides useful localization for sets if you
        // enumerate; if you prefer, attach at Empty for sets.)
        let item_rules = generate_validators_inner(
            &node.item.validators,
            quote!(item),
            Some(quote!(::crate::visitor::PathSegment::Empty)),
        )
        .map(|block| {
            let item_ident = format_ident!("__item");
            quote! {
                for #item_ident in &self.0 {
                    let item = #item_ident;
                    #block
                }
            }
        });

        wrap_validate_self_fn(merge_rules(set_rules, item_rules))
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
            let field_name = quote_one(&field.ident, to_str_lit);

            generate_field_value_validation_inner(
                &field.value,
                quote!(&self.#field_ident),
                &field_name,
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
/// If `seg` is Some(..), the issue is recorded at that relative segment.
/// Otherwise, issue is recorded at current node path.
fn generate_validators_inner(
    validators: &[TypeValidator],
    var_expr: TokenStream,
    seg: Option<TokenStream>,
) -> Option<TokenStream> {
    if validators.is_empty() {
        return None;
    }

    let exprs: Vec<TokenStream> = validators
        .iter()
        .map(|validator| {
            let ctor = validator.quote_constructor();
            match &seg {
                None => quote! {
                    if let Err(err) = #ctor.validate(#var_expr) {
                        ctx.add_issue(err.to_string());
                    }
                },
                Some(seg) => quote! {
                    if let Err(err) = #ctor.validate(#var_expr) {
                        ctx.add_issue_at(#seg, err.to_string());
                    }
                },
            }
        })
        .collect();

    Some(quote!(#(#exprs)*))
}

/// Wrap `inner` into `fn validate_self(&self, ctx: &mut dyn VisitorContext)` if present.
fn wrap_validate_self_fn(inner: Option<TokenStream>) -> TokenStream {
    match inner {
        None => quote!(),
        Some(inner) => quote! {
            fn validate_self(&self, ctx: &mut dyn ::crate::visitor::VisitorContext) {
                #inner
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

/// Generates validation logic for a `Value` including its cardinality.
/// If `seg` is Some(..), issues are recorded under that relative segment.
fn generate_value_validation_inner(
    value: &Value,
    var_expr: TokenStream,
    seg: Option<TokenStream>,
) -> Option<TokenStream> {
    let rules: Vec<TokenStream> = value
        .item
        .validators
        .iter()
        .map(|validator| {
            let ctor = validator.quote_constructor();
            match &seg {
                None => quote! {
                    if let Err(err) = #ctor.validate(v) {
                        ctx.add_issue(err.to_string());
                    }
                },
                Some(seg) => quote! {
                    if let Err(err) = #ctor.validate(v) {
                        ctx.add_issue_at(#seg, err.to_string());
                    }
                },
            }
        })
        .collect();

    cardinality_wrapper(value.cardinality(), rules, var_expr)
}

/// Field-level value validation, adds errors under field key.
fn generate_field_value_validation_inner(
    value: &Value,
    var_expr: TokenStream,
    field_key: &TokenStream,
) -> Option<TokenStream> {
    let seg = quote!(::crate::visitor::PathSegment::Field(#field_key));

    let rules: Vec<TokenStream> = value
        .item
        .validators
        .iter()
        .map(|validator| {
            let ctor = validator.quote_constructor();
            quote! {
                if let Err(err) = #ctor.validate(v) {
                    ctx.add_issue_at(#seg, err.to_string());
                }
            }
        })
        .collect();

    cardinality_wrapper(value.cardinality(), rules, var_expr)
}
