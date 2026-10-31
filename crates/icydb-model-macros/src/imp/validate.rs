//! Module: imp::validate
//! Responsibility: generated implementation tokens.
//! Does not own: runtime trait semantics.
//! Boundary: parsed nodes to impl tokens.

use crate::{
    imp::field_walk::{cardinality_wrapper, field_walk_bindings},
    prelude::*,
};
use quote::format_ident;

///
/// ---------------------------------------------------------------------------
/// ValidateAuto
/// ---------------------------------------------------------------------------
///

pub struct ValidateAutoTrait;

pub trait ValidateAutoFn {
    /// Emit schema-defined validation for this node.
    fn self_tokens(_: &Self, __paths: &CratePaths) -> TokenStream {
        quote!()
    }
}

/// Blanket impl macro – keeps local logic out of macro scope.
macro_rules! impl_validate_auto {
    ($($ty:ty),* $(,)?) => {
        $(impl Imp<$ty> for ValidateAutoTrait {
            fn strategy(node: &$ty) -> Option<TraitStrategy> {
        let paths = node.def().paths();
                let self_tokens = ValidateAutoFn::self_tokens(node, paths);

                let tokens = Implementor::new(node.def(), TraitKind::ValidateAuto)
                    .add_tokens(self_tokens)
                    .impl_tokens(paths);

                Some(TraitStrategy::from_impl(tokens))
            }
        })*
    };
}

impl_validate_auto!(Enum, List, Map, Newtype, Set, Tuple);

///
/// ---------------------------------------------------------------------------
/// Entity / Record
/// ---------------------------------------------------------------------------
///

impl Imp<Entity> for ValidateAutoTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        Some(field_list_validate_strategy(
            node.def(),
            &node.ty,
            &node.fields,
            paths,
        ))
    }
}

impl Imp<Record> for ValidateAutoTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        Some(field_list_validate_strategy(
            node.def(),
            &node.ty,
            &node.fields,
            paths,
        ))
    }
}

impl ValidateAutoFn for Enum {
    fn self_tokens(node: &Self, paths: &CratePaths) -> TokenStream {
        let type_rules =
            generate_validators_inner(&node.ty.validators, quote!(self), quote!(ctx), paths);
        let payload_rules = node
            .variants
            .iter()
            .any(|variant| {
                variant
                    .value
                    .as_ref()
                    .is_some_and(|value| !value.item.validators.is_empty())
            })
            .then(|| {
                let model = &paths.model;
                let arms = node.variants.iter().map(|variant| {
                    let ident = &variant.name;
                    let Some(value) = &variant.value else {
                        return quote! { Self::#ident => {} };
                    };
                    let name = ident.to_string();
                    if let Some(rules) = generate_field_value_validation_inner(
                        value,
                        quote!(value),
                        quote!(#model::visitor::PathSegment::Field(#name)),
                        paths,
                    ) {
                        quote! { Self::#ident(value) => { #rules } }
                    } else {
                        quote! { Self::#ident(_) => {} }
                    }
                });
                quote! { match self { #(#arms),* } }
            });
        wrap_validate_self_fn(merge_rules(type_rules, payload_rules), paths)
    }
}

impl ValidateAutoFn for Tuple {
    fn self_tokens(node: &Self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let mut rules =
            generate_validators_inner(&node.ty.validators, quote!(self), quote!(ctx), paths);
        for (index, value) in node.values.iter().enumerate() {
            let name = index.to_string();
            let index = syn::Index::from(index);
            let item_rules = generate_field_value_validation_inner(
                value,
                quote!(&self.#index),
                quote!(#model::visitor::PathSegment::Field(#name)),
                paths,
            );
            rules = merge_rules(rules, item_rules);
        }
        wrap_validate_self_fn(rules, paths)
    }
}

///
/// ---------------------------------------------------------------------------
/// List
/// ---------------------------------------------------------------------------
///

impl ValidateAutoFn for List {
    fn self_tokens(node: &Self, paths: &CratePaths) -> TokenStream {
        collection_validate_self_tokens(&node.ty.validators, &node.item.validators, paths)
    }
}

///
/// ---------------------------------------------------------------------------
/// Map
/// ---------------------------------------------------------------------------
///

impl ValidateAutoFn for Map {
    fn self_tokens(node: &Self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let map_rules =
            generate_validators_inner(&node.ty.validators, quote!(&self.0), quote!(ctx), paths);

        let key_rules = generate_validators_inner(
            &node.key.validators,
            quote!(k),
            quote!(&mut entry_ctx),
            paths,
        );

        let value_rules =
            generate_value_validation_inner(&node.value, quote!(v), quote!(&mut entry_ctx), paths);

        let entry_rules = match (key_rules, value_rules) {
            (None, None) => None,
            (k, v) => {
                // NOTE: Missing key/value rules are treated as empty blocks.
                let k = k.unwrap_or_default();
                let v = v.unwrap_or_default();

                Some(quote! {
                    for (i, (k, v)) in self.0.iter().enumerate() {
                        let mut entry_ctx = #model::visitor::ScopedContext::new(
                            ctx,
                            #model::visitor::PathSegment::Index(i),
                        );
                        #k
                        #v
                    }
                })
            }
        };

        wrap_validate_self_fn(merge_rules(map_rules, entry_rules), paths)
    }
}

///
/// ---------------------------------------------------------------------------
/// Newtype
/// ---------------------------------------------------------------------------
///

impl ValidateAutoFn for Newtype {
    fn self_tokens(node: &Self, paths: &CratePaths) -> TokenStream {
        newtype_validate_self_tokens(node, paths)
    }
}

///
/// ---------------------------------------------------------------------------
/// Set
/// ---------------------------------------------------------------------------
///

impl ValidateAutoFn for Set {
    fn self_tokens(node: &Self, paths: &CratePaths) -> TokenStream {
        collection_validate_self_tokens(&node.ty.validators, &node.item.validators, paths)
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

fn field_list_validate_strategy(
    def: &Def,
    ty: &Type,
    fields: &FieldList,
    paths: &CratePaths,
) -> TraitStrategy {
    TraitStrategy::from_impl(field_list(def, ty, fields, paths))
}

// List and set item validators share the same indexed traversal shape.
fn indexed_collection_item_rules(block: TokenStream, paths: &CratePaths) -> TokenStream {
    let model = &paths.model;
    let item_ident = format_ident!("__item");

    quote! {
        for (i, #item_ident) in self.0.iter().enumerate() {
            let item = #item_ident;
            let mut item_ctx = #model::visitor::ScopedContext::new(
                ctx,
                #model::visitor::PathSegment::Index(i),
            );
            #block
        }
    }
}

fn collection_validate_self_tokens(
    container_validators: &[TypeValidator],
    item_validators: &[TypeValidator],
    paths: &CratePaths,
) -> TokenStream {
    let container_rules =
        generate_validators_inner(container_validators, quote!(&self.0), quote!(ctx), paths);
    let item_rules =
        generate_validators_inner(item_validators, quote!(item), quote!(&mut item_ctx), paths)
            .map(|value| indexed_collection_item_rules(value, paths));

    wrap_validate_self_fn(merge_rules(container_rules, item_rules), paths)
}

/// Field-level validators for Records / Entities
fn field_list(def: &Def, ty: &Type, fields: &FieldList, paths: &CratePaths) -> TokenStream {
    let model = &paths.model;
    let bindings = field_walk_bindings(fields);
    let field_table_ident = format_ident!("__VALIDATE_FIELDS");

    let validate_helpers = fields
        .iter()
        .zip(bindings.iter())
        .filter_map(|(field, binding)| {
            let validation = generate_field_value_validation_inner(
                &field.value,
                binding.member_ref_from(quote!(node)),
                binding.path_segment(paths),
                paths,
            )?;
            let fn_ident = binding.validate_fn_ident();

            Some(quote! {
                fn #fn_ident(
                    node: &Self,
                    ctx: &mut dyn #model::visitor::VisitorContext,
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
                    #model::visitor::ValidateFieldDescriptor::new(Self::#validate_fn)
                })
            }
        });

    let inherent_tokens = Implementor::inherent(def)
        .set_tokens(quote! {
            #(#validate_helpers)*

            const #field_table_ident: &'static [#model::visitor::ValidateFieldDescriptor<Self>] =
                &[#(#descriptors),*];
        })
        .impl_tokens(paths);

    let type_rules = generate_validators_inner(&ty.validators, quote!(self), quote!(ctx), paths);
    let trait_tokens = Implementor::new(def, TraitKind::ValidateAuto)
        .set_tokens(wrap_validate_self_fn(
            Some(quote! {
                #type_rules
                #model::visitor::drive_validate_fields(self, ctx, Self::#field_table_ident);
            }),
            paths,
        ))
        .impl_tokens(paths);

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
    paths: &CratePaths,
) -> Option<TokenStream> {
    if validators.is_empty() {
        return None;
    }

    let exprs: Vec<TokenStream> = validators
        .iter()
        .map(|validator| validator_call(validator, var_expr.clone(), ctx_expr.clone(), paths))
        .collect();

    Some(quote!(#(#exprs)*))
}

fn newtype_validate_self_tokens(node: &Newtype, paths: &CratePaths) -> TokenStream {
    let type_rules =
        generate_validators_inner(&node.ty.validators, quote!(&self.0), quote!(ctx), paths);
    let item_rules =
        generate_validators_inner(&node.item.validators, quote!(&self.0), quote!(ctx), paths);

    wrap_validate_self_fn(merge_rules(type_rules, item_rules), paths)
}

/// Wrap inner tokens into `fn validate_self`
fn wrap_validate_self_fn(inner: Option<TokenStream>, paths: &CratePaths) -> TokenStream {
    let model = &paths.model;
    match inner {
        None => quote!(),
        Some(inner) => quote! {
            fn validate_self(&self, ctx: &mut dyn #model::visitor::VisitorContext) {
                #inner
            }
        },
    }
}

/// Value-level validation (no path manipulation)
fn generate_value_validation_inner(
    value: &Value,
    var_expr: TokenStream,
    ctx_expr: TokenStream,
    paths: &CratePaths,
) -> Option<TokenStream> {
    let rules: Vec<TokenStream> = value
        .item
        .validators
        .iter()
        .map(|validator| validator_call(validator, quote!(v), ctx_expr.clone(), paths))
        .collect();

    cardinality_wrapper(value.cardinality(), rules, var_expr)
}

/// Field-level value validation
fn generate_field_value_validation_inner(
    value: &Value,
    var_expr: TokenStream,
    seg: TokenStream,
    paths: &CratePaths,
) -> Option<TokenStream> {
    let model = &paths.model;
    let rules: Vec<TokenStream> = value
        .item
        .validators
        .iter()
        .map(|validator| validator_call(validator, quote!(v), quote!(&mut __field_ctx), paths))
        .collect();

    let body = cardinality_wrapper(value.cardinality(), rules, var_expr)?;

    Some(quote! {{
        let mut __field_ctx =
            #model::visitor::ScopedContext::new(ctx, #seg);
        #body
    }})
}

fn validator_call(
    validator: &TypeValidator,
    value: TokenStream,
    context: TokenStream,
    paths: &CratePaths,
) -> TokenStream {
    let model = &paths.model;
    let ctor = validator.quote_constructor();
    let callback_type = &validator.path;

    quote! {{
        let mut __callback_ctx = #model::visitor::CallbackContext::new(
            #context,
            #model::visitor::CallbackIdentity::new(
                #model::visitor::CallbackKind::Validator,
                ::core::any::type_name::<#callback_type>(),
            ),
        );
        #model::visitor::Validator::validate(
            &(#ctor),
            #value,
            &mut __callback_ctx);
    }}
}
