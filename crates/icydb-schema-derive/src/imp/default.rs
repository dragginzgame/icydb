use crate::prelude::*;

///
/// DefaultTrait
///

pub struct DefaultTrait {}

///
/// Entity
///

impl Imp<Entity> for DefaultTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        Some(default_strategy_entity(node))
    }
}

///
/// Enum
///

impl Imp<Enum> for DefaultTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let Some(default_variant) = node.default_variant() else {
            return Some(TraitStrategy::from_impl(quote!(compile_error!(
                "default variant is required for Default"
            ))));
        };
        let variant_ident = default_variant.effective_ident();

        // if the default variant carries a value, generate it as `(Default::default())`
        let inner = if default_variant.value.is_some() {
            quote!(Self::#variant_ident(Default::default()))
        } else {
            quote!(Self::#variant_ident)
        };

        let q = quote! {
            fn default() -> Self {
                #inner
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Default)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Record
///

impl Imp<Record> for DefaultTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(record_default_strategy(&node.def, &node.fields))
    }
}

// Records use explicit field defaults only when at least one field declares one.
fn record_default_strategy(def: &Def, fields: &FieldList) -> TraitStrategy {
    if fields.iter().all(|f| f.default.is_none()) {
        return TraitStrategy::from_derive(TraitKind::Default);
    }

    struct_default_strategy(def, fields.iter().map(record_default_assignment))
}

fn default_strategy_entity(node: &Entity) -> TraitStrategy {
    let fields = &node.fields;
    if fields.iter().all(|f| f.default.is_none()) {
        return TraitStrategy::from_derive(TraitKind::Default);
    }

    let primary_key = &node.primary_key.field;

    struct_default_strategy(
        node.def(),
        fields
            .iter()
            .map(|field| entity_default_assignment(field, primary_key)),
    )
}

///
/// Newtype
///

impl Imp<Newtype> for DefaultTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        // If no default we just want to derive
        let Some(default_expr) = &node.default else {
            return Some(TraitStrategy::from_derive(TraitKind::Default));
        };

        let q = quote! {
            fn default() -> Self {
                Self(#default_expr.into())
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Default)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

// Build one explicit `Default` impl for a struct-like node from field assignments.
fn struct_default_strategy(
    def: &Def,
    assignments: impl Iterator<Item = TokenStream>,
) -> TraitStrategy {
    let assignments: Vec<_> = assignments.collect();
    let tokens = Implementor::new(def, TraitKind::Default)
        .set_tokens(quote! {
            fn default() -> Self {
                Self { #(#assignments),* }
            }
        })
        .to_token_stream();

    TraitStrategy::from_impl(tokens)
}

// Record fields always lower through the declared default expression.
fn record_default_assignment(field: &Field) -> TokenStream {
    let ident = &field.ident;
    let expr = field.default_expr();

    quote!(#ident: #expr)
}

// Entity primary keys keep their special key-conversion/default behavior.
fn entity_default_assignment(field: &Field, primary_key: &Ident) -> TokenStream {
    let ident = &field.ident;

    if ident == primary_key {
        if let Some(default) = &field.default {
            quote!(#ident: (#default).into())
        } else {
            quote!(#ident: Default::default())
        }
    } else {
        let expr = field.default_expr();
        quote!(#ident: #expr)
    }
}
