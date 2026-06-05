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
    if !fields.iter().all(Field::has_rust_default) {
        return TraitStrategy::new();
    }

    if fields.iter().all(Field::default_matches_implicit_default) {
        return TraitStrategy::from_derive(TraitKind::Default);
    }

    struct_default_strategy(def, fields.iter().map(record_default_assignment))
}

fn default_strategy_entity(node: &Entity) -> TraitStrategy {
    let fields = &node.fields;
    if !fields.iter().all(Field::has_rust_default) {
        return TraitStrategy::new();
    }

    if fields.iter().all(Field::default_matches_implicit_default) {
        return TraitStrategy::from_derive(TraitKind::Default);
    }

    struct_default_strategy(
        node.def(),
        fields
            .iter()
            .map(|field| entity_default_assignment(field, node.primary_key.fields())),
    )
}

///
/// Newtype
///

impl Imp<Newtype> for DefaultTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let Some(default_expr) = &node.default else {
            return Some(TraitStrategy::new());
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

// Record fields lower only through schema-surface Rust construction values.
fn record_default_assignment(field: &Field) -> TokenStream {
    let ident = &field.ident;
    let expr = field
        .rust_default_expr()
        .expect("Default impl is generated only for constructible fields");

    quote!(#ident: #expr)
}

// Entity primary keys keep their key-conversion behavior when they have an
// explicit schema-surface construction value.
fn entity_default_assignment(field: &Field, primary_keys: &[Ident]) -> TokenStream {
    let ident = &field.ident;
    let expr = field
        .rust_default_expr()
        .expect("Default impl is generated only for constructible fields");

    if primary_keys.iter().any(|primary_key| ident == primary_key) {
        if let Some(FieldGeneration::Insert(generator)) = &field.generated {
            quote!(#ident: (#generator).into())
        } else {
            quote!(#ident: #expr)
        }
    } else {
        quote!(#ident: #expr)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
