//! Module: imp::default
//! Responsibility: generated implementation tokens.
//! Does not own: runtime trait semantics.
//! Boundary: parsed nodes to impl tokens.

#[cfg(test)]
mod tests;

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
        let variant_ident = &default_variant.ident;

        let mut implementor = Implementor::new(node.def(), TraitKind::Default);
        let inner = if let Some(value) = &default_variant.value {
            let value_type = value.type_expr();
            implementor = implementor.add_impl_constraint(quote!(#value_type: Default));
            quote!(Self::#variant_ident(Default::default()))
        } else {
            quote!(Self::#variant_ident)
        };

        let q = quote! {
            fn default() -> Self {
                #inner
            }
        };

        let tokens = implementor.set_tokens(q).to_token_stream();

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

// Explicit record defaults derive only when every field uses its implicit value.
fn record_default_strategy(def: &Def, fields: &FieldList) -> TraitStrategy {
    if fields.iter().all(Field::default_matches_implicit_default) {
        return TraitStrategy::from_derive(TraitKind::Default);
    }

    let Some(assignments) = fields
        .iter()
        .map(record_default_assignment)
        .collect::<Option<Vec<_>>>()
    else {
        return default_generation_invariant_error(def);
    };

    struct_default_strategy(def, assignments)
}

fn default_strategy_entity(node: &Entity) -> TraitStrategy {
    let fields = &node.fields;
    if fields.iter().all(Field::default_matches_implicit_default) {
        return TraitStrategy::from_derive(TraitKind::Default);
    }

    let Some(assignments) = fields
        .iter()
        .map(|field| entity_default_assignment(field, node.primary_key.fields()))
        .collect::<Option<Vec<_>>>()
    else {
        return default_generation_invariant_error(node.def());
    };

    struct_default_strategy(node.def(), assignments)
}

///
/// Newtype
///

impl Imp<Newtype> for DefaultTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let Some(default_expr) = &node.default else {
            return Some(default_generation_invariant_error(node.def()));
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

///
/// Tuple
///

impl Imp<Tuple> for DefaultTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let mut implementor = Implementor::new(node.def(), TraitKind::Default);
        for value in &node.values {
            let value_type = value.type_expr();
            implementor = implementor.add_impl_constraint(quote!(#value_type: Default));
        }
        let values = node.values.iter().map(|_| quote!(Default::default()));
        let tokens = implementor
            .set_tokens(quote! {
                fn default() -> Self {
                    Self(#(#values),*)
                }
            })
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

pub(crate) fn validate_struct_default_request(
    node_kind: &str,
    def: &Def,
    fields: &FieldList,
) -> Result<(), DarlingError> {
    let missing: Vec<_> = fields
        .iter()
        .filter(|field| !field.has_rust_default())
        .map(|field| format!("`{}`", field.ident))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }

    let (field_word, verb, value_word) = if missing.len() == 1 {
        ("field", "has", "value")
    } else {
        ("fields", "have", "values")
    };
    Err(DarlingError::custom(format!(
        "Default was requested for {node_kind} {}, but required {field_word} {} {verb} no Rust construction {value_word}",
        def.ident(),
        missing.join(", "),
    ))
    .with_span(&def.ident()))
}

// Build one explicit `Default` impl for a struct-like node from field assignments.
fn struct_default_strategy(
    def: &Def,
    assignments: impl IntoIterator<Item = TokenStream>,
) -> TraitStrategy {
    let assignments: Vec<_> = assignments.into_iter().collect();
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
fn record_default_assignment(field: &Field) -> Option<TokenStream> {
    let ident = &field.ident;
    let expr = field.rust_default_expr()?;

    Some(quote!(#ident: #expr))
}

// Entity primary keys keep their key-conversion behavior when they have an
// explicit schema-surface construction value.
fn entity_default_assignment(field: &Field, primary_keys: &[Ident]) -> Option<TokenStream> {
    let ident = &field.ident;
    let expr = field.rust_default_expr()?;

    Some(
        if primary_keys.iter().any(|primary_key| ident == primary_key) {
            if let Some(FieldGeneration::Insert(generator)) = &field.generated {
                quote!(#ident: (#generator).into())
            } else {
                quote!(#ident: #expr)
            }
        } else {
            quote!(#ident: #expr)
        },
    )
}

fn default_generation_invariant_error(def: &Def) -> TraitStrategy {
    let message = format!(
        "internal Default generation invariant failed for {} after validation",
        def.ident()
    );

    TraitStrategy::from_impl(quote!(compile_error!(#message);))
}
