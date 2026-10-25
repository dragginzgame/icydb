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
        let paths = node.def().paths();
        Some(default_strategy_entity(node, paths))
    }
}

///
/// Enum
///

impl Imp<Enum> for DefaultTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        let Some(default_variant) = node.default_variant() else {
            return Some(TraitStrategy::from_impl(quote!(compile_error!(
                "default variant is required for Default"
            ))));
        };
        let variant_ident = &default_variant.name;

        let mut implementor = Implementor::new(node.def(), TraitKind::Default);
        let inner = if let Some(value) = &default_variant.value {
            let value_type = value.type_expr(paths);
            implementor =
                implementor.add_impl_constraint(quote!(#value_type: ::core::default::Default));
            quote!(Self::#variant_ident(::core::default::Default::default()))
        } else {
            quote!(Self::#variant_ident)
        };

        let q = quote! {
            fn default() -> Self {
                #inner
            }
        };

        let tokens = implementor.set_tokens(q).impl_tokens(paths);

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Record
///

impl Imp<Record> for DefaultTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        Some(record_default_strategy(&node.def, &node.fields, paths))
    }
}

// Explicit record defaults derive only when every field uses its implicit value.
fn record_default_strategy(def: &Def, fields: &FieldList, paths: &CratePaths) -> TraitStrategy {
    if fields.iter().all(Field::default_matches_implicit_default) {
        return TraitStrategy::from_derive(TraitKind::Default);
    }

    let Some(assignments) = fields
        .iter()
        .map(|value| record_default_assignment(value, paths))
        .collect::<Option<Vec<_>>>()
    else {
        return default_generation_invariant_error(def);
    };

    struct_default_strategy(def, assignments, paths)
}

fn default_strategy_entity(node: &Entity, paths: &CratePaths) -> TraitStrategy {
    let fields = &node.fields;
    if fields.iter().all(Field::default_matches_implicit_default) {
        return TraitStrategy::from_derive(TraitKind::Default);
    }

    let Some(assignments) = fields
        .iter()
        .map(|field| entity_default_assignment(field, node.primary_key.fields(), paths))
        .collect::<Option<Vec<_>>>()
    else {
        return default_generation_invariant_error(node.def());
    };

    struct_default_strategy(node.def(), assignments, paths)
}

///
/// Newtype
///

impl Imp<Newtype> for DefaultTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        let Some(default_expr) = &node.default else {
            return Some(default_generation_invariant_error(node.def()));
        };

        let default_expr = if matches!(
            (node.item.primitive, default_expr),
            (Some(Primitive::Blob), Arg::String(_))
        ) {
            let Ok(expression) = schema_default_rust_expr(default_expr, node.item.primitive, paths)
            else {
                return Some(default_generation_invariant_error(node.def()));
            };
            expression
        } else {
            quote!(#default_expr.into())
        };
        let q = quote! {
            fn default() -> Self {
                Self(#default_expr)
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Default)
            .set_tokens(q)
            .impl_tokens(paths);

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for DefaultTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        let mut implementor = Implementor::new(node.def(), TraitKind::Default);
        for value in &node.values {
            let value_type = value.type_expr(paths);
            implementor =
                implementor.add_impl_constraint(quote!(#value_type: ::core::default::Default));
        }
        let values = node
            .values
            .iter()
            .map(|_| quote!(::core::default::Default::default()));
        let tokens = implementor
            .set_tokens(quote! {
                fn default() -> Self {
                    Self(#(#values),*)
                }
            })
            .impl_tokens(paths);

        Some(TraitStrategy::from_impl(tokens))
    }
}

pub(crate) fn validate_struct_default_request(
    node_kind: &str,
    def: &Def,
    fields: &FieldList,
    paths: &CratePaths,
) -> Result<(), DarlingError> {
    let missing: Vec<_> = fields
        .iter()
        .filter(|field| !field.has_rust_default(paths))
        .map(|field| format!("`{}`", field.name))
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
    paths: &CratePaths,
) -> TraitStrategy {
    let assignments: Vec<_> = assignments.into_iter().collect();
    let tokens = Implementor::new(def, TraitKind::Default)
        .set_tokens(quote! {
            fn default() -> Self {
                Self { #(#assignments),* }
            }
        })
        .impl_tokens(paths);

    TraitStrategy::from_impl(tokens)
}

// Record fields lower only through schema-surface Rust construction values.
fn record_default_assignment(field: &Field, paths: &CratePaths) -> Option<TokenStream> {
    let ident = &field.name;
    let expr = field.rust_default_expr(paths)?;

    Some(quote!(#ident: #expr))
}

// Entity primary keys keep their key-conversion behavior when they have an
// explicit schema-surface construction value.
fn entity_default_assignment(
    field: &Field,
    primary_keys: &[Ident],
    paths: &CratePaths,
) -> Option<TokenStream> {
    let ident = &field.name;
    let expr = field.rust_default_expr(paths)?;

    Some(
        if primary_keys.iter().any(|primary_key| ident == primary_key) {
            if field.is_identity_generated() {
                quote!(#ident: #expr)
            } else if let Some(FieldGeneration::Insert(generator)) = &field.generated {
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
