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
    if fields.iter().all(Field::default_matches_implicit_default) {
        return TraitStrategy::from_derive(TraitKind::Default);
    }

    struct_default_strategy(def, fields.iter().map(record_default_assignment))
}

fn default_strategy_entity(node: &Entity) -> TraitStrategy {
    let fields = &node.fields;
    if fields.iter().all(Field::default_matches_implicit_default) {
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{default_strategy_entity, record_default_strategy};
    use crate::{
        node::{
            Arg, Def, Entity, Field, FieldList, Item, PrimaryKey, PrimaryKeySource, Record, Type,
            Value,
        },
        trait_kind::{TraitBuilder, TraitKind},
    };
    use icydb_schema::types::Primitive;
    use quote::format_ident;
    use syn::parse_quote;

    fn field_with_primitive_default(ident: &str, primitive: Primitive, default: Arg) -> Field {
        Field {
            ident: format_ident!("{ident}"),
            value: Value {
                opt: false,
                many: false,
                item: Item {
                    primitive: Some(primitive),
                    ..Item::default()
                },
            },
            default: Some(default),
        }
    }

    fn redundant_default_entity() -> Entity {
        Entity {
            def: Def::new(syn::parse_quote!(
                struct RedundantDefaultEntity;
            )),
            store: syn::parse_quote!(UiDataStore),
            primary_key: PrimaryKey {
                field: format_ident!("id"),
                source: PrimaryKeySource::Internal,
            },
            name: None,
            indexes: vec![],
            fields: FieldList {
                fields: vec![
                    field_with_primitive_default(
                        "id",
                        Primitive::Nat64,
                        Arg::FuncPath(parse_quote!(u64::default)),
                    ),
                    field_with_primitive_default(
                        "name",
                        Primitive::Text,
                        Arg::FuncPath(parse_quote!(String::new)),
                    ),
                ],
            },
            ty: Type::default(),
            traits: TraitBuilder::default(),
        }
    }

    #[test]
    fn entity_defaults_derive_when_explicit_defaults_match_implicit_defaults() {
        let strategy = default_strategy_entity(&redundant_default_entity());

        assert_eq!(strategy.derive, Some(TraitKind::Default));
        assert!(
            strategy.imp.is_none(),
            "redundant defaults should not force a manual Default impl",
        );
    }

    #[test]
    fn entity_defaults_keep_manual_impl_for_custom_default_constructors() {
        let mut entity = redundant_default_entity();
        entity.fields.fields[0].default = Some(Arg::FuncPath(parse_quote!(Ulid::generate)));
        entity.fields.fields[0].value.item.primitive = Some(Primitive::Ulid);

        let strategy = default_strategy_entity(&entity);

        assert!(
            strategy.derive.is_none(),
            "custom defaults must still bypass derive(Default)",
        );
        assert!(
            strategy.imp.is_some(),
            "custom defaults still require an explicit Default impl",
        );
    }

    #[test]
    fn records_follow_the_same_redundant_default_rule() {
        let fields = FieldList {
            fields: vec![
                field_with_primitive_default(
                    "enabled",
                    Primitive::Bool,
                    Arg::FuncPath(parse_quote!(bool::default)),
                ),
                field_with_primitive_default(
                    "name",
                    Primitive::Text,
                    Arg::FuncPath(parse_quote!(String::new)),
                ),
            ],
        };
        let record = Record {
            def: Def::new(syn::parse_quote!(
                struct RedundantDefaultRecord;
            )),
            fields,
            traits: TraitBuilder::default(),
            ty: Type::default(),
        };

        let strategy = record_default_strategy(&record.def, &record.fields);

        assert_eq!(strategy.derive, Some(TraitKind::Default));
        assert!(
            strategy.imp.is_none(),
            "records with redundant defaults should derive Default",
        );
    }
}
