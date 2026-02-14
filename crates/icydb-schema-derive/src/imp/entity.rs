use crate::prelude::*;

///
/// EntityKindTrait
///

pub struct EntityKindTrait {}

impl Imp<Entity> for EntityKindTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let store = &node.store;

        let pk_entry = node
            .fields
            .get(&node.primary_key.field)
            .expect("primary key field must be validated before derive generation");
        let pk_ident = &node.primary_key.field;

        // PK key shape must always follow the declared field type.
        let pk_key_type = pk_entry.value.item.type_expr();

        let resolved_entity_name = node
            .name
            .as_ref()
            .map_or_else(|| node.def.ident().to_string(), LitStr::value);

        let entity_name = if let Some(name) = &node.name {
            quote!(#name)
        } else {
            let ident = node.def.ident();
            quote!(stringify!(#ident))
        };

        let field_refs: Vec<Ident> = node.fields.iter().map(Field::const_ident).collect();

        let indexes = node
            .indexes
            .iter()
            .map(|index| index.runtime_part(&resolved_entity_name, store))
            .collect::<Vec<_>>();

        let ident = node.def.ident();

        let storage_tokens = quote! {
            impl ::icydb::traits::EntityKey for #ident {
                type Key = #pk_key_type;
            }
        };

        let identity_tokens = Implementor::new(&node.def, TraitKind::EntityIdentity)
            .set_tokens(quote! {
                const ENTITY_NAME: &'static str = #entity_name;
                const PRIMARY_KEY: &'static str = stringify!(#pk_ident);
            })
            .to_token_stream();

        let schema_tokens = Implementor::new(&node.def, TraitKind::EntitySchema)
            .set_tokens(quote! {
                const FIELDS: &'static [&'static str] = &[
                    #( Self::#field_refs.as_str() ),*
                ];
                const INDEXES: &'static [&'static ::icydb::model::index::IndexModel] =
                    &[#(&#indexes),*];
                const MODEL: &'static ::icydb::model::entity::EntityModel =
                    &Self::__ENTITY_MODEL;
            })
            .to_token_stream();

        let placement_tokens = Implementor::new(&node.def, TraitKind::EntityPlacement)
            .set_tokens(quote! {
                type Store = #store;
                type Canister =
                    <Self::Store as ::icydb::traits::StoreKind>::Canister;
            })
            .to_token_stream();

        let kind_tokens = Implementor::new(&node.def, TraitKind::EntityKind)
            .set_tokens(quote! {})
            .to_token_stream();

        let mut tokens = TokenStream::new();
        tokens.extend(storage_tokens);
        tokens.extend(identity_tokens);
        tokens.extend(schema_tokens);
        tokens.extend(placement_tokens);
        tokens.extend(kind_tokens);

        let test_mod = format_ident!("__entity_model_test_{ident}");
        tokens.extend(quote! {
            #[cfg(test)]
            mod #test_mod {
                use super::*;

                #[test]
                fn model_consistency() {
                    let model = <#ident as ::icydb::traits::EntitySchema>::MODEL;
                    let names = <#ident as ::icydb::traits::EntitySchema>::FIELDS;

                    assert_eq!(model.fields.len(), names.len());
                    for (field, name) in model.fields.iter().zip(names.iter()) {
                        assert_eq!(field.name, *name);
                    }

                    assert!(model
                        .fields
                        .iter()
                        .any(|field| ::core::ptr::eq(field, model.primary_key)));
                }
            }
        });

        // Unit primary keys model singleton entities.
        if matches!(
            pk_entry.value.item.target(),
            ItemTarget::Primitive(Primitive::Unit)
        ) {
            tokens.extend(quote! {
                impl ::icydb::traits::SingletonEntity for #ident {}
            });
        }

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// EntityValueTrait
///

pub struct EntityValueTrait {}

impl Imp<Entity> for EntityValueTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let pk_ident = &node.primary_key.field;

        let tokens = Implementor::new(&node.def, TraitKind::EntityValue)
            .set_tokens(quote! {
                fn id(&self) -> ::icydb::types::Id<Self> {
                    ::icydb::types::Id::from_key(self.#pk_ident)
                }
            })
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
