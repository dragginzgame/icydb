use crate::prelude::*;

///
/// EntityKindTrait
///

pub struct EntityKindTrait {}

impl Imp<Entity> for EntityKindTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let store = &node.store;

        let Some(pk_entry) = node.fields.get(&node.primary_key.field) else {
            let msg = LitStr::new(
                &format!(
                    "primary key field '{}' not found in entity fields",
                    node.primary_key.field
                ),
                Span::call_site(),
            );
            return Some(TraitStrategy::from_impl(quote!(compile_error!(#msg))));
        };

        let pk_ident = &node.primary_key.field;
        let key_type = if let Some(target) = &pk_entry.value.item.relation {
            quote!(<#target as ::icydb::traits::EntityStorageKey>::Key)
        } else {
            pk_entry.value.item.type_expr()
        };

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
            .map(Index::runtime_part)
            .collect::<Vec<_>>();
        let ident = node.def.ident();

        let storage_tokens = quote! {
            impl ::icydb::traits::EntityStorageKey for #ident {
                type Key = #key_type;
            }
        };

        let identity_tokens = Implementor::new(&node.def, TraitKind::EntityIdentity)
            .set_tokens(quote! {
                const ENTITY_NAME: &'static str = #entity_name;
                const PRIMARY_KEY: &'static str = stringify!(#pk_ident);
                const IDENTITY_NAMESPACE: &'static str = #entity_name;
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
                type DataStore = #store;
                type Canister =
                    <Self::DataStore as ::icydb::traits::DataStoreKind>::Canister;
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

        // Existing consistency test stays valid
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

        // Unit-key logic remains tied to the schema layer.
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
        let pk_is_relation = node
            .fields
            .get(pk_ident)
            .is_some_and(|entry| entry.value.item.is_relation());

        let id_expr = if pk_is_relation {
            quote!({
                let value = ::icydb::traits::FieldValue::to_value(&self.#pk_ident);

                <::icydb::types::Id<Self> as ::icydb::traits::FieldValue>::from_value(&value)
                    .expect("relation primary key must decode into Id<Self>")
            })
        } else {
            quote!(self.#pk_ident)
        };

        let q = quote! {
            fn id(&self) -> ::icydb::types::Id<Self> {
                #id_expr
            }
        };

        let tokens = Implementor::new(&node.def, TraitKind::EntityValue)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
