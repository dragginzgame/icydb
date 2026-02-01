use crate::prelude::*;

///
/// EntityKindTrait
///

pub struct EntityKindTrait {}

impl Imp<Entity> for EntityKindTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let store = &node.store;
        let Some(pk_entry) = node.fields.get(&node.primary_key) else {
            let msg = LitStr::new(
                &format!(
                    "primary key field '{}' not found in entity fields",
                    node.primary_key
                ),
                Span::call_site(),
            );
            return Some(TraitStrategy::from_impl(quote!(compile_error!(#msg))));
        };
        let pk_const_ident = pk_entry.const_ident();
        let _pk_type = &pk_entry.value.item.type_expr();
        let entity_name = if let Some(name) = &node.name {
            quote!(#name)
        } else {
            let ident = node.def.ident();
            quote!(stringify!(#ident))
        };

        // instead of string literals, reference the inherent const idents
        let field_refs: Vec<Ident> = node.fields.iter().map(Field::const_ident).collect();

        // indexes
        let indexes = &node
            .indexes
            .iter()
            .map(Index::runtime_part)
            .collect::<Vec<_>>();

        // static definitions
        let mut q = quote! {
            type PrimaryKey = ::icydb::types::Ref<Self>;
            type DataStore = #store;
            type Canister = <Self::DataStore as ::icydb::traits::DataStoreKind>::Canister;

            const ENTITY_NAME: &'static str = #entity_name;
            const PRIMARY_KEY: &'static str = Self::#pk_const_ident.as_str();
            const FIELDS: &'static [&'static str]  = &[
                #( Self::#field_refs.as_str() ),*
            ];
            const INDEXES: &'static [&'static ::icydb::model::index::IndexModel]  = &[#(&#indexes),*];
            const MODEL: &'static ::icydb::model::entity::EntityModel = &Self::__ENTITY_MODEL;
        };

        // impls
        q.extend(key(node));

        let mut tokens = Implementor::new(&node.def, TraitKind::EntityKind)
            .set_tokens(q)
            .to_token_stream();

        let ident = node.def.ident();
        let test_mod = format_ident!("__entity_model_test_{ident}");
        tokens.extend(quote! {
            #[cfg(test)]
            mod #test_mod {
                use super::*;

                #[test]
                fn model_consistency() {
                    let model = <#ident as ::icydb::traits::EntityKind>::MODEL;
                    let names = <#ident as ::icydb::traits::EntityKind>::FIELDS;

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

        if matches!(
            pk_entry.value.item.target(),
            ItemTarget::Primitive(Primitive::Unit)
        ) {
            tokens.extend(quote! {
                impl ::icydb::traits::UnitKey for #ident {}
            });
        }

        Some(TraitStrategy::from_impl(tokens))
    }
}

// key
fn key(node: &Entity) -> TokenStream {
    let primary_key = &node.primary_key;

    quote! {
        fn key(&self) -> Self::PrimaryKey {
            self.primary_key()
        }

        fn primary_key(&self) -> Self::PrimaryKey {
            self.#primary_key
        }

        fn set_primary_key(&mut self, key: Self::PrimaryKey) {
            self.#primary_key = key;
        }
    }
}
