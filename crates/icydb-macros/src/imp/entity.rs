use crate::prelude::*;

///
/// EntityKindTrait
///

pub struct EntityKindTrait {}

impl Imp<Entity> for EntityKindTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let store = &node.store;
        let pk_field = &node.primary_key.to_string();
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
        let pk_type = &pk_entry.value.item.type_expr();
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
            type PrimaryKey = #pk_type;
            type Store = #store;
            type Canister = <Self::Store as ::icydb::traits::StoreKind>::Canister;

            const ENTITY_NAME: &'static str = #entity_name;
            const PRIMARY_KEY: &'static str = #pk_field;
            const FIELDS: &'static [&'static str]  = &[ #( Self::#field_refs ),* ];
            const INDEXES: &'static [&'static ::icydb::model::index::IndexModel]  = &[#(&#indexes),*];
        };

        // impls
        q.extend(key(node));

        let tokens = Implementor::new(&node.def, TraitKind::EntityKind)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

// key
fn key(node: &Entity) -> TokenStream {
    let primary_key = &node.primary_key;

    quote! {
        fn key(&self) -> Key {
            self.primary_key().into()
        }

        fn primary_key(&self) -> Self::PrimaryKey {
            self.#primary_key
        }

        fn set_primary_key(&mut self, key: Self::PrimaryKey) {
            self.#primary_key = key;
        }
    }
}
