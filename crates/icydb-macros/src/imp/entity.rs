use crate::prelude::*;

///
/// EntityKindTrait
///

pub struct EntityKindTrait {}

impl Imp<Entity> for EntityKindTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let store = &node.store;
        let pk_field = &node.primary_key.to_string();
        let pk_type = &node
            .fields
            .get(&node.primary_key)
            .unwrap()
            .value
            .item
            .type_expr();

        // instead of string literals, reference the inherent const idents
        let field_refs: Vec<Ident> = node.fields.iter().map(Field::const_ident).collect();

        // indexes
        let indexes = &node
            .indexes
            .iter()
            .map(Index::runtime_part)
            .collect::<Vec<_>>();

        // paths
        let cp = paths().core;

        // static definitions
        let mut q = quote! {
            type PrimaryKey = #pk_type;
            type Store = #store;
            type Canister = <Self::Store as #cp::traits::StoreKind>::Canister;

            const ENTITY_ID: u64 = #cp::hash::fnv1a_64(Self::PATH.as_bytes());
            const PRIMARY_KEY: &'static str = #pk_field;
            const FIELDS: &'static [&'static str]  = &[ #( Self::#field_refs ),* ];
            const INDEXES: &'static [&'static #cp::IndexSpec]  = &[#(&#indexes),*];
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
    }
}
