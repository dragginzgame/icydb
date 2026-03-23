use crate::prelude::*;

///
/// CollectionTrait
///

pub struct CollectionTrait {}

///
/// MapCollectionTrait
///

pub struct MapCollectionTrait {}

///
/// List
///

impl Imp<List> for CollectionTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let iter_ty = quote!(::std::slice::Iter<'a, #item>);

        Some(collection_trait_strategy(node.def(), item, iter_ty))
    }
}

///
/// Set
///

impl Imp<Set> for CollectionTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let iter_ty = quote!(::std::collections::btree_set::Iter<'a, #item>);

        Some(collection_trait_strategy(node.def(), item, iter_ty))
    }
}

///
/// Map
///

impl Imp<Map> for MapCollectionTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key = node.key.type_expr();
        let value = node.value.type_expr();
        let iter_ty = quote!(::std::collections::btree_map::Iter<'a, #key, #value>);

        Some(map_collection_trait_strategy(
            node.def(),
            key,
            value,
            iter_ty,
        ))
    }
}

fn collection_trait_strategy(def: &Def, item: TokenStream, iter_ty: TokenStream) -> TraitStrategy {
    let tokens = Implementor::new(def, TraitKind::Collection)
        .set_tokens(collection_trait_tokens(item, iter_ty))
        .to_token_stream();

    TraitStrategy::from_impl(tokens)
}

fn collection_trait_tokens(item: TokenStream, iter_ty: TokenStream) -> TokenStream {
    quote! {
        type Item = #item;

        type Iter<'a> = #iter_ty
        where
            Self: 'a;

        fn iter(&self) -> Self::Iter<'_> {
            self.0.iter()
        }

        fn len(&self) -> usize {
            self.0.len()
        }
    }
}

fn map_collection_trait_strategy(
    def: &Def,
    key: TokenStream,
    value: TokenStream,
    iter_ty: TokenStream,
) -> TraitStrategy {
    let tokens = Implementor::new(def, TraitKind::MapCollection)
        .set_tokens(map_collection_trait_tokens(key, value, iter_ty))
        .to_token_stream();

    TraitStrategy::from_impl(tokens)
}

fn map_collection_trait_tokens(
    key: TokenStream,
    value: TokenStream,
    iter_ty: TokenStream,
) -> TokenStream {
    quote! {
        type Key = #key;
        type Value = #value;

        type Iter<'a> = #iter_ty
        where
            Self: 'a;

        fn iter(&self) -> Self::Iter<'_> {
            self.0.iter()
        }

        fn len(&self) -> usize {
            self.0.len()
        }
    }
}
