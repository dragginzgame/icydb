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

        let q = quote! {
            type Item = #item;

            type Iter<'a> = ::std::slice::Iter<'a, #item>
            where
                Self: 'a;

            fn iter(&self) -> Self::Iter<'_> {
                self.0.iter()
            }

            fn len(&self) -> usize {
                self.0.len()
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Collection)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Set
///

impl Imp<Set> for CollectionTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = node.item.type_expr();

        let q = quote! {
            type Item = #item;

            type Iter<'a> = ::std::collections::hash_set::Iter<'a, #item>
            where
                Self: 'a;

            fn iter(&self) -> Self::Iter<'_> {
                self.0.iter()
            }

            fn len(&self) -> usize {
                self.0.len()
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Collection)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Map
///

impl Imp<Map> for MapCollectionTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key = node.key.type_expr();
        let value = node.value.type_expr();

        let q = quote! {
            type Key = #key;
            type Value = #value;

            type Iter<'a> = ::std::collections::hash_map::Iter<'a, #key, #value>
            where
                Self: 'a;

            fn iter(&self) -> Self::Iter<'_> {
                self.0.iter()
            }

            fn len(&self) -> usize {
                self.0.len()
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::MapCollection)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
