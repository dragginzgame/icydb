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

            fn iter<'a>(&'a self) -> Box<dyn ::std::iter::Iterator<Item = &'a Self::Item> + 'a> {
                Box::new(self.0.iter())
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

            fn iter<'a>(&'a self) -> Box<dyn ::std::iter::Iterator<Item = &'a Self::Item> + 'a> {
                Box::new(self.0.iter())
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

            fn iter<'a>(
                &'a self,
            ) -> Box<
                dyn ::std::iter::Iterator<Item = (&'a Self::Key, &'a Self::Value)> + 'a,
            > {
                Box::new(self.0.iter())
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
