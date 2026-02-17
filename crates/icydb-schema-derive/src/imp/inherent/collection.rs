use crate::{
    imp::inherent::{
        InherentTrait,
        model::{model_kind_from_item, model_kind_from_nested_value},
    },
    prelude::*,
};

///
/// List
///

impl Imp<List> for InherentTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let item_kind = model_kind_from_item(&node.item);
        let kind = quote!(::icydb::model::field::FieldKind::List(&#item_kind));
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::FieldKind = #kind;

            /// Appends an item to the list.
            pub fn push(&mut self, value: #item) {
                self.0.push(value);
            }

            /// Removes and returns the last item, if any.
            pub fn pop(&mut self) -> Option<#item> {
                self.0.pop()
            }

            /// Inserts an item at `index`, clamping out-of-bounds indices to the tail.
            pub fn insert(&mut self, index: usize, value: #item) {
                let idx = index.min(self.0.len());
                self.0.insert(idx, value);
            }

            /// Removes and returns the item at `index` if it exists.
            pub fn remove(&mut self, index: usize) -> Option<#item> {
                if index < self.0.len() {
                    Some(self.0.remove(index))
                } else {
                    None
                }
            }

            /// Clears all items from the list.
            pub fn clear(&mut self) {
                self.0.clear();
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(quote! { #tokens }))
    }
}

///
/// Set
///

impl Imp<Set> for InherentTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let item_kind = model_kind_from_item(&node.item);
        let kind = quote!(::icydb::model::field::FieldKind::Set(&#item_kind));
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::FieldKind = #kind;

            /// Inserts a value into the set. Returns true if it was newly inserted.
            pub fn insert(&mut self, value: #item) -> bool {
                self.0.insert(value)
            }

            /// Removes a value from the set. Returns true if it was present.
            pub fn remove(&mut self, value: &#item) -> bool {
                self.0.remove(value)
            }

            /// Clears all values from the set.
            pub fn clear(&mut self) {
                self.0.clear();
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(quote! { #tokens }))
    }
}

///
/// Map
///

impl Imp<Map> for InherentTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key_kind = model_kind_from_item(&node.key);
        let value_kind = model_kind_from_nested_value(&node.value);
        let key = node.key.type_expr();
        let value = node.value.type_expr();
        let kind = quote! {
            ::icydb::model::field::FieldKind::Map {
                key: &#key_kind,
                value: &#value_kind,
            }
        };

        let tokens = quote! {
            pub const KIND: ::icydb::model::field::FieldKind = #kind;

            /// Returns a reference to the value for `key`, if present.
            pub fn get(&self, key: &#key) -> Option<&#value> {
                self.0.get(key)
            }

            /// Inserts a key/value pair, returning the previous value if any.
            pub fn insert(&mut self, key: #key, value: #value) -> Option<#value> {
                self.0.insert(key, value)
            }

            /// Removes the value for `key`, returning it if present.
            pub fn remove(&mut self, key: &#key) -> Option<#value> {
                self.0.remove(key)
            }

            /// Clears all entries from the map.
            pub fn clear(&mut self) {
                self.0.clear();
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
