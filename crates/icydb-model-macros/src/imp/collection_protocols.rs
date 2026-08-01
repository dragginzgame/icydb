//! Module: imp::collection_protocols
//! Responsibility: generated standard collection protocol implementations.
//! Does not own: collection storage or application trait directives.
//! Boundary: list, set, and map nodes to standard iterator trait tokens.

use crate::prelude::*;

pub struct FromIteratorTrait {}

pub struct IntoIteratorTrait {}

impl Imp<List> for FromIteratorTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        Some(from_iterator_strategy(node.def(), quote!(#item), None))
    }
}

impl Imp<Map> for FromIteratorTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key = node.key.type_expr();
        let value = node.value.type_expr();
        Some(from_iterator_strategy(
            node.def(),
            quote!((#key, #value)),
            Some(quote!(#key: ::std::cmp::Ord)),
        ))
    }
}

impl Imp<Set> for FromIteratorTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        Some(from_iterator_strategy(
            node.def(),
            quote!(#item),
            Some(quote!(#item: ::std::cmp::Ord)),
        ))
    }
}

fn from_iterator_strategy(
    def: &Def,
    iterator_item: TokenStream,
    constraint: Option<TokenStream>,
) -> TraitStrategy {
    let mut implementor = Implementor::new(def, TraitKind::FromIterator)
        .add_trait_generic(iterator_item.clone())
        .set_tokens(quote! {
            fn from_iter<T: ::std::iter::IntoIterator<Item = #iterator_item>>(iter: T) -> Self {
                Self(iter.into_iter().collect())
            }
        });

    if let Some(constraint) = constraint {
        implementor = implementor.add_impl_constraint(constraint);
    }

    TraitStrategy::from_impl(implementor.to_token_stream())
}

impl Imp<List> for IntoIteratorTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let lifetime = available_iteration_lifetime(node.def());
        let mut tokens = owned_into_iterator_tokens(
            node.def(),
            quote!(#item),
            quote!(::std::vec::IntoIter<#item>),
        );
        tokens.extend(reference_into_iterator_tokens(
            node.def(),
            lifetime.clone(),
            false,
            quote!(&#lifetime #item),
            quote!(::std::slice::Iter<#lifetime, #item>),
        ));
        tokens.extend(reference_into_iterator_tokens(
            node.def(),
            lifetime.clone(),
            true,
            quote!(&#lifetime mut #item),
            quote!(::std::slice::IterMut<#lifetime, #item>),
        ));

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<Map> for IntoIteratorTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key = node.key.type_expr();
        let value = node.value.type_expr();
        let lifetime = available_iteration_lifetime(node.def());
        let mut tokens = owned_into_iterator_tokens(
            node.def(),
            quote!((#key, #value)),
            quote!(::std::collections::btree_map::IntoIter<#key, #value>),
        );
        tokens.extend(reference_into_iterator_tokens(
            node.def(),
            lifetime.clone(),
            false,
            quote!((&#lifetime #key, &#lifetime #value)),
            quote!(::std::collections::btree_map::Iter<#lifetime, #key, #value>),
        ));
        tokens.extend(reference_into_iterator_tokens(
            node.def(),
            lifetime.clone(),
            true,
            quote!((&#lifetime #key, &#lifetime mut #value)),
            quote!(::std::collections::btree_map::IterMut<#lifetime, #key, #value>),
        ));

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<Set> for IntoIteratorTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let lifetime = available_iteration_lifetime(node.def());
        let mut tokens = owned_into_iterator_tokens(
            node.def(),
            quote!(#item),
            quote!(::std::collections::btree_set::IntoIter<#item>),
        );
        tokens.extend(reference_into_iterator_tokens(
            node.def(),
            lifetime.clone(),
            false,
            quote!(&#lifetime #item),
            quote!(::std::collections::btree_set::Iter<#lifetime, #item>),
        ));

        Some(TraitStrategy::from_impl(tokens))
    }
}

fn owned_into_iterator_tokens(
    def: &Def,
    owned_item: TokenStream,
    owned_iterator: TokenStream,
) -> TokenStream {
    Implementor::new(def, TraitKind::IntoIterator)
        .set_tokens(quote! {
            type Item = #owned_item;
            type IntoIter = #owned_iterator;

            fn into_iter(self) -> Self::IntoIter {
                self.0.into_iter()
            }
        })
        .to_token_stream()
}

fn reference_into_iterator_tokens(
    def: &Def,
    lifetime: syn::Lifetime,
    mutable: bool,
    item: TokenStream,
    iterator: TokenStream,
) -> TokenStream {
    let ident = def.ident();
    let definition = def
        .item
        .as_ref()
        .unwrap_or_else(|| panic!("collection definition metadata must be attached"));
    let (_, ty_generics, _) = definition.generics.split_for_impl();
    let self_type = if mutable {
        quote!(&#lifetime mut #ident #ty_generics)
    } else {
        quote!(&#lifetime #ident #ty_generics)
    };
    let iterator_expression = if mutable {
        quote!(self.0.iter_mut())
    } else {
        quote!(self.0.iter())
    };

    Implementor::new(def, TraitKind::IntoIterator)
        .add_impl_generic(quote!(#lifetime))
        .set_trait_self_type(self_type)
        .set_tokens(quote! {
            type Item = #item;
            type IntoIter = #iterator;

            fn into_iter(self) -> Self::IntoIter {
                #iterator_expression
            }
        })
        .to_token_stream()
}

// Borrowed iterator impls introduce one lifetime beside the authored generics;
// choose a reserved spelling that cannot shadow an authored lifetime.
fn available_iteration_lifetime(def: &Def) -> syn::Lifetime {
    let definition = def
        .item
        .as_ref()
        .unwrap_or_else(|| panic!("collection definition metadata must be attached"));
    let mut suffix = 0usize;

    loop {
        let name = if suffix == 0 {
            "__icydb_iter".to_string()
        } else {
            format!("__icydb_iter_{suffix}")
        };
        if definition
            .generics
            .lifetimes()
            .all(|parameter| parameter.lifetime.ident != name.as_str())
        {
            return syn::Lifetime::new(&format!("'{name}"), Span::call_site());
        }
        suffix += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_iteration_uses_a_lifetime_not_owned_by_the_authored_type() {
        let def = Def::new(syn::parse_quote! {
            pub struct GenericCollection<'__icydb_iter> {
                marker: ::std::marker::PhantomData<&'__icydb_iter ()>,
            }
        });

        assert_eq!(
            available_iteration_lifetime(&def).to_string(),
            "'__icydb_iter_1",
        );
    }
}
