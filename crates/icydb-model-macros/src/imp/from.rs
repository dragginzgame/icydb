//! Module: imp::from
//! Responsibility: generated implementation tokens.
//! Does not own: runtime trait semantics.
//! Boundary: parsed nodes to impl tokens.

use crate::prelude::*;

///
/// FromTrait
/// to and from ::View types is handled with a blanket impl
///

pub struct FromTrait {}

///
/// List
///

impl Imp<List> for FromTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        let item = &node.item.type_expr(paths);
        Some(vec_collection_from_strategy(
            node.def(),
            item,
            quote!(entries.into_iter().map(Into::into).collect()),
            None,
            paths,
        ))
    }
}

///
/// Map
///

impl Imp<Map> for FromTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        let key = &node.key.type_expr(paths);
        let value = &node.value.type_expr(paths);
        Some(map_from_strategy(node.def(), key, value, paths))
    }
}

///
/// Newtype
///

impl Imp<Newtype> for FromTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        let item = &node.item.type_expr(paths);
        Some(newtype_from_strategy(node.def(), item, paths))
    }
}

///
/// Set
///

impl Imp<Set> for FromTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        let item = &node.item.type_expr(paths);
        Some(vec_collection_from_strategy(
            node.def(),
            item,
            quote!(entries.into_iter().map(Into::into).collect()),
            Some(quote!(#item: ::std::cmp::Ord)),
            paths,
        ))
    }
}

fn vec_collection_from_strategy(
    def: &Def,
    item: &TokenStream,
    body: TokenStream,
    extra_constraint: Option<TokenStream>,
    paths: &CratePaths,
) -> TraitStrategy {
    let mut implementor = Implementor::new(def, TraitKind::From)
        .set_tokens(vec_collection_from_tokens(body))
        .add_impl_constraint(quote!(I: Into<#item>))
        .add_impl_generic(quote!(I))
        .add_trait_generic(quote!(Vec<I>));

    if let Some(extra_constraint) = extra_constraint {
        implementor = implementor.add_impl_constraint(extra_constraint);
    }

    TraitStrategy::from_impl(implementor.impl_tokens(paths))
}

fn vec_collection_from_tokens(body: TokenStream) -> TokenStream {
    quote! {
        fn from(entries: Vec<I>) -> Self {
            Self(#body)
        }
    }
}

fn map_from_strategy(
    def: &Def,
    key: &TokenStream,
    value: &TokenStream,
    paths: &CratePaths,
) -> TraitStrategy {
    let tokens = Implementor::new(def, TraitKind::From)
        .set_tokens(quote! {
            fn from(entries: Vec<(IK, IV)>) -> Self {
                Self(
                    entries
                        .into_iter()
                        .map(|(key, value)| (key.into(), value.into()))
                        .collect(),
                )
            }
        })
        .add_impl_constraint(quote!(IK: Into<#key>))
        .add_impl_constraint(quote!(IV: Into<#value>))
        .add_impl_constraint(quote!(#key: ::std::cmp::Ord))
        .add_impl_generic(quote!(IK))
        .add_impl_generic(quote!(IV))
        .add_trait_generic(quote!(Vec<(IK, IV)>))
        .impl_tokens(paths);

    TraitStrategy::from_impl(tokens)
}

fn newtype_from_strategy(def: &Def, item: &TokenStream, paths: &CratePaths) -> TraitStrategy {
    let tokens = Implementor::new(def, TraitKind::From)
        .set_tokens(quote! {
            fn from(t: T) -> Self {
                Self(t.into())
            }
        })
        .add_impl_constraint(quote!(T: Into<#item>))
        .add_impl_generic(quote!(T))
        .add_trait_generic(quote!(T))
        .impl_tokens(paths);

    TraitStrategy::from_impl(tokens)
}
