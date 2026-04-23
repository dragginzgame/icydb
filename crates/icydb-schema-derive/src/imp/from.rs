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
        let item = &node.item.type_expr();
        Some(vec_collection_from_strategy(
            node.def(),
            item,
            quote!(::icydb::__macro::value_surface_from_vec_into::<#item, I>(entries)),
            None,
        ))
    }
}

///
/// Map
///

impl Imp<Map> for FromTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key = &node.key.type_expr();
        let value = &node.value.type_expr();
        Some(map_from_strategy(node.def(), key, value))
    }
}

///
/// Newtype
///

impl Imp<Newtype> for FromTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let item = &node.item.type_expr();
        Some(newtype_from_strategy(node.def(), item))
    }
}

///
/// Set
///

impl Imp<Set> for FromTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = &node.item.type_expr();
        Some(vec_collection_from_strategy(
            node.def(),
            item,
            quote!(::icydb::__macro::value_surface_from_vec_into_btree_set::<#item, I>(entries)),
            Some(quote!(#item: ::std::cmp::Ord)),
        ))
    }
}

fn vec_collection_from_strategy(
    def: &Def,
    item: &TokenStream,
    body: TokenStream,
    extra_constraint: Option<TokenStream>,
) -> TraitStrategy {
    let mut implementor = Implementor::new(def, TraitKind::From)
        .set_tokens(vec_collection_from_tokens(body))
        .add_impl_constraint(quote!(I: Into<#item>))
        .add_impl_generic(quote!(I))
        .add_trait_generic(quote!(Vec<I>));

    if let Some(extra_constraint) = extra_constraint {
        implementor = implementor.add_impl_constraint(extra_constraint);
    }

    TraitStrategy::from_impl(implementor.to_token_stream())
}

fn vec_collection_from_tokens(body: TokenStream) -> TokenStream {
    quote! {
        fn from(entries: Vec<I>) -> Self {
            Self(#body)
        }
    }
}

fn map_from_strategy(def: &Def, key: &TokenStream, value: &TokenStream) -> TraitStrategy {
    let tokens = Implementor::new(def, TraitKind::From)
        .set_tokens(quote! {
            fn from(entries: Vec<(IK, IV)>) -> Self {
                Self(::icydb::__macro::value_surface_from_vec_into_btree_map::<#key, #value, IK, IV>(
                    entries,
                ))
            }
        })
        .add_impl_constraint(quote!(IK: Into<#key>))
        .add_impl_constraint(quote!(IV: Into<#value>))
        .add_impl_constraint(quote!(#key: ::std::cmp::Ord))
        .add_impl_generic(quote!(IK))
        .add_impl_generic(quote!(IV))
        .add_trait_generic(quote!(Vec<(IK, IV)>))
        .to_token_stream();

    TraitStrategy::from_impl(tokens)
}

fn newtype_from_strategy(def: &Def, item: &TokenStream) -> TraitStrategy {
    let tokens = Implementor::new(def, TraitKind::From)
        .set_tokens(quote! {
            fn from(t: T) -> Self {
                Self(::icydb::__macro::value_surface_into::<#item, T>(t))
            }
        })
        .add_impl_constraint(quote!(T: Into<#item>))
        .add_impl_generic(quote!(T))
        .add_trait_generic(quote!(T))
        .to_token_stream();

    TraitStrategy::from_impl(tokens)
}
