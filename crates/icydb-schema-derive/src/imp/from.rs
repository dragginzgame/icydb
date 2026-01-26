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

        let q = quote! {
            fn from(entries: Vec<I>) -> Self {
                Self(entries
                    .into_iter()
                    .map(Into::into)
                    .collect())
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::From)
            .set_tokens(q)
            .add_impl_constraint(quote!(I: Into<#item>))
            .add_impl_generic(quote!(I))
            .add_trait_generic(quote!(Vec<I>))
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Map
///

impl Imp<Map> for FromTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key = &node.key.type_expr();
        let value = &node.value.type_expr();

        let q = quote! {
            fn from(entries: Vec<(IK, IV)>) -> Self {
                Self(entries
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect())
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::From)
            .set_tokens(q)
            .add_impl_constraint(quote!(IK: Into<#key>))
            .add_impl_constraint(quote!(IV: Into<#value>))
            .add_impl_generic(quote!(IK))
            .add_impl_generic(quote!(IV))
            .add_trait_generic(quote!(Vec<(IK, IV)>))
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Newtype
///

impl Imp<Newtype> for FromTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let item = &node.item.type_expr();

        let q = quote! {
            fn from(t: T) -> Self {
                Self(t.into())
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::From)
            .set_tokens(q)
            .add_impl_constraint(quote!(T: Into<#item>))
            .add_impl_generic(quote!(T))
            .add_trait_generic(quote!(T))
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Set
///

impl Imp<Set> for FromTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = &node.item.type_expr();

        let q = quote! {
            fn from(entries: Vec<I>) -> Self {
                Self(entries
                    .into_iter()
                    .map(Into::into)
                    .collect())
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::From)
            .set_tokens(q)
            .add_impl_constraint(quote!(I: Into<#item>))
            .add_impl_generic(quote!(I))
            .add_trait_generic(quote!(Vec<I>))
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
