use crate::prelude::*;

///
/// FilterableTrait
///

pub struct FilterableTrait;

impl Imp<Enum> for FilterableTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let q = quote! {
            type Filter = ::icydb::db::primitives::NoFilterKind;
            type ListFilter = ::icydb::db::primitives::NoFilterKind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Filterable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<List> for FilterableTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let q = quote! {
            type Filter = ::icydb::db::primitives::NoFilterKind;
            type ListFilter = ::icydb::db::primitives::NoFilterKind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Filterable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<Map> for FilterableTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let q = quote! {
            type Filter = ::icydb::db::primitives::NoFilterKind;
            type ListFilter = ::icydb::db::primitives::NoFilterKind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Filterable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<Newtype> for FilterableTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let item_ty = node.item.type_expr();

        let q = quote! {
            type Filter = <#item_ty as ::icydb::traits::Filterable>::Filter;
            type ListFilter = <#item_ty as ::icydb::traits::Filterable>::ListFilter;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Filterable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<Record> for FilterableTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let q = quote! {
            type Filter = ::icydb::db::primitives::NoFilterKind;
            type ListFilter = ::icydb::db::primitives::NoFilterKind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Filterable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<Set> for FilterableTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let q = quote! {
            type Filter = ::icydb::db::primitives::NoFilterKind;
            type ListFilter = ::icydb::db::primitives::NoFilterKind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Filterable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<Tuple> for FilterableTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let q = quote! {
            type Filter = ::icydb::db::primitives::NoFilterKind;
            type ListFilter = ::icydb::db::primitives::NoFilterKind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Filterable)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
