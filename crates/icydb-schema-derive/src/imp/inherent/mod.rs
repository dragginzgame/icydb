mod collection;
mod entity;
mod model;
mod relation;

use crate::{
    imp::inherent::{model::model_kind_from_item, relation::relation_accessor_tokens},
    prelude::*,
};

///
/// InherentTrait
///

pub struct InherentTrait {}

///
/// Enum
///

impl Imp<Enum> for InherentTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let kind = quote!(::icydb::model::field::FieldKind::Enum);
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::FieldKind = #kind;
        };

        Some(TraitStrategy::from_impl(
            Implementor::new(node.def(), TraitKind::Inherent)
                .set_tokens(tokens)
                .to_token_stream(),
        ))
    }
}

///
/// Newtype
///

impl Imp<Newtype> for InherentTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let kind = model_kind_from_item(&node.item);
        let mut tokens = quote! {
            pub const KIND: ::icydb::model::field::FieldKind = #kind;
        };

        if let Some(primitive) = node.primitive
            && primitive.supports_arithmetic()
        {
            tokens = quote! {
                #tokens

                /// Saturating addition.
                #[must_use]
                pub fn saturating_add(self, rhs: Self) -> Self {
                    Self(self.0.saturating_add(rhs.0))
                }

                /// Saturating subtraction.
                #[must_use]
                pub fn saturating_sub(self, rhs: Self) -> Self {
                    Self(self.0.saturating_sub(rhs.0))
                }
            };
        }

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Record
///

impl Imp<Record> for InherentTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let kind = quote!(::icydb::model::field::FieldKind::Structured { queryable: false });
        let relation_accessors = relation_accessor_tokens(node.fields.iter());

        let tokens = quote! {
            pub const KIND: ::icydb::model::field::FieldKind = #kind;
            #(#relation_accessors)*
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for InherentTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let kind = quote!(::icydb::model::field::FieldKind::Structured { queryable: false });
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::FieldKind = #kind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
