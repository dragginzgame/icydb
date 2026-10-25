//! Module: imp::numeric_value
//! Responsibility: generated implementation tokens.
//! Does not own: runtime trait semantics.
//! Boundary: parsed nodes to impl tokens.

use crate::prelude::*;

///
/// NumericValueTrait
///

pub struct NumericValueTrait {}

///
/// Newtype
///

impl Imp<Newtype> for NumericValueTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let paths = node.def().paths();
        let model = &paths.model;
        let item = &node.item.type_expr(paths);

        Some(TraitStrategy::from_impl(
            Implementor::new(node.def(), TraitKind::NumericValue)
                .set_tokens(quote! {
                    fn try_to_decimal(&self) -> ::core::option::Option<#model::schema::Decimal> {
                        #model::schema::NumericValue::try_to_decimal(&self.0)
                    }

                    fn try_from_decimal(value: #model::schema::Decimal) -> ::core::option::Option<Self> {
                        <#item as #model::schema::NumericValue>::try_from_decimal(value).map(Self)
                    }
                })
                .impl_tokens(paths),
        ))
    }
}
