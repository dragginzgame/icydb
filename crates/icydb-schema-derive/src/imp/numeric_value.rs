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
        let item = &node.item.type_expr();

        Some(TraitStrategy::from_impl(
            Implementor::new(node.def(), TraitKind::NumericValue)
                .set_tokens(quote! {
                    fn try_to_decimal(&self) -> Option<::icydb::types::Decimal> {
                        ::icydb::__macro::NumericValue::try_to_decimal(&self.0)
                    }

                    fn try_from_decimal(value: ::icydb::types::Decimal) -> Option<Self> {
                        <#item as ::icydb::__macro::NumericValue>::try_from_decimal(value).map(Self)
                    }
                })
                .to_token_stream(),
        ))
    }
}
