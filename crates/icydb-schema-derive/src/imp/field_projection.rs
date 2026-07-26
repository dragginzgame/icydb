//! Module: imp::field_projection
//! Responsibility: generated application-value projection by authored field order.
//! Does not own: persisted row decoding, accepted row layouts, or storage slots.
//! Boundary: generated entity values to application-facing runtime values.

use crate::prelude::*;

///
/// FieldProjectionTrait
///

pub struct FieldProjectionTrait {}

impl Imp<Entity> for FieldProjectionTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let projection_arms = node.fields.iter().enumerate().map(|(slot, field)| {
            let slot = syn::Index::from(slot);
            let ident = &field.ident;
            if field.value.item.is.is_some() {
                return quote!(#slot => None);
            }
            let value = match field.value.cardinality() {
                Cardinality::One => quote! {
                    Some(::icydb::__macro::runtime_value_to_value(&self.#ident))
                },
                Cardinality::Opt => quote! {
                    Some(match self.#ident.as_ref() {
                        Some(value) => ::icydb::__macro::runtime_value_to_value(value),
                        None => ::icydb::__macro::Value::Null,
                    })
                },
                Cardinality::Many => quote! {
                    Some(::icydb::__macro::Value::List(
                        self.#ident
                            .iter()
                            .map(::icydb::__macro::runtime_value_to_value)
                            .collect(),
                    ))
                },
            };
            quote!(#slot => #value)
        });
        let ident = node.def.ident();

        Some(TraitStrategy::from_impl(quote! {
            impl ::icydb::__macro::FieldProjection for #ident {
                fn get_value_by_index(
                    &self,
                    index: usize,
                ) -> Option<::icydb::__macro::Value> {
                    match index {
                        #(#projection_arms),*,
                        _ => None,
                    }
                }
            }
        }))
    }
}
