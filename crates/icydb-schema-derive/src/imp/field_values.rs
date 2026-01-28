use crate::prelude::*;

///
/// FieldValuesTrait
///

pub struct FieldValuesTrait {}

///
/// Entity
///

impl Imp<Entity> for FieldValuesTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let match_arms = node
            .fields
            .iter()
            .map(|field| {
                let field_ident = &field.ident;
                let field_name = field.ident.to_string();

                match field.value.cardinality() {
                    Cardinality::One => Some(quote! {
                        #field_name => Some(self.#field_ident.to_value()),
                    }),

                    Cardinality::Opt => Some(quote! {
                        #field_name => {
                            match self.#field_ident.as_ref() {
                                Some(inner) => Some(FieldValue::to_value(inner)),
                                None => Some(Value::None),
                            }
                        }
                    }),

                    Cardinality::Many => Some(quote! {
                        #field_name => {
                            let list = self.#field_ident
                                .iter()
                                .map(FieldValue::to_value)
                                .collect::<Vec<_>>();

                            Some(Value::List(list))
                        }
                    }),
                }
            })
            .collect::<Vec<_>>();

        // quote
        let q = quote! {
            fn get_value(&self, field: &str) -> Option<::icydb::value::Value> {
                use ::icydb::{traits::FieldValue, value::Value};

                match field {
                    #(#match_arms)*
                    _ => None,
                }
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::FieldValues)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
