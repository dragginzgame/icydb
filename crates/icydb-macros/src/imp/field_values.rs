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
                let field_const = &field.const_ident();

                match field.value.cardinality() {
                    Cardinality::One => Some(quote! {
                        Self::#field_const => Some(self.#field_ident.to_value()),
                    }),

                    Cardinality::Opt => Some(quote! {
                        Self::#field_const => {
                            match self.#field_ident.as_ref() {
                                Some(inner) => Some(FieldValue::to_value(inner)),
                                None => Some(Value::None),
                            }
                        }
                    }),

                    Cardinality::Many => Some(quote! {
                        Self::#field_const => {
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
        let cp = paths().core;
        let q = quote! {
            fn get_value(&self, field: &str) -> Option<#cp::Value> {
                use #cp::{traits::FieldValue, Value};

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
