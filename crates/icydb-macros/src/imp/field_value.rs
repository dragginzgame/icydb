use crate::prelude::*;

///
/// FieldValueTrait
///

pub struct FieldValueTrait {}

///
/// Enum
///

impl Imp<Enum> for FieldValueTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        // generate match arms
        let arms = node.variants.iter().map(|v| {
            let v_match = {
                let v_ident = &v.ident;

                if v.value.is_some() {
                    quote!(#v_ident(v))
                } else {
                    quote!(#v_ident)
                }
            };
            let v_name = &v.ident.to_string(); // schema variant name (String)
            let payload = if v.value.is_some() {
                quote!(.with_payload(::icydb::core::traits::FieldValue::to_value(v)))
            } else {
                quote!()
            };

            quote! {
                Self::#v_match => {
                    ValueEnum::new(
                        #v_name,
                        Some(Self::PATH)
                    ) #payload
                }
            }
        });

        // quote
        let q = quote! {
            fn to_value(&self) -> ::icydb::core::value::Value {
                use ::icydb::core::value::{ValueEnum, Value};

                let ev = match self {
                    #(#arms),*
                };

                Value::Enum(ev)
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::FieldValue)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// List
///

impl Imp<List> for FieldValueTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let q = quote! {
            fn to_value(&self) -> ::icydb::core::value::Value {
                ::icydb::core::value::Value::List(
                    self.iter()
                        .map(::icydb::core::traits::FieldValue::to_value)
                        .collect()
                )
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::FieldValue)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Newtype
///

impl Imp<Newtype> for FieldValueTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let q = quote! {
            fn to_value(&self) -> ::icydb::core::value::Value {
                self.0.to_value()
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::FieldValue)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Set
///

impl Imp<Set> for FieldValueTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let q = quote! {
            fn to_value(&self) -> ::icydb::core::value::Value {
                ::icydb::core::value::Value::List(
                    self.iter()
                        .map(::icydb::core::traits::FieldValue::to_value)
                        .collect()
                )
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::FieldValue)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
