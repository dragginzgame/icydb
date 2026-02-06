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
                quote!(.with_payload(::icydb::traits::FieldValue::to_value(v)))
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
        let enum_value = quote! {
            fn to_value_enum(&self) -> ::icydb::value::ValueEnum {
                use ::icydb::value::ValueEnum;

                match self {
                    #(#arms),*
                }
            }
        };

        let field_value = quote! {
            fn to_value(&self) -> ::icydb::value::Value {
                ::icydb::value::Value::Enum(::icydb::traits::EnumValue::to_value_enum(self))
            }
        };

        let mut tokens = TokenStream::new();
        tokens.extend(
            Implementor::new(node.def(), TraitKind::EnumValue)
                .set_tokens(enum_value)
                .to_token_stream(),
        );
        tokens.extend(
            Implementor::new(node.def(), TraitKind::FieldValue)
                .set_tokens(field_value)
                .to_token_stream(),
        );

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// List
///

impl Imp<List> for FieldValueTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let q = quote! {
            fn to_value(&self) -> ::icydb::value::Value {
                use ::icydb::traits::Collection;

                ::icydb::value::Value::List(
                    self.iter()
                        .map(::icydb::traits::FieldValue::to_value)
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
/// Map
///

impl Imp<Map> for FieldValueTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key_type = node.key.type_expr();
        let value_type = node.value.type_expr();

        let q = quote! {
            fn to_value(&self) -> ::icydb::value::Value {
                let entries = self
                    .0
                    .iter()
                    .map(|(key, value)| {
                        (
                            ::icydb::traits::FieldValue::to_value(key),
                            ::icydb::traits::FieldValue::to_value(value),
                        )
                    })
                    .collect();

                ::icydb::value::Value::from_map(entries).unwrap_or_else(|err| {
                    panic!(
                        "invalid map field value for {}: {err}",
                        <Self as ::icydb::traits::Path>::PATH,
                    )
                })
            }

            fn from_value(value: &::icydb::value::Value) -> Option<Self> {
                let ::icydb::value::Value::Map(entries) = value else {
                    return None;
                };

                if ::icydb::value::Value::validate_map_entries(entries.as_slice()).is_err() {
                    return None;
                }

                let mut map =
                    ::std::collections::HashMap::<#key_type, #value_type>::with_capacity(
                        entries.len(),
                    );
                for (entry_key, entry_value) in entries {
                    let key = <#key_type as ::icydb::traits::FieldValue>::from_value(entry_key)?;
                    let value =
                        <#value_type as ::icydb::traits::FieldValue>::from_value(entry_value)?;

                    if map.insert(key, value).is_some() {
                        return None;
                    }
                }

                Some(Self(map))
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
            fn to_value(&self) -> ::icydb::value::Value {
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
            fn to_value(&self) -> ::icydb::value::Value {
                use ::icydb::traits::Collection;

                ::icydb::value::Value::List(
                    self.iter()
                        .map(::icydb::traits::FieldValue::to_value)
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
