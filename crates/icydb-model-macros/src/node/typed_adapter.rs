//! Module: node::typed_adapter
//! Responsibility: source-bound public-value adapters for opted-in named types.
//! Does not own: accepted schema admission, persistence, or physical codecs.
//! Boundary: authored Rust named values to IcyDB `InputValue` / `OutputValue`.

use crate::{
    node::{Enum, List, Map, Newtype, Record, Set, Tuple},
    prelude::*,
};

fn named_adapter_impl_tokens(
    ident: &Ident,
    source: &LitStr,
    encode_body: TokenStream,
    decode_body: TokenStream,
) -> TokenStream {
    quote! {
        impl ::icydb::__macro::TypedNamedType for #ident {
            const SOURCE_KEY: &'static str = #source;
        }

        impl ::icydb::__macro::TypedInputValue for #ident {
            fn encode_typed_input(
                self,
                binding: &::icydb::db::TypedEntityBinding,
            ) -> Result<::icydb::value::InputValue, ::icydb::db::TypedAdapterError> {
                #encode_body
            }
        }

        impl ::icydb::__macro::TypedOutputValue for #ident {
            fn decode_typed_output(
                binding: &::icydb::db::TypedEntityBinding,
                value: &::icydb::value::OutputValue,
            ) -> Result<Self, ::icydb::db::TypedAdapterError> {
                #decode_body
            }
        }
    }
}

fn transparent_adapter_tokens(ident: &Ident, source: &LitStr, inner: TokenStream) -> TokenStream {
    let encode = quote! {
        <#inner as ::icydb::__macro::TypedInputValue>::encode_typed_input(self.0, binding)
    };
    let decode = quote! {
        <#inner as ::icydb::__macro::TypedOutputValue>::decode_typed_output(binding, value)
            .map(Self)
    };

    named_adapter_impl_tokens(ident, source, encode, decode)
}

pub(crate) fn enum_adapter_tokens(node: &Enum) -> TokenStream {
    if !node.typed_adapters {
        return TokenStream::new();
    }

    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let input_arms = node.variants.iter().map(|variant| {
        let variant_ident = &variant.name;
        let variant_source = quote_one(&variant.name, to_str_lit);
        if let Some(value) = &variant.value {
            let ty = value.type_expr();
            quote! {
                Self::#variant_ident(value) => {
                    let variant_name = binding
                        .enum_variant_name(#source, #variant_source)
                        .ok_or(::icydb::db::TypedAdapterError::FieldUnavailable)?;
                    let payload =
                        <#ty as ::icydb::__macro::TypedInputValue>::encode_typed_input(
                            value,
                            binding,
                        )?;
                    ::icydb::value::InputValueEnum::new(variant_name, Some(type_name))
                        .with_payload(payload)
                }
            }
        } else {
            quote! {
                Self::#variant_ident => {
                    let variant_name = binding
                        .enum_variant_name(#source, #variant_source)
                        .ok_or(::icydb::db::TypedAdapterError::FieldUnavailable)?;
                    ::icydb::value::InputValueEnum::new(variant_name, Some(type_name))
                }
            }
        }
    });
    let output_variants = node.variants.iter().map(|variant| {
        let variant_ident = &variant.name;
        let variant_source = quote_one(&variant.name, to_str_lit);
        if let Some(payload) = &variant.value {
            let ty = payload.type_expr();
            quote! {
                let variant_name = binding
                    .enum_variant_name(#source, #variant_source)
                    .ok_or(::icydb::db::TypedAdapterError::FieldUnavailable)?;
                if value.variant() == variant_name {
                    let payload = value
                        .payload()
                        .ok_or(::icydb::db::TypedAdapterError::ValueShapeMismatch)?;
                    return Ok(Self::#variant_ident(
                        <#ty as ::icydb::__macro::TypedOutputValue>::decode_typed_output(
                            binding,
                            payload,
                        )?
                    ));
                }
            }
        } else {
            quote! {
                let variant_name = binding
                    .enum_variant_name(#source, #variant_source)
                    .ok_or(::icydb::db::TypedAdapterError::FieldUnavailable)?;
                if value.variant() == variant_name {
                    if value.payload().is_some() {
                        return Err(::icydb::db::TypedAdapterError::ValueShapeMismatch);
                    }
                    return Ok(Self::#variant_ident);
                }
            }
        }
    });
    let encode = quote! {
        let type_name = binding
            .named_type_name(#source)
            .ok_or(::icydb::db::TypedAdapterError::FieldUnavailable)?;
        let value = match self {
            #(#input_arms),*
        };
        Ok(::icydb::value::InputValue::Enum(value))
    };
    let decode = quote! {
        let ::icydb::value::OutputValue::Enum(value) = value else {
            return Err(::icydb::db::TypedAdapterError::ValueShapeMismatch);
        };
        let type_name = binding
            .named_type_name(#source)
            .ok_or(::icydb::db::TypedAdapterError::FieldUnavailable)?;
        if value.path() != Some(type_name) {
            return Err(::icydb::db::TypedAdapterError::ValueShapeMismatch);
        }
        #(#output_variants)*
        Err(::icydb::db::TypedAdapterError::ValueShapeMismatch)
    };

    named_adapter_impl_tokens(&ident, &source, encode, decode)
}

pub(crate) fn record_adapter_tokens(node: &Record) -> TokenStream {
    if !node.typed_adapters {
        return TokenStream::new();
    }

    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let field_sources = node
        .fields
        .iter()
        .map(|field| quote_one(&field.name, to_str_lit))
        .collect::<Vec<_>>();
    let input_fields = node.fields.iter().map(|field| {
        let field_ident = &field.name;
        let field_source = quote_one(&field.name, to_str_lit);
        let ty = field.value.type_expr();
        quote! {
            let field_name = binding
                .composite_field_name(#source, #field_source)
                .ok_or(::icydb::db::TypedAdapterError::FieldUnavailable)?;
            fields.push((
                ::icydb::value::InputValue::Text(field_name.to_string()),
                <#ty as ::icydb::__macro::TypedInputValue>::encode_typed_input(
                    self.#field_ident,
                    binding,
                )?,
            ));
        }
    });
    let output_fields = node.fields.iter().enumerate().map(|(index, field)| {
        let field_ident = &field.name;
        let ty = field.value.type_expr();
        let index = syn::Index::from(index);
        quote! {
            #field_ident:
                <#ty as ::icydb::__macro::TypedOutputValue>::decode_typed_output(
                    binding,
                    values[#index],
                )?
        }
    });
    let field_count = field_sources.len();
    let encode = quote! {
        let mut fields = ::std::vec::Vec::with_capacity(#field_count);
        #(#input_fields)*
        Ok(::icydb::value::InputValue::Map(fields))
    };
    let decode = quote! {
        let values = binding.record_output_values(
            #source,
            &[#(#field_sources),*],
            value,
        )?;
        Ok(Self {
            #(#output_fields),*
        })
    };

    named_adapter_impl_tokens(&ident, &source, encode, decode)
}

pub(crate) fn newtype_adapter_tokens(node: &Newtype) -> TokenStream {
    if !node.typed_adapters {
        return TokenStream::new();
    }

    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    transparent_adapter_tokens(&ident, &source, node.item.type_expr())
}

pub(crate) fn list_adapter_tokens(node: &List) -> TokenStream {
    if !node.typed_adapters {
        return TokenStream::new();
    }

    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let item = node.item.type_expr();
    transparent_adapter_tokens(&ident, &source, quote!(::std::vec::Vec<#item>))
}

pub(crate) fn set_adapter_tokens(node: &Set) -> TokenStream {
    if !node.typed_adapters {
        return TokenStream::new();
    }

    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let item = node.item.type_expr();
    transparent_adapter_tokens(&ident, &source, quote!(::std::collections::BTreeSet<#item>))
}

pub(crate) fn map_adapter_tokens(node: &Map) -> TokenStream {
    if !node.typed_adapters {
        return TokenStream::new();
    }

    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let key = node.key.type_expr();
    let value = node.value.type_expr();
    transparent_adapter_tokens(
        &ident,
        &source,
        quote!(::std::collections::BTreeMap<#key, #value>),
    )
}

pub(crate) fn tuple_adapter_tokens(node: &Tuple) -> TokenStream {
    if !node.typed_adapters {
        return TokenStream::new();
    }

    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let input_values = node.values.iter().enumerate().map(|(index, value)| {
        let index = syn::Index::from(index);
        let ty = value.type_expr();
        quote! {
            <#ty as ::icydb::__macro::TypedInputValue>::encode_typed_input(
                self.#index,
                binding,
            )?
        }
    });
    let output_values = node.values.iter().enumerate().map(|(index, value)| {
        let index = syn::Index::from(index);
        let ty = value.type_expr();
        quote! {
            <#ty as ::icydb::__macro::TypedOutputValue>::decode_typed_output(
                binding,
                &values[#index],
            )?
        }
    });
    let value_count = node.values.len();
    let encode = quote! {
        Ok(::icydb::value::InputValue::List(::std::vec![
            #(#input_values),*
        ]))
    };
    let decode = quote! {
        let ::icydb::value::OutputValue::List(values) = value else {
            return Err(::icydb::db::TypedAdapterError::ValueShapeMismatch);
        };
        if values.len() != #value_count {
            return Err(::icydb::db::TypedAdapterError::ValueShapeMismatch);
        }
        Ok(Self(#(#output_values),*))
    };

    named_adapter_impl_tokens(&ident, &source, encode, decode)
}

#[cfg(test)]
mod tests {
    use super::{enum_adapter_tokens, record_adapter_tokens};
    use crate::node::{Def, Enum, Record};
    use darling::{FromMeta, ast::NestedMeta};
    use quote::quote;

    #[test]
    fn enum_adapter_uses_source_bound_type_and_variant_names() {
        let args = NestedMeta::parse_meta_list(quote!(
            name = "ChoiceSource",
            typed_adapters,
            variant(name = "Empty"),
            variant(name = "Count", value(item(prim = "Nat64")))
        ))
        .expect("enum adapter arguments should parse");
        let mut node = Enum::from_list(&args).expect("enum adapter node should lower");
        node.def = Def::new(syn::parse_quote!(
            pub struct Choice;
        ));

        let tokens = enum_adapter_tokens(&node).to_string();

        for expected in [
            "impl :: icydb :: __macro :: TypedNamedType for Choice",
            "const SOURCE_KEY : & 'static str = \"ChoiceSource\"",
            "named_type_name (\"ChoiceSource\")",
            "enum_variant_name (\"ChoiceSource\" , \"Empty\")",
            "enum_variant_name (\"ChoiceSource\" , \"Count\")",
            "TypedInputValue for Choice",
            "TypedOutputValue for Choice",
        ] {
            assert!(
                tokens.contains(expected),
                "expected generated enum adapter contract `{expected}` in: {tokens}",
            );
        }
    }

    #[test]
    fn record_adapter_uses_source_bound_members_and_exact_output_helper() {
        let args = NestedMeta::parse_meta_list(quote!(
            name = "ProfileSource",
            typed_adapters,
            fields(
                field(name = "label", value(item(prim = "Text", unbounded))),
                field(name = "count", value(item(prim = "Nat64")))
            )
        ))
        .expect("record adapter arguments should parse");
        let mut node = Record::from_list(&args).expect("record adapter node should lower");
        node.def = Def::new(syn::parse_quote!(
            pub struct Profile;
        ));

        let tokens = record_adapter_tokens(&node).to_string();

        for expected in [
            "impl :: icydb :: __macro :: TypedNamedType for Profile",
            "composite_field_name (\"ProfileSource\" , \"label\")",
            "composite_field_name (\"ProfileSource\" , \"count\")",
            "record_output_values (\"ProfileSource\"",
            "TypedInputValue for Profile",
            "TypedOutputValue for Profile",
        ] {
            assert!(
                tokens.contains(expected),
                "expected generated record adapter contract `{expected}` in: {tokens}",
            );
        }
    }
}
