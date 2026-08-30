//! Module: node::typed_adapter
//! Responsibility: source-bound model-value adapters for all named types.
//! Does not own: accepted schema admission, persistence, or physical codecs.
//! Boundary: authored Rust named values to a runtime-supplied adapter context.

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
        impl ::icydb_model::TypedNamedType for #ident {
            const SOURCE_KEY: &'static str = #source;
        }

        impl ::icydb_model::TypedInputValue for #ident {
            fn encode_typed_input<C>(
                self,
                context: &C,
            ) -> Result<C::PublicValue, ::icydb_model::TypedValueError>
            where
                C: ::icydb_model::TypedAdapterContext,
            {
                #encode_body
            }
        }

        impl ::icydb_model::TypedOutputValue for #ident {
            fn decode_typed_output<C>(
                context: &C,
                value: &C::PublicValue,
            ) -> Result<Self, ::icydb_model::TypedValueError>
            where
                C: ::icydb_model::TypedAdapterContext,
            {
                #decode_body
            }
        }
    }
}

fn transparent_adapter_tokens(ident: &Ident, source: &LitStr, inner: TokenStream) -> TokenStream {
    let encode = quote! {
        <#inner as ::icydb_model::TypedInputValue>::encode_typed_input(self.0, context)
    };
    let decode = quote! {
        <#inner as ::icydb_model::TypedOutputValue>::decode_typed_output(context, value)
            .map(Self)
    };

    named_adapter_impl_tokens(ident, source, encode, decode)
}

pub(crate) fn enum_adapter_tokens(node: &Enum) -> TokenStream {
    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let variant_sources = node
        .variants
        .iter()
        .map(|variant| quote_one(&variant.name, to_str_lit))
        .collect::<Vec<_>>();
    let input_arms = node.variants.iter().map(|variant| {
        let variant_ident = &variant.name;
        let variant_source = quote_one(&variant.name, to_str_lit);
        if let Some(value) = &variant.value {
            let ty = value.type_expr();
            quote! {
                Self::#variant_ident(value) => context.input_enum(
                    #source,
                    #variant_source,
                    Some(<#ty as ::icydb_model::TypedInputValue>::encode_typed_input(
                        value,
                        context,
                    )?),
                )
            }
        } else {
            quote! {
                Self::#variant_ident => context.input_enum(
                    #source,
                    #variant_source,
                    None,
                )
            }
        }
    });
    let output_variants = node.variants.iter().enumerate().map(|(ordinal, variant)| {
        let variant_ident = &variant.name;
        let ordinal = syn::Index::from(ordinal);
        if let Some(payload) = &variant.value {
            let ty = payload.type_expr();
            quote! {
                #ordinal => {
                    let payload = selected
                        .payload
                        .ok_or(::icydb_model::TypedValueError::ShapeMismatch)?;
                    Ok(Self::#variant_ident(
                        <#ty as ::icydb_model::TypedOutputValue>::decode_typed_output(
                            context,
                            payload,
                        )?
                    ))
                }
            }
        } else {
            quote! {
                #ordinal => {
                    if selected.payload.is_some() {
                        return Err(::icydb_model::TypedValueError::ShapeMismatch);
                    }
                    Ok(Self::#variant_ident)
                }
            }
        }
    });
    let encode = quote! {
        match self {
            #(#input_arms),*
        }
    };
    let decode = quote! {
        const DESCRIPTOR: ::icydb_model::TypedEnumDescriptor =
            ::icydb_model::TypedEnumDescriptor {
                type_source_key: #source,
                variants: &[#(#variant_sources),*],
            };
        let selected = context.output_enum(&DESCRIPTOR, value)?;
        match selected.ordinal {
            #(#output_variants),*,
            _ => Err(::icydb_model::TypedValueError::ShapeMismatch),
        }
    };

    named_adapter_impl_tokens(&ident, &source, encode, decode)
}

pub(crate) fn record_adapter_tokens(node: &Record) -> TokenStream {
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
            fields.push((
                #field_source,
                <#ty as ::icydb_model::TypedInputValue>::encode_typed_input(
                    self.#field_ident,
                    context,
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
                <#ty as ::icydb_model::TypedOutputValue>::decode_typed_output(
                    context,
                    values[#index],
                )?
        }
    });
    let field_count = field_sources.len();
    let encode = quote! {
        let mut fields = ::std::vec::Vec::with_capacity(#field_count);
        #(#input_fields)*
        context.input_record(#source, fields)
    };
    let decode = quote! {
        let values = context.output_record(
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
    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    transparent_adapter_tokens(&ident, &source, node.item.type_expr())
}

pub(crate) fn list_adapter_tokens(node: &List) -> TokenStream {
    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let item = node.item.type_expr();
    transparent_adapter_tokens(&ident, &source, quote!(::std::vec::Vec<#item>))
}

pub(crate) fn set_adapter_tokens(node: &Set) -> TokenStream {
    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let item = node.item.type_expr();
    transparent_adapter_tokens(&ident, &source, quote!(::std::collections::BTreeSet<#item>))
}

pub(crate) fn map_adapter_tokens(node: &Map) -> TokenStream {
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
    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let input_values = node.values.iter().enumerate().map(|(index, value)| {
        let index = syn::Index::from(index);
        let ty = value.type_expr();
        quote! {
            <#ty as ::icydb_model::TypedInputValue>::encode_typed_input(
                self.#index,
                context,
            )?
        }
    });
    let output_values = node.values.iter().enumerate().map(|(index, value)| {
        let index = syn::Index::from(index);
        let ty = value.type_expr();
        quote! {
            <#ty as ::icydb_model::TypedOutputValue>::decode_typed_output(
                context,
                &values[#index],
            )?
        }
    });
    let value_count = node.values.len();
    let encode = quote! {
        Ok(context.input_list(::std::vec![
            #(#input_values),*
        ]))
    };
    let decode = quote! {
        let values = context
            .output_list(value)
            .ok_or(::icydb_model::TypedValueError::ShapeMismatch)?;
        if values.len() != #value_count {
            return Err(::icydb_model::TypedValueError::ShapeMismatch);
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
    fn enum_adapter_uses_model_owned_source_bound_context() {
        let args = NestedMeta::parse_meta_list(quote!(
            name = "ChoiceSource",
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
            "impl :: icydb_model :: TypedNamedType for Choice",
            "const SOURCE_KEY : & 'static str = \"ChoiceSource\"",
            "input_enum (\"ChoiceSource\" , \"Empty\"",
            "input_enum (\"ChoiceSource\" , \"Count\"",
            "TypedEnumDescriptor",
            "type_source_key : \"ChoiceSource\"",
            "variants : & [\"Empty\" , \"Count\"]",
            "context . output_enum (& DESCRIPTOR , value)",
            "match selected . ordinal",
            "TypedInputValue for Choice",
            "TypedOutputValue for Choice",
        ] {
            assert!(
                tokens.contains(expected),
                "expected generated enum adapter contract `{expected}` in: {tokens}",
            );
        }
        assert_eq!(tokens.matches("output_enum").count(), 1);
        assert!(!tokens.contains(":: icydb ::"));
    }

    #[test]
    fn record_adapter_uses_source_bound_members_and_context() {
        let args = NestedMeta::parse_meta_list(quote!(
            name = "ProfileSource",
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
            "impl :: icydb_model :: TypedNamedType for Profile",
            "input_record (\"ProfileSource\"",
            "output_record (\"ProfileSource\"",
            "TypedInputValue for Profile",
            "TypedOutputValue for Profile",
        ] {
            assert!(
                tokens.contains(expected),
                "expected generated record adapter contract `{expected}` in: {tokens}",
            );
        }
        assert!(!tokens.contains(":: icydb ::"));
    }
}
