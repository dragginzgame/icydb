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
    encode_body: &TokenStream,
    decode_body: &TokenStream,
    paths: &CratePaths,
) -> TokenStream {
    let model = &paths.model;
    quote! {
        impl #model::TypedNamedType for #ident {
            const SOURCE_KEY: &'static str = #source;
        }

        impl #model::TypedInputValue for #ident {
            fn encode_typed_input<C>(
                self,
                context: &C,
            ) -> ::std::result::Result<C::PublicValue, #model::TypedValueError>
            where
                C: #model::TypedAdapterContext,
            {
                #encode_body
            }
        }

        impl #model::TypedOutputValue for #ident {
            fn decode_typed_output<C>(
                context: &C,
                value: C::PublicValue,
            ) -> ::std::result::Result<Self, #model::TypedValueError>
            where
                C: #model::TypedAdapterContext,
            {
                #decode_body
            }
        }
    }
}

fn transparent_adapter_tokens(
    ident: &Ident,
    source: &LitStr,
    inner: &TokenStream,
    paths: &CratePaths,
) -> TokenStream {
    let model = &paths.model;
    let encode = quote! {
        <#inner as #model::TypedInputValue>::encode_typed_input(self.0, context)
    };
    let decode = quote! {
        <#inner as #model::TypedOutputValue>::decode_typed_output(context, value)
            .map(Self)
    };

    named_adapter_impl_tokens(ident, source, &encode, &decode, paths)
}

pub(crate) fn enum_adapter_tokens(node: &Enum, paths: &CratePaths) -> TokenStream {
    let model = &paths.model;
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
            let ty = value.type_expr(paths);
            quote! {
                Self::#variant_ident(value) => context.input_enum(
                    #source,
                    #variant_source,
                    ::std::option::Option::Some(<#ty as #model::TypedInputValue>::encode_typed_input(
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
                    ::std::option::Option::None,
                )
            }
        }
    });
    let output_variants = node.variants.iter().enumerate().map(|(ordinal, variant)| {
        let variant_ident = &variant.name;
        let ordinal = syn::Index::from(ordinal);
        if let Some(payload) = &variant.value {
            let ty = payload.type_expr(paths);
            quote! {
                #ordinal => {
                    let payload = selected
                        .payload
                        .ok_or(#model::TypedValueError::ShapeMismatch)?;
                    ::std::result::Result::Ok(Self::#variant_ident(
                        <#ty as #model::TypedOutputValue>::decode_typed_output(
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
                        return ::std::result::Result::Err(#model::TypedValueError::ShapeMismatch);
                    }
                    ::std::result::Result::Ok(Self::#variant_ident)
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
        const DESCRIPTOR: #model::TypedEnumDescriptor =
            #model::TypedEnumDescriptor {
                type_source_key: #source,
                variants: &[#(#variant_sources),*],
            };
        let selected = context.output_enum(&DESCRIPTOR, value)?;
        match selected.ordinal {
            #(#output_variants),*,
            _ => ::std::result::Result::Err(#model::TypedValueError::ShapeMismatch),
        }
    };

    named_adapter_impl_tokens(&ident, &source, &encode, &decode, paths)
}

pub(crate) fn record_adapter_tokens(node: &Record, paths: &CratePaths) -> TokenStream {
    let model = &paths.model;
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
        let ty = field.value.type_expr(paths);
        quote! {
            fields.push((
                #field_source,
                <#ty as #model::TypedInputValue>::encode_typed_input(
                    self.#field_ident,
                    context,
                )?,
            ));
        }
    });
    let output_fields = node.fields.iter().map(|field| {
        let field_ident = &field.name;
        let ty = field.value.type_expr(paths);
        quote! {
            #field_ident:
                <#ty as #model::TypedOutputValue>::decode_typed_output(
                    context,
                    values.next().ok_or(#model::TypedValueError::ShapeMismatch)?,
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
        let mut values = context.output_record(
            #source,
            &[#(#field_sources),*],
            value,
        )?.into_iter();
        ::std::result::Result::Ok(Self {
            #(#output_fields),*
        })
    };

    named_adapter_impl_tokens(&ident, &source, &encode, &decode, paths)
}

pub(crate) fn newtype_adapter_tokens(node: &Newtype, paths: &CratePaths) -> TokenStream {
    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    transparent_adapter_tokens(&ident, &source, &node.item.type_expr(paths), paths)
}

pub(crate) fn list_adapter_tokens(node: &List, paths: &CratePaths) -> TokenStream {
    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let item = node.item.type_expr(paths);
    transparent_adapter_tokens(&ident, &source, &quote!(::std::vec::Vec<#item>), paths)
}

pub(crate) fn set_adapter_tokens(node: &Set, paths: &CratePaths) -> TokenStream {
    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let item = node.item.type_expr(paths);
    transparent_adapter_tokens(
        &ident,
        &source,
        &quote!(::std::collections::BTreeSet<#item>),
        paths,
    )
}

pub(crate) fn map_adapter_tokens(node: &Map, paths: &CratePaths) -> TokenStream {
    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let key = node.key.type_expr(paths);
    let value = node.value.type_expr(paths);
    transparent_adapter_tokens(
        &ident,
        &source,
        &quote!(::std::collections::BTreeMap<#key, #value>),
        paths,
    )
}

pub(crate) fn tuple_adapter_tokens(node: &Tuple, paths: &CratePaths) -> TokenStream {
    let model = &paths.model;
    let ident = node.def.ident();
    let source = node.current_name_literal(node.name.as_ref());
    let input_values = node.values.iter().enumerate().map(|(index, value)| {
        let index = syn::Index::from(index);
        let ty = value.type_expr(paths);
        quote! {
            <#ty as #model::TypedInputValue>::encode_typed_input(
                self.#index,
                context,
            )?
        }
    });
    let output_values = node.values.iter().map(|value| {
        let ty = value.type_expr(paths);
        quote! {
            <#ty as #model::TypedOutputValue>::decode_typed_output(
                context,
                values.next().ok_or(#model::TypedValueError::ShapeMismatch)?,
            )?
        }
    });
    let value_count = node.values.len();
    let encode = quote! {
        ::std::result::Result::Ok(context.input_list(::std::vec![
            #(#input_values),*
        ]))
    };
    let decode = quote! {
        let values = context
            .output_list(value)
            .ok_or(#model::TypedValueError::ShapeMismatch)?;
        if values.len() != #value_count {
            return ::std::result::Result::Err(#model::TypedValueError::ShapeMismatch);
        }
        let mut values = values.into_iter();
        ::std::result::Result::Ok(Self(#(#output_values),*))
    };

    named_adapter_impl_tokens(&ident, &source, &encode, &decode, paths)
}

#[cfg(test)]
mod tests;
