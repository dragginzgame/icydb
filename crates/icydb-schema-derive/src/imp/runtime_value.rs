//! Module: imp::runtime_value
//! Responsibility: generated implementation tokens.
//! Does not own: runtime trait semantics.
//! Boundary: parsed nodes to impl tokens.

use crate::prelude::*;

///
/// RuntimeValueTrait
///

pub struct RuntimeValueTrait {}

///
/// PersistedStructuralValueCodecTrait
///

pub struct PersistedStructuralValueCodecTrait {}

///
/// Enum
///

impl Imp<Enum> for RuntimeValueTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let (runtime_value_meta, runtime_value_decode) = enum_runtime_value_tokens(node);

        let mut tokens = TokenStream::new();
        tokens.extend(
            Implementor::new(node.def(), TraitKind::RuntimeValueMeta)
                .set_tokens(runtime_value_meta)
                .to_token_stream(),
        );
        tokens.extend(
            Implementor::new(node.def(), TraitKind::RuntimeValueDecode)
                .set_tokens(runtime_value_decode)
                .to_token_stream(),
        );
        tokens.extend(enum_input_value_tokens(node));

        Some(TraitStrategy::from_impl(tokens))
    }
}

impl Imp<Enum> for PersistedStructuralValueCodecTrait {
    fn strategy(_node: &Enum) -> Option<TraitStrategy> {
        None
    }
}

fn enum_variant_match_pattern(variant: &EnumVariant) -> TokenStream {
    let variant_ident = &variant.ident;

    if variant.value.is_some() {
        quote!(#variant_ident(v))
    } else {
        quote!(#variant_ident)
    }
}

fn enum_runtime_value_tokens(node: &Enum) -> (TokenStream, TokenStream) {
    let from_arms = enum_from_value_arms(node);

    (
        quote! {
            fn kind() -> ::icydb::__macro::RuntimeValueKind {
                ::icydb::__macro::RuntimeValueKind::Atomic
            }
        },
        quote! {
            fn from_value(value: &::icydb::__macro::Value) -> Option<Self> {
                let _ = value;
                None
            }

            fn from_value_with_enum_context(
                value: &::icydb::__macro::Value,
                context: &dyn ::icydb::__macro::RuntimeEnumContext,
            ) -> Option<Self> {
                let ::icydb::__macro::Value::Enum(v) = value else {
                    return None;
                };
                let selection = context.resolve_enum(v)?;
                if selection.path != <Self as ::icydb::__macro::Path>::PATH {
                    return None;
                }

                match selection.variant {
                    #(#from_arms),*,
                    _ => None,
                }
            }
        },
    )
}

fn enum_from_value_arms(node: &Enum) -> Vec<TokenStream> {
    node.variants
        .iter()
        .map(|variant| {
            let variant_ident = &variant.ident;
            let variant_name = variant.name_const_ident();

            if let Some(value) = &variant.value {
                let payload_ty = value.type_expr();

                quote! {
                    Self::#variant_name => {
                        let payload = selection.payload?;
                        let value =
                            ::icydb::__macro::runtime_value_from_value_with_enum_context::<#payload_ty>(
                                payload,
                                context,
                            )?;
                        Some(Self::#variant_ident(value))
                    }
                }
            } else {
                quote! {
                    Self::#variant_name => Some(Self::#variant_ident)
                }
            }
        })
        .collect()
}

// Generated authoring values stay name-based until accepted-catalog admission.
// Build InputValue recursively here without constructing the legacy runtime
// enum representation as an intermediate value.
fn enum_input_value_tokens(node: &Enum) -> TokenStream {
    let ident = node.def.ident();
    let arms = node.variants.iter().map(|variant| {
        let variant_match = enum_variant_match_pattern(variant);
        let variant_name = variant.name_const_ident();
        let payload = variant
            .value
            .as_ref()
            .map_or_else(TokenStream::new, |value| {
                let input = owned_value_to_input_expr(value, quote!(v));
                quote!(.with_payload(#input))
            });

        quote! {
            #ident::#variant_match => {
                ::icydb::__macro::InputValue::Enum(
                    ::icydb::__macro::InputValueEnum::new(
                        #ident::#variant_name,
                        Some(<#ident as ::icydb::__macro::Path>::PATH),
                    ) #payload
                )
            }
        }
    });

    input_value_impl_tokens(
        node.def(),
        quote! {
            match value {
                #(#arms),*
            }
        },
    )
}

fn input_value_impl_tokens(def: &Def, conversion: TokenStream) -> TokenStream {
    let ident = def.ident();

    quote! {
        impl From<#ident> for ::icydb::__macro::InputValue {
            fn from(value: #ident) -> Self {
                #conversion
            }
        }

        impl From<&#ident> for ::icydb::__macro::InputValue {
            fn from(value: &#ident) -> Self {
                Self::from(value.clone())
            }
        }
    }
}

fn owned_value_to_input_expr(value: &crate::node::Value, access: TokenStream) -> TokenStream {
    match value.cardinality() {
        Cardinality::One => quote!(::icydb::__macro::InputValue::from(#access)),
        Cardinality::Opt => quote! {
            match #access {
                Some(inner) => ::icydb::__macro::InputValue::from(inner),
                None => ::icydb::__macro::InputValue::Null,
            }
        },
        Cardinality::Many => quote! {
            ::icydb::__macro::InputValue::List(
                #access
                    .into_iter()
                    .map(::icydb::__macro::InputValue::from)
                    .collect(),
            )
        },
    }
}

fn runtime_value_impl_tokens(
    def: &Def,
    runtime_value_meta: TokenStream,
    runtime_value_encode: TokenStream,
    runtime_value_decode: TokenStream,
) -> TokenStream {
    let mut tokens = TokenStream::new();
    tokens.extend(
        Implementor::new(def, TraitKind::RuntimeValueMeta)
            .set_tokens(runtime_value_meta)
            .to_token_stream(),
    );
    tokens.extend(
        Implementor::new(def, TraitKind::RuntimeValueEncode)
            .set_tokens(runtime_value_encode)
            .to_token_stream(),
    );
    tokens.extend(
        Implementor::new(def, TraitKind::RuntimeValueDecode)
            .set_tokens(runtime_value_decode)
            .to_token_stream(),
    );

    tokens
}

fn structured_collection_runtime_value_tokens(
    kind: TokenStream,
    to_value: TokenStream,
    from_value: TokenStream,
) -> (TokenStream, TokenStream, TokenStream) {
    (
        quote! {
            fn kind() -> ::icydb::__macro::RuntimeValueKind {
                #kind
            }
        },
        quote! {
            fn to_value(&self) -> ::icydb::__macro::Value {
                #to_value
            }
        },
        quote! {
            fn from_value(value: &::icydb::__macro::Value) -> Option<Self> {
                #from_value
            }
        },
    )
}

fn newtype_runtime_value_tokens(item: &TokenStream) -> (TokenStream, TokenStream, TokenStream) {
    (
        quote! {
            fn kind() -> ::icydb::__macro::RuntimeValueKind {
                <#item as ::icydb::__macro::RuntimeValueMeta>::kind()
            }
        },
        quote! {
            fn to_value(&self) -> ::icydb::__macro::Value {
                ::icydb::__macro::runtime_value_to_value(&self.0)
            }
        },
        quote! {
            fn from_value(value: &::icydb::__macro::Value) -> Option<Self> {
                let inner = ::icydb::__macro::runtime_value_from_value::<#item>(value)?;
                Some(Self(inner))
            }

            fn from_value_with_enum_context(
                value: &::icydb::__macro::Value,
                context: &dyn ::icydb::__macro::RuntimeEnumContext,
            ) -> Option<Self> {
                let inner = ::icydb::__macro::runtime_value_from_value_with_enum_context::<#item>(
                    value,
                    context,
                )?;
                Some(Self(inner))
            }
        },
    )
}

fn field_to_value_expr(value: &crate::node::Value, access: TokenStream) -> TokenStream {
    match value.cardinality() {
        Cardinality::One => quote!(::icydb::__macro::runtime_value_to_value(&#access)),
        Cardinality::Opt => quote! {
            match #access.as_ref() {
                Some(inner) => ::icydb::__macro::runtime_value_to_value(inner),
                None => ::icydb::__macro::Value::Null,
            }
        },
        Cardinality::Many => quote! {
            ::icydb::__macro::Value::List(
                #access
                    .iter()
                    .map(::icydb::__macro::runtime_value_to_value)
                    .collect(),
            )
        },
    }
}

fn field_from_value_expr(value: &crate::node::Value, source: TokenStream) -> TokenStream {
    match value.cardinality() {
        Cardinality::One | Cardinality::Opt => {
            let ty = value.type_expr();
            quote!(::icydb::__macro::runtime_value_from_value::<#ty>(#source)?)
        }
        Cardinality::Many => {
            let item_ty = value.item.type_expr();
            quote!(::icydb::__macro::runtime_value_vec_from_value::<#item_ty>(#source)?)
        }
    }
}

fn field_from_value_with_enum_context_expr(
    value: &crate::node::Value,
    source: TokenStream,
) -> TokenStream {
    let ty = value.type_expr();
    quote! {
        ::icydb::__macro::runtime_value_from_value_with_enum_context::<#ty>(
            #source,
            context,
        )?
    }
}

fn record_runtime_value_tokens(node: &Record) -> (TokenStream, TokenStream, TokenStream) {
    let to_entries = node.fields.iter().map(|field| {
        let ident = &field.ident;
        let name = ident.to_string();
        let value_expr = field_to_value_expr(&field.value, quote!(self.#ident));

        quote! {
            (
                ::icydb::__macro::Value::Text(#name.to_string()),
                #value_expr,
            )
        }
    });
    let from_fields = node.fields.iter().map(|field| {
        let ident = &field.ident;
        let name = ident.to_string();
        let decode_expr = field_from_value_expr(
            &field.value,
            quote! {
                normalized.iter().find_map(|(entry_key, entry_value)| match entry_key {
                    ::icydb::__macro::Value::Text(entry_key) if entry_key == #name => Some(entry_value),
                    _ => None,
                })?
            },
        );

        quote!(#ident: #decode_expr)
    });
    let contextual_from_fields = node.fields.iter().map(|field| {
        let ident = &field.ident;
        let name = ident.to_string();
        let decode_expr = field_from_value_with_enum_context_expr(
            &field.value,
            quote! {
                normalized.iter().find_map(|(entry_key, entry_value)| match entry_key {
                    ::icydb::__macro::Value::Text(entry_key) if entry_key == #name => Some(entry_value),
                    _ => None,
                })?
            },
        );

        quote!(#ident: #decode_expr)
    });
    let field_count = node.fields.len();

    let (meta, encode, decode) = structured_collection_runtime_value_tokens(
        quote!(::icydb::__macro::RuntimeValueKind::Structured { queryable: false }),
        quote! {
            {
                let entries = vec![#(#to_entries),*];
                match ::icydb::__macro::Value::from_map(entries) {
                    Ok(value) => value,
                    Err(err) => {
                        debug_assert!(
                            false,
                            "generated record value surface must emit canonical map entries: {err}",
                        );
                        ::icydb::__macro::Value::Map(Vec::new())
                    }
                }
            }
        },
        quote! {
            {
                let ::icydb::__macro::Value::Map(entries) = value else {
                    return None;
                };
                let normalized = ::icydb::__macro::Value::normalize_map_entries(entries.clone()).ok()?;
                if normalized.len() != #field_count {
                    return None;
                }

                Some(Self {
                    #(#from_fields),*
                })
            }
        },
    );
    let contextual_decode = quote! {
        fn from_value_with_enum_context(
            value: &::icydb::__macro::Value,
            context: &dyn ::icydb::__macro::RuntimeEnumContext,
        ) -> Option<Self> {
            let ::icydb::__macro::Value::Map(entries) = value else {
                return None;
            };
            let normalized = ::icydb::__macro::Value::normalize_map_entries(entries.clone()).ok()?;
            if normalized.len() != #field_count {
                return None;
            }

            Some(Self {
                #(#contextual_from_fields),*
            })
        }
    };

    (meta, encode, quote!(#decode #contextual_decode))
}

fn tuple_runtime_value_tokens(node: &Tuple) -> (TokenStream, TokenStream, TokenStream) {
    let to_items = node.values.iter().enumerate().map(|(index, value)| {
        let slot = syn::Index::from(index);
        field_to_value_expr(value, quote!(self.#slot))
    });
    let from_items = node.values.iter().enumerate().map(|(index, value)| {
        let decode_expr = field_from_value_expr(
            value,
            quote! {
                items.get(#index)?
            },
        );

        quote!(#decode_expr)
    });
    let contextual_from_items = node.values.iter().enumerate().map(|(index, value)| {
        let decode_expr = field_from_value_with_enum_context_expr(
            value,
            quote! {
                items.get(#index)?
            },
        );

        quote!(#decode_expr)
    });
    let item_count = node.values.len();

    let (meta, encode, decode) = structured_collection_runtime_value_tokens(
        quote!(::icydb::__macro::RuntimeValueKind::Structured { queryable: false }),
        quote!(::icydb::__macro::Value::List(vec![#(#to_items),*])),
        quote! {
            {
                let ::icydb::__macro::Value::List(items) = value else {
                    return None;
                };
                if items.len() != #item_count {
                    return None;
                }

                Some(Self(#(#from_items),*))
            }
        },
    );
    let contextual_decode = quote! {
        fn from_value_with_enum_context(
            value: &::icydb::__macro::Value,
            context: &dyn ::icydb::__macro::RuntimeEnumContext,
        ) -> Option<Self> {
            let ::icydb::__macro::Value::List(items) = value else {
                return None;
            };
            if items.len() != #item_count {
                return None;
            }

            Some(Self(#(#contextual_from_items),*))
        }
    };

    (meta, encode, quote!(#decode #contextual_decode))
}

fn list_input_value_tokens(node: &List) -> TokenStream {
    input_value_impl_tokens(
        node.def(),
        quote! {
            ::icydb::__macro::InputValue::List(
                value
                    .0
                    .into_iter()
                    .map(::icydb::__macro::InputValue::from)
                    .collect(),
            )
        },
    )
}

fn map_input_value_tokens(node: &Map) -> TokenStream {
    input_value_impl_tokens(
        node.def(),
        quote! {
            ::icydb::__macro::InputValue::Map(
                value
                    .0
                    .into_iter()
                    .map(|(key, value)| {
                        (
                            ::icydb::__macro::InputValue::from(key),
                            ::icydb::__macro::InputValue::from(value),
                        )
                    })
                    .collect(),
            )
        },
    )
}

fn newtype_input_value_tokens(node: &Newtype) -> TokenStream {
    input_value_impl_tokens(
        node.def(),
        quote!(::icydb::__macro::InputValue::from(value.0)),
    )
}

fn set_input_value_tokens(node: &Set) -> TokenStream {
    input_value_impl_tokens(
        node.def(),
        quote! {
            ::icydb::__macro::InputValue::List(
                value
                    .0
                    .into_iter()
                    .map(::icydb::__macro::InputValue::from)
                    .collect(),
            )
        },
    )
}

fn record_input_value_tokens(node: &Record) -> TokenStream {
    let entries = node.fields.iter().map(|field| {
        let ident = &field.ident;
        let name = ident.to_string();
        let input = owned_value_to_input_expr(&field.value, quote!(value.#ident));

        quote! {
            (
                ::icydb::__macro::InputValue::Text(#name.to_string()),
                #input,
            )
        }
    });

    input_value_impl_tokens(
        node.def(),
        quote!(::icydb::__macro::InputValue::Map(vec![#(#entries),*])),
    )
}

fn tuple_input_value_tokens(node: &Tuple) -> TokenStream {
    let items = node.values.iter().enumerate().map(|(index, value)| {
        let slot = syn::Index::from(index);
        owned_value_to_input_expr(value, quote!(value.#slot))
    });

    input_value_impl_tokens(
        node.def(),
        quote!(::icydb::__macro::InputValue::List(vec![#(#items),*])),
    )
}

// Record payloads need a stable key order on encode and strict field accounting
// on decode so generated codecs remain deterministic and fail closed.
fn record_direct_persisted_structured_codec_tokens(node: &Record) -> TokenStream {
    if node.fields.is_empty() {
        return record_direct_persisted_empty_structured_codec_tokens();
    }

    let encode_entries = record_direct_persisted_encode_entries(node);
    let decode_field_slots = record_direct_persisted_decode_field_slots(node);
    let decode_match_arms = record_direct_persisted_decode_match_arms(node);
    let decode_fields = record_direct_persisted_decode_fields(node);
    let field_count = node.fields.len();

    quote! {
        fn encode_persisted_structured_payload(
            &self,
        ) -> Result<Vec<u8>, ::icydb::__macro::InternalError> {
            let entries = vec![#(#encode_entries),*];
            let entry_refs = entries
                .iter()
                .map(|(key_bytes, value_bytes)| (key_bytes.as_slice(), value_bytes.as_slice()))
                .collect::<Vec<_>>();

            Ok(::icydb::__macro::encode_generated_structural_map_payload_bytes(&entry_refs))
        }

        fn decode_persisted_structured_payload(
            bytes: &[u8],
        ) -> Result<Self, ::icydb::__macro::InternalError> {
            let entries = ::icydb::__macro::decode_generated_structural_map_payload_bytes(bytes)?;
            if entries.len() != #field_count {
                return Err(::icydb::__macro::generated_persisted_structured_payload_decode_failed(
                    format!(
                        "structured record payload field count mismatch: expected {}, got {}",
                        #field_count,
                        entries.len(),
                    ),
                ));
            }

            #(#decode_field_slots)*

            for (entry_key, entry_value) in entries {
                let entry_key = ::icydb::__macro::decode_generated_structural_text_payload_bytes(
                    entry_key,
                )?;

                match entry_key.as_str() {
                    #(#decode_match_arms),*,
                    _ => {
                        return Err(::icydb::__macro::generated_persisted_structured_payload_decode_failed(
                            format!(
                                "structured record payload contains unknown field `{}`",
                                entry_key,
                            ),
                        ));
                    }
                }
            }

            Ok(Self {
                #(#decode_fields),*
            })
        }
    }
}

fn record_direct_persisted_empty_structured_codec_tokens() -> TokenStream {
    quote! {
        fn encode_persisted_structured_payload(
            &self,
        ) -> Result<Vec<u8>, ::icydb::__macro::InternalError> {
            Ok(::icydb::__macro::encode_generated_structural_map_payload_bytes(&[]))
        }

        fn decode_persisted_structured_payload(
            bytes: &[u8],
        ) -> Result<Self, ::icydb::__macro::InternalError> {
            let entries = ::icydb::__macro::decode_generated_structural_map_payload_bytes(bytes)?;
            if !entries.is_empty() {
                return Err(::icydb::__macro::generated_persisted_structured_payload_decode_failed(
                    format!(
                        "structured record payload field count mismatch: expected 0, got {}",
                        entries.len(),
                    ),
                ));
            }

            Ok(Self {})
        }
    }
}

fn record_direct_persisted_encode_entries(node: &Record) -> Vec<TokenStream> {
    let mut sorted_fields: Vec<_> = node.fields.iter().collect();
    sorted_fields.sort_by_key(|field| field.ident.to_string());

    sorted_fields
        .iter()
        .map(|field| {
            let ident = &field.ident;
            let name = ident.to_string();
            let ty = field.value.type_expr();

            quote! {
                (
                    ::icydb::__macro::encode_generated_structural_text_payload_bytes(#name),
                    <#ty as ::icydb::__macro::PersistedStructuralValueCodec>
                        ::encode_persisted_structured_payload(&self.#ident)?,
                )
            }
        })
        .collect()
}

fn record_direct_persisted_decode_field_slots(node: &Record) -> Vec<TokenStream> {
    node.fields
        .iter()
        .map(|field| {
            let ident = &field.ident;
            let ty = field.value.type_expr();

            quote!(let mut #ident: ::std::option::Option<#ty> = ::std::option::Option::None;)
        })
        .collect()
}

fn record_direct_persisted_decode_match_arms(node: &Record) -> Vec<TokenStream> {
    node.fields
        .iter()
        .map(|field| {
            let ident = &field.ident;
            let name = ident.to_string();
            let ty = field.value.type_expr();

            quote! {
                #name => {
                    if #ident.is_some() {
                        return Err(::icydb::__macro::generated_persisted_structured_payload_decode_failed(
                            format!("structured record payload contains duplicate field `{}`", #name),
                        ));
                    }

                    #ident = ::std::option::Option::Some(
                        <#ty as ::icydb::__macro::PersistedStructuralValueCodec>
                            ::decode_persisted_structured_payload(entry_value)?,
                    );
                }
            }
        })
        .collect()
}

fn record_direct_persisted_decode_fields(node: &Record) -> Vec<TokenStream> {
    node.fields
        .iter()
        .map(|field| {
            let ident = &field.ident;
            let name = ident.to_string();

            quote! {
                #ident: #ident.ok_or_else(|| {
                    ::icydb::__macro::generated_persisted_structured_payload_decode_failed(
                        format!("structured record payload missing field `{}`", #name),
                    )
                })?
            }
        })
        .collect()
}

fn tuple_direct_persisted_structured_codec_tokens(node: &Tuple) -> TokenStream {
    let encode_items = node.values.iter().enumerate().map(|(index, value)| {
        let slot = syn::Index::from(index);
        let ty = value.type_expr();

        quote! {
            <#ty as ::icydb::__macro::PersistedStructuralValueCodec>
                ::encode_persisted_structured_payload(&self.#slot)?
        }
    });
    let decode_items = node.values.iter().enumerate().map(|(index, value)| {
        let ty = value.type_expr();

        quote! {
            <#ty as ::icydb::__macro::PersistedStructuralValueCodec>
                ::decode_persisted_structured_payload(item_bytes[#index])?
        }
    });
    let item_count = node.values.len();

    quote! {
        fn encode_persisted_structured_payload(
            &self,
        ) -> Result<Vec<u8>, ::icydb::__macro::InternalError> {
            let item_bytes = vec![#(#encode_items),*];
            let item_refs = item_bytes.iter().map(Vec::as_slice).collect::<Vec<_>>();

            Ok(::icydb::__macro::encode_generated_structural_list_payload_bytes(&item_refs))
        }

        fn decode_persisted_structured_payload(
            bytes: &[u8],
        ) -> Result<Self, ::icydb::__macro::InternalError> {
            let item_bytes = ::icydb::__macro::decode_generated_structural_list_payload_bytes(bytes)?;
            if item_bytes.len() != #item_count {
                return Err(::icydb::__macro::generated_persisted_structured_payload_decode_failed(
                    format!(
                        "structured tuple payload item count mismatch: expected {}, got {}",
                        #item_count,
                        item_bytes.len(),
                    ),
                ));
            }

            Ok(Self(#(#decode_items),*))
        }
    }
}

///
/// List
///

impl Imp<List> for RuntimeValueTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let (runtime_value_meta, runtime_value_encode, runtime_value_decode) =
            structured_collection_runtime_value_tokens(
                quote!(::icydb::__macro::RuntimeValueKind::Structured { queryable: true }),
                quote!(::icydb::__macro::runtime_value_collection_to_value(self)),
                quote!(::icydb::__macro::runtime_value_vec_from_value::<#item>(value).map(Self)),
            );
        let runtime_value_decode = quote! {
            #runtime_value_decode

            fn from_value_with_enum_context(
                value: &::icydb::__macro::Value,
                context: &dyn ::icydb::__macro::RuntimeEnumContext,
            ) -> Option<Self> {
                ::icydb::__macro::runtime_value_from_value_with_enum_context::<Vec<#item>>(
                    value,
                    context,
                )
                .map(Self)
            }
        };

        let input = list_input_value_tokens(node);
        if node.item.is.is_some() {
            return Some(runtime_value_strategy_without_encode(
                node.def(),
                runtime_value_meta,
                runtime_value_decode,
                input,
            ));
        }
        Some(runtime_value_strategy(
            node.def(),
            runtime_value_meta,
            runtime_value_encode,
            runtime_value_decode,
            input,
        ))
    }
}

impl Imp<List> for PersistedStructuralValueCodecTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let item = node.item.type_expr();

        Some(persisted_field_codec_strategy(
            node.def(),
            quote! {
                fn encode_persisted_structured_payload(
                    &self,
                ) -> Result<Vec<u8>, ::icydb::__macro::InternalError> {
                    <Vec<#item> as ::icydb::__macro::PersistedStructuralValueCodec>
                        ::encode_persisted_structured_payload(&self.0)
                }

                fn decode_persisted_structured_payload(
                    bytes: &[u8],
                ) -> Result<Self, ::icydb::__macro::InternalError> {
                    Ok(Self(
                        <Vec<#item> as ::icydb::__macro::PersistedStructuralValueCodec>
                            ::decode_persisted_structured_payload(bytes)?,
                    ))
                }
            },
        ))
    }
}

///
/// Map
///

impl Imp<Map> for RuntimeValueTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key_type = node.key.type_expr();
        let value_type = node.value.type_expr();
        let (runtime_value_meta, runtime_value_encode, runtime_value_decode) =
            structured_collection_runtime_value_tokens(
                quote!(::icydb::__macro::RuntimeValueKind::Structured { queryable: false }),
                quote!(::icydb::__macro::runtime_value_map_collection_to_value(
                    self,
                    <Self as ::icydb::__macro::Path>::PATH,
                )),
                quote!(
                    ::icydb::__macro::runtime_value_btree_map_from_value::<#key_type, #value_type>(value)
                        .map(Self)
                ),
            );
        let runtime_value_decode = quote! {
            #runtime_value_decode

            fn from_value_with_enum_context(
                value: &::icydb::__macro::Value,
                context: &dyn ::icydb::__macro::RuntimeEnumContext,
            ) -> Option<Self> {
                ::icydb::__macro::runtime_value_from_value_with_enum_context::<
                    ::std::collections::BTreeMap<#key_type, #value_type>
                >(value, context)
                .map(Self)
            }
        };

        let input = map_input_value_tokens(node);
        if node.key.is.is_some() || node.value.item.is.is_some() {
            return Some(runtime_value_strategy_without_encode(
                node.def(),
                runtime_value_meta,
                runtime_value_decode,
                input,
            ));
        }
        Some(runtime_value_strategy(
            node.def(),
            runtime_value_meta,
            runtime_value_encode,
            runtime_value_decode,
            input,
        ))
    }
}

impl Imp<Map> for PersistedStructuralValueCodecTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key_type = node.key.type_expr();
        let value_type = node.value.type_expr();

        Some(persisted_field_codec_strategy(
            node.def(),
            quote! {
                fn encode_persisted_structured_payload(
                    &self,
                ) -> Result<Vec<u8>, ::icydb::__macro::InternalError> {
                    <::std::collections::BTreeMap<#key_type, #value_type> as ::icydb::__macro::PersistedStructuralValueCodec>
                        ::encode_persisted_structured_payload(&self.0)
                }

                fn decode_persisted_structured_payload(
                    bytes: &[u8],
                ) -> Result<Self, ::icydb::__macro::InternalError> {
                    Ok(Self(
                        <::std::collections::BTreeMap<#key_type, #value_type> as ::icydb::__macro::PersistedStructuralValueCodec>
                            ::decode_persisted_structured_payload(bytes)?,
                    ))
                }
            },
        ))
    }
}

///
/// Newtype
///

impl Imp<Newtype> for RuntimeValueTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let (runtime_value_meta, runtime_value_encode, runtime_value_decode) =
            newtype_runtime_value_tokens(&item);

        if node.item.is.is_some() {
            return Some(runtime_value_strategy_without_encode(
                node.def(),
                runtime_value_meta,
                runtime_value_decode,
                newtype_input_value_tokens(node),
            ));
        }
        Some(runtime_value_strategy(
            node.def(),
            runtime_value_meta,
            runtime_value_encode,
            runtime_value_decode,
            newtype_input_value_tokens(node),
        ))
    }
}

impl Imp<Newtype> for PersistedStructuralValueCodecTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let item = node.item.type_expr();

        Some(persisted_field_codec_strategy(
            node.def(),
            quote! {
                fn encode_persisted_structured_payload(
                    &self,
                ) -> Result<Vec<u8>, ::icydb::__macro::InternalError> {
                    <#item as ::icydb::__macro::PersistedStructuralValueCodec>
                        ::encode_persisted_structured_payload(&self.0)
                }

                fn decode_persisted_structured_payload(
                    bytes: &[u8],
                ) -> Result<Self, ::icydb::__macro::InternalError> {
                    Ok(Self(
                        <#item as ::icydb::__macro::PersistedStructuralValueCodec>
                            ::decode_persisted_structured_payload(bytes)?,
                    ))
                }
            },
        ))
    }
}

///
/// Set
///

impl Imp<Set> for RuntimeValueTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let (runtime_value_meta, runtime_value_encode, runtime_value_decode) =
            structured_collection_runtime_value_tokens(
                quote!(::icydb::__macro::RuntimeValueKind::Structured { queryable: true }),
                quote!(::icydb::__macro::runtime_value_collection_to_value(self)),
                quote!(::icydb::__macro::runtime_value_btree_set_from_value::<#item>(value).map(Self)),
            );
        let runtime_value_decode = quote! {
            #runtime_value_decode

            fn from_value_with_enum_context(
                value: &::icydb::__macro::Value,
                context: &dyn ::icydb::__macro::RuntimeEnumContext,
            ) -> Option<Self> {
                ::icydb::__macro::runtime_value_from_value_with_enum_context::<
                    ::std::collections::BTreeSet<#item>
                >(value, context)
                .map(Self)
            }
        };

        let input = set_input_value_tokens(node);
        if node.item.is.is_some() {
            return Some(runtime_value_strategy_without_encode(
                node.def(),
                runtime_value_meta,
                runtime_value_decode,
                input,
            ));
        }
        Some(runtime_value_strategy(
            node.def(),
            runtime_value_meta,
            runtime_value_encode,
            runtime_value_decode,
            input,
        ))
    }
}

impl Imp<Set> for PersistedStructuralValueCodecTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = node.item.type_expr();

        Some(persisted_field_codec_strategy(
            node.def(),
            quote! {
                fn encode_persisted_structured_payload(
                    &self,
                ) -> Result<Vec<u8>, ::icydb::__macro::InternalError> {
                    <::std::collections::BTreeSet<#item> as ::icydb::__macro::PersistedStructuralValueCodec>
                        ::encode_persisted_structured_payload(&self.0)
                }

                fn decode_persisted_structured_payload(
                    bytes: &[u8],
                ) -> Result<Self, ::icydb::__macro::InternalError> {
                    Ok(Self(
                        <::std::collections::BTreeSet<#item> as ::icydb::__macro::PersistedStructuralValueCodec>
                            ::decode_persisted_structured_payload(bytes)?,
                    ))
                }
            },
        ))
    }
}

///
/// Record
///

impl Imp<Record> for RuntimeValueTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let (runtime_value_meta, runtime_value_encode, runtime_value_decode) =
            record_runtime_value_tokens(node);
        let input = record_input_value_tokens(node);
        if node
            .fields
            .iter()
            .any(|field| field.value.item.is.is_some())
        {
            return Some(runtime_value_strategy_without_encode(
                node.def(),
                runtime_value_meta,
                runtime_value_decode,
                input,
            ));
        }
        Some(runtime_value_strategy(
            node.def(),
            runtime_value_meta,
            runtime_value_encode,
            runtime_value_decode,
            input,
        ))
    }
}

impl Imp<Record> for PersistedStructuralValueCodecTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(persisted_field_codec_strategy(
            node.def(),
            record_direct_persisted_structured_codec_tokens(node),
        ))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for RuntimeValueTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let (runtime_value_meta, runtime_value_encode, runtime_value_decode) =
            tuple_runtime_value_tokens(node);
        let input = tuple_input_value_tokens(node);
        if node.values.iter().any(|value| value.item.is.is_some()) {
            return Some(runtime_value_strategy_without_encode(
                node.def(),
                runtime_value_meta,
                runtime_value_decode,
                input,
            ));
        }
        Some(runtime_value_strategy(
            node.def(),
            runtime_value_meta,
            runtime_value_encode,
            runtime_value_decode,
            input,
        ))
    }
}

impl Imp<Tuple> for PersistedStructuralValueCodecTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        Some(persisted_field_codec_strategy(
            node.def(),
            tuple_direct_persisted_structured_codec_tokens(node),
        ))
    }
}

fn runtime_value_strategy(
    def: &Def,
    runtime_value_meta: TokenStream,
    runtime_value_encode: TokenStream,
    runtime_value_decode: TokenStream,
    input_value: TokenStream,
) -> TraitStrategy {
    let mut tokens = runtime_value_impl_tokens(
        def,
        runtime_value_meta,
        runtime_value_encode,
        runtime_value_decode,
    );
    tokens.extend(input_value);
    TraitStrategy::from_impl(tokens)
}

fn runtime_value_strategy_without_encode(
    def: &Def,
    runtime_value_meta: TokenStream,
    runtime_value_decode: TokenStream,
    input_value: TokenStream,
) -> TraitStrategy {
    let mut tokens = TokenStream::new();
    tokens.extend(
        Implementor::new(def, TraitKind::RuntimeValueMeta)
            .set_tokens(runtime_value_meta)
            .to_token_stream(),
    );
    tokens.extend(
        Implementor::new(def, TraitKind::RuntimeValueDecode)
            .set_tokens(runtime_value_decode)
            .to_token_stream(),
    );
    tokens.extend(input_value);
    TraitStrategy::from_impl(tokens)
}

fn persisted_field_codec_strategy(
    def: &Def,
    persisted_structured_field_codec: TokenStream,
) -> TraitStrategy {
    TraitStrategy::from_impl(
        Implementor::new(def, TraitKind::PersistedStructuralValueCodec)
            .set_tokens(persisted_structured_field_codec)
            .to_token_stream(),
    )
}
