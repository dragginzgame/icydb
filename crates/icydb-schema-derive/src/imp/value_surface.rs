use crate::prelude::*;

///
/// ValueSurfaceTrait
///

pub struct ValueSurfaceTrait {}

///
/// Enum
///

impl Imp<Enum> for ValueSurfaceTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let to_value_enum_arms = enum_to_value_enum_arms(node);
        let enum_value = quote! {
            fn to_value_enum(&self) -> ::icydb::__macro::ValueEnum {
                use ::icydb::__macro::ValueEnum;

                match self {
                    #(#to_value_enum_arms),*
                }
            }
        };

        let (field_value_meta, value_surface_encode, value_surface_decode) =
            enum_field_value_tokens(node);

        let mut tokens = TokenStream::new();
        tokens.extend(
            Implementor::new(node.def(), TraitKind::EnumValue)
                .set_tokens(enum_value)
                .to_token_stream(),
        );
        tokens.extend(field_value_impl_tokens(
            node.def(),
            field_value_meta,
            value_surface_encode,
            value_surface_decode,
            persisted_field_meta_codec_tokens(),
            enum_direct_persisted_structured_codec_tokens(node),
        ));

        Some(TraitStrategy::from_impl(tokens))
    }
}

fn enum_to_value_enum_arms(node: &Enum) -> Vec<TokenStream> {
    node.variants
        .iter()
        .map(|variant| {
            let variant_match = enum_variant_match_pattern(variant);
            let variant_name = variant.ident.to_string();
            let payload_tokens = if variant.value.is_some() {
                quote!(.with_payload(::icydb::__macro::value_surface_to_value(v)))
            } else {
                quote!()
            };

            quote! {
                Self::#variant_match => {
                    ValueEnum::new(
                        #variant_name,
                        Some(Self::PATH)
                    ) #payload_tokens
                }
            }
        })
        .collect()
}

fn enum_variant_match_pattern(variant: &EnumVariant) -> TokenStream {
    let variant_ident = &variant.ident;

    if variant.value.is_some() {
        quote!(#variant_ident(v))
    } else {
        quote!(#variant_ident)
    }
}

fn enum_field_value_tokens(node: &Enum) -> (TokenStream, TokenStream, TokenStream) {
    let from_arms = enum_from_value_arms(node);

    (
        quote! {
            fn kind() -> ::icydb::__macro::ValueSurfaceKind {
                ::icydb::__macro::ValueSurfaceKind::Atomic
            }
        },
        quote! {
            fn to_value(&self) -> ::icydb::__macro::Value {
                ::icydb::__macro::Value::Enum(::icydb::__macro::EnumValue::to_value_enum(self))
            }
        },
        quote! {

            fn from_value(value: &::icydb::__macro::Value) -> Option<Self> {
                let ::icydb::__macro::Value::Enum(v) = value else {
                    return None;
                };
                if let Some(path) = v.path()
                    && path != <Self as ::icydb::traits::Path>::PATH
                {
                    return None;
                }

                match v.variant() {
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
            let variant_name = variant_ident.to_string();

            if let Some(value) = &variant.value {
                let payload_ty = value.type_expr();

                quote! {
                    #variant_name => {
                        let payload = v.payload()?;
                        let value =
                            ::icydb::__macro::value_surface_from_value::<#payload_ty>(payload)?;
                        Some(Self::#variant_ident(value))
                    }
                }
            } else {
                quote! {
                    #variant_name => Some(Self::#variant_ident)
                }
            }
        })
        .collect()
}

// Enums can leave the Value bridge only when every payload-bearing variant
// already targets a direct persisted bytes codec.
fn enum_direct_persisted_structured_codec_tokens(node: &Enum) -> TokenStream {
    let encode_arms = node.variants.iter().map(|variant| {
        let variant_ident = &variant.ident;
        let variant_name = variant_ident.to_string();

        if let Some(value) = &variant.value {
            let payload_ty = value.type_expr();

            quote! {
                Self::#variant_ident(value) => {
                    let payload = <#payload_ty as ::icydb::__macro::PersistedStructuredFieldCodec>
                        ::encode_persisted_structured_payload(value)?;
                    ::icydb::db::encode_generated_structural_enum_payload_bytes(
                        #variant_name,
                        Some(Self::PATH),
                        Some(payload.as_slice()),
                    )
                }
            }
        } else {
            quote! {
                Self::#variant_ident => {
                    ::icydb::db::encode_generated_structural_enum_payload_bytes(
                        #variant_name,
                        Some(Self::PATH),
                        None,
                    )
                }
            }
        }
    });
    let decode_arms = node.variants.iter().map(|variant| {
        let variant_ident = &variant.ident;
        let variant_name = variant_ident.to_string();

        if let Some(value) = &variant.value {
            let payload_ty = value.type_expr();

            quote! {
                #variant_name => {
                    let payload = payload.ok_or_else(|| {
                        ::icydb::db::generated_persisted_structured_payload_decode_failed(
                            format!(
                                "structured enum payload missing payload for variant `{}`",
                                #variant_name,
                            ),
                        )
                    })?;
                    let value = <#payload_ty as ::icydb::__macro::PersistedStructuredFieldCodec>
                        ::decode_persisted_structured_payload(payload)?;

                    Ok(Self::#variant_ident(value))
                }
            }
        } else {
            quote! {
                #variant_name => {
                    if payload.is_some() {
                        return Err(::icydb::db::generated_persisted_structured_payload_decode_failed(
                            format!(
                                "structured enum payload must not carry payload for variant `{}`",
                                #variant_name,
                            ),
                        ));
                    }

                    Ok(Self::#variant_ident)
                }
            }
        }
    });

    quote! {
        fn encode_persisted_structured_payload(
            &self,
        ) -> Result<Vec<u8>, ::icydb::db::InternalError> {
            Ok(match self {
                #(#encode_arms),*
            })
        }

        fn decode_persisted_structured_payload(
            bytes: &[u8],
        ) -> Result<Self, ::icydb::db::InternalError> {
            let (variant, path, payload) =
                ::icydb::db::decode_generated_structural_enum_payload_bytes(bytes)?;
            if path.as_deref() != Some(Self::PATH) {
                return Err(::icydb::db::generated_persisted_structured_payload_decode_failed(
                    format!(
                        "structured enum payload path mismatch: expected `{}`, got {:?}",
                        Self::PATH,
                        path,
                    ),
                ));
            }

            match variant.as_str() {
                #(#decode_arms),*,
                _ => Err(::icydb::db::generated_persisted_structured_payload_decode_failed(
                    format!("structured enum payload contains unknown variant `{}`", variant),
                )),
            }
        }
    }
}

fn field_value_impl_tokens(
    def: &Def,
    field_value_meta: TokenStream,
    value_surface_encode: TokenStream,
    value_surface_decode: TokenStream,
    persisted_field_meta_codec: TokenStream,
    persisted_structured_field_codec: TokenStream,
) -> TokenStream {
    let mut tokens = TokenStream::new();
    tokens.extend(
        Implementor::new(def, TraitKind::ValueSurfaceMeta)
            .set_tokens(field_value_meta)
            .to_token_stream(),
    );
    tokens.extend(
        Implementor::new(def, TraitKind::ValueSurfaceEncode)
            .set_tokens(value_surface_encode)
            .to_token_stream(),
    );
    tokens.extend(
        Implementor::new(def, TraitKind::ValueSurfaceDecode)
            .set_tokens(value_surface_decode)
            .to_token_stream(),
    );
    tokens.extend(
        Implementor::new(def, TraitKind::PersistedFieldMetaCodec)
            .set_tokens(persisted_field_meta_codec)
            .to_token_stream(),
    );
    tokens.extend(
        Implementor::new(def, TraitKind::PersistedStructuredFieldCodec)
            .set_tokens(persisted_structured_field_codec)
            .to_token_stream(),
    );
    tokens
}

fn persisted_field_meta_codec_tokens() -> TokenStream {
    quote! {
        fn encode_persisted_slot_payload_by_meta(
            &self,
            field_name: &'static str,
        ) -> Result<Vec<u8>, ::icydb::__macro::InternalError> {
            ::icydb::db::encode_persisted_custom_slot_payload(self, field_name)
        }

        fn decode_persisted_slot_payload_by_meta(
            bytes: &[u8],
            field_name: &'static str,
        ) -> Result<Self, ::icydb::__macro::InternalError> {
            ::icydb::db::decode_persisted_custom_slot_payload(bytes, field_name)
        }

        fn encode_persisted_option_slot_payload_by_meta(
            value: &Option<Self>,
            field_name: &'static str,
        ) -> Result<Vec<u8>, ::icydb::__macro::InternalError> {
            ::icydb::db::encode_persisted_custom_slot_payload(value, field_name)
        }

        fn decode_persisted_option_slot_payload_by_meta(
            bytes: &[u8],
            field_name: &'static str,
        ) -> Result<Option<Self>, ::icydb::__macro::InternalError> {
            ::icydb::db::decode_persisted_custom_slot_payload(bytes, field_name)
        }
    }
}

fn structured_collection_field_value_tokens(
    kind: TokenStream,
    to_value: TokenStream,
    from_value: TokenStream,
) -> (TokenStream, TokenStream, TokenStream) {
    (
        quote! {
            fn kind() -> ::icydb::__macro::ValueSurfaceKind {
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

fn newtype_field_value_tokens(item: &TokenStream) -> (TokenStream, TokenStream, TokenStream) {
    (
        quote! {
            fn kind() -> ::icydb::__macro::ValueSurfaceKind {
                <#item as ::icydb::__macro::ValueSurfaceMeta>::kind()
            }
        },
        quote! {
            fn to_value(&self) -> ::icydb::__macro::Value {
                ::icydb::__macro::value_surface_to_value(&self.0)
            }
        },
        quote! {
            fn from_value(value: &::icydb::__macro::Value) -> Option<Self> {
                let inner = ::icydb::__macro::value_surface_from_value::<#item>(value)?;
                Some(Self(inner))
            }
        },
    )
}

fn field_to_value_expr(value: &crate::node::Value, access: TokenStream) -> TokenStream {
    match value.cardinality() {
        Cardinality::One => quote!(::icydb::__macro::value_surface_to_value(&#access)),
        Cardinality::Opt => quote! {
            match #access.as_ref() {
                Some(inner) => ::icydb::__macro::value_surface_to_value(inner),
                None => ::icydb::__macro::Value::Null,
            }
        },
        Cardinality::Many => quote! {
            ::icydb::__macro::Value::List(
                #access
                    .iter()
                    .map(::icydb::__macro::value_surface_to_value)
                    .collect(),
            )
        },
    }
}

fn field_from_value_expr(value: &crate::node::Value, source: TokenStream) -> TokenStream {
    match value.cardinality() {
        Cardinality::One | Cardinality::Opt => {
            let ty = value.type_expr();
            quote!(::icydb::__macro::value_surface_from_value::<#ty>(#source)?)
        }
        Cardinality::Many => {
            let item_ty = value.item.type_expr();
            quote!(::icydb::__macro::value_surface_vec_from_value::<#item_ty>(#source)?)
        }
    }
}

fn record_field_value_tokens(node: &Record) -> (TokenStream, TokenStream, TokenStream) {
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
    let field_count = node.fields.len();

    structured_collection_field_value_tokens(
        quote!(::icydb::__macro::ValueSurfaceKind::Structured { queryable: false }),
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
    )
}

fn tuple_field_value_tokens(node: &Tuple) -> (TokenStream, TokenStream, TokenStream) {
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
    let item_count = node.values.len();

    structured_collection_field_value_tokens(
        quote!(::icydb::__macro::ValueSurfaceKind::Structured { queryable: false }),
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
        ) -> Result<Vec<u8>, ::icydb::db::InternalError> {
            let entries = vec![#(#encode_entries),*];
            let entry_refs = entries
                .iter()
                .map(|(key_bytes, value_bytes)| (key_bytes.as_slice(), value_bytes.as_slice()))
                .collect::<Vec<_>>();

            Ok(::icydb::db::encode_generated_structural_map_payload_bytes(&entry_refs))
        }

        fn decode_persisted_structured_payload(
            bytes: &[u8],
        ) -> Result<Self, ::icydb::db::InternalError> {
            let entries = ::icydb::db::decode_generated_structural_map_payload_bytes(bytes)?;
            if entries.len() != #field_count {
                return Err(::icydb::db::generated_persisted_structured_payload_decode_failed(
                    format!(
                        "structured record payload field count mismatch: expected {}, got {}",
                        #field_count,
                        entries.len(),
                    ),
                ));
            }

            #(#decode_field_slots)*

            for (entry_key, entry_value) in entries {
                let entry_key = ::icydb::db::decode_generated_structural_text_payload_bytes(
                    entry_key,
                )?;

                match entry_key.as_str() {
                    #(#decode_match_arms),*,
                    _ => {
                        return Err(::icydb::db::generated_persisted_structured_payload_decode_failed(
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
        ) -> Result<Vec<u8>, ::icydb::db::InternalError> {
            Ok(::icydb::db::encode_generated_structural_map_payload_bytes(&[]))
        }

        fn decode_persisted_structured_payload(
            bytes: &[u8],
        ) -> Result<Self, ::icydb::db::InternalError> {
            let entries = ::icydb::db::decode_generated_structural_map_payload_bytes(bytes)?;
            if !entries.is_empty() {
                return Err(::icydb::db::generated_persisted_structured_payload_decode_failed(
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
                    ::icydb::db::encode_generated_structural_text_payload_bytes(#name),
                    <#ty as ::icydb::__macro::PersistedStructuredFieldCodec>
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
                        return Err(::icydb::db::generated_persisted_structured_payload_decode_failed(
                            format!("structured record payload contains duplicate field `{}`", #name),
                        ));
                    }

                    #ident = ::std::option::Option::Some(
                        <#ty as ::icydb::__macro::PersistedStructuredFieldCodec>
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
                    ::icydb::db::generated_persisted_structured_payload_decode_failed(
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
            <#ty as ::icydb::__macro::PersistedStructuredFieldCodec>
                ::encode_persisted_structured_payload(&self.#slot)?
        }
    });
    let decode_items = node.values.iter().enumerate().map(|(index, value)| {
        let ty = value.type_expr();

        quote! {
            <#ty as ::icydb::__macro::PersistedStructuredFieldCodec>
                ::decode_persisted_structured_payload(item_bytes[#index])?
        }
    });
    let item_count = node.values.len();

    quote! {
        fn encode_persisted_structured_payload(
            &self,
        ) -> Result<Vec<u8>, ::icydb::db::InternalError> {
            let item_bytes = vec![#(#encode_items),*];
            let item_refs = item_bytes.iter().map(Vec::as_slice).collect::<Vec<_>>();

            Ok(::icydb::db::encode_generated_structural_list_payload_bytes(&item_refs))
        }

        fn decode_persisted_structured_payload(
            bytes: &[u8],
        ) -> Result<Self, ::icydb::db::InternalError> {
            let item_bytes = ::icydb::db::decode_generated_structural_list_payload_bytes(bytes)?;
            if item_bytes.len() != #item_count {
                return Err(::icydb::db::generated_persisted_structured_payload_decode_failed(
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

impl Imp<List> for ValueSurfaceTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let (field_value_meta, value_surface_encode, value_surface_decode) =
            structured_collection_field_value_tokens(
                quote!(::icydb::__macro::ValueSurfaceKind::Structured { queryable: true }),
                quote!(::icydb::__macro::value_surface_collection_to_value(self)),
                quote!(::icydb::__macro::value_surface_vec_from_value::<#item>(value).map(Self)),
            );

        Some(field_value_strategy(
            node.def(),
            field_value_meta,
            value_surface_encode,
            value_surface_decode,
            quote! {
                fn encode_persisted_structured_payload(
                    &self,
                ) -> Result<Vec<u8>, ::icydb::db::InternalError> {
                    <Vec<#item> as ::icydb::__macro::PersistedStructuredFieldCodec>
                        ::encode_persisted_structured_payload(&self.0)
                }

                fn decode_persisted_structured_payload(
                    bytes: &[u8],
                ) -> Result<Self, ::icydb::db::InternalError> {
                    Ok(Self(
                        <Vec<#item> as ::icydb::__macro::PersistedStructuredFieldCodec>
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

impl Imp<Map> for ValueSurfaceTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key_type = node.key.type_expr();
        let value_type = node.value.type_expr();
        let (field_value_meta, value_surface_encode, value_surface_decode) =
            structured_collection_field_value_tokens(
                quote!(::icydb::__macro::ValueSurfaceKind::Structured { queryable: false }),
                quote!(::icydb::__macro::value_surface_map_collection_to_value(
                    self,
                    <Self as ::icydb::traits::Path>::PATH,
                )),
                quote!(
                    ::icydb::__macro::value_surface_btree_map_from_value::<#key_type, #value_type>(value)
                        .map(Self)
                ),
            );

        Some(field_value_strategy(
            node.def(),
            field_value_meta,
            value_surface_encode,
            value_surface_decode,
            quote! {
                fn encode_persisted_structured_payload(
                    &self,
                ) -> Result<Vec<u8>, ::icydb::db::InternalError> {
                    <::std::collections::BTreeMap<#key_type, #value_type> as ::icydb::__macro::PersistedStructuredFieldCodec>
                        ::encode_persisted_structured_payload(&self.0)
                }

                fn decode_persisted_structured_payload(
                    bytes: &[u8],
                ) -> Result<Self, ::icydb::db::InternalError> {
                    Ok(Self(
                        <::std::collections::BTreeMap<#key_type, #value_type> as ::icydb::__macro::PersistedStructuredFieldCodec>
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

impl Imp<Newtype> for ValueSurfaceTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let (field_value_meta, value_surface_encode, value_surface_decode) =
            newtype_field_value_tokens(&item);

        Some(field_value_strategy(
            node.def(),
            field_value_meta,
            value_surface_encode,
            value_surface_decode,
            quote! {
                fn encode_persisted_structured_payload(
                    &self,
                ) -> Result<Vec<u8>, ::icydb::db::InternalError> {
                    <#item as ::icydb::__macro::PersistedStructuredFieldCodec>
                        ::encode_persisted_structured_payload(&self.0)
                }

                fn decode_persisted_structured_payload(
                    bytes: &[u8],
                ) -> Result<Self, ::icydb::db::InternalError> {
                    Ok(Self(
                        <#item as ::icydb::__macro::PersistedStructuredFieldCodec>
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

impl Imp<Set> for ValueSurfaceTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let (field_value_meta, value_surface_encode, value_surface_decode) =
            structured_collection_field_value_tokens(
                quote!(::icydb::__macro::ValueSurfaceKind::Structured { queryable: true }),
                quote!(::icydb::__macro::value_surface_collection_to_value(self)),
                quote!(::icydb::__macro::value_surface_btree_set_from_value::<#item>(value).map(Self)),
            );

        Some(field_value_strategy(
            node.def(),
            field_value_meta,
            value_surface_encode,
            value_surface_decode,
            quote! {
                fn encode_persisted_structured_payload(
                    &self,
                ) -> Result<Vec<u8>, ::icydb::db::InternalError> {
                    <::std::collections::BTreeSet<#item> as ::icydb::__macro::PersistedStructuredFieldCodec>
                        ::encode_persisted_structured_payload(&self.0)
                }

                fn decode_persisted_structured_payload(
                    bytes: &[u8],
                ) -> Result<Self, ::icydb::db::InternalError> {
                    Ok(Self(
                        <::std::collections::BTreeSet<#item> as ::icydb::__macro::PersistedStructuredFieldCodec>
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

impl Imp<Record> for ValueSurfaceTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let (field_value_meta, value_surface_encode, value_surface_decode) =
            record_field_value_tokens(node);
        Some(field_value_strategy(
            node.def(),
            field_value_meta,
            value_surface_encode,
            value_surface_decode,
            record_direct_persisted_structured_codec_tokens(node),
        ))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for ValueSurfaceTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let (field_value_meta, value_surface_encode, value_surface_decode) =
            tuple_field_value_tokens(node);
        Some(field_value_strategy(
            node.def(),
            field_value_meta,
            value_surface_encode,
            value_surface_decode,
            tuple_direct_persisted_structured_codec_tokens(node),
        ))
    }
}

fn field_value_strategy(
    def: &Def,
    field_value_meta: TokenStream,
    value_surface_encode: TokenStream,
    value_surface_decode: TokenStream,
    persisted_structured_field_codec: TokenStream,
) -> TraitStrategy {
    TraitStrategy::from_impl(field_value_impl_tokens(
        def,
        field_value_meta,
        value_surface_encode,
        value_surface_decode,
        persisted_field_meta_codec_tokens(),
        persisted_structured_field_codec,
    ))
}
