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
        let to_value_enum_arms = enum_to_value_enum_arms(node);
        let enum_value = quote! {
            fn to_value_enum(&self) -> ::icydb::__macro::ValueEnum {
                use ::icydb::__macro::ValueEnum;

                match self {
                    #(#to_value_enum_arms),*
                }
            }
        };

        let field_value = enum_field_value_tokens(node);

        let mut tokens = TokenStream::new();
        tokens.extend(
            Implementor::new(node.def(), TraitKind::EnumValue)
                .set_tokens(enum_value)
                .to_token_stream(),
        );
        tokens.extend(field_value_impl_tokens(node.def(), field_value));

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
                quote!(.with_payload(::icydb::__macro::FieldValue::to_value(v)))
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

fn enum_field_value_tokens(node: &Enum) -> TokenStream {
    let from_arms = enum_from_value_arms(node);

    quote! {
        fn kind() -> ::icydb::__macro::FieldValueKind {
            ::icydb::__macro::FieldValueKind::Atomic
        }

        fn to_value(&self) -> ::icydb::__macro::Value {
            ::icydb::__macro::Value::Enum(::icydb::__macro::EnumValue::to_value_enum(self))
        }

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
    }
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
                            <#payload_ty as ::icydb::__macro::FieldValue>::from_value(payload)?;
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

fn field_value_impl_tokens(def: &Def, tokens: TokenStream) -> TokenStream {
    Implementor::new(def, TraitKind::FieldValue)
        .set_tokens(tokens)
        .to_token_stream()
}

fn structured_collection_field_value_tokens(
    kind: TokenStream,
    to_value: TokenStream,
    from_value: TokenStream,
) -> TokenStream {
    quote! {
        fn kind() -> ::icydb::__macro::FieldValueKind {
            #kind
        }

        fn to_value(&self) -> ::icydb::__macro::Value {
            #to_value
        }

        fn from_value(value: &::icydb::__macro::Value) -> Option<Self> {
            #from_value
        }
    }
}

fn newtype_field_value_tokens(item: &TokenStream) -> TokenStream {
    quote! {
        fn kind() -> ::icydb::__macro::FieldValueKind {
            <#item as ::icydb::__macro::FieldValue>::kind()
        }

        fn to_value(&self) -> ::icydb::__macro::Value {
            self.0.to_value()
        }

        fn from_value(value: &::icydb::__macro::Value) -> Option<Self> {
            let inner = <#item as ::icydb::__macro::FieldValue>::from_value(value)?;
            Some(Self(inner))
        }
    }
}

fn field_to_value_expr(value: &crate::node::Value, access: TokenStream) -> TokenStream {
    match value.cardinality() {
        Cardinality::One => quote!(::icydb::__macro::FieldValue::to_value(&#access)),
        Cardinality::Opt => quote! {
            match #access.as_ref() {
                Some(inner) => ::icydb::__macro::FieldValue::to_value(inner),
                None => ::icydb::__macro::Value::Null,
            }
        },
        Cardinality::Many => quote! {
            ::icydb::__macro::Value::List(
                #access
                    .iter()
                    .map(::icydb::__macro::FieldValue::to_value)
                    .collect(),
            )
        },
    }
}

fn field_from_value_expr(value: &crate::node::Value, source: TokenStream) -> TokenStream {
    match value.cardinality() {
        Cardinality::One | Cardinality::Opt => {
            let ty = value.type_expr();
            quote!(<#ty as ::icydb::__macro::FieldValue>::from_value(#source)?)
        }
        Cardinality::Many => {
            let item_ty = value.item.type_expr();
            quote!(::icydb::__macro::field_value_vec_from_value::<#item_ty>(#source)?)
        }
    }
}

fn record_field_value_tokens(node: &Record) -> TokenStream {
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
        quote!(::icydb::__macro::FieldValueKind::Structured { queryable: false }),
        quote! {
            {
                let entries = vec![#(#to_entries),*];
                match ::icydb::__macro::Value::from_map(entries) {
                    Ok(value) => value,
                    Err(err) => {
                        debug_assert!(
                            false,
                            "generated record FieldValue must emit canonical map entries: {err}",
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

fn tuple_field_value_tokens(node: &Tuple) -> TokenStream {
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
        quote!(::icydb::__macro::FieldValueKind::Structured { queryable: false }),
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

///
/// List
///

impl Imp<List> for FieldValueTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let tokens = structured_collection_field_value_tokens(
            quote!(::icydb::__macro::FieldValueKind::Structured { queryable: true }),
            quote!(::icydb::__macro::field_value_collection_to_value(self)),
            quote!(::icydb::__macro::field_value_vec_from_value::<#item>(value).map(Self)),
        );

        Some(field_value_strategy(node.def(), tokens))
    }
}

///
/// Map
///

impl Imp<Map> for FieldValueTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key_type = node.key.type_expr();
        let value_type = node.value.type_expr();
        let tokens = structured_collection_field_value_tokens(
            quote!(::icydb::__macro::FieldValueKind::Structured { queryable: false }),
            quote!(::icydb::__macro::field_value_map_collection_to_value(
                self,
                <Self as ::icydb::traits::Path>::PATH,
            )),
            quote!(
                ::icydb::__macro::field_value_btree_map_from_value::<#key_type, #value_type>(value)
                    .map(Self)
            ),
        );

        Some(field_value_strategy(node.def(), tokens))
    }
}

///
/// Newtype
///

impl Imp<Newtype> for FieldValueTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let tokens = newtype_field_value_tokens(&item);

        Some(field_value_strategy(node.def(), tokens))
    }
}

///
/// Set
///

impl Imp<Set> for FieldValueTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let tokens = structured_collection_field_value_tokens(
            quote!(::icydb::__macro::FieldValueKind::Structured { queryable: true }),
            quote!(::icydb::__macro::field_value_collection_to_value(self)),
            quote!(::icydb::__macro::field_value_btree_set_from_value::<#item>(value).map(Self)),
        );

        Some(field_value_strategy(node.def(), tokens))
    }
}

///
/// Record
///

impl Imp<Record> for FieldValueTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(field_value_strategy(
            node.def(),
            record_field_value_tokens(node),
        ))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for FieldValueTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        Some(field_value_strategy(
            node.def(),
            tuple_field_value_tokens(node),
        ))
    }
}

fn field_value_strategy(def: &Def, tokens: TokenStream) -> TraitStrategy {
    TraitStrategy::from_impl(field_value_impl_tokens(def, tokens))
}
