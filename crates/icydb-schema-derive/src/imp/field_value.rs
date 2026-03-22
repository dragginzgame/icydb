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
            fn to_value_enum(&self) -> ::icydb::value::ValueEnum {
                use ::icydb::value::ValueEnum;

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
                quote!(.with_payload(::icydb::traits::FieldValue::to_value(v)))
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
        fn kind() -> ::icydb::traits::FieldValueKind {
            ::icydb::traits::FieldValueKind::Atomic
        }

        fn to_value(&self) -> ::icydb::value::Value {
            ::icydb::value::Value::Enum(::icydb::traits::EnumValue::to_value_enum(self))
        }

        fn from_value(value: &::icydb::value::Value) -> Option<Self> {
            let ::icydb::value::Value::Enum(v) = value else {
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
                            <#payload_ty as ::icydb::traits::FieldValue>::from_value(payload)?;
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
        fn kind() -> ::icydb::traits::FieldValueKind {
            #kind
        }

        fn to_value(&self) -> ::icydb::value::Value {
            #to_value
        }

        fn from_value(value: &::icydb::value::Value) -> Option<Self> {
            #from_value
        }
    }
}

fn opaque_structured_field_value_tokens() -> TokenStream {
    structured_collection_field_value_tokens(
        quote!(::icydb::traits::FieldValueKind::Structured { queryable: false }),
        quote!(::icydb::value::Value::Null),
        quote!(None),
    )
}

fn newtype_field_value_tokens(item: &TokenStream) -> TokenStream {
    quote! {
        fn kind() -> ::icydb::traits::FieldValueKind {
            <#item as ::icydb::traits::FieldValue>::kind()
        }

        fn to_value(&self) -> ::icydb::value::Value {
            self.0.to_value()
        }

        fn from_value(value: &::icydb::value::Value) -> Option<Self> {
            let inner = <#item as ::icydb::traits::FieldValue>::from_value(value)?;
            Some(Self(inner))
        }
    }
}

///
/// List
///

impl Imp<List> for FieldValueTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let tokens = structured_collection_field_value_tokens(
            quote!(::icydb::traits::FieldValueKind::Structured { queryable: true }),
            quote!(::icydb::traits::field_value_collection_to_value(self)),
            quote!(::icydb::traits::field_value_vec_from_value::<#item>(value).map(Self)),
        );

        Some(TraitStrategy::from_impl(field_value_impl_tokens(
            node.def(),
            tokens,
        )))
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
            quote!(::icydb::traits::FieldValueKind::Structured { queryable: false }),
            quote!(::icydb::traits::field_value_map_collection_to_value(
                self,
                <Self as ::icydb::traits::Path>::PATH,
            )),
            quote!(
                ::icydb::traits::field_value_btree_map_from_value::<#key_type, #value_type>(value)
                    .map(Self)
            ),
        );

        Some(TraitStrategy::from_impl(field_value_impl_tokens(
            node.def(),
            tokens,
        )))
    }
}

///
/// Newtype
///

impl Imp<Newtype> for FieldValueTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let tokens = newtype_field_value_tokens(&item);

        Some(TraitStrategy::from_impl(field_value_impl_tokens(
            node.def(),
            tokens,
        )))
    }
}

///
/// Set
///

impl Imp<Set> for FieldValueTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item = node.item.type_expr();
        let tokens = structured_collection_field_value_tokens(
            quote!(::icydb::traits::FieldValueKind::Structured { queryable: true }),
            quote!(::icydb::traits::field_value_collection_to_value(self)),
            quote!(::icydb::traits::field_value_btree_set_from_value::<#item>(value).map(Self)),
        );

        Some(TraitStrategy::from_impl(field_value_impl_tokens(
            node.def(),
            tokens,
        )))
    }
}

///
/// Record
///

impl Imp<Record> for FieldValueTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        Some(TraitStrategy::from_impl(field_value_impl_tokens(
            node.def(),
            opaque_structured_field_value_tokens(),
        )))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for FieldValueTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        Some(TraitStrategy::from_impl(field_value_impl_tokens(
            node.def(),
            opaque_structured_field_value_tokens(),
        )))
    }
}
