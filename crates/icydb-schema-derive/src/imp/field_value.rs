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

        let from_arms = node.variants.iter().map(|v| {
            let v_ident = &v.ident;
            let v_name = v_ident.to_string();

            if let Some(value) = &v.value {
                let payload_ty = value.type_expr();

                quote! {
                    #v_name => {
                        let payload = v.payload()?;
                        let value =
                            <#payload_ty as ::icydb::traits::FieldValue>::from_value(payload)?;
                        Some(Self::#v_ident(value))
                    }
                }
            } else {
                quote! {
                    #v_name => Some(Self::#v_ident)
                }
            }
        });

        let field_value = quote! {
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
        let item = node.item.type_expr();

        let q = quote! {
            fn kind() -> ::icydb::traits::FieldValueKind {
                ::icydb::traits::FieldValueKind::Structured { queryable: true }
            }

            fn to_value(&self) -> ::icydb::value::Value {
                ::icydb::traits::field_value_collection_to_value(self)
            }

            fn from_value(value: &::icydb::value::Value) -> Option<Self> {
                ::icydb::traits::field_value_vec_from_value::<#item>(value).map(Self)
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
            fn kind() -> ::icydb::traits::FieldValueKind {
                ::icydb::traits::FieldValueKind::Structured { queryable: false }
            }

            fn to_value(&self) -> ::icydb::value::Value {
                ::icydb::traits::field_value_map_collection_to_value(
                    self,
                    <Self as ::icydb::traits::Path>::PATH,
                )
            }

            fn from_value(value: &::icydb::value::Value) -> Option<Self> {
                ::icydb::traits::field_value_btree_map_from_value::<#key_type, #value_type>(value)
                    .map(Self)
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
        let item = node.item.type_expr();

        let q = quote! {
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
        let item = node.item.type_expr();

        let q = quote! {
            fn kind() -> ::icydb::traits::FieldValueKind {
                ::icydb::traits::FieldValueKind::Structured { queryable: true }
            }

            fn to_value(&self) -> ::icydb::value::Value {
                ::icydb::traits::field_value_collection_to_value(self)
            }

            fn from_value(value: &::icydb::value::Value) -> Option<Self> {
                ::icydb::traits::field_value_btree_set_from_value::<#item>(value).map(Self)
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::FieldValue)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Record
///

impl Imp<Record> for FieldValueTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let q = quote! {
            fn kind() -> ::icydb::traits::FieldValueKind {
                ::icydb::traits::FieldValueKind::Structured { queryable: false }
            }

            fn to_value(&self) -> ::icydb::value::Value {
                ::icydb::value::Value::Null
            }

            fn from_value(_value: &::icydb::value::Value) -> Option<Self> {
                None
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::FieldValue)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for FieldValueTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let q = quote! {
            fn kind() -> ::icydb::traits::FieldValueKind {
                ::icydb::traits::FieldValueKind::Structured { queryable: false }
            }

            fn to_value(&self) -> ::icydb::value::Value {
                ::icydb::value::Value::Null
            }

            fn from_value(_value: &::icydb::value::Value) -> Option<Self> {
                None
            }
        };

        let tokens = Implementor::new(node.def(), TraitKind::FieldValue)
            .set_tokens(q)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
