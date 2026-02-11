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
                        let payload = v.payload.as_deref()?;
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
                if let Some(path) = &v.path
                    && path != <Self as ::icydb::traits::Path>::PATH
                {
                    return None;
                }

                match v.variant.as_str() {
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
                use ::icydb::traits::Collection;

                ::icydb::value::Value::List(
                    self.iter()
                        .map(::icydb::traits::FieldValue::to_value)
                        .collect()
                )
            }

            fn from_value(value: &::icydb::value::Value) -> Option<Self> {
                let ::icydb::value::Value::List(values) = value else {
                    return None;
                };

                let mut out = ::std::vec::Vec::<#item>::with_capacity(values.len());
                for value in values {
                    out.push(<#item as ::icydb::traits::FieldValue>::from_value(value)?);
                }

                Some(Self(out))
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
                let mut entries: ::std::vec::Vec<(
                    ::icydb::value::Value,
                    ::icydb::value::Value,
                )> = self
                    .0
                    .iter()
                    .map(|(key, value)| {
                        (
                            ::icydb::traits::FieldValue::to_value(key),
                            ::icydb::traits::FieldValue::to_value(value),
                        )
                    })
                    .collect();

                if let Err(err) = ::icydb::value::Value::validate_map_entries(entries.as_slice()) {
                    debug_assert!(
                        false,
                        "invalid map field value for {}: {err}",
                        <Self as ::icydb::traits::Path>::PATH,
                    );
                    return ::icydb::value::Value::Map(entries);
                }

                entries.sort_by(|(left_key, _), (right_key, _)| {
                    ::icydb::value::Value::canonical_cmp_key(left_key, right_key)
                });

                for i in 1..entries.len() {
                    let (left_key, _) = &entries[i - 1];
                    let (right_key, _) = &entries[i];
                    if ::icydb::value::Value::canonical_cmp_key(left_key, right_key)
                        == ::std::cmp::Ordering::Equal
                    {
                        debug_assert!(
                            false,
                            "duplicate map key in {} after FieldValue::to_value canonicalization",
                            <Self as ::icydb::traits::Path>::PATH,
                        );
                        break;
                    }
                }

                ::icydb::value::Value::Map(entries)
            }

            fn from_value(value: &::icydb::value::Value) -> Option<Self> {
                let ::icydb::value::Value::Map(entries) = value else {
                    return None;
                };

                let normalized = ::icydb::value::Value::normalize_map_entries(entries.clone()).ok()?;
                if normalized.as_slice() != entries.as_slice() {
                    return None;
                }

                let mut map = ::std::collections::BTreeMap::<#key_type, #value_type>::new();
                for (entry_key, entry_value) in normalized {
                    let key = <#key_type as ::icydb::traits::FieldValue>::from_value(&entry_key)?;
                    let value =
                        <#value_type as ::icydb::traits::FieldValue>::from_value(&entry_value)?;

                    map.insert(key, value);
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
                use ::icydb::traits::Collection;

                ::icydb::value::Value::List(
                    self.iter()
                        .map(::icydb::traits::FieldValue::to_value)
                        .collect()
                )
            }

            fn from_value(value: &::icydb::value::Value) -> Option<Self> {
                let ::icydb::value::Value::List(values) = value else {
                    return None;
                };

                let mut out = ::std::collections::BTreeSet::<#item>::new();
                for value in values {
                    let item = <#item as ::icydb::traits::FieldValue>::from_value(value)?;
                    if !out.insert(item) {
                        return None;
                    }
                }

                Some(Self(out))
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
