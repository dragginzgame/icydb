mod collection;
mod entity;
mod model;
mod relation;

use crate::{
    imp::inherent::{
        model::{model_kind_from_item, model_kind_from_value, model_storage_decode_from_value},
        relation::relation_accessor_tokens,
    },
    prelude::*,
};

///
/// InherentTrait
///

pub struct InherentTrait {}

///
/// Enum
///

impl Imp<Enum> for InherentTrait {
    fn strategy(node: &Enum) -> Option<TraitStrategy> {
        let variants = enum_variant_model_tokens(node);
        let tokens = quote! {
            pub const VARIANTS: &'static [::icydb::model::field::EnumVariantModel] = &[
                #(#variants),*
            ];
            pub const KIND: ::icydb::model::field::FieldKind =
                ::icydb::model::field::FieldKind::Enum {
                    path: Self::PATH,
                    variants: Self::VARIANTS,
                };
            pub const STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                ::icydb::model::field::FieldStorageDecode::ByKind;
        };

        Some(TraitStrategy::from_impl(inherent_impl_tokens(
            node.def(),
            tokens,
        )))
    }
}

fn enum_variant_model_tokens(node: &Enum) -> Vec<TokenStream> {
    node.variants
        .iter()
        .map(|variant| {
            let ident = variant.ident.to_string();
            let payload_kind = enum_variant_payload_kind_tokens(variant.value.as_ref());
            let payload_storage_decode =
                enum_variant_payload_storage_decode_tokens(variant.value.as_ref());

            quote!(::icydb::model::field::EnumVariantModel::new(
                #ident,
                #payload_kind,
                #payload_storage_decode,
            ))
        })
        .collect()
}

fn enum_variant_payload_kind_tokens(value: Option<&Value>) -> TokenStream {
    if let Some(value) = value {
        if enum_payload_supports_structural_descriptor(value) {
            let kind = model_kind_from_value(value);
            quote!(Some(&#kind))
        } else {
            quote!(None)
        }
    } else {
        quote!(None)
    }
}

fn enum_variant_payload_storage_decode_tokens(value: Option<&Value>) -> TokenStream {
    if let Some(value) = value {
        model_storage_decode_from_value(value)
    } else {
        quote!(::icydb::model::field::FieldStorageDecode::ByKind)
    }
}

// Keep enum payload structural metadata conservative so generated const tables
// do not form recursive `KIND` cycles for indirect or wrapper-owned payloads.
fn enum_payload_supports_structural_descriptor(value: &Value) -> bool {
    if value.opt || value.many || value.item.indirect || value.item.relation.is_some() {
        return false;
    }

    match value.item.target() {
        crate::node::ItemTarget::Primitive(_) => true,
        crate::node::ItemTarget::Is(path) => path.segments.len() == 1,
    }
}

///
/// Newtype
///

impl Imp<Newtype> for InherentTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let kind = model_kind_from_item(&node.item);
        let arithmetic_tokens = newtype_arithmetic_tokens(node);
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::FieldKind = #kind;
            pub const STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                ::icydb::model::field::FieldStorageDecode::ByKind;
            #arithmetic_tokens
        };

        Some(TraitStrategy::from_impl(inherent_impl_tokens(
            node.def(),
            tokens,
        )))
    }
}

fn newtype_arithmetic_tokens(node: &Newtype) -> TokenStream {
    if let Some(primitive) = node.primitive
        && primitive.supports_arithmetic()
    {
        quote! {
            /// Saturating addition.
            #[must_use]
            pub fn saturating_add(self, rhs: Self) -> Self {
                Self(self.0.saturating_add(rhs.0))
            }

            /// Saturating subtraction.
            #[must_use]
            pub fn saturating_sub(self, rhs: Self) -> Self {
                Self(self.0.saturating_sub(rhs.0))
            }
        }
    } else {
        TokenStream::new()
    }
}

///
/// Record
///

impl Imp<Record> for InherentTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let relation_accessors = relation_accessor_tokens(node.fields.iter());
        let tokens = structured_inherent_tokens(quote! {
            #(#relation_accessors)*
        });

        Some(TraitStrategy::from_impl(inherent_impl_tokens(
            node.def(),
            tokens,
        )))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for InherentTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        Some(TraitStrategy::from_impl(inherent_impl_tokens(
            node.def(),
            structured_inherent_tokens(TokenStream::new()),
        )))
    }
}

fn structured_inherent_tokens(extra_tokens: TokenStream) -> TokenStream {
    quote! {
        pub const KIND: ::icydb::model::field::FieldKind =
            ::icydb::model::field::FieldKind::Structured { queryable: false };
        pub const STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
            ::icydb::model::field::FieldStorageDecode::ByKind;
        #extra_tokens
    }
}

fn inherent_impl_tokens(def: &Def, tokens: TokenStream) -> TokenStream {
    Implementor::new(def, TraitKind::Inherent)
        .set_tokens(tokens)
        .to_token_stream()
}
