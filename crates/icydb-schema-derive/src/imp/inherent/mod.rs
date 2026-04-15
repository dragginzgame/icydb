mod collection;
mod entity;
mod model;

use crate::prelude::*;

pub(crate) use model::{
    model_kind_from_item, model_kind_from_value, model_storage_decode_from_value,
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
        let inherent_tokens = quote! {
            pub(crate) const __VARIANTS: &'static [::icydb::model::field::EnumVariantModel] = &[
                #(#variants),*
            ];
            pub(crate) const __KIND: ::icydb::model::field::FieldKind =
                ::icydb::model::field::FieldKind::Enum {
                    path: Self::PATH,
                    variants: Self::__VARIANTS,
                };
            pub(crate) const __STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                ::icydb::model::field::FieldStorageDecode::Value;
        };
        let meta_impl = field_type_meta_impl_tokens(
            node.def(),
            quote!(Self::__KIND),
            quote!(Self::__STORAGE_DECODE),
        );
        let inherent_impl = inherent_impl_tokens(node.def(), inherent_tokens);

        Some(TraitStrategy::from_impl(quote! {
            #meta_impl
            #inherent_impl
        }))
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
        let inherent_impl = inherent_impl_tokens(
            node.def(),
            quote! {
                pub(crate) const __KIND: ::icydb::model::field::FieldKind = #kind;
                pub(crate) const __STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                    ::icydb::model::field::FieldStorageDecode::Value;
            },
        );
        let meta_impl = field_type_meta_impl_tokens(
            node.def(),
            quote!(Self::__KIND),
            quote!(Self::__STORAGE_DECODE),
        );

        Some(TraitStrategy::from_impl(quote! {
            #inherent_impl
            #meta_impl
        }))
    }
}

///
/// Record
///

impl Imp<Record> for InherentTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let inherent_impl = inherent_impl_tokens(
            node.def(),
            quote! {
                pub(crate) const __KIND: ::icydb::model::field::FieldKind =
                    ::icydb::model::field::FieldKind::Structured { queryable: false };
                pub(crate) const __STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                    ::icydb::model::field::FieldStorageDecode::Value;
            },
        );
        let meta_impl = field_type_meta_impl_tokens(
            node.def(),
            quote!(Self::__KIND),
            quote!(Self::__STORAGE_DECODE),
        );

        Some(TraitStrategy::from_impl(quote! {
            #inherent_impl
            #meta_impl
        }))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for InherentTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let inherent_impl = inherent_impl_tokens(
            node.def(),
            quote! {
                pub(crate) const __KIND: ::icydb::model::field::FieldKind =
                    ::icydb::model::field::FieldKind::Structured { queryable: false };
                pub(crate) const __STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                    ::icydb::model::field::FieldStorageDecode::Value;
            },
        );
        let meta_impl = field_type_meta_impl_tokens(
            node.def(),
            quote!(Self::__KIND),
            quote!(Self::__STORAGE_DECODE),
        );

        Some(TraitStrategy::from_impl(quote! {
            #inherent_impl
            #meta_impl
        }))
    }
}

fn inherent_impl_tokens(def: &Def, tokens: TokenStream) -> TokenStream {
    Implementor::new(def, TraitKind::Inherent)
        .set_tokens(tokens)
        .to_token_stream()
}

// Emit the shared type-metadata impl so generated model assembly reads through
// one trait boundary instead of per-type inherent constants.
fn field_type_meta_impl_tokens(
    def: &Def,
    kind: TokenStream,
    storage_decode: TokenStream,
) -> TokenStream {
    Implementor::new(def, TraitKind::FieldTypeMeta)
        .set_tokens(quote! {
            const KIND: ::icydb::model::field::FieldKind = #kind;
            const STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                #storage_decode;
        })
        .to_token_stream()
}
