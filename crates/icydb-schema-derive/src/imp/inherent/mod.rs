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
        let variants = node.variants.iter().map(|variant| {
            let ident = variant.ident.to_string();
            let payload_kind = if let Some(value) = &variant.value {
                if enum_payload_supports_structural_descriptor(value) {
                    let kind = model_kind_from_value(value);
                    quote!(Some(&#kind))
                } else {
                    quote!(None)
                }
            } else {
                quote!(None)
            };
            let payload_storage_decode = if let Some(value) = &variant.value {
                model_storage_decode_from_value(value)
            } else {
                quote!(::icydb::model::field::FieldStorageDecode::ByKind)
            };

            quote!(::icydb::model::field::EnumVariantModel::new(
                #ident,
                #payload_kind,
                #payload_storage_decode,
            ))
        });

        let kind = quote!(::icydb::model::field::FieldKind::Enum {
            path: Self::PATH,
            variants: Self::VARIANTS,
        });
        let tokens = quote! {
            pub const VARIANTS: &'static [::icydb::model::field::EnumVariantModel] = &[
                #(#variants),*
            ];
            pub const KIND: ::icydb::model::field::FieldKind = #kind;
            pub const STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                ::icydb::model::field::FieldStorageDecode::ByKind;
        };

        Some(TraitStrategy::from_impl(
            Implementor::new(node.def(), TraitKind::Inherent)
                .set_tokens(tokens)
                .to_token_stream(),
        ))
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
        let mut tokens = quote! {
            pub const KIND: ::icydb::model::field::FieldKind = #kind;
            pub const STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                ::icydb::model::field::FieldStorageDecode::ByKind;
        };

        if let Some(primitive) = node.primitive
            && primitive.supports_arithmetic()
        {
            tokens = quote! {
                #tokens

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
            };
        }

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Record
///

impl Imp<Record> for InherentTrait {
    fn strategy(node: &Record) -> Option<TraitStrategy> {
        let kind = quote!(::icydb::model::field::FieldKind::Structured { queryable: false });
        let relation_accessors = relation_accessor_tokens(node.fields.iter());

        let tokens = quote! {
            pub const KIND: ::icydb::model::field::FieldKind = #kind;
            pub const STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                ::icydb::model::field::FieldStorageDecode::ByKind;
            #(#relation_accessors)*
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

///
/// Tuple
///

impl Imp<Tuple> for InherentTrait {
    fn strategy(node: &Tuple) -> Option<TraitStrategy> {
        let kind = quote!(::icydb::model::field::FieldKind::Structured { queryable: false });
        let tokens = quote! {
            pub const KIND: ::icydb::model::field::FieldKind = #kind;
            pub const STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                ::icydb::model::field::FieldStorageDecode::ByKind;
        };

        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
