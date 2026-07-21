//! Module: imp::inherent
//! Responsibility: generated implementation tokens.
//! Does not own: runtime trait semantics.
//! Boundary: parsed nodes to impl tokens.

mod collection;
mod entity;
mod model;

use crate::prelude::*;

pub(crate) use model::{
    composite_element_model_expr, composite_field_model_expr, composite_newtype_inner_model_expr,
    model_field_expr, model_kind_from_value, model_storage_decode_from_value,
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
        let variant_name_consts = enum_variant_name_const_tokens(node);
        let payload_kind_resolvers = enum_variant_payload_kind_resolver_tokens(node);
        let variants = enum_variant_model_tokens(node);
        let inherent_tokens = quote! {
            #(#variant_name_consts)*
            #(#payload_kind_resolvers)*
            pub(crate) const __VARIANTS: &'static [::icydb::model::field::EnumVariantModel] = &[
                #(#variants),*
            ];
            pub(crate) const __KIND: ::icydb::model::field::FieldKind =
                ::icydb::model::field::FieldKind::Enum {
                    path: Self::PATH,
                    variants: Self::__VARIANTS,
                };
            pub(crate) const __STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                ::icydb::model::field::FieldStorageDecode::CatalogValue;
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

fn enum_variant_name_const_tokens(node: &Enum) -> Vec<TokenStream> {
    node.variants
        .iter()
        .map(|variant| {
            let const_ident = variant.name_const_ident();
            let variant_name = variant.ident.to_string();

            quote! {
                pub const #const_ident: &'static str = #variant_name;
            }
        })
        .collect()
}

fn enum_variant_model_tokens(node: &Enum) -> Vec<TokenStream> {
    node.variants
        .iter()
        .enumerate()
        .map(|(index, variant)| {
            let ident = variant.name_const_ident();
            let payload_kind = enum_variant_payload_kind_tokens(index, variant.value.as_ref());
            let payload_storage_decode =
                enum_variant_payload_storage_decode_tokens(variant.value.as_ref());

            quote!(::icydb::model::field::EnumVariantModel::generated_with_payload_kind_resolver(
                Self::#ident,
                #payload_kind,
                #payload_storage_decode,
            ))
        })
        .collect()
}

fn enum_variant_payload_kind_tokens(index: usize, value: Option<&Value>) -> TokenStream {
    if value.is_some() {
        let resolver = format_ident!("__icydb_enum_payload_kind_{index}");
        quote!(Some(Self::#resolver))
    } else {
        quote!(None)
    }
}

fn enum_variant_payload_kind_resolver_tokens(node: &Enum) -> Vec<TokenStream> {
    node.variants
        .iter()
        .enumerate()
        .filter_map(|(index, variant)| {
            let value = variant.value.as_ref()?;
            let resolver = format_ident!("__icydb_enum_payload_kind_{index}");
            let kind = model_kind_from_value(value);
            Some(quote! {
                fn #resolver() -> ::icydb::model::field::FieldKind {
                    #kind
                }
            })
        })
        .collect()
}

fn enum_variant_payload_storage_decode_tokens(value: Option<&Value>) -> TokenStream {
    if let Some(value) = value {
        model_storage_decode_from_value(value)
    } else {
        quote!(::icydb::model::field::FieldStorageDecode::ByKind)
    }
}

///
/// Newtype
///

impl Imp<Newtype> for InherentTrait {
    fn strategy(node: &Newtype) -> Option<TraitStrategy> {
        let inner = composite_newtype_inner_model_expr(&node.item);
        let inherent_impl = inherent_impl_tokens(
            node.def(),
            quote! {
                pub(crate) const __COMPOSITE_SHAPE: ::icydb::model::field::CompositeShapeModel =
                    ::icydb::model::field::CompositeShapeModel::Newtype(#inner);
                pub(crate) const __KIND: ::icydb::model::field::FieldKind =
                    ::icydb::model::field::FieldKind::Composite {
                        path: Self::PATH,
                        codec: ::icydb::model::field::CompositeCodec::StructuralV1,
                        shape: &Self::__COMPOSITE_SHAPE,
                    };
                pub(crate) const __STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                    ::icydb::model::field::FieldStorageDecode::CatalogValue;
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
        let nested_fields = node.fields.iter().map(model_field_expr).collect::<Vec<_>>();
        let composite_fields = node
            .fields
            .iter()
            .map(composite_field_model_expr)
            .collect::<Vec<_>>();
        let inherent_impl = inherent_impl_tokens(
            node.def(),
            quote! {
                pub(crate) const __COMPOSITE_FIELDS: &'static [::icydb::model::field::CompositeFieldModel] = &[
                    #(#composite_fields),*
                ];
                pub(crate) const __COMPOSITE_SHAPE: ::icydb::model::field::CompositeShapeModel =
                    ::icydb::model::field::CompositeShapeModel::Record(
                        Self::__COMPOSITE_FIELDS,
                    );
                pub(crate) const __KIND: ::icydb::model::field::FieldKind =
                    ::icydb::model::field::FieldKind::Composite {
                        path: Self::PATH,
                        codec: ::icydb::model::field::CompositeCodec::StructuralV1,
                        shape: &Self::__COMPOSITE_SHAPE,
                    };
                pub(crate) const __STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                    ::icydb::model::field::FieldStorageDecode::CatalogValue;
            },
        );
        let meta_impl = field_type_meta_impl_tokens_with_nested_fields(
            node.def(),
            quote!(Self::__KIND),
            quote!(Self::__STORAGE_DECODE),
            quote!(&[#(#nested_fields),*]),
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
        let elements = node
            .values
            .iter()
            .map(composite_element_model_expr)
            .collect::<Vec<_>>();
        let inherent_impl = inherent_impl_tokens(
            node.def(),
            quote! {
                pub(crate) const __COMPOSITE_ELEMENTS: &'static [::icydb::model::field::CompositeElementModel] = &[
                    #(#elements),*
                ];
                pub(crate) const __COMPOSITE_SHAPE: ::icydb::model::field::CompositeShapeModel =
                    ::icydb::model::field::CompositeShapeModel::Tuple(
                        Self::__COMPOSITE_ELEMENTS,
                    );
                pub(crate) const __KIND: ::icydb::model::field::FieldKind =
                    ::icydb::model::field::FieldKind::Composite {
                        path: Self::PATH,
                        codec: ::icydb::model::field::CompositeCodec::StructuralV1,
                        shape: &Self::__COMPOSITE_SHAPE,
                    };
                pub(crate) const __STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                    ::icydb::model::field::FieldStorageDecode::CatalogValue;
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
    field_type_meta_impl_tokens_with_nested_fields(def, kind, storage_decode, quote!(&[]))
}

fn field_type_meta_impl_tokens_with_nested_fields(
    def: &Def,
    kind: TokenStream,
    storage_decode: TokenStream,
    nested_fields: TokenStream,
) -> TokenStream {
    Implementor::new(def, TraitKind::FieldTypeMeta)
        .set_tokens(quote! {
            const KIND: ::icydb::model::field::FieldKind = #kind;
            const STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                #storage_decode;
            const NESTED_FIELDS: &'static [::icydb::model::field::FieldModel] = #nested_fields;
        })
        .to_token_stream()
}
