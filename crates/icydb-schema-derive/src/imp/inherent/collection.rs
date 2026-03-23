use crate::{
    imp::inherent::{
        InherentTrait,
        model::{model_kind_from_item, model_kind_from_nested_value},
    },
    prelude::*,
};

///
/// List
///

impl Imp<List> for InherentTrait {
    fn strategy(node: &List) -> Option<TraitStrategy> {
        let item_kind = model_kind_from_item(&node.item);
        let kind = quote!(::icydb::model::field::FieldKind::List(&#item_kind));
        let inherent_impl = collection_internal_metadata_impl_tokens(node.def(), kind.clone());
        let meta_impl = collection_field_type_meta_impl_tokens(node.def());

        Some(TraitStrategy::from_impl(quote! {
            #inherent_impl
            #meta_impl
        }))
    }
}

///
/// Set
///

impl Imp<Set> for InherentTrait {
    fn strategy(node: &Set) -> Option<TraitStrategy> {
        let item_kind = model_kind_from_item(&node.item);
        let kind = quote!(::icydb::model::field::FieldKind::Set(&#item_kind));
        let inherent_impl = collection_internal_metadata_impl_tokens(node.def(), kind.clone());
        let meta_impl = collection_field_type_meta_impl_tokens(node.def());

        Some(TraitStrategy::from_impl(quote! {
            #inherent_impl
            #meta_impl
        }))
    }
}

///
/// Map
///

impl Imp<Map> for InherentTrait {
    fn strategy(node: &Map) -> Option<TraitStrategy> {
        let key_kind = model_kind_from_item(&node.key);
        let value_kind = model_kind_from_nested_value(&node.value);
        let kind = quote! {
            ::icydb::model::field::FieldKind::Map {
                key: &#key_kind,
                value: &#value_kind,
            }
        };
        let inherent_impl = collection_internal_metadata_impl_tokens(node.def(), kind.clone());
        let meta_impl = collection_field_type_meta_impl_tokens(node.def());

        Some(TraitStrategy::from_impl(quote! {
            #inherent_impl
            #meta_impl
        }))
    }
}

// Collection wrappers share one static metadata contract even though their
// runtime behavior is now owned by the underlying collection itself.
fn collection_field_type_meta_impl_tokens(def: &Def) -> TokenStream {
    Implementor::new(def, TraitKind::FieldTypeMeta)
        .set_tokens(quote! {
            const KIND: ::icydb::model::field::FieldKind = Self::__KIND;
            const STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                Self::__STORAGE_DECODE;
        })
        .to_token_stream()
}

// Keep generated metadata available to local model assembly without exposing it
// as public ergonomic surface.
fn collection_internal_metadata_impl_tokens(def: &Def, kind: TokenStream) -> TokenStream {
    Implementor::new(def, TraitKind::Inherent)
        .set_tokens(quote! {
            pub(crate) const __KIND: ::icydb::model::field::FieldKind = #kind;
            pub(crate) const __STORAGE_DECODE: ::icydb::model::field::FieldStorageDecode =
                ::icydb::model::field::FieldStorageDecode::ByKind;
        })
        .to_token_stream()
}
