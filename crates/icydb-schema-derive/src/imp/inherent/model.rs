use crate::{
    helper::quote_option,
    node::{Item, ItemTarget, Value},
};
use icydb_schema::types::{Cardinality, Primitive};
use proc_macro2::TokenStream;
use quote::quote;

/// Returns the persisted model kind for a value.
///
/// This preserves semantic field intent (relation vs primitive)
/// while keeping relation key representation storage-compatible.
pub fn model_kind_from_value(value: &Value) -> TokenStream {
    let base = model_kind_from_item(&value.item);
    match value.cardinality() {
        Cardinality::Many => quote!(::icydb::model::field::FieldKind::List(&#base)),
        Cardinality::One | Cardinality::Opt => base,
    }
}

/// Returns the persisted model kind for a nested value (e.g. map values).
pub fn model_kind_from_nested_value(value: &Value) -> TokenStream {
    model_kind_from_value(value)
}

/// Returns the persisted model kind for an item.
///
/// Relation items emit `FieldKind::Relation` metadata while preserving
/// the declared/derived storage key shape as `key_kind`.
pub fn model_kind_from_item(item: &Item) -> TokenStream {
    let key_kind = model_storage_kind_from_item(item);
    let Some(target) = &item.relation else {
        return key_kind;
    };

    let strength = if item.strong {
        quote!(::icydb::model::field::RelationStrength::Strong)
    } else if item.weak {
        quote!(::icydb::model::field::RelationStrength::Weak)
    } else {
        // Default relation strength is weak unless `strong` is explicitly set.
        quote!(::icydb::model::field::RelationStrength::Weak)
    };

    quote! {
        ::icydb::model::field::FieldKind::Relation {
            target_path: <#target as ::icydb::traits::Path>::PATH,
            target_entity_name: <#target as ::icydb::traits::EntityIdentity>::ENTITY_NAME,
            target_store_path:
                <<#target as ::icydb::traits::EntityPlacement>::Store as ::icydb::traits::Path>::PATH,
            key_kind: &#key_kind,
            strength: #strength,
        }
    }
}

/// Returns the persisted storage shape for an item.
///
/// This intentionally ignores relation semantics and reflects only the
/// underlying key representation used at persistence boundaries.
fn model_storage_kind_from_item(item: &Item) -> TokenStream {
    match item.target() {
        ItemTarget::Primitive(prim) => model_kind_from_primitive(prim, item.scale),
        ItemTarget::Is(path) => quote!(#path::KIND),
    }
}

/// Returns the persisted model kind for a primitive type.
pub fn model_kind_from_primitive(prim: Primitive, decimal_scale: Option<u32>) -> TokenStream {
    match prim {
        Primitive::Account => quote!(::icydb::model::field::FieldKind::Account),
        Primitive::Blob => quote!(::icydb::model::field::FieldKind::Blob),
        Primitive::Bool => quote!(::icydb::model::field::FieldKind::Bool),
        Primitive::Date => quote!(::icydb::model::field::FieldKind::Date),
        Primitive::Decimal => {
            let scale = quote_option(decimal_scale.as_ref(), |scale| quote!(#scale));
            quote!(::icydb::model::field::FieldKind::Decimal { scale: #scale })
        }
        Primitive::Duration => quote!(::icydb::model::field::FieldKind::Duration),
        Primitive::Float32 => quote!(::icydb::model::field::FieldKind::Float32),
        Primitive::Float64 => quote!(::icydb::model::field::FieldKind::Float64),
        Primitive::Int => quote!(::icydb::model::field::FieldKind::IntBig),
        Primitive::Int8 | Primitive::Int16 | Primitive::Int32 | Primitive::Int64 => {
            quote!(::icydb::model::field::FieldKind::Int)
        }
        Primitive::Int128 => quote!(::icydb::model::field::FieldKind::Int128),
        Primitive::Nat => quote!(::icydb::model::field::FieldKind::UintBig),
        Primitive::Nat8 | Primitive::Nat16 | Primitive::Nat32 | Primitive::Nat64 => {
            quote!(::icydb::model::field::FieldKind::Uint)
        }
        Primitive::Nat128 => quote!(::icydb::model::field::FieldKind::Uint128),
        Primitive::Principal => quote!(::icydb::model::field::FieldKind::Principal),
        Primitive::Subaccount => quote!(::icydb::model::field::FieldKind::Subaccount),
        Primitive::Text => quote!(::icydb::model::field::FieldKind::Text),
        Primitive::Timestamp => quote!(::icydb::model::field::FieldKind::Timestamp),
        Primitive::Ulid => quote!(::icydb::model::field::FieldKind::Ulid),
        Primitive::Unit => quote!(::icydb::model::field::FieldKind::Unit),
    }
}
