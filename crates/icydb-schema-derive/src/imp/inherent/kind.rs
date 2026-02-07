use crate::node::{Item, ItemTarget, Value};
use icydb_schema::types::{Cardinality, Primitive};
use proc_macro2::TokenStream;
use quote::quote;

/// Returns the persisted model kind for a value.
///
/// This reflects the *storage shape* only.
/// Relations and identity semantics are not represented here.
pub fn model_kind_from_value(value: &Value) -> TokenStream {
    let base = model_kind_from_item(&value.item);
    match value.cardinality() {
        Cardinality::Many => quote!(::icydb::model::field::EntityFieldKind::List(&#base)),
        Cardinality::One | Cardinality::Opt => base,
    }
}

/// Returns the persisted model kind for a nested value (e.g. map values).
pub fn model_kind_from_nested_value(value: &Value) -> TokenStream {
    model_kind_from_value(value)
}

/// Returns the persisted model kind for an item.
///
/// This function must not inspect relations or identity metadata.
/// It reflects only the declared target type.
pub fn model_kind_from_item(item: &Item) -> TokenStream {
    match item.target() {
        ItemTarget::Primitive(prim) => model_kind_from_primitive(prim),
        ItemTarget::Is(path) => quote!(#path::KIND),
    }
}

/// Returns the persisted model kind for a primitive type.
pub fn model_kind_from_primitive(prim: Primitive) -> TokenStream {
    match prim {
        Primitive::Account => quote!(::icydb::model::field::EntityFieldKind::Account),
        Primitive::Blob => quote!(::icydb::model::field::EntityFieldKind::Blob),
        Primitive::Bool => quote!(::icydb::model::field::EntityFieldKind::Bool),
        Primitive::Date => quote!(::icydb::model::field::EntityFieldKind::Date),
        Primitive::Decimal => quote!(::icydb::model::field::EntityFieldKind::Decimal),
        Primitive::Duration => quote!(::icydb::model::field::EntityFieldKind::Duration),
        Primitive::E8s => quote!(::icydb::model::field::EntityFieldKind::E8s),
        Primitive::E18s => quote!(::icydb::model::field::EntityFieldKind::E18s),
        Primitive::Float32 => quote!(::icydb::model::field::EntityFieldKind::Float32),
        Primitive::Float64 => quote!(::icydb::model::field::EntityFieldKind::Float64),
        Primitive::Int => quote!(::icydb::model::field::EntityFieldKind::IntBig),
        Primitive::Int8 | Primitive::Int16 | Primitive::Int32 | Primitive::Int64 => {
            quote!(::icydb::model::field::EntityFieldKind::Int)
        }
        Primitive::Int128 => quote!(::icydb::model::field::EntityFieldKind::Int128),
        Primitive::Nat => quote!(::icydb::model::field::EntityFieldKind::UintBig),
        Primitive::Nat8 | Primitive::Nat16 | Primitive::Nat32 | Primitive::Nat64 => {
            quote!(::icydb::model::field::EntityFieldKind::Uint)
        }
        Primitive::Nat128 => quote!(::icydb::model::field::EntityFieldKind::Uint128),
        Primitive::Principal => quote!(::icydb::model::field::EntityFieldKind::Principal),
        Primitive::Subaccount => quote!(::icydb::model::field::EntityFieldKind::Subaccount),
        Primitive::Text => quote!(::icydb::model::field::EntityFieldKind::Text),
        Primitive::Timestamp => quote!(::icydb::model::field::EntityFieldKind::Timestamp),
        Primitive::Ulid => quote!(::icydb::model::field::EntityFieldKind::Ulid),
        Primitive::Unit => quote!(::icydb::model::field::EntityFieldKind::Unit),
    }
}
