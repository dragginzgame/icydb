use crate::node::{Item, ItemTarget, Value};
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
/// Relation items emit `EntityFieldKind::Relation` metadata while preserving
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
        ::icydb::model::field::EntityFieldKind::Relation {
            target_path: <#target as ::icydb::traits::Path>::PATH,
            target_entity_name: <#target as ::icydb::traits::EntityIdentity>::ENTITY_NAME,
            target_store_path:
                <<#target as ::icydb::traits::EntityPlacement>::DataStore as ::icydb::traits::Path>::PATH,
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
