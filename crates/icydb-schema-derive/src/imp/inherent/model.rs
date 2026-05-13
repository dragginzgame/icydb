use crate::{
    node::{Field, Item, ItemTarget, Value},
    prelude::quote_option,
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

/// Returns one runtime `FieldModel` expression for a generated field.
///
/// This keeps the field-kind lowering logic in one derive-side owner so entity
/// model generation does not duplicate field metadata assembly inline.
pub fn model_field_expr(field: &Field) -> TokenStream {
    let name = field.ident.to_string();
    let kind = model_kind_from_value(&field.value);
    let storage_decode = model_storage_decode_from_value(&field.value);
    let nested_fields = model_nested_fields_from_value(&field.value);
    let nullable = matches!(field.value.cardinality(), Cardinality::Opt);
    let insert_generation = field.insert_generation_expr();
    let write_management = field.write_management_expr();
    let database_default = field.database_default_expr();

    quote!(::icydb::model::field::FieldModel::generated_with_storage_decode_nullability_write_policies_database_default_and_nested_fields(
        #name,
        #kind,
        #storage_decode,
        #nullable,
        #insert_generation,
        #write_management,
        #database_default,
        #nested_fields,
    ))
}

/// Returns the persisted model kind for a nested value (e.g. map values).
pub fn model_kind_from_nested_value(value: &Value) -> TokenStream {
    model_kind_from_value(value)
}

/// Returns the persisted field decode contract for a value.
pub fn model_storage_decode_from_value(value: &Value) -> TokenStream {
    model_storage_decode_from_item(&value.item)
}

/// Returns nested field metadata for generated record items.
pub fn model_nested_fields_from_value(value: &Value) -> TokenStream {
    if matches!(value.cardinality(), Cardinality::Many) {
        return quote!(&[]);
    }

    match value.item.target() {
        ItemTarget::Primitive(_) => quote!(&[]),
        ItemTarget::Is(path) => quote!(<#path as ::icydb::traits::FieldTypeMeta>::NESTED_FIELDS),
    }
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
            target_entity_name: <#target as ::icydb::traits::EntitySchema>::NAME,
            target_entity_tag: <#target as ::icydb::traits::EntityKind>::ENTITY_TAG,
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
        ItemTarget::Primitive(prim) => {
            // Decimal scale and text length are validated by `Item::validate`.
            let decimal_scale = item.scale.unwrap_or(0);
            model_kind_from_primitive(prim, decimal_scale, item.max_len)
        }
        ItemTarget::Is(path) => quote!(<#path as ::icydb::traits::FieldTypeMeta>::KIND),
    }
}

/// Returns the persisted structural decode contract for an item.
fn model_storage_decode_from_item(item: &Item) -> TokenStream {
    match item.target() {
        ItemTarget::Primitive(_) => quote!(::icydb::model::field::FieldStorageDecode::ByKind),
        ItemTarget::Is(path) => {
            quote!(<#path as ::icydb::traits::FieldTypeMeta>::STORAGE_DECODE)
        }
    }
}

/// Returns the persisted model kind for a primitive type.
pub fn model_kind_from_primitive(
    prim: Primitive,
    decimal_scale: u32,
    max_len: Option<u32>,
) -> TokenStream {
    match prim {
        Primitive::Account => quote!(::icydb::model::field::FieldKind::Account),
        Primitive::Blob => {
            let max_len = quote_option(max_len.as_ref(), |max_len| quote!(#max_len));
            quote!(::icydb::model::field::FieldKind::Blob { max_len: #max_len })
        }
        Primitive::Bool => quote!(::icydb::model::field::FieldKind::Bool),
        Primitive::Date => quote!(::icydb::model::field::FieldKind::Date),
        Primitive::Decimal => {
            quote!(::icydb::model::field::FieldKind::Decimal { scale: #decimal_scale })
        }
        Primitive::Duration => quote!(::icydb::model::field::FieldKind::Duration),
        Primitive::Float32 => quote!(::icydb::model::field::FieldKind::Float32),
        Primitive::Float64 => quote!(::icydb::model::field::FieldKind::Float64),
        Primitive::Int => quote!(::icydb::model::field::FieldKind::IntBig),
        Primitive::Int8 | Primitive::Int16 | Primitive::Int32 | Primitive::Int64 => {
            quote!(::icydb::model::field::FieldKind::Int)
        }
        Primitive::Int128 => quote!(::icydb::model::field::FieldKind::Int128),
        Primitive::Nat => quote!(::icydb::model::field::FieldKind::NatBig),
        Primitive::Nat8 | Primitive::Nat16 | Primitive::Nat32 | Primitive::Nat64 => {
            quote!(::icydb::model::field::FieldKind::Nat)
        }
        Primitive::Nat128 => quote!(::icydb::model::field::FieldKind::Nat128),
        Primitive::Principal => quote!(::icydb::model::field::FieldKind::Principal),
        Primitive::Subaccount => quote!(::icydb::model::field::FieldKind::Subaccount),
        Primitive::Text => {
            let max_len = quote_option(max_len.as_ref(), |max_len| quote!(#max_len));
            quote!(::icydb::model::field::FieldKind::Text { max_len: #max_len })
        }
        Primitive::Timestamp => quote!(::icydb::model::field::FieldKind::Timestamp),
        Primitive::Ulid => quote!(::icydb::model::field::FieldKind::Ulid),
        Primitive::Unit => quote!(::icydb::model::field::FieldKind::Unit),
    }
}
