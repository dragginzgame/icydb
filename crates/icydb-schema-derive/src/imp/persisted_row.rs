use crate::prelude::*;
use syn::{
    AngleBracketedGenericArguments, GenericArgument, PathArguments, Type, parse2 as parse_type,
};

///
/// PersistedRowTrait
///

pub struct PersistedRowTrait {}

impl Imp<Entity> for PersistedRowTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let field_materializers = node.fields.iter().enumerate().map(|(slot, field)| {
            let slot = syn::Index::from(slot);
            let ident = &field.ident;
            let field_name = ident.to_string();

            let missing_expr = if field.default.is_some() {
                let expr = field.default_expr();
                quote!(#expr)
            } else if field.write_management.is_some() {
                quote!(Default::default())
            } else {
                match field.value.cardinality() {
                    Cardinality::Opt => quote!(None),
                    Cardinality::One | Cardinality::Many => quote! {
                        return Err(::icydb::db::InternalError::missing_persisted_slot(#field_name))
                    },
                }
            };
            let decode_expr = if field.value.item.is.is_some() {
                persisted_custom_field_decode_expr(field, field_name.as_str())
            } else {
                let field_ty = field.value.type_expr();
                persisted_field_decode_expr(&field_ty, field_name.as_str())
            };

            quote! {
                #ident: match slots.get_bytes(#slot) {
                    Some(bytes) => #decode_expr,
                    None => #missing_expr,
                }
            }
        });

        let slot_writes = node.fields.iter().enumerate().map(|(slot, field)| {
            let slot = syn::Index::from(slot);
            let ident = &field.ident;
            let field_name = ident.to_string();
            let encode_expr = if field.value.item.is.is_some() {
                persisted_custom_field_encode_expr(field, quote!(&self.#ident), field_name.as_str())
            } else {
                let field_ty = field.value.type_expr();
                persisted_field_encode_expr(&field_ty, quote!(&self.#ident), field_name.as_str())
            };

            quote! {
                let payload = #encode_expr;
                out.write_slot(#slot, Some(payload.as_slice()))?;
            }
        });

        let tokens = Implementor::new(node.def(), TraitKind::PersistedRow)
            .set_tokens(quote! {
                fn materialize_from_slots(
                    slots: &mut dyn ::icydb::db::SlotReader,
                ) -> Result<Self, ::icydb::db::InternalError> {
                    Ok(Self {
                        #(#field_materializers),*
                    })
                }

                fn write_slots(
                    &self,
                    out: &mut dyn ::icydb::db::SlotWriter,
                ) -> Result<(), ::icydb::db::InternalError> {
                    #(#slot_writes)*

                    Ok(())
                }
            })
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}

fn persisted_custom_field_decode_expr(field: &Field, field_name: &str) -> TokenStream {
    match field.value.cardinality() {
        Cardinality::One | Cardinality::Opt => {
            let field_ty = field.value.type_expr();
            quote!(
                ::icydb::db::decode_persisted_custom_slot_payload::<#field_ty>(
                    bytes,
                    #field_name,
                )?
            )
        }
        Cardinality::Many => {
            let item_ty = field.value.item.type_expr();
            quote!(
                ::icydb::db::decode_persisted_custom_many_slot_payload::<#item_ty>(
                    bytes,
                    #field_name,
                )?
            )
        }
    }
}

fn persisted_custom_field_encode_expr(
    field: &Field,
    field_expr: TokenStream,
    field_name: &str,
) -> TokenStream {
    match field.value.cardinality() {
        Cardinality::One | Cardinality::Opt => quote!(
            ::icydb::db::encode_persisted_custom_slot_payload(
                #field_expr,
                #field_name,
            )?
        ),
        Cardinality::Many => quote!(
            ::icydb::db::encode_persisted_custom_many_slot_payload(
                #field_expr,
                #field_name,
            )?
        ),
    }
}

fn persisted_field_decode_expr(field_ty: &TokenStream, field_name: &str) -> TokenStream {
    match classify_persisted_field_type(field_ty) {
        PersistedFieldType::OptionScalar(inner_ty) => quote!(
            ::icydb::db::decode_persisted_option_scalar_slot_payload::<#inner_ty>(
                bytes,
                #field_name,
            )?
        ),
        PersistedFieldType::OptionStructural(inner_ty) => quote!(
            ::icydb::db::decode_persisted_option_slot_payload::<#inner_ty>(
                bytes,
                #field_name,
            )?
        ),
        PersistedFieldType::Scalar(parsed) => quote!(
            ::icydb::db::decode_persisted_scalar_slot_payload::<#parsed>(
                bytes,
                #field_name,
            )?
        ),
        PersistedFieldType::Structural(parsed) => quote!(
            ::icydb::db::decode_persisted_non_null_slot_payload::<#parsed>(
                bytes,
                #field_name,
            )?
        ),
    }
}

fn persisted_field_encode_expr(
    field_ty: &TokenStream,
    field_expr: TokenStream,
    field_name: &str,
) -> TokenStream {
    match classify_persisted_field_type(field_ty) {
        PersistedFieldType::OptionScalar(inner_ty) => quote!(
            ::icydb::db::encode_persisted_option_scalar_slot_payload::<#inner_ty>(
                #field_expr,
                #field_name,
            )?
        ),
        PersistedFieldType::OptionStructural(_) | PersistedFieldType::Structural(_) => quote!(
            ::icydb::db::encode_persisted_slot_payload(
                #field_expr,
                #field_name,
            )?
        ),
        PersistedFieldType::Scalar(_) => quote!(
            ::icydb::db::encode_persisted_scalar_slot_payload(
                #field_expr,
                #field_name,
            )?
        ),
    }
}

// Classifies one generated field type into the persisted-row emission lanes.
enum PersistedFieldType {
    OptionScalar(Type),
    OptionStructural(Type),
    Scalar(Type),
    Structural(Type),
}

fn classify_persisted_field_type(field_ty: &TokenStream) -> PersistedFieldType {
    let parsed = parse_type::<Type>(field_ty.clone()).expect("generated field type must parse");

    if let Some(inner_ty) = option_inner_type(&parsed) {
        if is_scalar_type(&inner_ty) {
            return PersistedFieldType::OptionScalar(inner_ty);
        }

        return PersistedFieldType::OptionStructural(inner_ty);
    }

    if is_scalar_type(&parsed) {
        return PersistedFieldType::Scalar(parsed);
    }

    PersistedFieldType::Structural(parsed)
}

fn option_inner_type(ty: &Type) -> Option<Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) =
        &segment.arguments
    else {
        return None;
    };
    let Some(GenericArgument::Type(inner_ty)) = args.first() else {
        return None;
    };
    Some(inner_ty.clone())
}

fn is_scalar_type(ty: &Type) -> bool {
    if is_unit_tuple(ty) {
        return true;
    }

    matches!(
        path_last_ident(ty).as_deref(),
        Some(
            "bool"
                | "Bool"
                | "i8"
                | "Int8"
                | "i16"
                | "Int16"
                | "i32"
                | "Int32"
                | "i64"
                | "Int64"
                | "u8"
                | "Nat8"
                | "u16"
                | "Nat16"
                | "u32"
                | "Nat32"
                | "u64"
                | "Nat64"
                | "Blob"
                | "String"
                | "Text"
                | "Date"
                | "Duration"
                | "Float32"
                | "Float64"
                | "Principal"
                | "Subaccount"
                | "Timestamp"
                | "Ulid"
                | "Unit"
        )
    ) || is_vec_u8(ty)
}

fn is_unit_tuple(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}

fn is_vec_u8(ty: &Type) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    let Some(segment) = path.path.segments.last() else {
        return false;
    };
    if segment.ident != "Vec" {
        return false;
    }
    let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) =
        &segment.arguments
    else {
        return false;
    };
    let Some(GenericArgument::Type(inner_ty)) = args.first() else {
        return false;
    };

    matches!(path_last_ident(inner_ty).as_deref(), Some("u8"))
}

fn path_last_ident(ty: &Type) -> Option<String> {
    let Type::Path(path) = ty else {
        return None;
    };

    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}
