use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    AngleBracketedGenericArguments, Data, DeriveInput, Error, Fields, GenericArgument,
    PathArguments, Type,
};

// derive_persisted_row
pub fn derive_persisted_row(input: TokenStream) -> TokenStream {
    let input: DeriveInput = match syn::parse2(input) {
        Ok(input) => input,
        Err(err) => return err.to_compile_error(),
    };

    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let fields = if let Data::Struct(data) = &input.data {
        if let Fields::Named(named) = &data.fields {
            &named.named
        } else {
            let err = Error::new_spanned(
                &data.fields,
                "PersistedRow can only be derived for structs with named fields",
            );
            return err.to_compile_error();
        }
    } else {
        let err = Error::new_spanned(
            &input.ident,
            "PersistedRow can only be derived for structs with named fields",
        );
        return err.to_compile_error();
    };

    let materializers = fields.iter().enumerate().map(|(slot, field)| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_ty = &field.ty;
        let field_name = field_ident.to_string();
        let missing_expr = match classify_field(field_ty) {
            FieldCardinality::Opt => quote!(None),
            FieldCardinality::One | FieldCardinality::Many => quote! {
                return Err(::icydb::db::InternalError::missing_persisted_slot(#field_name))
            },
        };
        let decode_expr = persisted_field_decode_expr(field_ty, field_name.as_str());

        quote! {
            #field_ident: match slots.get_bytes(#slot) {
                Some(bytes) => #decode_expr,
                None => #missing_expr,
            }
        }
    });

    let slot_writes = fields.iter().enumerate().map(|(slot, field)| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_name = field_ident.to_string();
        let encode_expr =
            persisted_field_encode_expr(&field.ty, quote!(&self.#field_ident), field_name.as_str());

        quote! {
            let payload = #encode_expr;
            out.write_slot(#slot, Some(payload.as_slice()))?;
        }
    });

    let slot_projects = fields.iter().enumerate().map(|(slot, field)| {
        let field_ty = &field.ty;
        let field_name = field.ident.as_ref().expect("named field").to_string();
        let project_expr = persisted_field_project_expr(field_ty, field_name.as_str(), slot);

        quote! {
            #slot => #project_expr,
        }
    });

    quote! {
        impl #impl_generics ::icydb::db::PersistedRow for #ident #ty_generics #where_clause {
            fn materialize_from_slots(
                slots: &mut dyn ::icydb::db::SlotReader,
            ) -> Result<Self, ::icydb::db::InternalError> {
                Ok(Self {
                    #(#materializers),*
                })
            }

            fn write_slots(
                &self,
                out: &mut dyn ::icydb::db::SlotWriter,
            ) -> Result<(), ::icydb::db::InternalError> {
                #(#slot_writes)*

                Ok(())
            }

            fn project_slot(
                slots: &mut dyn ::icydb::db::SlotReader,
                slot: usize,
            ) -> Result<Option<::icydb::value::Value>, ::icydb::db::InternalError> {
                match slot {
                    #(#slot_projects)*
                    _ => Err(::icydb::db::InternalError::index_invariant(format!(
                        "slot lookup outside derived persisted row bounds: entity='{}' slot={slot}",
                        <Self as ::icydb::traits::Path>::PATH,
                    ))),
                }
            }
        }
    }
}

///
/// FieldCardinality
///

#[derive(Clone, Copy)]
enum FieldCardinality {
    One,
    Opt,
    Many,
}

fn classify_field(ty: &Type) -> FieldCardinality {
    if is_path_ident(ty, "Option") {
        FieldCardinality::Opt
    } else if is_path_ident(ty, "Vec") {
        FieldCardinality::Many
    } else {
        FieldCardinality::One
    }
}

fn is_path_ident(ty: &Type, ident: &str) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };

    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == ident)
}

fn persisted_field_decode_expr(field_ty: &Type, field_name: &str) -> TokenStream {
    if let Some(inner_ty) = option_inner_scalar_type(field_ty) {
        return quote!(
            ::icydb::db::decode_persisted_option_scalar_slot_payload::<#inner_ty>(
                bytes,
                #field_name,
            )?
        );
    }

    if is_scalar_type(field_ty) {
        return quote!(
            ::icydb::db::decode_persisted_scalar_slot_payload::<#field_ty>(
                bytes,
                #field_name,
            )?
        );
    }

    quote!(
        ::icydb::db::decode_persisted_slot_payload::<#field_ty>(
            bytes,
            #field_name,
        )?
    )
}

fn persisted_field_encode_expr(
    field_ty: &Type,
    field_expr: TokenStream,
    field_name: &str,
) -> TokenStream {
    if let Some(inner_ty) = option_inner_scalar_type(field_ty) {
        return quote!(
            ::icydb::db::encode_persisted_option_scalar_slot_payload::<#inner_ty>(
                #field_expr,
                #field_name,
            )?
        );
    }

    if is_scalar_type(field_ty) {
        return quote!(
            ::icydb::db::encode_persisted_scalar_slot_payload(
                #field_expr,
                #field_name,
            )?
        );
    }

    quote!(
        ::icydb::db::encode_persisted_slot_payload(
            #field_expr,
            #field_name,
        )?
    )
}

fn persisted_field_project_expr(field_ty: &Type, _field_name: &str, slot: usize) -> TokenStream {
    if option_inner_scalar_type(field_ty).is_some() || is_scalar_type(field_ty) {
        return quote!(
            Ok(match slots.get_scalar(#slot)? {
                Some(::icydb::db::ScalarSlotValueRef::Null) => Some(::icydb::value::Value::Null),
                Some(::icydb::db::ScalarSlotValueRef::Value(value)) => Some(value.into_value()),
                None => None,
            })
        );
    }

    quote!(
        Ok(<Self as ::icydb::traits::FieldProjection>::get_value_by_index(
            &Self::materialize_from_slots(slots)?,
            #slot,
        ))
    )
}

fn option_inner_scalar_type(ty: &Type) -> Option<Type> {
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
    if is_scalar_type(inner_ty) {
        return Some(inner_ty.clone());
    }

    None
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
