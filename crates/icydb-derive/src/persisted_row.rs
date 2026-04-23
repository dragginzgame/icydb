use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    AngleBracketedGenericArguments, Data, DeriveInput, Error, Field, Fields, GenericArgument,
    LitInt, PathArguments, Type,
};

// derive_persisted_row
//
// The proc-macro entrypoint still owns field discovery, hint parsing, and the
// final impl emission in one place so errors stay attached to the originating
// field spans.
#[expect(clippy::too_many_lines)]
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

    let parsed_fields: Vec<(&Field, PersistedFieldHints)> = match fields
        .iter()
        .map(|field| parse_persisted_field_hints(field).map(|hints| (field, hints)))
        .collect()
    {
        Ok(parsed_fields) => parsed_fields,
        Err(err) => return err.to_compile_error(),
    };

    for (field, hints) in &parsed_fields {
        if let Err(err) = ensure_persisted_field_storage_supported(field, *hints) {
            return err.to_compile_error();
        }
    }

    let materializers = parsed_fields
        .iter()
        .enumerate()
        .map(|(slot, (field, hints))| {
            let field_ident = field.ident.as_ref().expect("named field");
            let field_ty = &field.ty;
            let field_name = field_ident.to_string();
            let missing_expr = match classify_field(field_ty) {
                FieldCardinality::Opt => quote!(None),
                FieldCardinality::One | FieldCardinality::Many => quote! {
                    return Err(::icydb::db::InternalError::missing_persisted_slot(#field_name))
                },
            };
            let decode_expr = persisted_field_decode_expr(field_ty, field_name.as_str(), *hints);

            quote! {
                #field_ident: match slots.get_bytes(#slot) {
                    Some(bytes) => #decode_expr,
                    None => #missing_expr,
                }
            }
        });

    let slot_writes = parsed_fields
        .iter()
        .enumerate()
        .map(|(slot, (field, hints))| {
            let field_ident = field.ident.as_ref().expect("named field");
            let field_name = field_ident.to_string();
            let encode_expr = persisted_field_encode_expr(
                &field.ty,
                quote!(&self.#field_ident),
                field_name.as_str(),
                *hints,
            );

            quote! {
                let payload = #encode_expr;
                out.write_slot(#slot, Some(payload.as_slice()))?;
            }
        });

    let slot_projects = parsed_fields
        .iter()
        .enumerate()
        .map(|(slot, (field, _hints))| {
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
            ) -> Result<Option<::icydb::__macro::Value>, ::icydb::db::InternalError> {
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

///
/// PersistedFieldHints
///
/// PersistedFieldHints carries the explicit field-level codec hints accepted by
/// the metadata-free `PersistedRow` derive.
/// These hints only exist to recover schema facts that cannot be inferred from
/// the Rust type alone, such as decimal scale.
///

#[derive(Clone, Copy, Default)]
struct PersistedFieldHints {
    decimal_scale: Option<u32>,
    meta_storage: bool,
    value_storage: bool,
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

fn persisted_field_decode_expr(
    field_ty: &Type,
    field_name: &str,
    hints: PersistedFieldHints,
) -> TokenStream {
    if hints.meta_storage {
        if let Some(inner_ty) = option_inner_type(field_ty) {
            return quote!(
                ::icydb::db::decode_persisted_option_slot_payload_by_meta::<#inner_ty>(
                    bytes,
                    #field_name,
                )?
            );
        }

        return quote!(
            ::icydb::db::decode_persisted_slot_payload_by_meta::<#field_ty>(
                bytes,
                #field_name,
            )?
        );
    }

    if hints.value_storage {
        return quote!(
            ::icydb::db::decode_persisted_custom_slot_payload::<#field_ty>(
                bytes,
                #field_name,
            )?
        );
    }

    if let Some(inner_ty) = option_inner_scalar_type(field_ty) {
        return quote!(
            ::icydb::db::decode_persisted_option_scalar_slot_payload::<#inner_ty>(
                bytes,
                #field_name,
            )?
        );
    }

    if let Some((inner_ty, inferred_kind)) = option_inner_by_kind_type(field_ty, hints) {
        return quote!(
            ::icydb::db::decode_persisted_option_slot_payload_by_kind::<#inner_ty>(
                bytes,
                #inferred_kind,
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

    if let Some(inferred_kind) = inferred_field_kind_expr(field_ty, hints.decimal_scale) {
        return quote!(
            ::icydb::db::decode_persisted_non_null_slot_payload_by_kind::<#field_ty>(
                bytes,
                #inferred_kind,
                #field_name,
            )?
        );
    }

    unreachable!("validated persisted-row field must lower through one explicit storage contract")
}

fn persisted_field_encode_expr(
    field_ty: &Type,
    field_expr: TokenStream,
    field_name: &str,
    hints: PersistedFieldHints,
) -> TokenStream {
    if hints.meta_storage {
        if let Some(inner_ty) = option_inner_type(field_ty) {
            return quote!(
                ::icydb::db::encode_persisted_option_slot_payload_by_meta::<#inner_ty>(
                    #field_expr,
                    #field_name,
                )?
            );
        }

        return quote!(
            ::icydb::db::encode_persisted_slot_payload_by_meta(
                #field_expr,
                #field_name,
            )?
        );
    }

    if hints.value_storage {
        return quote!(
            ::icydb::db::encode_persisted_custom_slot_payload(
                #field_expr,
                #field_name,
            )?
        );
    }

    if let Some(inner_ty) = option_inner_scalar_type(field_ty) {
        return quote!(
            ::icydb::db::encode_persisted_option_scalar_slot_payload::<#inner_ty>(
                #field_expr,
                #field_name,
            )?
        );
    }

    if let Some((_inner_ty, inferred_kind)) = option_inner_by_kind_type(field_ty, hints) {
        return quote!(
            ::icydb::db::encode_persisted_slot_payload_by_kind(
                #field_expr,
                #inferred_kind,
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

    if let Some(inferred_kind) = inferred_field_kind_expr(field_ty, hints.decimal_scale) {
        return quote!(
            ::icydb::db::encode_persisted_slot_payload_by_kind(
                #field_expr,
                #inferred_kind,
                #field_name,
            )?
        );
    }

    unreachable!("validated persisted-row field must lower through one explicit storage contract")
}

fn persisted_field_project_expr(field_ty: &Type, _field_name: &str, slot: usize) -> TokenStream {
    if option_inner_scalar_type(field_ty).is_some() || is_scalar_type(field_ty) {
        return quote!(
            Ok(match slots.get_scalar(#slot)? {
                Some(::icydb::db::ScalarSlotValueRef::Null) => Some(::icydb::__macro::Value::Null),
                Some(::icydb::db::ScalarSlotValueRef::Value(value)) => Some(value.into_value()),
                None => None,
            })
        );
    }

    quote!(
        Ok(<Self as ::icydb::__macro::FieldProjection>::get_value_by_index(
            &Self::materialize_from_slots(slots)?,
            #slot,
        ))
    )
}

fn option_inner_scalar_type(ty: &Type) -> Option<Type> {
    let inner_ty = option_inner_type(ty)?;
    if is_scalar_type(inner_ty) {
        return Some(inner_ty.clone());
    }

    None
}

fn option_inner_by_kind_type(ty: &Type, hints: PersistedFieldHints) -> Option<(Type, TokenStream)> {
    let inner_ty = option_inner_type(ty)?.clone();
    let inferred_kind = inferred_field_kind_expr(&inner_ty, hints.decimal_scale)?;

    Some((inner_ty, inferred_kind))
}

fn option_inner_type(ty: &Type) -> Option<&Type> {
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

    Some(inner_ty)
}

fn inferred_field_kind_expr(ty: &Type, decimal_scale: Option<u32>) -> Option<TokenStream> {
    if is_vec_u8(ty) {
        return Some(quote!(::icydb::model::field::FieldKind::Blob));
    }

    if is_unit_tuple(ty) {
        return Some(quote!(::icydb::model::field::FieldKind::Unit));
    }

    if let Some(inner_ty) = box_inner_type(ty) {
        return inferred_field_kind_expr(inner_ty, decimal_scale);
    }

    if let Some(inner_ty) = vec_inner_type(ty) {
        let inner_kind = inferred_field_kind_expr(inner_ty, decimal_scale)?;
        return Some(quote!(::icydb::model::field::FieldKind::List(&#inner_kind)));
    }

    if let Some(inner_ty) = btree_set_inner_type(ty) {
        let inner_kind = inferred_field_kind_expr(inner_ty, decimal_scale)?;
        return Some(quote!(::icydb::model::field::FieldKind::Set(&#inner_kind)));
    }

    if let Some((key_ty, value_ty)) = btree_map_inner_types(ty) {
        let key_kind = inferred_field_kind_expr(key_ty, decimal_scale)?;
        let value_kind = inferred_field_kind_expr(value_ty, decimal_scale)?;
        return Some(quote!(
            ::icydb::model::field::FieldKind::Map {
                key: &#key_kind,
                value: &#value_kind,
            }
        ));
    }

    match path_last_ident(ty).as_deref()? {
        "Account" => Some(quote!(::icydb::model::field::FieldKind::Account)),
        "bool" | "Bool" => Some(quote!(::icydb::model::field::FieldKind::Bool)),
        "i8" | "Int8" | "i16" | "Int16" | "i32" | "Int32" | "i64" | "Int64" => {
            Some(quote!(::icydb::model::field::FieldKind::Int))
        }
        "u8" | "Nat8" | "u16" | "Nat16" | "u32" | "Nat32" | "u64" | "Nat64" => {
            Some(quote!(::icydb::model::field::FieldKind::Uint))
        }
        "Blob" => Some(quote!(::icydb::model::field::FieldKind::Blob)),
        "String" | "Text" => Some(quote!(::icydb::model::field::FieldKind::Text)),
        "Date" => Some(quote!(::icydb::model::field::FieldKind::Date)),
        "Decimal" => decimal_scale
            .map(|scale| quote!(::icydb::model::field::FieldKind::Decimal { scale: #scale })),
        "Duration" => Some(quote!(::icydb::model::field::FieldKind::Duration)),
        "Float32" => Some(quote!(::icydb::model::field::FieldKind::Float32)),
        "Float64" => Some(quote!(::icydb::model::field::FieldKind::Float64)),
        "Int128" => Some(quote!(::icydb::model::field::FieldKind::Int128)),
        "Int" | "IntBig" => Some(quote!(::icydb::model::field::FieldKind::IntBig)),
        "Nat" | "UintBig" | "NatBig" => Some(quote!(::icydb::model::field::FieldKind::UintBig)),
        "Principal" => Some(quote!(::icydb::model::field::FieldKind::Principal)),
        "Subaccount" => Some(quote!(::icydb::model::field::FieldKind::Subaccount)),
        "Timestamp" => Some(quote!(::icydb::model::field::FieldKind::Timestamp)),
        "Uint128" | "Nat128" => Some(quote!(::icydb::model::field::FieldKind::Uint128)),
        "Ulid" => Some(quote!(::icydb::model::field::FieldKind::Ulid)),
        "Unit" => Some(quote!(::icydb::model::field::FieldKind::Unit)),
        _ => None,
    }
}

fn parse_persisted_field_hints(field: &Field) -> Result<PersistedFieldHints, Error> {
    let mut hints = PersistedFieldHints::default();

    for attr in &field.attrs {
        if !attr.path().is_ident("icydb") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("scale") {
                let value = meta.value()?;
                let literal: LitInt = value.parse()?;
                let scale = literal.base10_parse::<u32>()?;
                if hints.decimal_scale.replace(scale).is_some() {
                    return Err(meta.error("duplicate scale hint"));
                }

                return Ok(());
            }

            if meta.path.is_ident("meta") {
                if hints.meta_storage {
                    return Err(meta.error("duplicate meta hint"));
                }

                hints.meta_storage = true;
                return Ok(());
            }

            if meta.path.is_ident("value") {
                if hints.value_storage {
                    return Err(meta.error("duplicate value hint"));
                }

                hints.value_storage = true;
                return Ok(());
            }

            Err(meta.error("unsupported icydb persisted-row field hint"))
        })?;
    }

    if hints.meta_storage && hints.value_storage {
        return Err(Error::new_spanned(
            &field.ty,
            "#[icydb(meta)] cannot be combined with #[icydb(value)]",
        ));
    }

    if hints.meta_storage && hints.decimal_scale.is_some() {
        return Err(Error::new_spanned(
            &field.ty,
            "#[icydb(meta)] cannot be combined with #[icydb(scale = ...)]",
        ));
    }

    if hints.value_storage && hints.decimal_scale.is_some() {
        return Err(Error::new_spanned(
            &field.ty,
            "#[icydb(value)] cannot be combined with #[icydb(scale = ...)]",
        ));
    }

    if hints.decimal_scale.is_some() && !type_contains_decimal(&field.ty) {
        return Err(Error::new_spanned(
            &field.ty,
            "#[icydb(scale = ...)] is only supported for Decimal fields or containers of Decimal",
        ));
    }

    Ok(hints)
}

fn ensure_persisted_field_storage_supported(
    field: &Field,
    hints: PersistedFieldHints,
) -> Result<(), Error> {
    if hints.meta_storage || hints.value_storage {
        return Ok(());
    }

    let field_ty = &field.ty;
    let supported = option_inner_scalar_type(field_ty).is_some()
        || option_inner_by_kind_type(field_ty, hints).is_some()
        || is_scalar_type(field_ty)
        || inferred_field_kind_expr(field_ty, hints.decimal_scale).is_some();
    if supported {
        return Ok(());
    }

    Err(Error::new_spanned(
        field_ty,
        "PersistedRow derive fields require an explicit structural contract; use #[icydb(meta)], #[icydb(value)], or #[icydb(scale = ...)]",
    ))
}

fn type_contains_decimal(ty: &Type) -> bool {
    if matches!(path_last_ident(ty).as_deref(), Some("Decimal")) {
        return true;
    }

    if let Some(inner_ty) = option_inner_type(ty) {
        return type_contains_decimal(inner_ty);
    }

    if let Some(inner_ty) = box_inner_type(ty) {
        return type_contains_decimal(inner_ty);
    }

    if let Some(inner_ty) = vec_inner_type(ty) {
        return type_contains_decimal(inner_ty);
    }

    if let Some(inner_ty) = btree_set_inner_type(ty) {
        return type_contains_decimal(inner_ty);
    }

    if let Some((key_ty, value_ty)) = btree_map_inner_types(ty) {
        return type_contains_decimal(key_ty) || type_contains_decimal(value_ty);
    }

    false
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
    let Some(inner_ty) = vec_inner_type(ty) else {
        return false;
    };

    matches!(path_last_ident(inner_ty).as_deref(), Some("u8"))
}

fn vec_inner_type(ty: &Type) -> Option<&Type> {
    generic_path_arg(ty, "Vec", 0)
}

fn btree_set_inner_type(ty: &Type) -> Option<&Type> {
    generic_path_arg(ty, "BTreeSet", 0)
}

fn btree_map_inner_types(ty: &Type) -> Option<(&Type, &Type)> {
    Some((
        generic_path_arg(ty, "BTreeMap", 0)?,
        generic_path_arg(ty, "BTreeMap", 1)?,
    ))
}

fn box_inner_type(ty: &Type) -> Option<&Type> {
    generic_path_arg(ty, "Box", 0)
}

fn generic_path_arg<'a>(ty: &'a Type, ident: &str, index: usize) -> Option<&'a Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != ident {
        return None;
    }
    let PathArguments::AngleBracketed(AngleBracketedGenericArguments { args, .. }) =
        &segment.arguments
    else {
        return None;
    };

    match args.iter().nth(index) {
        Some(GenericArgument::Type(inner_ty)) => Some(inner_ty),
        _ => None,
    }
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
