use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    AngleBracketedGenericArguments, Data, DeriveInput, Error, Field, Fields, GenericArgument,
    PathArguments, Type,
};

/// Derive the low-level persisted-row bridge for one named-field struct.
///
/// This macro is intentionally mechanical: it maps fields to slot indexes and
/// delegates every storage decision to `PersistedFieldSlotCodec` on the field
/// type.
pub fn derive_persisted_row(input: TokenStream) -> TokenStream {
    let input: DeriveInput = match syn::parse2(input) {
        Ok(input) => input,
        Err(err) => return err.to_compile_error(),
    };

    let ident = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let fields = match named_struct_fields(&input) {
        Ok(fields) => fields,
        Err(err) => return err.to_compile_error(),
    };

    if let Err(err) = reject_persisted_field_hints(fields) {
        return err.to_compile_error();
    }

    let parsed_fields: Vec<&Field> = fields.iter().collect();
    let field_codec_assertions = parsed_fields
        .iter()
        .map(|field| persisted_field_codec_assertion(field));

    let materializers = parsed_fields.iter().enumerate().map(|(slot, field)| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_name = field_ident.to_string();

        if let Some(inner_ty) = option_inner_type(&field.ty) {
            quote! {
                #field_ident: match slots.get_bytes(#slot) {
                    Some(bytes) => {
                        <#inner_ty as ::icydb::__macro::PersistedFieldSlotCodec>::decode_persisted_option_slot(
                            bytes,
                            #field_name,
                        )?
                    }
                    None => None,
                }
            }
        } else {
            let field_ty = &field.ty;
            quote! {
                #field_ident: match slots.get_bytes(#slot) {
                    Some(bytes) => {
                        <#field_ty as ::icydb::__macro::PersistedFieldSlotCodec>::decode_persisted_slot(
                            bytes,
                            #field_name,
                        )?
                    }
                    None => {
                        return Err(::icydb::db::InternalError::missing_persisted_slot(#field_name));
                    }
                }
            }
        }
    });

    let slot_writes = parsed_fields.iter().enumerate().map(|(slot, field)| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_name = field_ident.to_string();

        if let Some(inner_ty) = option_inner_type(&field.ty) {
            quote! {
                let payload =
                    <#inner_ty as ::icydb::__macro::PersistedFieldSlotCodec>::encode_persisted_option_slot(
                        &self.#field_ident,
                        #field_name,
                    )?;
                out.write_slot(#slot, Some(payload.as_slice()))?;
            }
        } else {
            let field_ty = &field.ty;
            quote! {
                let payload =
                    <#field_ty as ::icydb::__macro::PersistedFieldSlotCodec>::encode_persisted_slot(
                        &self.#field_ident,
                        #field_name,
                    )?;
                out.write_slot(#slot, Some(payload.as_slice()))?;
            }
        }
    });

    quote! {
        #(#field_codec_assertions)*

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
        }
    }
}

fn named_struct_fields(
    input: &DeriveInput,
) -> Result<&syn::punctuated::Punctuated<Field, syn::token::Comma>, Error> {
    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            &input.ident,
            "PersistedRow can only be derived for structs with named fields",
        ));
    };

    let Fields::Named(named) = &data.fields else {
        return Err(Error::new_spanned(
            &data.fields,
            "PersistedRow can only be derived for structs with named fields",
        ));
    };

    Ok(&named.named)
}

// Reject all field-level storage hints. Persisted row storage is selected from
// the field type's `PersistedFieldSlotCodec` implementation only.
fn reject_persisted_field_hints(
    fields: &syn::punctuated::Punctuated<Field, syn::token::Comma>,
) -> Result<(), Error> {
    for field in fields {
        for attr in &field.attrs {
            if !attr.path().is_ident("icydb") {
                continue;
            }

            return Err(Error::new_spanned(
                attr,
                "#[icydb(...)] persisted-row field hints have been removed; use schema-derived field metadata or a type that implements PersistedFieldSlotCodec",
            ));
        }
    }

    Ok(())
}

// Emit one field-local trait assertion so missing persisted codecs fail with a
// named generated symbol instead of a generic helper bound error.
fn persisted_field_codec_assertion(field: &Field) -> TokenStream {
    let field_ident = field.ident.as_ref().expect("named field");
    let asserted_ty = option_inner_type(&field.ty).unwrap_or(&field.ty);

    emit_persisted_trait_assertion(
        field_ident,
        quote!(::icydb::__macro::PersistedFieldSlotCodec),
        asserted_ty,
        "PERSISTED_FIELD_SLOT_CODEC",
    )
}

// Generate a descriptive compile-time assertion symbol for one persisted-row
// field contract so trait failures point at the owning storage lane.
fn emit_persisted_trait_assertion(
    field_ident: &syn::Ident,
    trait_path: TokenStream,
    asserted_ty: &Type,
    trait_label: &str,
) -> TokenStream {
    let assert_ident = format_ident!(
        "__ICYDB_FIELD_{}_MUST_IMPLEMENT_{}_TO_BE_STORED",
        field_ident.to_string().to_ascii_uppercase(),
        trait_label,
    );

    quote! {
        const _: () = {
            fn #assert_ident<T: #trait_path>() {}
            let _ = #assert_ident::<#asserted_ty>;
        };
    }
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
