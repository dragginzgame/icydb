//! Module: persisted_row
//! Responsibility: generated persisted-row materialization bridge for named structs.
//! Does not own: field persistence policy, store layout authority, or migrations.
//! Boundary: maps accepted runtime slot values into Rust fields.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    AngleBracketedGenericArguments, Data, DeriveInput, Error, Field, Fields, GenericArgument,
    PathArguments, Type,
};

/// Derive the low-level persisted-row bridge for one named-field struct.
///
/// This macro is intentionally mechanical: it maps fields to slot indexes and
/// decodes the accepted runtime values supplied by `SlotReader`.
pub(crate) fn derive_persisted_row(input: TokenStream) -> TokenStream {
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

    let parsed_fields: Vec<&Field> = fields.iter().collect();
    let field_decode_assertions = parsed_fields
        .iter()
        .map(|field| runtime_value_decode_assertion(field));

    let materializers = parsed_fields.iter().enumerate().map(|(slot, field)| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_name = field_ident.to_string();
        let field_ty = &field.ty;

        if option_inner_type(field_ty).is_some() {
            quote! {
                #field_ident: match slots.get_value(#slot)? {
                    Some(value) => {
                        ::icydb::__macro::decode_generated_runtime_field_value::<#field_ty>(
                            &value,
                            slots.runtime_enum_context(),
                            #field_name,
                        )?
                    }
                    None => None,
                }
            }
        } else {
            quote! {
                #field_ident: match slots.get_value(#slot)? {
                    Some(value) => {
                        ::icydb::__macro::decode_generated_runtime_field_value::<#field_ty>(
                            &value,
                            slots.runtime_enum_context(),
                            #field_name,
                        )?
                    }
                    None => {
                        return Err(::icydb::__macro::InternalError::missing_persisted_slot(#field_name));
                    }
                }
            }
        }
    });

    quote! {
        #(#field_decode_assertions)*

        impl #impl_generics ::icydb::__macro::PersistedRow for #ident #ty_generics #where_clause {
            fn materialize_from_slots(
                slots: &mut dyn ::icydb::__macro::SlotReader,
            ) -> Result<Self, ::icydb::__macro::InternalError> {
                Ok(Self {
                    #(#materializers),*
                })
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

// Emit one field-local assertion so unsupported runtime materialization fails
// with a named generated symbol instead of a generic helper bound error.
fn runtime_value_decode_assertion(field: &Field) -> TokenStream {
    let field_ident = field.ident.as_ref().expect("named field");
    let asserted_ty = &field.ty;

    emit_persisted_trait_assertion(
        field_ident,
        quote!(::icydb::__macro::RuntimeValueDecode),
        asserted_ty,
        "RUNTIME_VALUE_DECODE",
    )
}

// Generate a descriptive compile-time assertion symbol for one persisted-row
// field contract so trait failures point at the owning decode boundary.
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
