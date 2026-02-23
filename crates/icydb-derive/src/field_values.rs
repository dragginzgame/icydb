use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, Type};

// derive_field_values
pub fn derive_field_values(input: TokenStream) -> TokenStream {
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
                "FieldValues can only be derived for structs with named fields",
            );
            return err.to_compile_error();
        }
    } else {
        let err = Error::new_spanned(
            &input.ident,
            "FieldValues can only be derived for structs with named fields",
        );
        return err.to_compile_error();
    };

    let by_name_match_arms = fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_name = field_ident.to_string();
        let field_value_expr = field_value_expr(field_ident, &field.ty);

        quote! {
            #field_name => #field_value_expr,
        }
    });

    let by_index_match_arms = fields.iter().enumerate().map(|(index, field)| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_value_expr = field_value_expr(field_ident, &field.ty);

        quote! {
            #index => #field_value_expr,
        }
    });

    quote! {
        impl #impl_generics ::icydb::traits::FieldValues for #ident #ty_generics #where_clause {
            fn get_value(&self, field: &str) -> Option<::icydb::value::Value> {
                use ::icydb::{traits::FieldValue, value::Value};

                match field {
                    #(#by_name_match_arms)*
                    _ => None,
                }
            }

            fn get_value_by_index(&self, index: usize) -> Option<::icydb::value::Value> {
                use ::icydb::{traits::FieldValue, value::Value};

                match index {
                    #(#by_index_match_arms)*
                    _ => None,
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

fn field_value_expr(field_ident: &syn::Ident, field_ty: &Type) -> TokenStream {
    match classify_field(field_ty) {
        FieldCardinality::One => quote! {
            Some(self.#field_ident.to_value())
        },
        FieldCardinality::Opt => quote! {
            match self.#field_ident.as_ref() {
                Some(inner) => Some(FieldValue::to_value(inner)),
                None => Some(Value::Null),
            }
        },
        FieldCardinality::Many => quote! {
            {
                let list = self.#field_ident
                    .iter()
                    .map(FieldValue::to_value)
                    .collect::<Vec<_>>();

                Some(Value::List(list))
            }
        },
    }
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
