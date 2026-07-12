//! Module: field_projection
//! Responsibility: generated field-index projection for named structs.
//! Does not own: runtime value conversion or schema field ordering authority.
//! Boundary: maps validated Rust fields to macro-facing projection values.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, Type};

pub(crate) fn derive_field_projection(input: TokenStream) -> TokenStream {
    derive_projection(input, ProjectionKind::Runtime)
}

pub(crate) fn derive_authored_field_projection(input: TokenStream) -> TokenStream {
    derive_projection(input, ProjectionKind::Authored)
}

#[derive(Clone, Copy)]
enum ProjectionKind {
    Authored,
    Runtime,
}

impl ProjectionKind {
    const fn derive_name(self) -> &'static str {
        match self {
            Self::Authored => "AuthoredFieldProjection",
            Self::Runtime => "FieldProjection",
        }
    }

    fn field_expr(self, field_ident: &syn::Ident, field_ty: &Type) -> TokenStream {
        match self {
            Self::Authored => field_input_value_expr(field_ident, field_ty),
            Self::Runtime => field_value_expr(field_ident, field_ty),
        }
    }
}

fn derive_projection(input: TokenStream, projection: ProjectionKind) -> TokenStream {
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
                format!(
                    "{} can only be derived for structs with named fields",
                    projection.derive_name(),
                ),
            );
            return err.to_compile_error();
        }
    } else {
        let err = Error::new_spanned(
            &input.ident,
            format!(
                "{} can only be derived for structs with named fields",
                projection.derive_name(),
            ),
        );
        return err.to_compile_error();
    };

    let by_index_match_arms = fields.iter().enumerate().map(|(index, field)| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_value_expr = projection.field_expr(field_ident, &field.ty);

        quote! {
            #index => #field_value_expr,
        }
    });

    match projection {
        ProjectionKind::Authored => quote! {
            impl #impl_generics ::icydb::__macro::AuthoredFieldProjection for #ident #ty_generics #where_clause {
                fn get_input_value_by_index(
                    &self,
                    index: usize,
                ) -> Option<::icydb::__macro::InputValue> {
                    use ::icydb::__macro::InputValue;

                    match index {
                        #(#by_index_match_arms)*
                        _ => None,
                    }
                }
            }
        },
        ProjectionKind::Runtime => quote! {
            impl #impl_generics ::icydb::__macro::FieldProjection for #ident #ty_generics #where_clause {
                fn get_value_by_index(&self, index: usize) -> Option<::icydb::__macro::Value> {
                    use ::icydb::__macro::Value;

                    match index {
                        #(#by_index_match_arms)*
                        _ => None,
                    }
                }
            }
        },
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
            Some(::icydb::__macro::runtime_value_to_value(&self.#field_ident))
        },
        FieldCardinality::Opt => quote! {
            match self.#field_ident.as_ref() {
                Some(inner) => Some(::icydb::__macro::runtime_value_to_value(inner)),
                None => Some(Value::Null),
            }
        },
        FieldCardinality::Many => quote! {
            {
                let list = self.#field_ident
                    .iter()
                    .map(::icydb::__macro::runtime_value_to_value)
                    .collect::<Vec<_>>();

                Some(Value::List(list))
            }
        },
    }
}

fn field_input_value_expr(field_ident: &syn::Ident, field_ty: &Type) -> TokenStream {
    match classify_field(field_ty) {
        FieldCardinality::One => quote! {
            Some(::icydb::__macro::InputValue::from(self.#field_ident.clone()))
        },
        FieldCardinality::Opt => quote! {
            match self.#field_ident.as_ref() {
                Some(inner) => Some(::icydb::__macro::InputValue::from(inner.clone())),
                None => Some(InputValue::Null),
            }
        },
        FieldCardinality::Many => quote! {
            Some(InputValue::List(
                self.#field_ident
                    .iter()
                    .cloned()
                    .map(::icydb::__macro::InputValue::from)
                    .collect(),
            ))
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
