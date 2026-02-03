use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Error, Fields};

use crate::util::where_clause_with_bounds;

pub fn derive_partial_eq(input: TokenStream) -> TokenStream {
    let input: DeriveInput = match syn::parse2(input) {
        Ok(input) => input,
        Err(err) => return err.to_compile_error(),
    };
    let ident = &input.ident;
    let (impl_generics, ty_generics, _) = input.generics.split_for_impl();

    match &input.data {
        Data::Struct(data) => {
            let (eq_body, bounds) = struct_partial_eq_body(data);
            let where_tokens =
                where_clause_with_bounds(input.generics.where_clause.as_ref(), &bounds);

            quote! {
                impl #impl_generics ::std::cmp::PartialEq for #ident #ty_generics #where_tokens {
                    fn eq(&self, other: &Self) -> bool {
                        #eq_body
                    }
                }
            }
        }
        Data::Enum(data) => {
            let (eq_body, bounds) = enum_partial_eq_body(data);
            let where_tokens =
                where_clause_with_bounds(input.generics.where_clause.as_ref(), &bounds);

            quote! {
                impl #impl_generics ::std::cmp::PartialEq for #ident #ty_generics #where_tokens {
                    fn eq(&self, other: &Self) -> bool {
                        #eq_body
                    }
                }
            }
        }
        Data::Union(_) => {
            Error::new_spanned(&input.ident, "PartialEq cannot be derived for unions")
                .to_compile_error()
        }
    }
}

fn struct_partial_eq_body(data: &syn::DataStruct) -> (TokenStream, Vec<TokenStream>) {
    let mut bounds = Vec::new();

    match &data.fields {
        Fields::Named(fields) => {
            for field in &fields.named {
                let ty = &field.ty;
                bounds.push(quote!(#ty: ::std::cmp::PartialEq));
            }

            let comparisons = fields.named.iter().map(|field| {
                let ident = field.ident.as_ref().expect("named field");
                quote! {
                    if self.#ident != other.#ident {
                        return false;
                    }
                }
            });

            (
                quote! {
                    #(#comparisons)*
                    true
                },
                bounds,
            )
        }
        Fields::Unnamed(fields) => {
            for field in &fields.unnamed {
                let ty = &field.ty;
                bounds.push(quote!(#ty: ::std::cmp::PartialEq));
            }

            let comparisons = fields.unnamed.iter().enumerate().map(|(idx, _)| {
                let index = syn::Index::from(idx);
                quote! {
                    if self.#index != other.#index {
                        return false;
                    }
                }
            });

            (
                quote! {
                    #(#comparisons)*
                    true
                },
                bounds,
            )
        }
        Fields::Unit => (quote!(true), bounds),
    }
}

#[expect(clippy::too_many_lines)]
fn enum_partial_eq_body(data: &syn::DataEnum) -> (TokenStream, Vec<TokenStream>) {
    let mut bounds = Vec::new();

    for variant in &data.variants {
        match &variant.fields {
            Fields::Named(fields) => {
                for field in &fields.named {
                    let ty = &field.ty;
                    bounds.push(quote!(#ty: ::std::cmp::PartialEq));
                }
            }
            Fields::Unnamed(fields) => {
                for field in &fields.unnamed {
                    let ty = &field.ty;
                    bounds.push(quote!(#ty: ::std::cmp::PartialEq));
                }
            }
            Fields::Unit => {}
        }
    }

    let cmp_arms = data.variants.iter().map(|variant| {
        let name = &variant.ident;
        match &variant.fields {
            Fields::Named(fields) => {
                let self_bindings = fields.named.iter().map(|field| {
                    let ident = field.ident.as_ref().expect("named field");
                    quote!(#ident)
                });
                let other_bindings = fields.named.iter().map(|field| {
                    let ident = field.ident.as_ref().expect("named field");
                    let other_ident = format_ident!("other_{}", ident);
                    quote!(#ident: #other_ident)
                });
                let comparisons = fields.named.iter().map(|field| {
                    let ident = field.ident.as_ref().expect("named field");
                    let other_ident = format_ident!("other_{}", ident);
                    quote! {
                        if #ident != #other_ident {
                            return false;
                        }
                    }
                });

                quote! {
                    (Self::#name { #(#self_bindings),* }, Self::#name { #(#other_bindings),* }) => {
                        #(#comparisons)*
                        true
                    }
                }
            }
            Fields::Unnamed(fields) => {
                let self_bindings = (0..fields.unnamed.len()).map(|idx| {
                    let ident = format_ident!("field_{idx}");
                    quote!(#ident)
                });
                let other_bindings = (0..fields.unnamed.len()).map(|idx| {
                    let ident = format_ident!("other_{idx}");
                    quote!(#ident)
                });
                let comparisons = (0..fields.unnamed.len()).map(|idx| {
                    let ident = format_ident!("field_{idx}");
                    let other_ident = format_ident!("other_{idx}");
                    quote! {
                        if #ident != #other_ident {
                            return false;
                        }
                    }
                });

                quote! {
                    (Self::#name ( #(#self_bindings),* ), Self::#name ( #(#other_bindings),* )) => {
                        #(#comparisons)*
                        true
                    }
                }
            }
            Fields::Unit => quote! {
                (Self::#name, Self::#name) => true
            },
        }
    });

    (
        quote! {
            match (self, other) {
                #(#cmp_arms,)*
                _ => false,
            }
        },
        bounds,
    )
}
