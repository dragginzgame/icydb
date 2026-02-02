use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::newtype::{self, NewtypeInput};

pub fn derive_add(input: TokenStream) -> TokenStream {
    derive_op(
        input,
        OpSpec::binary("Add", quote!(::icydb::traits::Add), "add", quote!(+)),
    )
}

pub fn derive_add_assign(input: TokenStream) -> TokenStream {
    derive_op(
        input,
        OpSpec::assign(
            "AddAssign",
            quote!(::icydb::traits::AddAssign),
            "add_assign",
            quote!(+=),
        ),
    )
}

pub fn derive_sub(input: TokenStream) -> TokenStream {
    derive_op(
        input,
        OpSpec::binary("Sub", quote!(::icydb::traits::Sub), "sub", quote!(-)),
    )
}

pub fn derive_sub_assign(input: TokenStream) -> TokenStream {
    derive_op(
        input,
        OpSpec::assign(
            "SubAssign",
            quote!(::icydb::traits::SubAssign),
            "sub_assign",
            quote!(-=),
        ),
    )
}

pub fn derive_mul(input: TokenStream) -> TokenStream {
    derive_op(
        input,
        OpSpec::binary("Mul", quote!(::icydb::traits::Mul), "mul", quote!(*)),
    )
}

pub fn derive_mul_assign(input: TokenStream) -> TokenStream {
    derive_op(
        input,
        OpSpec::assign(
            "MulAssign",
            quote!(::icydb::traits::MulAssign),
            "mul_assign",
            quote!(*=),
        ),
    )
}

pub fn derive_div(input: TokenStream) -> TokenStream {
    derive_op(
        input,
        OpSpec::binary("Div", quote!(::icydb::traits::Div), "div", quote!(/)),
    )
}

pub fn derive_div_assign(input: TokenStream) -> TokenStream {
    derive_op(
        input,
        OpSpec::assign(
            "DivAssign",
            quote!(::icydb::traits::DivAssign),
            "div_assign",
            quote!(/=),
        ),
    )
}

pub fn derive_rem(input: TokenStream) -> TokenStream {
    derive_op(
        input,
        OpSpec::binary("Rem", quote!(::icydb::traits::Rem), "rem", quote!(%)),
    )
}

pub fn derive_sum(input: TokenStream) -> TokenStream {
    let newtype = match newtype::parse_newtype(input, "Sum") {
        Ok(newtype) => newtype,
        Err(err) => return err.to_compile_error(),
    };

    let ident = newtype.ident;
    let inner = newtype.inner;
    let (impl_generics, ty_generics, where_clause) = newtype.generics.split_for_impl();
    let self_ty = quote!(#ident #ty_generics);

    quote! {
        impl #impl_generics ::std::iter::Sum for #self_ty #where_clause {
            fn sum<I: ::std::iter::Iterator<Item = Self>>(iter: I) -> Self {
                Self(iter.map(|v| v.0).sum())
            }
        }

        impl #impl_generics ::std::iter::Sum<#inner> for #self_ty #where_clause {
            fn sum<I: ::std::iter::Iterator<Item = #inner>>(iter: I) -> Self {
                Self(iter.sum())
            }
        }
    }
}

///
/// OpSpec
///

struct OpSpec {
    label: &'static str,
    trait_path: TokenStream,
    method: &'static str,
    op_token: TokenStream,
    is_assign: bool,
}

impl OpSpec {
    const fn binary(
        label: &'static str,
        trait_path: TokenStream,
        method: &'static str,
        op_token: TokenStream,
    ) -> Self {
        Self {
            label,
            trait_path,
            method,
            op_token,
            is_assign: false,
        }
    }

    const fn assign(
        label: &'static str,
        trait_path: TokenStream,
        method: &'static str,
        op_token: TokenStream,
    ) -> Self {
        Self {
            label,
            trait_path,
            method,
            op_token,
            is_assign: true,
        }
    }
}

fn derive_op(input: TokenStream, spec: OpSpec) -> TokenStream {
    let newtype = match newtype::parse_newtype(input, spec.label) {
        Ok(newtype) => newtype,
        Err(err) => return err.to_compile_error(),
    };

    if spec.is_assign {
        expand_assign(newtype, spec)
    } else {
        expand_binary(newtype, spec)
    }
}

fn expand_binary(newtype: NewtypeInput, spec: OpSpec) -> TokenStream {
    let ident = newtype.ident;
    let inner = newtype.inner;
    let (impl_generics, ty_generics, where_clause) = newtype.generics.split_for_impl();
    let self_ty = quote!(#ident #ty_generics);
    let trait_path = spec.trait_path;
    let method = format_ident!("{}", spec.method);
    let op_token = spec.op_token;

    quote! {
        impl #impl_generics #trait_path<Self> for #self_ty #where_clause {
            type Output = Self;

            fn #method(self, other: Self) -> Self::Output {
                Self(self.0 #op_token other.0)
            }
        }

        impl #impl_generics #trait_path<#inner> for #self_ty #where_clause {
            type Output = Self;

            fn #method(self, other: #inner) -> Self::Output {
                Self(self.0 #op_token other)
            }
        }

        impl #impl_generics #trait_path<#self_ty> for #inner #where_clause {
            type Output = #self_ty;

            fn #method(self, other: #self_ty) -> Self::Output {
                #ident(self #op_token other.0)
            }
        }
    }
}

fn expand_assign(newtype: NewtypeInput, spec: OpSpec) -> TokenStream {
    let ident = newtype.ident;
    let inner = newtype.inner;
    let (impl_generics, ty_generics, where_clause) = newtype.generics.split_for_impl();
    let self_ty = quote!(#ident #ty_generics);
    let trait_path = spec.trait_path;
    let method = format_ident!("{}", spec.method);
    let op_token = spec.op_token;

    quote! {
        impl #impl_generics #trait_path<#inner> for #self_ty #where_clause {
            fn #method(&mut self, other: #inner) {
                self.0 #op_token other;
            }
        }

        impl #impl_generics #trait_path<#self_ty> for #self_ty #where_clause {
            fn #method(&mut self, other: #self_ty) {
                self.0 #op_token other.0;
            }
        }
    }
}
