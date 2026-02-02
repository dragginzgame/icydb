use proc_macro::TokenStream;

mod field_values;
mod newtype;
mod ops;
mod partial_ord;
mod util;

#[proc_macro_derive(Add)]
pub fn derive_add(input: TokenStream) -> TokenStream {
    ops::derive_add(input.into()).into()
}

#[proc_macro_derive(AddAssign)]
pub fn derive_add_assign(input: TokenStream) -> TokenStream {
    ops::derive_add_assign(input.into()).into()
}

#[proc_macro_derive(Sub)]
pub fn derive_sub(input: TokenStream) -> TokenStream {
    ops::derive_sub(input.into()).into()
}

#[proc_macro_derive(SubAssign)]
pub fn derive_sub_assign(input: TokenStream) -> TokenStream {
    ops::derive_sub_assign(input.into()).into()
}

#[proc_macro_derive(Mul)]
pub fn derive_mul(input: TokenStream) -> TokenStream {
    ops::derive_mul(input.into()).into()
}

#[proc_macro_derive(MulAssign)]
pub fn derive_mul_assign(input: TokenStream) -> TokenStream {
    ops::derive_mul_assign(input.into()).into()
}

#[proc_macro_derive(Div)]
pub fn derive_div(input: TokenStream) -> TokenStream {
    ops::derive_div(input.into()).into()
}

#[proc_macro_derive(DivAssign)]
pub fn derive_div_assign(input: TokenStream) -> TokenStream {
    ops::derive_div_assign(input.into()).into()
}

#[proc_macro_derive(Rem)]
pub fn derive_rem(input: TokenStream) -> TokenStream {
    ops::derive_rem(input.into()).into()
}

#[proc_macro_derive(Sum)]
pub fn derive_sum(input: TokenStream) -> TokenStream {
    ops::derive_sum(input.into()).into()
}

#[proc_macro_derive(FieldValues)]
pub fn derive_field_values(input: TokenStream) -> TokenStream {
    field_values::derive_field_values(input.into()).into()
}

#[proc_macro_derive(PartialOrd)]
pub fn derive_partial_ord(input: TokenStream) -> TokenStream {
    partial_ord::derive_partial_ord(input.into()).into()
}
