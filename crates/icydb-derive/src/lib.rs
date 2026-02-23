mod display;
mod field_projection;
mod inner;
mod newtype;
mod ops;

use proc_macro::TokenStream;

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

#[proc_macro_derive(FieldProjection)]
pub fn derive_field_projection(input: TokenStream) -> TokenStream {
    field_projection::derive_field_projection(input.into()).into()
}

#[proc_macro_derive(Display)]
pub fn derive_display(input: TokenStream) -> TokenStream {
    display::derive_display(input.into()).into()
}

#[proc_macro_derive(Inner)]
pub fn derive_inner(input: TokenStream) -> TokenStream {
    inner::derive_inner(input.into()).into()
}
