use crate::{
    prelude::*,
    view::{ValueFilter, ValueUpdate, ValueView, traits::ViewExpr},
};

///
/// FieldView
///

pub struct FieldView<'a>(pub &'a Field);

impl ViewExpr for FieldView<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let f = self.0;
        let ident = &f.ident;
        let ty = ValueView(&f.value).expr()?;

        quote!(pub #ident: #ty).into()
    }
}

///
/// FieldUpdate
///

pub struct FieldUpdate<'a>(pub &'a Field);

impl ViewExpr for FieldUpdate<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let f = self.0;
        let ident = &f.ident;
        let ty = ValueUpdate(&f.value).expr()?;

        quote!(pub #ident: Option<#ty>).into()
    }
}

///
/// FieldFilter
///

pub struct FieldFilter<'a>(pub &'a Field);

impl ViewExpr for FieldFilter<'_> {
    fn expr(&self) -> Option<TokenStream> {
        let f = self.0;
        let ident = &f.ident;
        let ty = ValueFilter(&f.value).expr()?;

        quote!(pub #ident: Option<#ty>).into()
    }
}
