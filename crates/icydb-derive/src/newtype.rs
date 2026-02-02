use proc_macro2::TokenStream;
use syn::{Data, DeriveInput, Error, Fields, Generics, Ident, Type};

///
/// NewtypeInput
///

pub struct NewtypeInput {
    pub ident: Ident,
    pub inner: Type,
    pub generics: Generics,
}

pub fn parse_newtype(input: TokenStream, label: &str) -> Result<NewtypeInput, Error> {
    let input: DeriveInput = syn::parse2(input)?;
    let message = format!("{label} can only be derived for tuple structs with a single field");

    let inner = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => fields.unnamed[0].ty.clone(),
            _ => return Err(Error::new_spanned(&data.fields, message)),
        },
        _ => return Err(Error::new_spanned(&input.ident, message)),
    };

    Ok(NewtypeInput {
        ident: input.ident,
        inner,
        generics: input.generics,
    })
}
