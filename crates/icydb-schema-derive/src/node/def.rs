//! Module: node::def
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::prelude::*;

///
/// Def
///
/// the default gets overridden after the initial darling::from_list() call
/// the schema doesn't care about the generics as they're not useful as static text
///

#[derive(Debug, Default)]
pub struct Def {
    pub(crate) item: Option<ItemStruct>,
}

impl Def {
    /// Build one derive-side definition wrapper from the parsed item.
    #[must_use]
    pub const fn new(item: ItemStruct) -> Self {
        Self { item: Some(item) }
    }

    /// Return the parsed item identifier.
    ///
    /// # Panics
    ///
    /// Panics if called before darling has attached the parsed item metadata.
    pub fn ident(&self) -> Ident {
        self.item
            .as_ref()
            .unwrap_or_else(|| panic!("derive definition metadata must include the parsed item"))
            .ident
            .clone()
    }
}

impl HasSchemaPart for Def {
    fn schema_part(&self) -> TokenStream {
        let ident = quote_one(&self.ident(), to_str_lit);

        // quote
        quote! {
            ::icydb::schema::node::Def::new(module_path!(), #ident)
        }
    }
}
