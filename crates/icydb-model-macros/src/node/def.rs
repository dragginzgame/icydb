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
    paths: Option<CratePaths>,
}

impl Def {
    /// Build one derive-side definition wrapper from the parsed item.
    #[must_use]
    pub const fn new(item: ItemStruct, paths: CratePaths) -> Self {
        Self {
            item: Some(item),
            paths: Some(paths),
        }
    }

    /// Return the invocation paths attached before validation and emission.
    ///
    /// # Panics
    /// Panics before parsed definition metadata has been attached.
    pub(crate) const fn paths(&self) -> &CratePaths {
        self.paths
            .as_ref()
            .expect("derive definition must include resolved crate paths")
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
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let ident = quote_one(&self.ident(), to_str_lit);

        // quote
        quote! {
            #model::node::Def::new(module_path!(), #ident)
        }
    }
}
