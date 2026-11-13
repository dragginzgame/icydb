//! Module: node::list
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{
    imp::{
        FromIteratorTrait, FromTrait, IntoIteratorTrait, NormalizeAutoTrait, ValidateAutoTrait,
        VisitableTrait,
    },
    prelude::*,
};

///
/// List
///

#[derive(Debug, FromMeta)]
pub struct List {
    #[darling(default, skip)]
    pub(crate) def: Def,

    #[darling(default)]
    pub(crate) name: Option<LitStr>,

    pub(crate) item: Item,

    #[darling(default)]
    pub(crate) ty: Type,

    #[darling(default)]
    pub(crate) traits: TraitBuilder,
}

impl ValidateNode for List {
    fn validate(&self) -> Result<(), DarlingError> {
        self.validate_traits()?;
        self.item.validate()?;

        Ok(())
    }
}

impl_sequence_node!(List, List);

impl HasType for List {
    fn type_part(&self, paths: &CratePaths) -> TokenStream {
        let ident = self.def.ident();
        let item = &self.item.type_expr(paths);

        quote! {
            #[repr(transparent)]
            pub struct #ident(pub ::std::vec::Vec<#item>);
        }
    }
}

impl List {
    pub(crate) fn generate_tokens(&self, paths: &CratePaths) -> TokenStream {
        let mut tokens = TokenStream::new();
        let base = self.all_tokens(paths);
        let typed_adapter = crate::node::typed_adapter::list_adapter_tokens(self, paths);
        tokens.extend(quote! {
            #base
            #typed_adapter
        });
        tokens
    }
}

#[cfg(test)]
mod tests;
