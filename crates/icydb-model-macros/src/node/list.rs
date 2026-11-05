//! Module: node::list
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{imp::*, prelude::*};

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

impl HasDef for List {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for List {
    fn validate(&self) -> Result<(), DarlingError> {
        self.validate_traits()?;
        self.item.validate()?;

        Ok(())
    }
}

impl HasSchema for List {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::List
    }
}

impl HasSchemaPart for List {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let def = self.def.schema_part(paths);
        let name = self.current_name_literal(self.name.as_ref());
        let item = self.item.schema_part(paths);
        let ty = self.ty.schema_part(paths);

        // quote
        quote! {
            #model::node::List::new(#def, #name, #item, #ty)
        }
    }
}

impl_collection_has_traits!(List, List);

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
