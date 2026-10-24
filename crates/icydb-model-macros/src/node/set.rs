//! Module: node::set
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{imp::*, prelude::*};

///
/// Set
///

#[derive(Debug, FromMeta)]
pub struct Set {
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

impl HasDef for Set {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Set {
    fn validate(&self) -> Result<(), DarlingError> {
        self.validate_traits()?;
        self.item.validate()?;

        Ok(())
    }
}

impl HasSchema for Set {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Set
    }
}

impl HasSchemaPart for Set {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let def = self.def.schema_part(paths);
        let name = self.current_name_literal(self.name.as_ref());
        let item = self.item.schema_part(paths);
        let ty = self.ty.schema_part(paths);

        // quote
        quote! {
            #model::node::Set::new(#def, #name, #item, #ty)
        }
    }
}

impl_collection_has_traits!(Set, Set);

impl HasType for Set {
    fn type_part(&self, paths: &CratePaths) -> TokenStream {
        let ident = self.def.ident();
        let item = &self.item.type_expr(paths);

        quote! {
            #[repr(transparent)]
            pub struct #ident(pub ::std::collections::BTreeSet<#item>);
        }
    }
}

impl Set {
    pub(crate) fn generate_tokens(&self, paths: &CratePaths) -> TokenStream {
        let mut tokens = TokenStream::new();
        let base = self.all_tokens(paths);
        let typed_adapter = crate::node::typed_adapter::set_adapter_tokens(self, paths);
        tokens.extend(quote! {
            #base
            #typed_adapter
        });
        tokens
    }
}
