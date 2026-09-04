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
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();
        let name = self.current_name_literal(self.name.as_ref());
        let item = self.item.schema_part();
        let ty = self.ty.schema_part();

        // quote
        quote! {
            ::icydb_model::node::Set::new(#def, #name, #item, #ty)
        }
    }
}

impl_collection_has_traits!(Set, Set);

impl HasType for Set {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let item = &self.item.type_expr();

        quote! {
            #[repr(transparent)]
            pub struct #ident(pub ::std::collections::BTreeSet<#item>);
        }
    }
}

impl ToTokens for Set {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let base = self.all_tokens();
        let typed_adapter = crate::node::typed_adapter::set_adapter_tokens(self);
        tokens.extend(quote! {
            #base
            #typed_adapter
        });
    }
}
