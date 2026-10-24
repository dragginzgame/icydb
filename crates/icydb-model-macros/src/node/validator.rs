//! Module: node::validator
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::prelude::*;

///
/// Validator
///

#[derive(Debug, FromMeta)]
pub struct Validator {
    #[darling(default, skip)]
    pub(crate) def: Def,
}

impl HasDef for Validator {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Validator {
    fn validate(&self) -> Result<(), DarlingError> {
        Ok(())
    }
}

impl HasSchema for Validator {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Validator
    }
}

impl HasSchemaPart for Validator {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let def = self.def.schema_part(paths);

        // quote
        quote! {
            #model::node::Validator::new(#def)
        }
    }
}

impl HasTraits for Validator {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = generated_node_trait_set();
        traits.add(TraitKind::Default);

        traits.into_vec()
    }
}

impl HasType for Validator {
    fn type_part(&self, __paths: &CratePaths) -> TokenStream {
        let item = &self.def.item;

        quote!(#item)
    }
}

impl Validator {
    pub(crate) fn generate_tokens(&self, paths: &CratePaths) -> TokenStream {
        let mut tokens = TokenStream::new();
        tokens.extend(self.all_tokens(paths));
        tokens
    }
}
