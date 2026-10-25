//! Module: node::normalizer
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{
    node::{HasDef, HasSchema},
    prelude::*,
};

///
/// Normalizer
///

#[derive(Debug, FromMeta)]
pub struct Normalizer {
    #[darling(default, skip)]
    pub(crate) def: Def,
}

impl HasDef for Normalizer {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Normalizer {
    fn validate(&self) -> Result<(), DarlingError> {
        Ok(())
    }
}

impl HasSchema for Normalizer {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Normalizer
    }
}

impl HasSchemaPart for Normalizer {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let def = self.def.schema_part(paths);

        // quote
        quote! {
            #model::node::Normalizer::new(#def)
        }
    }
}

impl HasTraits for Normalizer {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = generated_node_trait_set();
        traits.add(TraitKind::Default);

        traits.into_vec()
    }
}

impl HasType for Normalizer {
    fn type_part(&self, __paths: &CratePaths) -> TokenStream {
        let mut item = self.def.item.clone();
        if let Some(item) = &mut item {
            // The shared emitter owns outer attributes for every node family.
            item.attrs.clear();
        }
        quote!(#item)
    }
}

impl Normalizer {
    pub(crate) fn generate_tokens(&self, paths: &CratePaths) -> TokenStream {
        let mut tokens = TokenStream::new();
        tokens.extend(self.all_tokens(paths));
        tokens
    }
}
