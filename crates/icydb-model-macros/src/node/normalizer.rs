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
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();

        // quote
        quote! {
            ::icydb_model::node::Normalizer::new(#def)
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
    fn type_part(&self) -> TokenStream {
        let item = &self.def.item;

        quote!(#item)
    }
}

impl ToTokens for Normalizer {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
