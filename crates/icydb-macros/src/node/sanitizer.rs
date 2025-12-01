use crate::{
    node::{HasDef, HasSchema},
    prelude::*,
};

///
/// Sanitizer
///

#[derive(Debug, FromMeta)]
pub struct Sanitizer {
    #[darling(default, skip)]
    pub def: Def,
}

impl HasDef for Sanitizer {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl HasSchema for Sanitizer {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Sanitizer
    }
}

impl HasSchemaPart for Sanitizer {
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();

        // quote
        let sp = paths().schema;
        quote! {
            #sp::node::Sanitizer {
                def: #def,
            }
        }
    }
}

impl HasTraits for Sanitizer {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = TraitBuilder::default().with_type_traits().build();
        traits.add(TraitKind::Default);

        traits.into_vec()
    }
}

impl HasType for Sanitizer {
    fn type_part(&self) -> TokenStream {
        let item = &self.def.item;

        quote!(#item)
    }
}

impl ToTokens for Sanitizer {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
