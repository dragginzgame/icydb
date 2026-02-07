use crate::prelude::*;

///
/// Validator
///

#[derive(Debug, FromMeta)]
pub struct Validator {
    #[darling(default, skip)]
    pub def: Def,
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
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();

        // quote
        quote! {
            ::icydb::schema::node::Validator {
                def: #def,
            }
        }
    }
}

impl HasTraits for Validator {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = TraitBuilder::default().build();
        traits.add(TraitKind::Default);

        traits.into_vec()
    }
}

impl HasType for Validator {
    fn type_part(&self) -> TokenStream {
        let item = &self.def.item;

        quote!(#item)
    }
}

impl ToTokens for Validator {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
