use crate::prelude::*;

///
/// Canister
/// regardless of the path, the name is used to uniquely identify each canister
///

#[derive(Debug, FromMeta)]
pub struct Canister {
    #[darling(skip, default)]
    pub def: Def,

    // inclusive range of ic memories
    pub memory_min: u8,
    pub memory_max: u8,
}

impl HasDef for Canister {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl HasSchema for Canister {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Canister
    }
}

impl HasSchemaPart for Canister {
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();
        let memory_min = self.memory_min;
        let memory_max = self.memory_max;

        // quote
        let sp = paths().schema;
        quote! {
            #sp::node::Canister{
                def: #def,
                memory_min: #memory_min,
                memory_max: #memory_max,
            }
        }
    }
}

impl HasTraits for Canister {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = TraitBuilder::default().build();
        traits.add(TraitKind::CanisterKind);

        traits.into_vec()
    }
}

impl HasType for Canister {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();

        quote! {
            pub struct #ident;
        }
    }
}

impl ToTokens for Canister {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
