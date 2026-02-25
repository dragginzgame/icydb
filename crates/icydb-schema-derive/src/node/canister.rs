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
    pub commit_memory_id: u8,
}

impl HasDef for Canister {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Canister {
    fn validate(&self) -> Result<(), DarlingError> {
        if self.memory_min > self.memory_max {
            return Err(DarlingError::custom(
                "memory_min must be equal to or less than memory_max",
            )
            .with_span(&self.def.ident()));
        }

        if self.commit_memory_id < self.memory_min || self.commit_memory_id > self.memory_max {
            return Err(DarlingError::custom(format!(
                "commit_memory_id {} outside of range {}-{}",
                self.commit_memory_id, self.memory_min, self.memory_max
            ))
            .with_span(&self.def.ident()));
        }

        Ok(())
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
        let commit_memory_id = self.commit_memory_id;

        // quote
        quote! {
            ::icydb::schema::node::Canister{
                def: #def,
                memory_min: #memory_min,
                memory_max: #memory_max,
                commit_memory_id: #commit_memory_id,
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

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::CanisterKind => {
                let commit_memory_id = self.commit_memory_id;
                let tokens = Implementor::new(self.def(), t)
                    .set_tokens(quote! {
                        const COMMIT_MEMORY_ID: u8 = #commit_memory_id;
                    })
                    .to_token_stream();

                Some(TraitStrategy::from_impl(tokens))
            }
            _ => None,
        }
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
