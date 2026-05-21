use crate::prelude::*;
use crate::validate::memory::{memory_id_out_of_range_error, memory_id_reserved_error};

///
/// Canister
/// regardless of the path, the name is used to uniquely identify each canister
///

#[derive(Debug, FromMeta)]
pub struct Canister {
    #[darling(skip, default)]
    pub(crate) def: Def,

    pub(crate) memory_namespace: String,

    // inclusive range of ic memories
    pub(crate) memory_min: u8,
    pub(crate) memory_max: u8,
    commit_memory_id: u8,
}

impl HasDef for Canister {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Canister {
    fn validate(&self) -> Result<(), DarlingError> {
        if !crate::validate::memory::stable_key_segment_is_canonical(&self.memory_namespace) {
            return Err(DarlingError::custom(
                "memory_namespace must use lowercase ASCII letters, digits, and underscores",
            )
            .with_span(&self.def.ident()));
        }
        if self.memory_min > self.memory_max {
            return Err(DarlingError::custom(
                "memory_min must be equal to or less than memory_max",
            )
            .with_span(&self.def.ident()));
        }

        if let Some(message) = memory_id_out_of_range_error(
            "commit_memory_id",
            self.commit_memory_id,
            self.memory_min,
            self.memory_max,
        ) {
            return Err(DarlingError::custom(message).with_span(&self.def.ident()));
        }
        if let Some(message) =
            crate::validate::memory::app_memory_id_error("commit_memory_id", self.commit_memory_id)
        {
            return Err(DarlingError::custom(message).with_span(&self.def.ident()));
        }
        if let Some(message) = memory_id_reserved_error("commit_memory_id", self.commit_memory_id) {
            return Err(DarlingError::custom(message).with_span(&self.def.ident()));
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
        let memory_namespace = &self.memory_namespace;
        let memory_min = self.memory_min;
        let memory_max = self.memory_max;
        let commit_memory_id = self.commit_memory_id;

        // quote
        quote! {
            ::icydb::schema::node::Canister::new(
                #def,
                #memory_namespace,
                #memory_min,
                #memory_max,
                #commit_memory_id,
            )
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
                let commit_stable_key = self.commit_stable_key();
                let tokens = Implementor::new(self.def(), t)
                    .set_tokens(quote! {
                        const COMMIT_MEMORY_ID: u8 = #commit_memory_id;
                        const COMMIT_STABLE_KEY: &'static str = #commit_stable_key;
                    })
                    .to_token_stream();

                Some(TraitStrategy::from_impl(tokens))
            }
            _ => None,
        }
    }
}

impl Canister {
    fn commit_stable_key(&self) -> String {
        icydb_schema::node::stable_memory_key(&self.memory_namespace, "commit", "control")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_commit_stable_key_matches_schema_formatter() {
        let item: syn::ItemStruct = syn::parse_quote! {
            pub struct DemoCanister;
        };
        let canister = Canister {
            def: Def::new(item),
            memory_namespace: "demo_rpg".to_string(),
            memory_min: 100,
            memory_max: 254,
            commit_memory_id: 254,
        };
        let schema_canister = icydb_schema::node::Canister::new(
            icydb_schema::node::Def::new("demo::rpg", "DemoCanister"),
            "demo_rpg",
            100,
            254,
            254,
        );

        assert_eq!(
            canister.commit_stable_key(),
            schema_canister.commit_stable_key(),
            "derive-generated CanisterKind::COMMIT_STABLE_KEY must match schema allocation metadata",
        );
        assert_eq!(
            canister.commit_stable_key(),
            "icydb.demo_rpg.commit.control.v1",
        );
    }
}
