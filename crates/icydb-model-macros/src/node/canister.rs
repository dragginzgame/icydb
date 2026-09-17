//! Module: node::canister
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::prelude::*;

/// Macro vocabulary only; the model owns profile defaults and page counts.
#[derive(Debug, FromMeta)]
#[darling(rename_all = "snake_case")]
enum MemoryProfile {
    Compact,
    General,
    HighHeadroom,
}

impl ToTokens for MemoryProfile {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let variant = format_ident!("{self:?}");
        tokens.extend(quote!(::icydb_model::node::CanisterMemoryProfile::#variant));
    }
}

///
/// Canister
/// regardless of the path, the name is used to uniquely identify each canister
///

#[derive(Debug, FromMeta)]
pub struct Canister {
    #[darling(skip, default)]
    pub(crate) def: Def,

    pub(crate) memory_namespace: String,
    #[darling(default)]
    memory_profile: Option<MemoryProfile>,

    #[darling(default)]
    migrations: Option<MigrationPlan>,
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
                "memory_namespace must begin with a lowercase ASCII letter and contain only lowercase ASCII letters, digits, and underscores",
            )
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
        let memory_namespace = &self.memory_namespace;
        let memory_profile = self
            .memory_profile
            .as_ref()
            .map(|profile| quote!(.with_memory_profile(#profile)));
        let migration_plan = self
            .migrations
            .as_ref()
            .map_or_else(|| quote!(None), MigrationPlan::constructor_tokens);

        // quote
        quote! {
            ::icydb_model::node::Canister::new(
                #def,
                #memory_namespace,
                #migration_plan,
            ) #memory_profile
        }
    }
}

impl HasTraits for Canister {
    fn traits(&self) -> Vec<TraitKind> {
        generated_node_trait_set().into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        let _ = t;
        None
    }
}

impl Canister {
    #[cfg(test)]
    fn commit_stable_key(&self) -> String {
        stable_memory_key(&self.memory_namespace, "commit", "control")
    }

    #[cfg(test)]
    fn integrity_progress_stable_key(&self) -> String {
        stable_memory_key(&self.memory_namespace, "integrity", "progress")
    }

    #[cfg(test)]
    fn startup_stable_key(&self) -> String {
        stable_memory_key(&self.memory_namespace, "startup", "control")
    }
}

#[cfg(test)]
fn stable_memory_key(memory_namespace: &str, allocation: &str, role: &str) -> String {
    format!("icydb.{memory_namespace}.{allocation}.{role}.v1")
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

    fn parse_canister(extra: TokenStream) -> Result<Canister, DarlingError> {
        let items = darling::ast::NestedMeta::parse_meta_list(quote! {
            memory_namespace = "test",
            #extra
        })
        .expect("test macro arguments should parse");
        Canister::from_list(&items)
    }

    #[test]
    fn memory_profile_accepts_only_closed_names_and_omission() {
        assert!(parse_canister(quote!()).unwrap().memory_profile.is_none());
        for (name, expected) in [
            ("compact", "Compact"),
            ("general", "General"),
            ("high_headroom", "HighHeadroom"),
        ] {
            let canister = parse_canister(quote!(memory_profile = #name)).unwrap();
            let profile = canister.memory_profile.unwrap();
            assert_eq!(format!("{profile:?}"), expected);
            assert_eq!(
                profile.to_token_stream().to_string(),
                format!(":: icydb_model :: node :: CanisterMemoryProfile :: {expected}"),
            );
        }
        assert!(parse_canister(quote!(memory_profile = "unknown")).is_err());
        assert!(parse_canister(quote!(memory_profile = 16)).is_err());
    }

    #[test]
    fn generated_commit_stable_key_matches_schema_formatter() {
        let item: syn::ItemStruct = syn::parse_quote! {
            pub struct DemoCanister;
        };
        let canister = Canister {
            def: Def::new(item),
            memory_namespace: "demo_rpg".to_string(),
            memory_profile: None,
            migrations: None,
        };
        assert_eq!(
            canister.commit_stable_key(),
            "icydb.demo_rpg.commit.control.v1",
        );
        assert_eq!(
            canister.integrity_progress_stable_key(),
            "icydb.demo_rpg.integrity.progress.v1",
        );
        assert_eq!(
            canister.startup_stable_key(),
            "icydb.demo_rpg.startup.control.v1",
        );
    }
}
