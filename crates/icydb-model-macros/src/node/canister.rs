//! Module: node::canister
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::prelude::*;
use crate::validate::memory::{memory_id_out_of_range_error, memory_id_reserved_error};

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

    // inclusive range of ic memories
    pub(crate) memory_min: u8,
    pub(crate) memory_max: u8,
    commit_memory_id: u8,
    startup_memory_id: u8,
    #[darling(default)]
    integrity_progress_memory_id: Option<u8>,

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
        if let Some(message) = memory_id_out_of_range_error(
            "startup_memory_id",
            self.startup_memory_id,
            self.memory_min,
            self.memory_max,
        ) {
            return Err(DarlingError::custom(message).with_span(&self.def.ident()));
        }
        if let Some(message) = crate::validate::memory::app_memory_id_error(
            "startup_memory_id",
            self.startup_memory_id,
        ) {
            return Err(DarlingError::custom(message).with_span(&self.def.ident()));
        }
        if let Some(message) = memory_id_reserved_error("startup_memory_id", self.startup_memory_id)
        {
            return Err(DarlingError::custom(message).with_span(&self.def.ident()));
        }
        let integrity_progress_memory_id = self.integrity_progress_memory_id();
        if let Some(message) = memory_id_out_of_range_error(
            "integrity_progress_memory_id",
            integrity_progress_memory_id,
            self.memory_min,
            self.memory_max,
        ) {
            return Err(DarlingError::custom(message).with_span(&self.def.ident()));
        }
        if let Some(message) = crate::validate::memory::app_memory_id_error(
            "integrity_progress_memory_id",
            integrity_progress_memory_id,
        ) {
            return Err(DarlingError::custom(message).with_span(&self.def.ident()));
        }
        if let Some(message) =
            memory_id_reserved_error("integrity_progress_memory_id", integrity_progress_memory_id)
        {
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
        let memory_profile = self
            .memory_profile
            .as_ref()
            .map(|profile| quote!(.with_memory_profile(#profile)));
        let memory_min = self.memory_min;
        let memory_max = self.memory_max;
        let commit_memory_id = self.commit_memory_id;
        let startup_memory_id = self.startup_memory_id;
        let integrity_progress_memory_id = self.integrity_progress_memory_id();
        let migration_plan = self
            .migrations
            .as_ref()
            .map_or_else(|| quote!(None), MigrationPlan::constructor_tokens);

        // quote
        quote! {
            ::icydb_model::node::Canister::new(
                #def,
                #memory_namespace,
                #memory_min,
                #memory_max,
                #commit_memory_id,
                #startup_memory_id,
                #integrity_progress_memory_id,
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
    fn integrity_progress_memory_id(&self) -> u8 {
        self.integrity_progress_memory_id
            .unwrap_or_else(|| self.commit_memory_id.saturating_sub(2))
    }

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
            memory_min = 100, memory_max = 254,
            commit_memory_id = 254, startup_memory_id = 252,
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
            memory_min: 100,
            memory_max: 254,
            commit_memory_id: 254,
            startup_memory_id: 252,
            integrity_progress_memory_id: Some(253),
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
