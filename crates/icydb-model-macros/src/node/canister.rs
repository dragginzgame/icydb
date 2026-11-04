//! Module: node::canister
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::prelude::*;

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
    migrations: Option<MigrationPlan>,

    #[darling(default)]
    migration_attempt: u64,
}

impl HasDef for Canister {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Canister {
    fn validate(&self) -> Result<(), DarlingError> {
        if self.migration_attempt != 0 && self.migrations.is_none() {
            return Err(DarlingError::custom(
                "migration_attempt requires a declared migration plan",
            )
            .with_span(&self.def.ident()));
        }
        if let Some(migrations) = &self.migrations {
            migrations.validate(self.def.paths())?;
        }
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
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let def = self.def.schema_part(paths);
        let memory_namespace = &self.memory_namespace;
        let migration_attempt = self.migration_attempt;
        let migration_plan = self.migrations.as_ref().map_or_else(
            || quote!(::core::option::Option::None),
            |value| MigrationPlan::constructor_tokens(value, paths),
        );

        // quote
        quote! {
            #model::node::Canister::new(
                #def,
                #memory_namespace,
                #migration_plan,
            ).with_migration_attempt(#migration_attempt)
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
    fn type_part(&self, __paths: &CratePaths) -> TokenStream {
        let ident = self.def.ident();

        quote! {
            pub struct #ident;
        }
    }
}

impl Canister {
    pub(crate) fn generate_tokens(&self, paths: &CratePaths) -> TokenStream {
        let mut tokens = TokenStream::new();
        tokens.extend(self.all_tokens(paths));
        tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_attempt_is_validated_and_emitted_at_authoring_boundary() {
        let item: syn::ItemStruct = syn::parse_quote! { pub struct RetryCanister; };
        let meta: syn::Meta = syn::parse_quote! {
            canister(memory_namespace = "retry", migration_attempt = 7,
                migrations(entity_migration(entity = "Item", from = 1,
                    transforms(fill(to = "score", literal(nat = 1))))))
        };
        let paths = crate::crate_path::CratePaths::test();
        let mut canister = Canister::from_meta(&meta).unwrap();
        canister.def = Def::new(item, crate::crate_path::CratePaths::test());
        canister.validate().unwrap();
        assert!(
            canister
                .schema_part(&paths)
                .to_string()
                .contains("with_migration_attempt (7u64)")
        );
        canister.migrations = None;
        assert!(canister.validate().is_err());
    }

    #[test]
    fn generated_commit_stable_key_matches_schema_formatter() {
        let item: syn::ItemStruct = syn::parse_quote! {
            pub struct DemoCanister;
        };
        let canister = Canister {
            def: Def::new(item, crate::crate_path::CratePaths::test()),
            memory_namespace: "demo_rpg".to_string(),
            migrations: None,
            migration_attempt: 0,
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
