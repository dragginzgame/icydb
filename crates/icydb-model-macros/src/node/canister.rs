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
            || quote!(::std::option::Option::None),
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

impl Canister {}

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
mod tests;

#[cfg(test)]
mod helper_tests;
