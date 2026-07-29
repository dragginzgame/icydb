//! Module: db::schema
//! Responsibility: generated accepted-schema report endpoint tokens.
//! Does not own: schema validation, entity description semantics, or controller policy.
//! Boundary: emits controller-gated accepted-schema reports for immutable
//! authored entity identities.

use proc_macro2::TokenStream;
use quote::quote;

///
/// SchemaSurfaceTokens
///
/// Generated token bundle for the opted-in accepted-schema report endpoint.
/// The endpoint remains generated because only codegen knows the concrete
/// entity source keys bound to one canister.
///

pub(super) struct SchemaSurfaceTokens {
    entity_sources: Vec<String>,
}

impl SchemaSurfaceTokens {
    pub(super) const fn empty() -> Self {
        Self {
            entity_sources: Vec::new(),
        }
    }

    pub(super) fn push_entity(&mut self, entity_source: &str) {
        self.entity_sources.push(entity_source.to_owned());
    }
}

impl quote::ToTokens for SchemaSurfaceTokens {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let entity_sources = &self.entity_sources;

        tokens.extend(quote! {
            fn icydb_schema_surface_require_controller() -> Result<(), ::icydb::Error> {
                let caller = ::icydb::__reexports::ic_cdk::api::msg_caller();
                if !::icydb::__reexports::ic_cdk::api::is_controller(&caller) {
                    return Err(::icydb::Error::from_runtime_boundary(
                        ::icydb::diagnostic::RuntimeBoundaryCode::SchemaSurfaceControllerRequired,
                        ::icydb::ErrorOrigin::Interface,
                    ));
                }

                Ok(())
            }

            #[::icydb::__reexports::ic_cdk::query(name = "icydb_schema")]
            fn __icydb_schema() -> Result<Vec<::icydb::db::EntitySchemaDescription>, ::icydb::Error> {
                icydb_schema_surface_require_controller()?;

                Ok(vec![
                    #(db()?.try_describe_entity_by_source_key(#entity_sources)?),*
                ])
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::SchemaSurfaceTokens;

    fn compact_tokens(tokens: proc_macro2::TokenStream) -> String {
        tokens
            .to_string()
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect()
    }

    #[test]
    fn generated_schema_surface_resolves_current_entity_names() {
        let mut surface_tokens = SchemaSurfaceTokens::empty();
        surface_tokens.push_entity("Character");

        let surface = compact_tokens(quote!(#surface_tokens));

        assert!(surface.contains("name=\"icydb_schema\""));
        assert!(surface.contains("fn__icydb_schema("));
        assert!(surface.contains("try_describe_entity_by_source_key(\"Character\")"));
        assert!(!surface.contains("crate::Character"));
    }
}
