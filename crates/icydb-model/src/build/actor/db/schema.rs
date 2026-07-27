//! Module: db::schema
//! Responsibility: generated accepted-schema report endpoint tokens.
//! Does not own: schema validation, entity description semantics, or controller policy.
//! Boundary: emits controller-gated accepted-schema reports for authored
//! entity routes.

use proc_macro2::TokenStream;
use quote::quote;

///
/// SchemaSurfaceTokens
///
/// Generated token bundle for the opted-in accepted-schema report endpoint.
/// The endpoint remains generated because only codegen knows the concrete
/// entity paths bound to one canister.
///

pub(super) struct SchemaSurfaceTokens {
    entity_paths: Vec<String>,
}

impl SchemaSurfaceTokens {
    pub(super) const fn empty() -> Self {
        Self {
            entity_paths: Vec::new(),
        }
    }

    pub(super) fn push_entity(&mut self, entity_path: &str, _entity_name: &str) {
        self.entity_paths.push(entity_path.to_owned());
    }
}

impl quote::ToTokens for SchemaSurfaceTokens {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let entity_paths = &self.entity_paths;

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
                    #(db()?.try_describe_entity_by_name(#entity_paths)?),*
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
    fn generated_schema_surface_uses_public_icydb_endpoint_names() {
        let mut surface_tokens = SchemaSurfaceTokens::empty();
        surface_tokens.push_entity("crate::Character", "Character");

        let surface = compact_tokens(quote!(#surface_tokens));

        assert!(surface.contains("name=\"icydb_schema\""));
        assert!(surface.contains("fn__icydb_schema("));
        assert!(!surface.contains("icydb_schema_check"));
        assert!(surface.contains("try_describe_entity_by_name(\"crate::Character\")"));
    }
}
