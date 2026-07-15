//! Module: db::schema
//! Responsibility: generated accepted-schema report endpoint tokens.
//! Does not own: schema validation, entity description semantics, or controller policy.
//! Boundary: emits controller-gated schema report helpers for concrete canister entities.

use proc_macro2::TokenStream;
use quote::quote;

///
/// SchemaSurfaceTokens
///
/// Generated token bundle for the opted-in accepted-schema report endpoint.
/// The endpoint remains generated because only codegen knows the concrete
/// entity types bound to one canister.
///

pub(super) struct SchemaSurfaceTokens {
    entity_tys: Vec<syn::Path>,
}

impl SchemaSurfaceTokens {
    pub(super) const fn empty() -> Self {
        Self {
            entity_tys: Vec::new(),
        }
    }

    pub(super) fn push_entity(&mut self, entity_ty: &syn::Path) {
        self.entity_tys.push(entity_ty.clone());
    }
}

impl quote::ToTokens for SchemaSurfaceTokens {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let entity_tys = &self.entity_tys;

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
                    #(db()?.try_describe_entity::<#entity_tys>()?),*
                ])
            }

            #[::icydb::__reexports::ic_cdk::query(name = "icydb_schema_check")]
            fn __icydb_schema_check() -> Result<Vec<::icydb::db::EntitySchemaCheckDescription>, ::icydb::Error> {
                icydb_schema_surface_require_controller()?;

                Ok(vec![
                    #(::icydb::db::EntitySchemaCheckDescription::new(
                        db()?.describe_entity::<#entity_tys>(),
                        db()?.try_describe_entity::<#entity_tys>()?,
                    )),*
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
        let entity_ty: syn::Path = syn::parse_quote!(crate::Character);
        let mut surface_tokens = SchemaSurfaceTokens::empty();
        surface_tokens.push_entity(&entity_ty);

        let surface = compact_tokens(quote!(#surface_tokens));

        assert!(surface.contains("name=\"icydb_schema\""));
        assert!(surface.contains("name=\"icydb_schema_check\""));
        assert!(surface.contains("fn__icydb_schema("));
        assert!(surface.contains("fn__icydb_schema_check("));
    }
}
