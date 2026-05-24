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
                    return Err(::icydb::Error::new(
                        ::icydb::ErrorKind::Runtime(::icydb::RuntimeErrorKind::Unsupported),
                        ::icydb::ErrorOrigin::Interface,
                        "IcyDB schema report requires a controller caller",
                    ));
                }

                Ok(())
            }

            #[::icydb::__reexports::ic_cdk::query]
            fn __icydb_schema() -> Result<Vec<::icydb::db::EntitySchemaDescription>, ::icydb::Error> {
                icydb_schema_surface_require_controller()?;

                Ok(vec![
                    #(db().try_describe_entity::<#entity_tys>()?),*
                ])
            }

            #[::icydb::__reexports::ic_cdk::query]
            fn __icydb_schema_check() -> Result<Vec<::icydb::db::EntitySchemaCheckDescription>, ::icydb::Error> {
                icydb_schema_surface_require_controller()?;

                Ok(vec![
                    #(::icydb::db::EntitySchemaCheckDescription::new(
                        db().describe_entity::<#entity_tys>(),
                        db().try_describe_entity::<#entity_tys>()?,
                    )),*
                ])
            }
        });
    }
}
