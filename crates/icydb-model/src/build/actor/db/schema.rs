//! Module: db::schema
//! Responsibility: generated accepted-schema report capability tokens.
//! Does not own: schema validation, entity description semantics, or controller policy.
//! Boundary: emits only the private accepted-schema handler for immutable
//! authored entity identities.

use proc_macro2::TokenStream;
use quote::quote;

///
/// SchemaSurfaceTokens
///
/// Generated token bundle for the private accepted-schema report capability.
///

pub(super) struct SchemaSurfaceTokens {
    icydb: TokenStream,
    entity_sources: Vec<String>,
}

impl SchemaSurfaceTokens {
    pub(super) const fn empty(icydb: TokenStream) -> Self {
        Self {
            icydb,
            entity_sources: Vec::new(),
        }
    }

    pub(super) fn push_entity(&mut self, entity_source: &str) {
        self.entity_sources.push(entity_source.to_owned());
    }
}

impl quote::ToTokens for SchemaSurfaceTokens {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let icydb = &self.icydb;
        let entity_sources = &self.entity_sources;

        tokens.extend(quote! {
            pub(crate) fn __icydb_endpoint_handler_schema(
            ) -> ::std::result::Result<::std::vec::Vec<#icydb::db::EntitySchemaDescription>, #icydb::Error> {
                #icydb::__macro::into_deliverable_schema_reply(::std::vec![
                    #(db()?.try_describe_entity_by_source_key(#entity_sources)?),*
                ])
            }
        });
    }
}

#[cfg(test)]
mod tests;
