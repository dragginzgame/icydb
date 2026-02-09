use crate::prelude::*;
use icydb_schema::types::Cardinality;

// Generates typed Id accessors for every relation-backed field.
pub fn relation_accessor_tokens<'a>(fields: impl Iterator<Item = &'a Field>) -> Vec<TokenStream> {
    fields
        .filter_map(relation_accessor_tokens_for_field)
        .collect()
}

fn relation_accessor_tokens_for_field(field: &Field) -> Option<TokenStream> {
    let relation = field.value.item.relation.as_ref()?;
    let field_ident = &field.ident;
    let method_ident = accessor_ident(field);

    Some(match field.value.cardinality() {
        Cardinality::One => {
            quote! {
                /// Returns the typed relation ID for this field.
                #[must_use]
                pub fn #method_ident(&self) -> ::icydb::types::Id<#relation> {
                    ::icydb::types::Id::from_key(self.#field_ident)
                }
            }
        }
        Cardinality::Opt => {
            quote! {
                /// Returns the typed relation ID for this field, if present.
                #[must_use]
                pub fn #method_ident(&self) -> Option<::icydb::types::Id<#relation>> {
                    self.#field_ident.map(::icydb::types::Id::from_key)
                }
            }
        }
        Cardinality::Many => {
            quote! {
                /// Returns typed relation IDs for all values in this field.
                #[must_use]
                pub fn #method_ident(&self) -> Vec<::icydb::types::Id<#relation>> {
                    self.#field_ident
                        .iter()
                        .copied()
                        .map(::icydb::types::Id::from_key)
                        .collect()
                }
            }
        }
    })
}

fn accessor_ident(field: &Field) -> Ident {
    // Keep the field name verbatim and append the suffix.
    // Do not singularize plural field names (`orders` -> `orders_ids`).
    let suffix = match field.value.cardinality() {
        Cardinality::Many => "ids",
        Cardinality::One | Cardinality::Opt => "id",
    };
    format_ident!("{}_{}", field.ident, suffix)
}
